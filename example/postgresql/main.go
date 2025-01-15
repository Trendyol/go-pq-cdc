package main

import (
	"context"
	"errors"
	cdc "github.com/vskurikhin/go-pq-cdc"
	"github.com/vskurikhin/go-pq-cdc/config"
	"github.com/vskurikhin/go-pq-cdc/pq/message/format"
	"github.com/vskurikhin/go-pq-cdc/pq/publication"
	"github.com/vskurikhin/go-pq-cdc/pq/replication"
	"github.com/vskurikhin/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"time"
)

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db"

	CREATE TABLE users (
	 user_id integer PRIMARY KEY,
	 name text NOT NULL
	);
*/

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 1000000) AS i;
*/

type Message struct {
	Query string
	Args  []any
	Ack   func() error
}

var (
	UpsertQuery = "INSERT INTO users (user_id, name) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET name = excluded.name;"
	DeleteQuery = "DELETE FROM users WHERE user_id = $1;"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db")
	if err != nil {
		slog.Error("new pool", "error", err)
		os.Exit(1)
	}

	messages := make(chan Message, 10000)
	go Produce(ctx, pool, messages)

	cfg := config.Config{
		Host:     "127.0.0.1",
		Username: "cdc_user",
		Password: "cdc_pass",
		Database: "cdc_db",
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{publication.Table{
				Name:            "users",
				ReplicaIdentity: publication.ReplicaIdentityDefault,
			}},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, FilteredMapper(messages))
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	connector.Start(ctx)
}

func FilteredMapper(messages chan Message) replication.ListenerFunc {
	return func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			messages <- Message{
				Query: UpsertQuery,
				Args:  []any{msg.Decoded["id"].(int32), msg.Decoded["name"].(string)},
				Ack:   ctx.Ack,
			}
		case *format.Delete:
			messages <- Message{
				Query: DeleteQuery,
				Args:  []any{msg.OldDecoded["id"].(int32)},
				Ack:   ctx.Ack,
			}
		case *format.Update:
			messages <- Message{
				Query: UpsertQuery,
				Args:  []any{msg.NewDecoded["id"].(int32), msg.NewDecoded["name"].(string)},
				Ack:   ctx.Ack,
			}
		}
	}
}

func Produce(ctx context.Context, w *pgxpool.Pool, messages <-chan Message) {
	var lastAck func() error
	counter := 0
	bulkSize := 10000

	queue := make([]*pgx.QueuedQuery, bulkSize)

	for {
		select {
		case event := <-messages:
			lastAck = event.Ack

			queue[counter] = &pgx.QueuedQuery{SQL: event.Query, Arguments: event.Args}
			counter++
			if counter == bulkSize {
				batchResults := w.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue})
				err := Exec(batchResults, counter)
				if err != nil {
					slog.Error("batch results", "error", err)
					continue
				}
				slog.Info("postgresql write", "count", counter)
				counter = 0
				if err = event.Ack(); err != nil {
					slog.Error("ack", "error", err)
				}
			}

		case <-time.After(time.Millisecond):
			if counter > 0 {
				batchResults := w.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue[:counter]})
				err := Exec(batchResults, counter)
				if err != nil {
					slog.Error("batch results", "error", err)
					continue
				}
				slog.Info("postgresql write", "count", counter)
				counter = 0
				if err = lastAck(); err != nil {
					slog.Error("ack", "error", err)
				}
			}
		}
	}
}

func Exec(br pgx.BatchResults, sqlCount int) error {
	defer br.Close()
	var batchErr error
	for t := 0; t < sqlCount; t++ {
		_, err := br.Exec()
		if err != nil {
			batchErr = errors.Join(batchErr, err)
		}
	}
	return batchErr
}
