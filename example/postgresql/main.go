package main

import (
	"context"
	"errors"
	"github.com/3n0ugh/dcpg"
	"github.com/3n0ugh/dcpg/message/format"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"time"
)

/*
	psql "postgres://dcp_user:dcp_pass@127.0.0.1:5433/dcp_db"

	CREATE TABLE users (
	 user_id integer PRIMARY KEY,
	 name text NOT NULL
	);
*/

/*
	psql "postgres://dcp_user:dcp_pass@127.0.0.1/dcp_db?replication=database"

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
	pool, err := pgxpool.New(ctx, "postgres://dcp_user:dcp_pass@127.0.0.1:5433/dcp_db")
	if err != nil {
		slog.Error("new pool", "error", err)
		os.Exit(1)
	}

	cfg := dcpg.Config{
		Host:     "127.0.0.1",
		Username: "dcp_user",
		Password: "dcp_pass",
		Database: "dcp_db",
		Publication: dcpg.PublicationConfig{
			Name:         "dcp_publication",
			Create:       true,
			DropIfExists: true,
		},
		Slot: dcpg.SlotConfig{
			Name:   "dcp_slot",
			Create: true,
		},
	}

	connector, err := dcpg.NewConnector(ctx, cfg)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	ch, err := connector.Start(ctx)
	if err != nil {
		slog.Error("connector start", "error", err)
		os.Exit(1)
	}

	Produce(ctx, pool, Filter(ch))
}

func Filter(ch <-chan dcpg.Context) <-chan Message {
	messages := make(chan Message, 128)

	go func() {
		for {
			event, ok := <-ch
			if !ok {
				os.Exit(1)
			}

			switch msg := event.Message.(type) {
			case *format.Insert:
				messages <- Message{
					Query: UpsertQuery,
					Args:  []any{msg.Decoded["id"].(int32), msg.Decoded["name"].(string)},
					Ack:   event.Ack,
				}
			case *format.Delete:
				messages <- Message{
					Query: DeleteQuery,
					Args:  []any{msg.OldDecoded["id"].(int32)},
					Ack:   event.Ack,
				}
			case *format.Update:
				messages <- Message{
					Query: UpsertQuery,
					Args:  []any{msg.NewDecoded["id"].(int32), msg.NewDecoded["name"].(string)},
					Ack:   event.Ack,
				}
			}
		}
	}()

	return messages
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
