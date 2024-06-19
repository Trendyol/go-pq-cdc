package main

import (
	"context"
	"encoding/json"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"os"
	"time"
)

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
	FROM generate_series(1, 100) AS i;
*/

type Message struct {
	Message kafka.Message
	Ack     func() error
}

func main() {
	ctx := context.Background()

	w := &kafka.Writer{
		Addr:                   kafka.TCP("redpanda:9092"),
		Topic:                  "cdc.test.produce",
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              10000,
		AllowAutoTopicCreation: true,
	}

	defer func() {
		err := w.Close()
		if err != nil {
			slog.Error("kafka writer close", "error", err)
		}
	}()

	messages := make(chan Message, 10000)
	go Produce(ctx, w, messages)

	cfg := config.Config{
		Host:      "localhost:5432",
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Publication: publication.Config{
			Name: "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{publication.Table{
				Name:            "users",
				ReplicaIdentity: publication.ReplicaIdentityFull,
			}},
		},
		Slot: slot.Config{
			Name:                          "cdc_slot",
			SlotActivityCheckerIntervalMS: 3000,
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
			encoded, _ := json.Marshal(msg.Decoded)
			messages <- Message{
				Message: kafka.Message{
					Key:   []byte(uuid.NewString()),
					Value: encoded,
					Time:  time.Now(),
				},
				Ack: ctx.Ack,
			}
		case *format.Delete:
			slog.Info("delete message received", "old", msg.OldDecoded)
		case *format.Update:
			slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
		}
	}
}

func Produce(ctx context.Context, w *kafka.Writer, messages <-chan Message) {
	var err error
	var lastAck func() error
	message := make([]kafka.Message, 100000)
	counter := 0

	for {
		select {
		case event := <-messages:
			message[counter] = event.Message
			lastAck = event.Ack
			counter++

			if counter == 100000 {
				err = w.WriteMessages(ctx, message...)
				if err != nil {
					slog.Error("kafka produce", "error", err)
					continue
				}
				slog.Info("kafka produce", "count", counter)
				counter = 0
				if err = event.Ack(); err != nil {
					slog.Error("ack", "error", err)
				}
			}
		case <-time.After(100 * time.Millisecond):
			if counter > 0 {
				err = w.WriteMessages(ctx, message[:counter]...)
				if err != nil {
					slog.Error("kafka produce", "error", err)
					continue
				}
				slog.Info("kafka produce time", "count", counter)
				counter = 0
				if err = lastAck(); err != nil {
					slog.Error("ack", "error", err)
				}
			}
		}
	}
}
