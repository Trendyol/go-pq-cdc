package main

import (
	"context"
	"encoding/json"
	"github.com/3n0ugh/dcpg"
	"github.com/3n0ugh/dcpg/message/format"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"net/http"
	"os"
	"time"
)

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
	FROM generate_series(1, 100) AS i;
*/

type Message struct {
	Message kafka.Message
	Ack     func() error
}

var (
	messageSuccessProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dcpg_processed_success_ops_total",
		Help: "The total number of successfully processed messages",
	})

	messageFailProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dcpg_processed_fail_ops_total",
		Help: "The total number of not successfully processed messages",
	})
)

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":2112", nil)
		if err != nil {
			slog.Error("prometheus metrics handler", "error", err)
			os.Exit(1)
		}
	}()

	ctx := context.Background()

	w := &kafka.Writer{
		Addr:                   kafka.TCP("redpanda:9092"),
		Topic:                  "dcpg.test.produce",
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

	cfg := dcpg.Config{
		Host:     "postgres:5432",
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

	Produce(ctx, w, Filter(ch))
}

func Filter(ch <-chan dcpg.Context) <-chan Message {
	messages := make(chan Message, 10000)

	go func() {
		for {
			event, ok := <-ch
			if !ok {
				os.Exit(1)
			}

			switch msg := event.Message.(type) {
			case *format.Insert:
				encoded, _ := json.Marshal(msg.Decoded)
				messages <- Message{
					Message: kafka.Message{
						Key:   []byte(uuid.NewString()),
						Value: encoded,
						Time:  time.Now(),
					},
					Ack: event.Ack,
				}
			case *format.Delete:
				slog.Info("delete message received", "old", msg.OldDecoded)
			case *format.Update:
				slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
			}
		}
	}()

	return messages
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
					messageFailProcessed.Add(float64(counter))
					slog.Error("kafka produce", "error", err)
					continue
				}
				messageSuccessProcessed.Add(float64(counter))
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
					messageFailProcessed.Add(float64(counter))
					slog.Error("kafka produce", "error", err)
					continue
				}
				messageSuccessProcessed.Add(float64(counter))
				slog.Info("kafka produce time", "count", counter)
				counter = 0
				if err = lastAck(); err != nil {
					slog.Error("ack", "error", err)
				}
			}
		}
	}
}
