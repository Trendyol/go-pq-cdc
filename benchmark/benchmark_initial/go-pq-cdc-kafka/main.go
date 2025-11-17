package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/publication"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/segmentio/kafka-go"
	gokafka "github.com/segmentio/kafka-go"
)

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db?replication=database"

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 10000000) AS i;
*/

type Message struct {
	Message kafka.Message
	Ack     func() error
}

func main() {
	ctx := context.Background()

	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      "postgres",
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: true,
			Snapshot: cdcconfig.SnapshotConfig{
				Mode: cdcconfig.SnapshotModeSnapshotOnly,
				Tables: []publication.Table{
					{
						Name:   "users",
						Schema: "public",
					},
				},
				ChunkSize:         8000,
				Enabled:           true,
				ClaimTimeout:      60 * time.Second,
				HeartbeatInterval: 10 * time.Second,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 2112,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.users": "cdc.test.produce",
			},
			Brokers:                     []string{"redpanda:9092"},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10000,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(msg *cdc.Message) []gokafka.Message {
	// slog.Info("change captured", "message", msg)

	if msg.Type.IsSnapshot() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		return []gokafka.Message{
			{
				Headers: nil,
				Key:     []byte(strconv.Itoa(int(msg.NewData["id"].(int32)))),
				Value:   newData,
			},
		}
	}

	return []gokafka.Message{}
}
