package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

// docker-compose up --scale go-pq-cdc-kafka=3 -d
func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:      "postgres",
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Snapshot: config.SnapshotConfig{
			Enabled: true,
			Mode:    config.SnapshotModeSnapshotOnly,
			Tables: publication.Tables{
				publication.Table{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
				},
			},
			ChunkSize:         100,
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
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

func Handler(ctx *replication.ListenerContext) {
	time.Sleep(500 * time.Millisecond)
	msg := ctx.Message.(*format.Snapshot)

	switch msg.EventType {
	case format.SnapshotEventTypeBegin:
		log.Printf("📸 SNAPSHOT BEGIN | LSN: %s | Time: %s",
			msg.LSN.String(),
			msg.ServerTime.Format("15:04:05"))

	case format.SnapshotEventTypeData:
		slog.Info("snapshot message received", "data", msg.Data)

	case format.SnapshotEventTypeEnd:
		log.Printf("📸 SNAPSHOT END | LSN: %s | Time: %s",
			msg.LSN.String(),
			msg.ServerTime.Format("15:04:05"))
	}
}
