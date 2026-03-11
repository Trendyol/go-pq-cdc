package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rowCount = 500

func main() {
	ctx := context.Background()

	// --- CDC Connector Configuration -------------------------------------
	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      5433,
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				publication.Table{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot_streaming",
			SlotActivityCheckerInterval: 3000,
			ProtoVersion:                2,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
		},
	}

	// --- Message Counter and Handler ------------------------------------------
	var insertCount atomic.Int64
	done := make(chan struct{})

	handler := func(lCtx *replication.ListenerContext) {
		switch msg := lCtx.Message.(type) {
		case *format.Insert:
			n := insertCount.Add(1)
			// Log the first and last few, and every 100th message
			if n <= 3 || n%100 == 0 || n >= int64(rowCount)-2 {
				slog.Info("INSERT alındı",
					"sayaç", n,
					"id", msg.Decoded["id"],
					"name", msg.Decoded["name"],
				)
			}
			if n == int64(rowCount) {
				close(done)
			}
		}

		if err := lCtx.Ack(); err != nil {
			slog.Error("ack hatası", "error", err)
		}
	}

	// --- Start the CDC Connector -------------------------------------------
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		slog.Error("connector oluşturulamadı", "error", err)
		os.Exit(1)
	}
	defer connector.Close()

	go connector.Start(ctx)

	// Wait until the connector is ready
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := connector.WaitUntilReady(waitCtx); err != nil {
		slog.Error("connector hazır olamadı", "error", err)
		os.Exit(1)
	}
	cancel()

	slog.Info("CDC consumer hazır, büyük transaction başlatılıyor...",
		"satırSayısı", rowCount,
	)

	// --- Large Transaction: Insert 500 Rows ------------------------------
	pool, err := pgxpool.New(ctx, "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db")
	if err != nil {
		slog.Error("pgxpool oluşturulamadı", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	tx, err := pool.Begin(ctx)
	if err != nil {
		slog.Error("transaction başlatılamadı", "error", err)
		os.Exit(1)
	}

	for i := 1; i <= rowCount; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)",
			i, fmt.Sprintf("user-%d", i))
		if err != nil {
			slog.Error("insert hatası", "error", err, "row", i)
			_ = tx.Rollback(ctx)
			os.Exit(1)
		}
	}

	slog.Info("Transaction COMMIT edildi, mesajlar bekleniyor...")

	// --- Wait for the Results --------------------------------------------------
	select {
	case <-done:
		slog.Info("BAŞARILI: Tüm mesajlar alındı!",
			"beklenen", rowCount,
			"alınan", insertCount.Load(),
		)
	case <-time.After(30 * time.Second):
		slog.Error("ZAMAN AŞIMI: Tüm mesajlar alınamadı!",
			"beklenen", rowCount,
			"alınan", insertCount.Load(),
			"kayıp", int64(rowCount)-insertCount.Load(),
		)
		os.Exit(1)
	}
}
