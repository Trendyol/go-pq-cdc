package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

var (
	snapshotBeginCount int64
	snapshotDataCount  int64
	snapshotEndCount   int64
	startTime          time.Time
)

func main() {
	startTime = time.Now()

	// Parse command-line flags
	configPath := flag.String("config", "./config.yml", "Path to configuration file")
	flag.Parse()

	log.Println("🚀 Starting Snapshot-Only Mode Example")
	log.Println("📖 Mode: snapshot_only (finite, no CDC)")
	log.Println(strings.Repeat("=", 80))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Load config
	cfg, err := config.ReadConfigYAML(*configPath)
	if err != nil {
		log.Fatalf("❌ Config error: %v", err)
	}

	log.Printf("📝 Configuration loaded from: %s", *configPath)
	log.Printf("   Database: %s@%s:%d/%s", cfg.Username, cfg.Host, cfg.Port, cfg.Database)
	log.Printf("   Tables: %d", len(cfg.Publication.Tables))
	log.Printf("   Chunk Size: %d", cfg.Snapshot.ChunkSize)

	// Create connector
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		log.Fatalf("❌ Connector error: %v", err)
	}
	defer connector.Close()

	// Start metrics printer
	stopMetrics := make(chan struct{})
	go printMetrics(ctx, stopMetrics)

	// Create done channel to know when Start() exits
	done := make(chan struct{})

	// Start connector
	log.Println("\n📸 Starting snapshot process...")
	go func() {
		connector.Start(ctx)
		close(done)
	}()

	// Wait for either completion or signal
	select {
	case <-done:
		log.Println("\n✅ Snapshot completed successfully!")
	case sig := <-sigCh:
		log.Printf("\n⚠️  Received signal: %v", sig)
		log.Println("🛑 Cancelling snapshot...")
		cancel()
		<-done
	}

	// Stop metrics printer
	close(stopMetrics)

	// Print final statistics
	printFinalStats()

	log.Println("\n👋 Snapshot-only process finished. Exiting...")
}

func handler(ctx *replication.ListenerContext) {
	switch v := ctx.Message.(type) {
	case *format.Snapshot:
		handleSnapshot(v)

	default:
		log.Printf("⚠️  Unexpected message type in snapshot-only mode: %T", ctx.Message)
	}

	if err := ctx.Ack(); err != nil {
		log.Printf("⚠️  Ack error: %v", err)
	}
}

func handleSnapshot(s *format.Snapshot) {
	switch s.EventType {
	case format.SnapshotEventTypeBegin:
		atomic.AddInt64(&snapshotBeginCount, 1)
		log.Printf("\n📸 SNAPSHOT BEGIN")
		log.Printf("   LSN: %s", s.LSN.String())
		log.Printf("   Time: %s", s.ServerTime.Format("2006-01-02 15:04:05"))
		log.Println(strings.Repeat("-", 80))

	case format.SnapshotEventTypeData:
		count := atomic.AddInt64(&snapshotDataCount, 1)

		// Print first 5 rows and then every 100th row
		if count <= 5 || count%100 == 0 {
			printSnapshotData(s, count)
		}

	case format.SnapshotEventTypeEnd:
		atomic.AddInt64(&snapshotEndCount, 1)
		log.Println(strings.Repeat("-", 80))
		log.Printf("📸 SNAPSHOT END")
		log.Printf("   LSN: %s", s.LSN.String())
		log.Printf("   Total Rows: %d", atomic.LoadInt64(&snapshotDataCount))
		log.Printf("   Duration: %s", time.Since(startTime).Round(time.Millisecond))
	}
}

func printSnapshotData(s *format.Snapshot, count int64) {
	// Truncate data for readability
	dataJSON, _ := json.Marshal(s.Data)
	dataStr := string(dataJSON)
	if len(dataStr) > 200 {
		dataStr = dataStr[:200] + "..."
	}

	log.Printf("📊 Row #%-6d | %s.%-20s | %s",
		count,
		s.Schema,
		s.Table,
		dataStr)
}

func printMetrics(ctx context.Context, stop chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(startTime).Round(time.Second)
			dataCount := atomic.LoadInt64(&snapshotDataCount)

			var throughput int64
			if elapsed.Seconds() > 0 {
				throughput = int64(float64(dataCount) / elapsed.Seconds())
			}

			log.Printf("\n⏱️  PROGRESS | Elapsed: %s | Rows: %d | Throughput: %d rows/sec\n",
				elapsed,
				dataCount,
				throughput)
		}
	}
}

func printFinalStats() {
	elapsed := time.Since(startTime)
	dataCount := atomic.LoadInt64(&snapshotDataCount)

	var avgThroughput int64
	if elapsed.Seconds() > 0 {
		avgThroughput = int64(float64(dataCount) / elapsed.Seconds())
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Snapshot Events:\n")
	fmt.Printf("  • BEGIN:  %d\n", atomic.LoadInt64(&snapshotBeginCount))
	fmt.Printf("  • DATA:   %d rows\n", dataCount)
	fmt.Printf("  • END:    %d\n", atomic.LoadInt64(&snapshotEndCount))

	fmt.Printf("\nPerformance:\n")
	fmt.Printf("  • Total Duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  • Average Throughput: %d rows/sec\n", avgThroughput)

	if dataCount > 0 {
		fmt.Printf("  • Avg Time per Row:  %.2f ms\n", elapsed.Seconds()*1000/float64(dataCount))
	}

	fmt.Println(strings.Repeat("=", 80))
}
