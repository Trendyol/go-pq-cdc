package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
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
	cdcInsertCount     int64
	cdcUpdateCount     int64
	cdcDeleteCount     int64
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "./config.yml", "Path to configuration file")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load config
	cfg, err := config.ReadConfigYAML(*configPath)
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	// Create connector
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		log.Fatalf("Connector error: %v", err)
	}

	// Start metrics printer
	go printMetrics(ctx)

	// Create done channel to know when Start() exits
	done := make(chan struct{})

	// Start connector
	go func() {
		connector.Start(ctx)
		close(done)
	}()

	if err := connector.WaitUntilReady(context.TODO()); err != nil {
		log.Fatalf("Timeout waiting for connector ready: %v", err)
	}
	log.Println("✅ CDC connector is ready!")

	// Wait for signal
	<-done
	log.Println("\n🛑 Shutting down...")

	// Cancel context to stop all goroutines
	cancel()

	// Close connector properly
	connector.Close()

	// Give goroutines time to cleanup
	time.Sleep(500 * time.Millisecond)

	printFinalStats()
}

func handler(ctx *replication.ListenerContext) {
	switch v := ctx.Message.(type) {
	case *format.Insert:
		atomic.AddInt64(&cdcInsertCount, 1)
		printCDCEvent("INSERT", v.TableNamespace, v.TableName, v.Decoded)

	case *format.Update:
		atomic.AddInt64(&cdcUpdateCount, 1)
		printCDCEvent("UPDATE", v.TableNamespace, v.TableName, v.NewDecoded)

	case *format.Delete:
		atomic.AddInt64(&cdcDeleteCount, 1)
		printCDCEvent("DELETE", v.TableNamespace, v.TableName, v.OldDecoded)

	case *format.Snapshot:
		handleSnapshot(v)

	default:
		log.Printf("⚠️  Unknown message type: %T", ctx.Message)
	}

	if err := ctx.Ack(); err != nil {
		log.Printf("⚠️  Ack error: %v", err)
	}
}

func handleSnapshot(s *format.Snapshot) {
	switch s.EventType {
	case format.SnapshotEventTypeBegin:
		atomic.AddInt64(&snapshotBeginCount, 1)
		log.Printf("📸 SNAPSHOT BEGIN | LSN: %s | Time: %s",
			s.LSN.String(),
			s.ServerTime.Format("15:04:05"))

	case format.SnapshotEventTypeData:
		count := atomic.AddInt64(&snapshotDataCount, 1)
		if count%100 == 0 || count <= 5 {
			//printSnapshotData(s, count)
		}

	case format.SnapshotEventTypeEnd:
		atomic.AddInt64(&snapshotEndCount, 1)
		log.Printf("📸 SNAPSHOT END | LSN: %s | Total Rows: %d",
			s.LSN.String(),
			atomic.LoadInt64(&snapshotDataCount))
	}
}

func printSnapshotData(s *format.Snapshot, count int64) {
	dataJSON, _ := json.Marshal(s.Data)
	log.Printf("📸 SNAPSHOT DATA [%d] | Table: %s.%s | Data: %s",
		count,
		s.Schema,
		s.Table,
		string(dataJSON))
}

func printCDCEvent(eventType, schema, table string, data map[string]any) {
	dataJSON, _ := json.Marshal(data)
	log.Printf("🔄 CDC %s | Table: %s.%s | Data: %s",
		eventType,
		schema,
		table,
		string(dataJSON))
}

func printMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("📊 METRICS | Snapshot: BEGIN=%d DATA=%d END=%d | CDC: INSERT=%d UPDATE=%d DELETE=%d",
				atomic.LoadInt64(&snapshotBeginCount),
				atomic.LoadInt64(&snapshotDataCount),
				atomic.LoadInt64(&snapshotEndCount),
				atomic.LoadInt64(&cdcInsertCount),
				atomic.LoadInt64(&cdcUpdateCount),
				atomic.LoadInt64(&cdcDeleteCount))
		}
	}
}

func printFinalStats() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Snapshot Events:\n")
	fmt.Printf("  - BEGIN:  %d\n", atomic.LoadInt64(&snapshotBeginCount))
	fmt.Printf("  - DATA:   %d\n", atomic.LoadInt64(&snapshotDataCount))
	fmt.Printf("  - END:    %d\n", atomic.LoadInt64(&snapshotEndCount))
	fmt.Printf("\nCDC Events:\n")
	fmt.Printf("  - INSERT: %d\n", atomic.LoadInt64(&cdcInsertCount))
	fmt.Printf("  - UPDATE: %d\n", atomic.LoadInt64(&cdcUpdateCount))
	fmt.Printf("  - DELETE: %d\n", atomic.LoadInt64(&cdcDeleteCount))
	fmt.Printf("\nTotal Events: %d\n",
		atomic.LoadInt64(&snapshotBeginCount)+
			atomic.LoadInt64(&snapshotDataCount)+
			atomic.LoadInt64(&snapshotEndCount)+
			atomic.LoadInt64(&cdcInsertCount)+
			atomic.LoadInt64(&cdcUpdateCount)+
			atomic.LoadInt64(&cdcDeleteCount))
	fmt.Println(strings.Repeat("=", 80))
}
