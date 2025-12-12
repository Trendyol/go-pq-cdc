package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoWalsenderDuringSnapshot validates Issue #56 behavior:
// - During snapshot execution, there should be zero walsender connections
// - Walsender connections should only exist after snapshot completes
// - CDC streaming should work normally after snapshot
func TestNoWalsenderDuringSnapshot(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with larger dataset for longer snapshot duration
	tableName := "snapshot_no_walsender_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_no_walsender"
	cdcCfg.Publication.Name = "pub_no_walsender"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25 // Small chunks for longer snapshot duration

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert test data BEFORE snapshot
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 100 rows for snapshot test...")
	for i := 1; i <= 100; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataCount := 0
	snapshotEndReceived := false
	cdcInsertReceived := false

	// Walsender tracking
	maxWalsendersDuringSnapshot := 0
	var snapshotInProgress atomic.Bool
	stopWalsenderCheck := make(chan struct{})

	messageCh := make(chan any, 200)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	// Start connector with snapshot
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	// Separate connection for walsender checks
	checkConn, err := newPostgresConn()
	require.NoError(t, err)
	defer checkConn.Close(ctx)

	// Background goroutine to check walsender count during snapshot
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopWalsenderCheck:
				return
			case <-ticker.C:
				if snapshotInProgress.Load() {
					count, err := countWalsendersForSlot(ctx, checkConn, cdcCfg.Slot.Name)
					if err != nil {
						t.Logf("⚠️  Failed to count walsenders: %v", err)
						continue
					}
					if count > maxWalsendersDuringSnapshot {
						maxWalsendersDuringSnapshot = count
					}
					if count > 0 {
						t.Logf("⚠️  Walsender detected during snapshot: count=%d", count)
					}
				}
			}
		}
	}()

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Collect snapshot events
	timeout := time.After(30 * time.Second)
	snapshotCompleted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			switch m := msg.(type) {
			case *format.Snapshot:
				switch m.EventType {
				case format.SnapshotEventTypeBegin:
					snapshotBeginReceived = true
					snapshotInProgress.Store(true)
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", m.LSN.String())
				case format.SnapshotEventTypeData:
					snapshotDataCount++
					if snapshotDataCount%25 == 0 {
						t.Logf("📸 Snapshot progress: %d/100 rows received", snapshotDataCount)
					}
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotInProgress.Store(false)
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s", m.LSN.String())
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot events. Received %d DATA events", snapshotDataCount)
		}
	}

	// Stop walsender checking
	close(stopWalsenderCheck)

	// Wait a bit for CDC to start
	time.Sleep(500 * time.Millisecond)

	// Check walsender count AFTER snapshot (should exist for CDC streaming)
	walsenderCountAfterSnapshot, err := countWalsendersForSlot(ctx, checkConn, cdcCfg.Slot.Name)
	require.NoError(t, err)
	t.Logf("📊 Walsender count after snapshot: %d", walsenderCountAfterSnapshot)

	// Now insert new data after snapshot (CDC phase)
	t.Log("📝 Inserting new data for CDC test...")
	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'CDCUser_%d', %d)",
			tableName, 1000+i, i, 30)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Collect CDC insert event
	cdcTimeout := time.After(5 * time.Second)
collectCDC:
	for {
		select {
		case msg := <-messageCh:
			if _, ok := msg.(*format.Insert); ok {
				cdcInsertReceived = true
				t.Log("🔄 CDC INSERT received")
				break collectCDC
			}
		case <-cdcTimeout:
			t.Log("⚠️  Timeout waiting for CDC INSERT event")
			break collectCDC
		}
	}

	// === Assertions ===

	t.Run("Verify No Walsender During Snapshot", func(t *testing.T) {
		assert.Equal(t, 0, maxWalsendersDuringSnapshot,
			"There should be zero walsender connections during snapshot phase (Issue #56)")
		t.Logf("✅ Max walsenders during snapshot: %d (expected: 0)", maxWalsendersDuringSnapshot)
	})

	t.Run("Verify Exactly One Walsender After Snapshot", func(t *testing.T) {
		assert.Equal(t, 1, walsenderCountAfterSnapshot,
			"There should be exactly 1 walsender connection after snapshot (stream only, slot uses standard DSN)")
		t.Logf("✅ Walsender count after snapshot: %d (expected: 1)", walsenderCountAfterSnapshot)
	})

	t.Run("Verify CDC Streaming Works", func(t *testing.T) {
		assert.True(t, cdcInsertReceived, "CDC should receive INSERT events after snapshot")
		t.Log("✅ CDC streaming is working")
	})

	t.Run("Verify Snapshot Completed", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Equal(t, 100, snapshotDataCount, "Should receive 100 DATA events from snapshot")

		// Query snapshot job metadata
		query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		completed := string(results[0].Rows[0][0]) == "t"
		assert.True(t, completed, "Job should be marked as completed")

		t.Logf("✅ Snapshot completed: %t, rows captured: %d", completed, snapshotDataCount)
	})
}
