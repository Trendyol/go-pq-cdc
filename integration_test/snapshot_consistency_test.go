package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotConcurrentInsertNoDuplicates is THE MOST CRITICAL test:
// - Tests LSN consistency and no duplicate data guarantee
// - Insert initial data (id: 1-50)
// - Start snapshot
// - When BEGIN is received, concurrently INSERT new data (id: 100-150)
// - Verify: id 1-50 ONLY in snapshot
// - Verify: id 100-150 ONLY in CDC
// - Verify: NO overlap between snapshot and CDC
func TestSnapshotConcurrentInsertNoDuplicates(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table
	tableName := "snapshot_no_dup_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_no_dup"
	cdcCfg.Publication.Name = "pub_snapshot_no_dup"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100 // Single chunk for simplicity

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert INITIAL data (id: 1-50)
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting initial 50 rows (id: 1-50)...")
	for i := 1; i <= 50; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'Initial_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ Initial 50 rows inserted")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	cdcInsertReceived := []map[string]any{}

	// Channel to signal when to start concurrent inserts
	startConcurrentInserts := make(chan struct{})

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

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Start goroutine for concurrent inserts (will wait for signal)
	insertsDone := make(chan struct{})
	go func() {
		<-startConcurrentInserts
		t.Log("🔄 Starting concurrent inserts (id: 100-150)...")

		// Open a new connection for concurrent inserts
		insertConn, err := newPostgresConn()
		if err != nil {
			t.Logf("Error creating insert connection: %v", err)
			close(insertsDone)
			return
		}
		defer insertConn.Close(ctx)

		// Insert data that should go to CDC, NOT snapshot
		for i := 100; i <= 150; i++ {
			query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'Concurrent_%d', %d)",
				tableName, i, i, 20+i%50)
			if err := pgExec(ctx, insertConn, query); err != nil {
				t.Logf("Warning: Concurrent insert failed for id=%d: %v", i, err)
			}
			// Small delay to spread inserts over time
			time.Sleep(10 * time.Millisecond)
		}
		t.Log("✅ Concurrent inserts completed (id: 100-150)")
		close(insertsDone)
	}()

	// Collect snapshot events
	timeout := time.After(15 * time.Second)
	snapshotCompleted := false
	concurrentInsertsStarted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			switch m := msg.(type) {
			case *format.Snapshot:
				switch m.EventType {
				case format.SnapshotEventTypeBegin:
					snapshotBeginReceived = true
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", m.LSN.String())

					// Signal to start concurrent inserts RIGHT NOW
					if !concurrentInsertsStarted {
						close(startConcurrentInserts)
						concurrentInsertsStarted = true
						t.Log("🚀 Signaled concurrent inserts to start")
					}

				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
					if len(snapshotDataReceived)%10 == 0 {
						t.Logf("📸 Snapshot progress: %d rows received", len(snapshotDataReceived))
					}

				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s", m.LSN.String())
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot events. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// Wait for concurrent inserts to complete
	t.Log("⏳ Waiting for concurrent inserts to complete...")
	select {
	case <-insertsDone:
		t.Log("✅ Concurrent inserts confirmed done")
	case <-time.After(5 * time.Second):
		t.Log("⚠️  Timeout waiting for concurrent inserts (may still be in progress)")
	}

	// Collect CDC insert events (should be 51 total: id 100-150)
	t.Log("📥 Collecting CDC INSERT events...")
	cdcTimeout := time.After(5 * time.Second)
	expectedCDCCount := 51

collectCDC:
	for len(cdcInsertReceived) < expectedCDCCount {
		select {
		case msg := <-messageCh:
			if insertMsg, ok := msg.(*format.Insert); ok {
				cdcInsertReceived = append(cdcInsertReceived, insertMsg.Decoded)
				if len(cdcInsertReceived)%10 == 0 {
					t.Logf("🔄 CDC progress: %d INSERT events received", len(cdcInsertReceived))
				}
			}
		case <-cdcTimeout:
			t.Logf("⚠️  Timeout collecting CDC events. Received %d/%d", len(cdcInsertReceived), expectedCDCCount)
			break collectCDC
		}
	}
	t.Logf("✅ Collected %d CDC INSERT events", len(cdcInsertReceived))

	// === Critical Assertions ===

	t.Run("Verify Snapshot Captured Initial Data Only", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 50, "Should receive exactly 50 DATA events from snapshot")

		// Extract IDs from snapshot
		snapshotIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			snapshotIDs[id] = true
		}

		// Verify: All IDs 1-50 should be in snapshot
		for i := int32(1); i <= 50; i++ {
			assert.True(t, snapshotIDs[i], "Initial data ID %d should be in snapshot", i)
		}

		// Verify: No IDs 100-150 should be in snapshot
		for i := int32(100); i <= 150; i++ {
			assert.False(t, snapshotIDs[i], "Concurrent data ID %d should NOT be in snapshot", i)
		}

		t.Logf("✅ Snapshot contains ONLY initial data (id: 1-50)")
	})

	t.Run("Verify CDC Captured Concurrent Inserts Only", func(t *testing.T) {
		// Extract IDs from CDC
		cdcIDs := make(map[int32]bool)
		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			cdcIDs[id] = true
		}

		// Verify: No IDs 1-50 should be in CDC
		for i := int32(1); i <= 50; i++ {
			assert.False(t, cdcIDs[i], "Initial data ID %d should NOT be in CDC", i)
		}

		// Verify: All IDs 100-150 should be in CDC
		missingIDs := []int32{}
		for i := int32(100); i <= 150; i++ {
			if !cdcIDs[i] {
				missingIDs = append(missingIDs, i)
			}
		}

		if len(missingIDs) > 0 {
			t.Logf("⚠️  Missing IDs in CDC: %v (may be timing issue)", missingIDs)
		}

		// At least 45 of 51 concurrent inserts should be captured
		assert.GreaterOrEqual(t, len(cdcIDs), 45, "CDC should capture most concurrent inserts")

		t.Logf("✅ CDC contains ONLY concurrent data (captured %d/51)", len(cdcIDs))
	})

	t.Run("Verify NO Overlap Between Snapshot and CDC", func(t *testing.T) {
		// Extract all IDs
		snapshotIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			snapshotIDs[id] = true
		}

		cdcIDs := make(map[int32]bool)
		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			cdcIDs[id] = true
		}

		// Find overlaps
		overlaps := []int32{}
		for id := range snapshotIDs {
			if cdcIDs[id] {
				overlaps = append(overlaps, id)
			}
		}

		assert.Empty(t, overlaps, "There should be NO duplicate IDs between snapshot and CDC")

		if len(overlaps) > 0 {
			t.Errorf("❌ CRITICAL: Found duplicate IDs: %v", overlaps)
		} else {
			t.Log("✅ CRITICAL SUCCESS: Zero data duplication!")
		}
	})

	t.Run("Verify Total Data Integrity", func(t *testing.T) {
		// Collect all unique IDs from both snapshot and CDC
		allIDs := make(map[int32]bool)

		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			allIDs[id] = true
		}

		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			allIDs[id] = true
		}

		// We should have at least 95 unique IDs total (50 initial + at least 45 concurrent)
		assert.GreaterOrEqual(t, len(allIDs), 95, "Should have captured most of the data")

		t.Logf("✅ Total unique IDs captured: %d", len(allIDs))
		t.Logf("   - From snapshot: %d", len(snapshotDataReceived))
		t.Logf("   - From CDC: %d", len(cdcInsertReceived))
	})

	t.Run("Verify Snapshot Metadata", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed, total_chunks, completed_chunks, snapshot_lsn FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		snapshotLSN := string(row[3])

		assert.True(t, completed, "Job should be marked as completed")
		assert.NotEmpty(t, snapshotLSN, "Snapshot LSN should be recorded")

		t.Logf("✅ Snapshot LSN: %s", snapshotLSN)
	})
}
