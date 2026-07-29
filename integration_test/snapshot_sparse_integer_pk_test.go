package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sparseIntegerPKIDs = []int{1, 10000, 20000}

func collectSnapshotUntilEnd(t *testing.T, messageCh <-chan any, expectedData int, timeout time.Duration) []map[string]any {
	t.Helper()
	var data []map[string]any
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-messageCh:
			m, ok := msg.(*format.Snapshot)
			if !ok {
				continue
			}
			switch m.EventType {
			case format.SnapshotEventTypeData:
				data = append(data, m.Data)
			case format.SnapshotEventTypeEnd:
				return data
			}
		case <-deadline:
			t.Fatalf("timeout waiting for snapshot END; got %d/%d DATA events", len(data), expectedData)
		}
	}
}

func queryChunkStats(t *testing.T, ctx context.Context, slotName string) (totalChunks, emptyChunks, totalRows int, strategy string) {
	t.Helper()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	q := fmt.Sprintf(`
		SELECT
			COUNT(*)::text,
			COUNT(*) FILTER (WHERE COALESCE(rows_processed, 0) = 0)::text,
			COALESCE(SUM(rows_processed), 0)::text,
			(SELECT partition_strategy FROM cdc_snapshot_chunks WHERE slot_name = '%s' LIMIT 1)
		FROM cdc_snapshot_chunks
		WHERE slot_name = '%s'
	`, slotName, slotName)

	results, err := execQuery(ctx, conn, q)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)

	row := results[0].Rows[0]
	totalChunks, err = strconv.Atoi(string(row[0]))
	require.NoError(t, err)
	emptyChunks, err = strconv.Atoi(string(row[1]))
	require.NoError(t, err)
	totalRows, err = strconv.Atoi(string(row[2]))
	require.NoError(t, err)
	strategy = string(row[3])
	return totalChunks, emptyChunks, totalRows, strategy
}

// TestSnapshotSparseIntegerPK_AutoFallsBackToCTID: sparse INT PKs must not auto-pick
// integer_range (PK-span chunks); auto-detect falls back to ctid_block.
func TestSnapshotSparseIntegerPK_AutoFallsBackToCTID(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_sparse_auto_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_sparse_auto"
	cdcCfg.Publication.Name = "pub_snapshot_sparse_auto"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
			// auto (empty) → sparse INT PK should prefer ctid_block
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 1000
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	require.NoError(t, createTestTable(ctx, postgresConn, tableName))
	t.Log("Inserting 3 sparse integer PK rows: 1, 10000, 20000")
	for _, id := range sparseIntegerPKIDs {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, id, id, 20+id%50)
		require.NoError(t, pgExec(ctx, postgresConn, query))
	}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	data := collectSnapshotUntilEnd(t, messageCh, 3, 60*time.Second)
	assert.Len(t, data, 3, "should receive exactly 3 DATA events")

	totalChunks, _, totalRows, strategy := queryChunkStats(t, ctx, cdcCfg.Slot.Name)
	t.Logf("sparse auto: strategy=%s totalChunks=%d rows=%d", strategy, totalChunks, totalRows)

	assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy,
		"auto must fall back to ctid_block for sparse integer PK")
	assert.Equal(t, 3, totalRows)
	assert.Less(t, totalChunks, 20, "must not create integer_range span chunks (20)")
	assert.GreaterOrEqual(t, totalChunks, 1)
}

// TestSnapshotSparseIntegerPK_CTIDAvoidsChunkExplosion: same sparse rows with ctid_block
// should not create one chunk per PK range window.
func TestSnapshotSparseIntegerPK_CTIDAvoidsChunkExplosion(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_sparse_ctid_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_sparse_ctid"
	cdcCfg.Publication.Name = "pub_snapshot_sparse_ctid"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:                      tableName,
			Schema:                    "public",
			ReplicaIdentity:           publication.ReplicaIdentityFull,
			SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 1000
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	require.NoError(t, createTestTable(ctx, postgresConn, tableName))
	for _, id := range sparseIntegerPKIDs {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, id, id, 20+id%50)
		require.NoError(t, pgExec(ctx, postgresConn, query))
	}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	data := collectSnapshotUntilEnd(t, messageCh, 3, 60*time.Second)
	require.Len(t, data, 3)

	received := map[int32]bool{}
	for _, row := range data {
		id := row["id"].(int32)
		received[id] = true
	}
	for _, id := range sparseIntegerPKIDs {
		assert.True(t, received[int32(id)], "missing id %d", id)
	}

	totalChunks, _, totalRows, strategy := queryChunkStats(t, ctx, cdcCfg.Slot.Name)
	t.Logf("sparse ctid: strategy=%s totalChunks=%d rows=%d", strategy, totalChunks, totalRows)

	assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy)
	assert.Equal(t, 3, totalRows)
	assert.Less(t, totalChunks, 20, "ctid_block must not explode like integer_range (20 chunks)")
	assert.GreaterOrEqual(t, totalChunks, 1)
}

// TestSnapshotDenseIntegerPK_AutoIsEfficient: sequential IDs → single chunk under same chunkSize.
func TestSnapshotDenseIntegerPK_AutoIsEfficient(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_dense_auto_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_dense_auto"
	cdcCfg.Publication.Name = "pub_snapshot_dense_auto"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 1000
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	require.NoError(t, createTestTable(ctx, postgresConn, tableName))
	for i := 1; i <= 20; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		require.NoError(t, pgExec(ctx, postgresConn, query))
	}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	data := collectSnapshotUntilEnd(t, messageCh, 20, 60*time.Second)
	assert.Len(t, data, 20)

	totalChunks, emptyChunks, totalRows, strategy := queryChunkStats(t, ctx, cdcCfg.Slot.Name)
	t.Logf("dense auto: strategy=%s totalChunks=%d emptyChunks=%d rows=%d",
		strategy, totalChunks, emptyChunks, totalRows)

	assert.Equal(t, string(snapshot.PartitionStrategyIntegerRange), strategy)
	assert.Equal(t, 1, totalChunks, "dense 1..20 with chunkSize=1000 → 1 chunk")
	assert.Equal(t, 20, totalRows)
	assert.Equal(t, 0, emptyChunks)
}
