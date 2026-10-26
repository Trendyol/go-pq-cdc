package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// partitionedRootFixture creates a root whose tree exercises the leaf-discovery edge cases:
// a leaf whose name needs quoting, a sub-partitioned branch (two levels) and an empty leaf.
// Rows are padded so every non-empty leaf spans several ctid chunks at ChunkSize 50.
// Returns the ids expected per region.
func partitionedRootFixture(t *testing.T, ctx context.Context, tableName string) map[string][]int {
	t.Helper()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(`
		DROP TABLE IF EXISTS %[1]s CASCADE;
		CREATE TABLE %[1]s (
			id BIGINT NOT NULL,
			region TEXT NOT NULL,
			status TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (id, region)
		) PARTITION BY LIST (region);
		CREATE TABLE "%[1]s_EU" PARTITION OF %[1]s FOR VALUES IN ('eu');
		CREATE TABLE %[1]s_us PARTITION OF %[1]s FOR VALUES IN ('us') PARTITION BY RANGE (id);
		CREATE TABLE %[1]s_us_low PARTITION OF %[1]s_us FOR VALUES FROM (0) TO (1000);
		CREATE TABLE %[1]s_us_high PARTITION OF %[1]s_us FOR VALUES FROM (1000) TO (100000);
		CREATE TABLE %[1]s_ap PARTITION OF %[1]s FOR VALUES IN ('ap');
		INSERT INTO %[1]s
		SELECT i, 'eu', CASE WHEN i %% 2 = 0 THEN 'active' ELSE 'inactive' END, repeat('x', 200)
		FROM generate_series(1, 300) i;
		INSERT INTO %[1]s
		SELECT i, 'us', CASE WHEN i %% 2 = 0 THEN 'active' ELSE 'inactive' END, repeat('x', 200)
		FROM generate_series(301, 600) i;
		INSERT INTO %[1]s
		SELECT i, 'us', CASE WHEN i %% 2 = 0 THEN 'active' ELSE 'inactive' END, repeat('x', 200)
		FROM generate_series(1001, 1200) i;
	`, tableName)))

	expected := map[string][]int{}
	for i := 1; i <= 1200; i++ {
		if i > 600 && i <= 1000 || i%2 != 0 {
			continue
		}
		region := "us"
		if i <= 300 {
			region = "eu"
		}
		expected[region] = append(expected[region], i)
	}
	return expected
}

func partitionedRootConfig(tableName, slotName string) config.Config {
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Publication.Name = "pub_" + slotName
	cdcCfg.Publication.Tables = publication.Tables{{
		Name:            tableName,
		Schema:          "public",
		ReplicaIdentity: publication.ReplicaIdentityDefault,
		Partitioned:     true,
		// Qualified with the root name on purpose: it must keep resolving when the
		// chunk query targets a leaf.
		QueryCondition: tableName + ".status = 'active'",
	}}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = config.SnapshotModeInitial
	cdcCfg.Snapshot.ChunkSize = 50
	return cdcCfg
}

// snapshotRows collects snapshot data events until END and returns how many times each id was seen.
type snapshotRows struct {
	seen   map[int]int
	tables map[string]struct{}
	done   chan struct{}
	mu     sync.Mutex
}

func newSnapshotRows() *snapshotRows {
	return &snapshotRows{seen: map[int]int{}, tables: map[string]struct{}{}, done: make(chan struct{})}
}

func (r *snapshotRows) handle(msg *format.Snapshot) {
	switch msg.EventType {
	case format.SnapshotEventTypeData:
		r.mu.Lock()
		r.seen[int(msg.Data["id"].(int64))]++
		r.tables[msg.Schema+"."+msg.Table] = struct{}{}
		r.mu.Unlock()
	case format.SnapshotEventTypeEnd:
		close(r.done)
	}
}

func (r *snapshotRows) requireExactlyOnce(t *testing.T, expected []int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range expected {
		require.Equalf(t, 1, r.seen[id], "id %d must be delivered exactly once", id)
	}
	require.Len(t, r.seen, len(expected), "no rows outside the expected set")
}

// TestSnapshotPartitionedRootDeliversEveryRowOnce: every row matching the query condition is
// delivered exactly once under the root's identity, and chunks point at leaves only.
func TestSnapshotPartitionedRootDeliversEveryRowOnce(t *testing.T) {
	ctx := context.Background()
	tableName := "snapshot_partitioned_root"
	cdcCfg := partitionedRootConfig(tableName, "slot_snapshot_partitioned_root")
	expected := partitionedRootFixture(t, ctx, tableName)

	rows := newSnapshotRows()
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lc *replication.ListenerContext) {
		if msg, ok := lc.Message.(*format.Snapshot); ok {
			rows.handle(msg)
		}
		_ = lc.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		cleanupSnapshotTest(t, ctx, tableName+" CASCADE", cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})
	go connector.Start(ctx)

	select {
	case <-rows.done:
	case <-time.After(30 * time.Second):
		t.Fatal("snapshot did not complete")
	}

	rows.requireExactlyOnce(t, append(expected["eu"], expected["us"]...))
	assert.Equal(t, map[string]struct{}{"public." + tableName: {}}, rows.tables, "events carry the root identity")

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)
	results, err := execQuery(ctx, conn, fmt.Sprintf(`
		SELECT physical_table_name, COUNT(*)
		FROM cdc_snapshot_chunks
		WHERE slot_name = '%s'
		GROUP BY physical_table_name
		ORDER BY physical_table_name COLLATE "C"`, cdcCfg.Slot.Name))
	require.NoError(t, err)
	leaves := map[string]string{}
	for _, row := range results[0].Rows {
		leaves[string(row[0])] = string(row[1])
	}
	// The sub-partitioned parent (_us) is not a leaf and must not be scanned.
	require.ElementsMatch(t,
		[]string{`"` + tableName + `_EU"`, tableName + "_us_low", tableName + "_us_high", tableName + "_ap"},
		keysOf(leaves))
	assert.NotEqual(t, "1", leaves[`"`+tableName+`_EU"`], "fixture must span several chunks per leaf, otherwise duplicates cannot show")
}

// TestSnapshotPartitionedRootCompletesWhenLeafDropped: a leaf dropped after chunk planning
// (retention job) must not leave its chunks failing forever; the snapshot still completes
// with the rows of the surviving leaves.
func TestSnapshotPartitionedRootCompletesWhenLeafDropped(t *testing.T) {
	ctx := context.Background()
	tableName := "snapshot_partitioned_drop"
	cdcCfg := partitionedRootConfig(tableName, "slot_snapshot_partitioned_drop")
	expected := partitionedRootFixture(t, ctx, tableName)

	dropConn, err := newPostgresConn()
	require.NoError(t, err)

	rows := newSnapshotRows()
	var dropOnce sync.Once
	var dropErr error
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lc *replication.ListenerContext) {
		if msg, ok := lc.Message.(*format.Snapshot); ok {
			// The single worker is blocked in this handler on the first ("..._EU") leaf,
			// so the us_high chunks are planned but not yet read.
			if msg.EventType == format.SnapshotEventTypeData {
				dropOnce.Do(func() {
					dropErr = pgExec(ctx, dropConn, fmt.Sprintf("DROP TABLE %s_us_high", tableName))
				})
			}
			rows.handle(msg)
		}
		_ = lc.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		dropConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName+" CASCADE", cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})
	go connector.Start(ctx)

	select {
	case <-rows.done:
	case <-time.After(30 * time.Second):
		t.Fatal("snapshot did not complete after a leaf was dropped")
	}
	require.NoError(t, dropErr)

	var surviving []int
	for _, id := range append(expected["eu"], expected["us"]...) {
		if id < 1000 {
			surviving = append(surviving, id)
		}
	}
	rows.requireExactlyOnce(t, surviving)
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
