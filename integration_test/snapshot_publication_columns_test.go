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

// TestSnapshotInitialWithPublicationColumnList verifies that when a publication
// table specifies a column list, the initial snapshot only includes those
// columns in its DATA events. Columns outside the list must be absent.
func TestSnapshotInitialWithPublicationColumnList(t *testing.T) {
	ctx := context.Background()

	tableName := "snap_pub_col_list_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snap_pub_col_list"
	cdcCfg.Publication.Name = "pub_snap_pub_col_list"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "title"},
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with extra columns that should not be replicated
	err = pgExec(ctx, postgresConn, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id    SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT NOT NULL DEFAULT '',
			price  NUMERIC(10, 2) DEFAULT 0.00
		);
	`, tableName, tableName))
	require.NoError(t, err)

	// Pre-insert rows so they are captured by the snapshot
	rowCount := 3
	for i := 1; i <= rowCount; i++ {
		err = pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, title, author, price) VALUES(%d, 'Title %d', 'Author %d', %d.99)",
			tableName, i, i, i, i*10,
		))
		require.NoError(t, err)
	}

	snapshotData := make([]map[string]any, 0, rowCount)
	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		case *format.Insert:
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

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	// Collect snapshot events until END
	snapshotCompleted := false
	timeout := time.After(10 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			if snap, ok := msg.(*format.Snapshot); ok {
				switch snap.EventType {
				case format.SnapshotEventTypeData:
					snapshotData = append(snapshotData, snap.Data)
					t.Logf("📸 Snapshot DATA: %v", snap.Data)
				case format.SnapshotEventTypeEnd:
					snapshotCompleted = true
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot to complete")
		}
	}

	t.Run("Snapshot data contains only specified columns", func(t *testing.T) {
		require.Len(t, snapshotData, rowCount, "should receive one DATA event per pre-inserted row")

		for _, row := range snapshotData {
			assert.Contains(t, row, "id", "id must be present")
			assert.Contains(t, row, "title", "title must be present")
			_, hasAuthor := row["author"]
			_, hasPrice := row["price"]
			assert.False(t, hasAuthor, "author should not be in snapshot data")
			assert.False(t, hasPrice, "price should not be in snapshot data")
		}
	})

	// Verify column list is also respected for subsequent CDC events
	t.Run("CDC events after snapshot also respect column list", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, title, author, price) VALUES(100, 'CDC Title', 'CDC Author', 99.99)",
			tableName,
		))
		require.NoError(t, err)

		select {
		case msg := <-messageCh:
			ins, ok := msg.(*format.Insert)
			require.True(t, ok, "expected Insert message")
			assert.Contains(t, ins.Decoded, "id")
			assert.Contains(t, ins.Decoded, "title")
			_, hasAuthor := ins.Decoded["author"]
			_, hasPrice := ins.Decoded["price"]
			assert.False(t, hasAuthor, "author should not be in CDC insert message")
			assert.False(t, hasPrice, "price should not be in CDC insert message")
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for CDC insert event")
		}
	})
}

// TestSnapshotOnlyWithColumnList verifies that in snapshot_only mode, setting
// a column list on snapshot.Tables causes the snapshot to select only those
// columns. Columns outside the list must not appear in DATA events.
func TestSnapshotOnlyWithColumnList(t *testing.T) {
	ctx := context.Background()

	tableName := "snap_only_col_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:    tableName,
			Schema:  "public",
			Columns: []string{"id", "name"},
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with extra columns
	err = pgExec(ctx, postgresConn, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id    SERIAL PRIMARY KEY,
			name  TEXT NOT NULL,
			email TEXT NOT NULL DEFAULT '',
			age   INT DEFAULT 0
		);
	`, tableName, tableName))
	require.NoError(t, err)

	// Pre-insert rows
	rowCount := 4
	for i := 1; i <= rowCount; i++ {
		err = pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, name, email, age) VALUES(%d, 'User%d', 'user%d@example.com', %d)",
			tableName, i, i, i, 20+i,
		))
		require.NoError(t, err)
	}

	snapshotData := make([]map[string]any, 0, rowCount)
	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snap, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh <- snap
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	completeCh := make(chan struct{})
	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	snapshotCompleted := false
	timeout := time.After(10 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			snap := msg.(*format.Snapshot)
			switch snap.EventType {
			case format.SnapshotEventTypeData:
				snapshotData = append(snapshotData, snap.Data)
				t.Logf("📸 Snapshot DATA: %v", snap.Data)
			case format.SnapshotEventTypeEnd:
				snapshotCompleted = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot_only to complete")
		}
	}

	// Wait for connector to exit (snapshot_only mode terminates after completion)
	select {
	case <-completeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Connector did not exit after snapshot_only completion")
	}

	t.Run("Snapshot data contains only specified columns", func(t *testing.T) {
		require.Len(t, snapshotData, rowCount)

		for _, row := range snapshotData {
			assert.Contains(t, row, "id", "id must be present")
			assert.Contains(t, row, "name", "name must be present")
			_, hasEmail := row["email"]
			_, hasAge := row["age"]
			assert.False(t, hasEmail, "email should not be in snapshot data")
			assert.False(t, hasAge, "age should not be in snapshot data")
		}
	})
}

// TestSnapshotInitialMultipleTablesWithColumnLists verifies that when multiple
// tables are configured with different column lists, each table's snapshot DATA
// events contain only its own specified columns, and excluded columns are absent
// from both tables.
func TestSnapshotInitialMultipleTablesWithColumnLists(t *testing.T) {
	ctx := context.Background()

	tableA := "snap_multi_col_a"
	tableB := "snap_multi_col_b"

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snap_multi_col"
	cdcCfg.Publication.Name = "pub_snap_multi_col"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableA,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "val_a"},
		},
		{
			Name:            tableB,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "val_x"},
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create both tables with extra columns
	err = pgExec(ctx, postgresConn, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id    SERIAL PRIMARY KEY,
			val_a TEXT NOT NULL,
			val_b TEXT NOT NULL DEFAULT ''
		);
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id    SERIAL PRIMARY KEY,
			val_x TEXT NOT NULL,
			val_y TEXT NOT NULL DEFAULT ''
		);
	`, tableA, tableA, tableB, tableB))
	require.NoError(t, err)

	// Insert rows into each table
	for i := 1; i <= 3; i++ {
		err = pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, val_a, val_b) VALUES(%d, 'a%d', 'b%d')", tableA, i, i, i,
		))
		require.NoError(t, err)
		err = pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, val_x, val_y) VALUES(%d, 'x%d', 'y%d')", tableB, i, i, i,
		))
		require.NoError(t, err)
	}

	snapshotDataByTable := map[string][]map[string]any{
		tableA: {},
		tableB: {},
	}
	messageCh := make(chan any, 200)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snap, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh <- snap
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		// Clean up both tables via cleanupSnapshotTest for the slot/publication,
		// then drop the second table separately.
		cleanupSnapshotTest(t, ctx, tableA, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
		conn, cErr := newPostgresConn()
		if cErr == nil {
			_ = pgExec(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableB))
			conn.Close(ctx)
		}
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	snapshotCompleted := false
	timeout := time.After(15 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			snap := msg.(*format.Snapshot)
			switch snap.EventType {
			case format.SnapshotEventTypeData:
				if _, ok := snapshotDataByTable[snap.Table]; ok {
					snapshotDataByTable[snap.Table] = append(snapshotDataByTable[snap.Table], snap.Data)
				}
				t.Logf("📸 Snapshot DATA [%s]: %v", snap.Table, snap.Data)
			case format.SnapshotEventTypeEnd:
				snapshotCompleted = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for multi-table snapshot to complete")
		}
	}

	t.Run("Table A snapshot contains only id and val_a", func(t *testing.T) {
		rows := snapshotDataByTable[tableA]
		require.Len(t, rows, 3, "table A should have 3 snapshot rows")
		for _, row := range rows {
			assert.Contains(t, row, "id")
			assert.Contains(t, row, "val_a")
			_, hasValB := row["val_b"]
			assert.False(t, hasValB, "val_b must not appear in table A snapshot data")
		}
	})

	t.Run("Table B snapshot contains only id and val_x", func(t *testing.T) {
		rows := snapshotDataByTable[tableB]
		require.Len(t, rows, 3, "table B should have 3 snapshot rows")
		for _, row := range rows {
			assert.Contains(t, row, "id")
			assert.Contains(t, row, "val_x")
			_, hasValY := row["val_y"]
			assert.False(t, hasValY, "val_y must not appear in table B snapshot data")
		}
	})
}
