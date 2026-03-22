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

// TestPublicationWithColumnList verifies that a publication created with a
// column list only replicates the specified columns. When columns are defined
// on a table entry, the CREATE PUBLICATION statement should include the column
// list and CDC messages should only contain those columns.
func TestPublicationWithColumnList(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_pub_column_list"
	cdcCfg.Publication.Name = "pub_column_list"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Tables = []publication.Table{
		{
			Name:            "books",
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "name"},
		},
	}

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	t.Run("Insert should contain only specified columns", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "INSERT INTO books(id, name) VALUES(1, 'test-book')")
		require.NoError(t, err)

		select {
		case m := <-messageCh:
			ins := m.(*format.Insert)
			assert.Equal(t, int32(1), ins.Decoded["id"])
			assert.Equal(t, "test-book", ins.Decoded["name"])
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert message")
		}
	})
}

// TestPublicationWithPartialColumnList verifies that specifying a subset of
// columns results in only those columns being replicated. Columns not in the
// publication column list should be absent from CDC messages.
func TestPublicationWithPartialColumnList(t *testing.T) {
	ctx := context.Background()

	// Create a multi-column table for this test
	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS multi_col_books;
		CREATE TABLE multi_col_books (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			price NUMERIC(10,2) DEFAULT 0.00
		);
	`)
	require.NoError(t, err)

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_pub_partial_cols"
	cdcCfg.Publication.Name = "pub_partial_cols"
	cdcCfg.Publication.CreateIfNotExists = true
	// Only replicate id and title — author and price should be excluded
	cdcCfg.Publication.Tables = []publication.Table{
		{
			Name:            "multi_col_books",
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "title"},
		},
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS multi_col_books")
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	t.Run("Insert should only contain id and title columns", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "INSERT INTO multi_col_books(id, title, author, price) VALUES(1, 'Go Programming', 'John Doe', 29.99)")
		require.NoError(t, err)

		select {
		case m := <-messageCh:
			ins := m.(*format.Insert)
			assert.Equal(t, int32(1), ins.Decoded["id"])
			assert.Equal(t, "Go Programming", ins.Decoded["title"])
			// author and price should not be present
			_, hasAuthor := ins.Decoded["author"]
			_, hasPrice := ins.Decoded["price"]
			assert.False(t, hasAuthor, "author column should not be in replicated message")
			assert.False(t, hasPrice, "price column should not be in replicated message")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert message")
		}
	})
}

// TestPublicationColumnListCreateAndInfo verifies that creating a publication
// with a column list succeeds and that Info() can query it back without errors.
func TestPublicationColumnListCreateAndInfo(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_col_list_info_test"

	// Cleanup
	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	})

	// Ensure clean state and table exists
	_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS books;
		CREATE TABLE books (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		);
	`)
	require.NoError(t, err)

	pubConfig := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{
				Name:            "books",
				Schema:          "public",
				ReplicaIdentity: publication.ReplicaIdentityDefault,
				Columns:         []string{"id", "name"},
			},
		},
	}

	pub := publication.New(pubConfig, postgresConn)

	// Create the publication
	createdCfg, err := pub.Create(ctx)
	require.NoError(t, err, "Create() should succeed for publication with column list")
	require.NotNil(t, createdCfg)

	// Query back publication info
	info, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed for publication with column list")
	assert.Equal(t, pubName, info.Name)
	assert.NotEmpty(t, info.Tables, "Tables should not be empty")

	// Verify the books table is in the publication
	found := false
	for _, table := range info.Tables {
		if table.Name == "books" && table.Schema == "public" {
			found = true
			break
		}
	}
	assert.True(t, found, "books table should be in the publication")
}

// TestPublicationColumnListValidation verifies that specifying columns with
// FULL replica identity is rejected by validation.
func TestPublicationColumnListValidation(t *testing.T) {
	table := publication.Table{
		Name:            "books",
		Schema:          "public",
		ReplicaIdentity: publication.ReplicaIdentityFull,
		Columns:         []string{"id", "name"},
	}

	err := table.Validate()
	require.Error(t, err, "columns with FULL replica identity should be rejected")
	assert.Contains(t, err.Error(), "cannot specify columns when replica identity is FULL")
}

// TestPublicationColumnListWithUpdatesAndDeletes verifies that update and
// delete messages also respect the column list filter when using DEFAULT
// replica identity.
func TestPublicationColumnListWithUpdatesAndDeletes(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS col_filter_books;
		CREATE TABLE col_filter_books (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			description TEXT DEFAULT ''
		);
	`)
	require.NoError(t, err)

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_pub_col_upd_del"
	cdcCfg.Publication.Name = "pub_col_upd_del"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Operations = publication.Operations{"INSERT", "UPDATE", "DELETE"}
	cdcCfg.Publication.Tables = []publication.Table{
		{
			Name:            "col_filter_books",
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Columns:         []string{"id", "title"},
		},
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Update, *format.Delete:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS col_filter_books")
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	t.Run("Insert filtered columns only", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "INSERT INTO col_filter_books(id, title, description) VALUES(1, 'Test Book', 'A description')")
		require.NoError(t, err)

		select {
		case m := <-messageCh:
			ins := m.(*format.Insert)
			assert.Equal(t, int32(1), ins.Decoded["id"])
			assert.Equal(t, "Test Book", ins.Decoded["title"])
			_, hasDesc := ins.Decoded["description"]
			assert.False(t, hasDesc, "description should not be replicated")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert message")
		}
	})

	t.Run("Update filtered columns only", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "UPDATE col_filter_books SET title = 'Updated Book', description = 'New desc' WHERE id = 1")
		require.NoError(t, err)

		select {
		case m := <-messageCh:
			upd := m.(*format.Update)
			assert.Equal(t, int32(1), upd.NewDecoded["id"])
			assert.Equal(t, "Updated Book", upd.NewDecoded["title"])
			_, hasDesc := upd.NewDecoded["description"]
			assert.False(t, hasDesc, "description should not be in update message")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for update message")
		}
	})

	t.Run("Delete with filtered columns", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "DELETE FROM col_filter_books WHERE id = 1")
		require.NoError(t, err)

		select {
		case m := <-messageCh:
			del := m.(*format.Delete)
			// With DEFAULT replica identity, old decoded should have the PK
			assert.Equal(t, int32(1), del.OldDecoded["id"])
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for delete message")
		}
	})
}
