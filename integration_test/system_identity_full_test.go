package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaIdentityDefault(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_default"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityDefault

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	defer func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	}()

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	t.Run("should return old value is nil when update message received", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for range 10 {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, booksNew[i].Map(), m.(*format.Update).NewDecoded)
			assert.Nil(t, m.(*format.Update).OldDecoded["id"])
		}
	})
}

func TestReplicaIdentityFull(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_full"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	t.Run("should return new value and old value when update message received", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for range 10 {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, booksNew[i].Map(), m.(*format.Update).NewDecoded)
			assert.Equal(t, books[i].Map(), m.(*format.Update).OldDecoded)
		}
	})
}

func TestReplicaIdentityNothing(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_nothing"
	cdcCfg.Publication.Operations = publication.Operations{publication.OperationInsert}
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityNothing

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	handlerFunc := func(ctx *replication.ListenerContext) {
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	replicaIdentity, replicaIdentityIndex := getReplicaIdentity(ctx, t, postgresConn, "public", "books")
	assert.Equal(t, publication.ReplicaIdentityNothing, replicaIdentity)
	assert.Empty(t, replicaIdentityIndex)
}

func TestReplicaIdentityUsingIndex(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_using_index"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityUsingIndex
	cdcCfg.Publication.Tables[0].ReplicaIdentityIndex = "books_name_unique_idx"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	err = pgExec(ctx, postgresConn, "CREATE UNIQUE INDEX books_name_unique_idx ON public.books(name);")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	handlerFunc := func(ctx *replication.ListenerContext) {
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	replicaIdentity, replicaIdentityIndex := getReplicaIdentity(ctx, t, postgresConn, "public", "books")
	assert.Equal(t, publication.ReplicaIdentityUsingIndex, replicaIdentity)
	assert.Equal(t, "books_name_unique_idx", replicaIdentityIndex)
}

func TestReplicaIdentityUsingIndexUpdateUsesKeyTuple(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_using_index_key_tuple"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityUsingIndex
	cdcCfg.Publication.Tables[0].ReplicaIdentityIndex = "books_name_unique_idx"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	err = pgExec(ctx, postgresConn, "CREATE UNIQUE INDEX books_name_unique_idx ON public.books(name);")
	require.NoError(t, err)

	updates := make(chan *format.Update, 5)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Update); ok {
			updates <- msg
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

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	err = pgExec(ctx, postgresConn, "INSERT INTO books(id, name) VALUES(1, 'book-old');")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = pgExec(ctx, postgresConn, "UPDATE books SET name = 'book-new' WHERE id = 1;")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	select {
	case msg := <-updates:
		assert.Equal(t, uint8(format.UpdateTupleTypeKey), msg.OldTupleType)
		assert.NotNil(t, msg.OldDecoded)
		assert.Len(t, msg.OldDecoded, 2)
		assert.Nil(t, msg.OldDecoded["id"])
		assert.Equal(t, "book-old", msg.OldDecoded["name"])
		assert.NotNil(t, msg.NewDecoded)
		assert.Len(t, msg.NewDecoded, 2)
		assert.Equal(t, int32(1), msg.NewDecoded["id"])
		assert.Equal(t, "book-new", msg.NewDecoded["name"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update message")
	}
}

func TestReplicaIdentityUsingIndexMissingIndexReturnsError(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_using_index_missing_index"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityUsingIndex
	cdcCfg.Publication.Tables[0].ReplicaIdentityIndex = "books_name_unique_idx_missing"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	t.Cleanup(func() {
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	handlerFunc := func(ctx *replication.ListenerContext) {
		_ = ctx.Ack()
	}

	_, err = cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if assert.Error(t, err) {
		assert.ErrorContains(t, err, "does not exist")
		assert.ErrorContains(t, err, "books_name_unique_idx_missing")
	}
}

func getReplicaIdentity(ctx context.Context, t *testing.T, conn pq.Connection, schema, table string) (string, string) {
	t.Helper()

	query := fmt.Sprintf(`
		SELECT c.relreplident, idx.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisreplident
		LEFT JOIN pg_class idx ON idx.oid = i.indexrelid
		WHERE n.nspname = '%s' AND c.relname = '%s'
	`, schema, table)

	resultReader := conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if !assert.NoError(t, err) {
		_ = resultReader.Close()
		t.FailNow()
	}

	if !assert.NoError(t, resultReader.Close()) {
		t.FailNow()
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		t.Fatalf("replica identity row not found for %s.%s", schema, table)
	}

	row := results[0].Rows[0]
	if len(row) < 2 {
		t.Fatalf("unexpected row length for replica identity query")
	}

	return publication.ReplicaIdentityMap[string(row[0])], string(row[1])
}
