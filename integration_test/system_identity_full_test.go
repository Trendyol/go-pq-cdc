package integration

import (
	"context"
	"fmt"
	cdc "github.com/vskurikhin/go-pq-cdc"
	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/vskurikhin/go-pq-cdc/pq/message/format"
	"github.com/vskurikhin/go-pq-cdc/pq/publication"
	"github.com/vskurikhin/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var _ replication.Listeners = (*listenersContainerReplica)(nil)

type listenersContainerReplica struct {
	messageCh chan any
}

func (l *listenersContainerReplica) ListenerFunc() replication.ListenerFunc {
	return func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			l.messageCh <- msg
		}
		_ = ctx.Ack()
	}
}

func (l *listenersContainerReplica) SendLSNHookFunc() replication.SendLSNHookFunc {
	return func(pq.LSN) {
	}
}

func (l *listenersContainerReplica) SinkHookFunc() replication.SinkHookFunc {
	return func(xLogData *replication.XLogData) {
	}
}

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
	lc := &listenersContainerReplica{
		messageCh: messageCh,
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, lc)
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

		for i := 0; i < 10; i++ {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := 0; i < 5; i++ {
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
	lc := &listenersContainerReplica{
		messageCh: messageCh,
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, lc)
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

		for i := 0; i < 10; i++ {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := 0; i < 5; i++ {
			m := <-messageCh
			assert.Equal(t, booksNew[i].Map(), m.(*format.Update).NewDecoded)
			assert.Equal(t, books[i].Map(), m.(*format.Update).OldDecoded)
		}
	})
}
