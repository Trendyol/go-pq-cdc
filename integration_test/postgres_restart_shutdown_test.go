package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

// TestPostgresRestartCancelsReplicationShutdown verifies the real server
// disconnect path. The listener deliberately waits for its context so the
// test proves that a PostgreSQL restart causes graceful stream shutdown rather
// than leaving the replication session active.
func TestPostgresRestartCancelsReplicationShutdown(t *testing.T) {
	ctx := context.Background()
	cfg := Config
	cfg.Publication.Name = "cdc_publication_postgres_restart"
	cfg.Slot.Name = "slot_test_postgres_restart"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, postgresConn, cfg))

	listenerStarted := make(chan struct{})
	listenerStopped := make(chan struct{})
	listenerStartedOnce := make(chan struct{})
	listenerStoppedOnce := make(chan struct{})
	listener := func(lCtx *replication.ListenerContext) {
		if _, ok := lCtx.Message.(*format.Insert); !ok {
			return
		}
		select {
		case <-listenerStartedOnce:
		default:
			close(listenerStartedOnce)
			close(listenerStarted)
		}
		<-lCtx.Context.Done()
		select {
		case <-listenerStoppedOnce:
		default:
			close(listenerStoppedOnce)
			close(listenerStopped)
		}
	}

	connector, err := cdc.NewConnector(ctx, cfg, listener)
	require.NoError(t, err)
	defer func() {
		connector.Close()
		_ = postgresConn.Close(ctx)
		_ = RestoreDB(ctx)
	}()

	runCtx, cancel := context.WithCancel(ctx)
	startDone := make(chan struct{})
	go func() {
		connector.Start(runCtx)
		close(startDone)
	}()

	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	require.NoError(t, connector.WaitUntilReady(readyCtx))
	readyCancel()
	require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO books(id, name) VALUES(900001, 'restart')"))

	select {
	case <-listenerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not start")
	}

	require.NoError(t, Container.Stop(ctx, nil))
	require.NoError(t, Container.Start(ctx))

	// Wait until PostgreSQL accepts connections again before checking the slot.
	deadline := time.Now().Add(10 * time.Second)
	var restartedConn = postgresConn
	for time.Now().Before(deadline) {
		restartedConn, err = newPostgresConn()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err)
	defer restartedConn.Close(ctx)

	select {
	case <-listenerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("listener context was not cancelled after PostgreSQL restart")
	}

	active, err := replicationSlotActive(ctx, restartedConn, cfg.Slot.Name)
	require.NoError(t, err)
	require.False(t, active, "replication slot is still active after PostgreSQL restart")

	closeDone := make(chan struct{})
	go func() {
		connector.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connector.Close did not finish after PostgreSQL restart")
	}
	cancel()
	select {
	case <-startDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connector did not finish shutdown after PostgreSQL restart")
	}
}

func replicationSlotActive(ctx context.Context, conn pq.Connection, slotName string) (bool, error) {
	reader := conn.Exec(ctx, fmt.Sprintf(
		"SELECT active FROM pg_replication_slots WHERE slot_name = '%s'",
		slotName,
	))
	results, err := reader.ReadAll()
	if err != nil {
		_ = reader.Close()
		return false, err
	}
	if err := reader.Close(); err != nil {
		return false, err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("replication slot %q was not found", slotName)
	}
	return string(results[0].Rows[0][0]) == "t", nil
}
