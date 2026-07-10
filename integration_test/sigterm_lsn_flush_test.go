package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

func TestSIGTERMFlushesPendingConfirmedLSN(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_sigterm_flush"

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)

		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		handlerConn, err := newPostgresConn()
		require.NoError(t, err)

		var once sync.Once
		var confirmedBefore string
		ackCheckedBeforeSignal := make(chan struct{})
		handlerErr := make(chan error, 1)
		handlerFunc := func(lCtx *replication.ListenerContext) {
			if err := lCtx.Ack(); err != nil {
				handlerErr <- err
				return
			}
			_, confirmedAfterAck, err := readSlotLSNs(ctx, handlerConn, cdcCfg.Slot.Name)
			if err != nil {
				handlerErr <- err
				return
			}
			if confirmedAfterAck != confirmedBefore {
				handlerErr <- fmt.Errorf(
					"confirmed_flush_lsn advanced before SIGTERM: before=%s after_ack=%s",
					confirmedBefore,
					confirmedAfterAck,
				)
				return
			}
			once.Do(func() {
				close(ackCheckedBeforeSignal)
			})
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err)

		startDone := make(chan struct{})
		go func() {
			defer close(startDone)
			connector.Start(ctx)
		}()

		t.Cleanup(func() {
			connector.Close()
			require.NoError(t, handlerConn.Close(ctx))
			_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cdcCfg.Publication.Name))
			_ = pgExec(ctx, postgresConn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", cdcCfg.Slot.Name, cdcCfg.Slot.Name))
			require.NoError(t, RestoreDB(ctx))
			require.NoError(t, postgresConn.Close(ctx))
		})

		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		_, confirmedBefore, err = readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
		require.NoError(t, err)

		require.NoError(t, pgExec(ctx, postgresConn, "INSERT INTO books(id, name) VALUES(9090, 'sigterm-flush')"))

		select {
		case <-ackCheckedBeforeSignal:
		case err := <-handlerErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert ack check")
		}

		require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))

		select {
		case <-startDone:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for connector to stop after SIGTERM")
		}

		connector.Close()

		require.Eventually(t, func() bool {
			_, confirmedAfter, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
			if err != nil {
				return false
			}
			return confirmedAfter != "" && confirmedAfter != confirmedBefore
		}, 5*time.Second, 100*time.Millisecond, "expected confirmed_flush_lsn to advance during SIGTERM shutdown")
	})
}
