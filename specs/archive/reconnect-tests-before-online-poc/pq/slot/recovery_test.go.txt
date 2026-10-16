package slot

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestInfoRecoversClosedConnection(t *testing.T) {
	for _, midQuery := range []bool{false, true} {
		name := "already closed"
		if midQuery {
			name = "connection dies during query"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			s, stats := newWireSlot(t, slotWireHooks{
				query: func(_ context.Context, backend *pgproto3.Backend) error {
					if midQuery && calls.Add(1) == 1 {
						return errors.New("connection reset")
					}
					return sendSlotInfo(backend)
				},
			})
			if !midQuery {
				require.NoError(t, s.conn.Close(t.Context()))
			}
			info, err := s.Info(t.Context())
			require.NoError(t, err)
			require.Equal(t, "test_slot", info.Name)
			require.Equal(t, Logical, info.Type)
			require.EqualValues(t, 2, stats.connections.Load())
			wantQueries := 1
			if midQuery {
				wantQueries = 2
			}
			require.EqualValues(t, wantQueries, stats.queries.Load())
		})
	}
}

func TestInfoStopsAfterOneQueryRetry(t *testing.T) {
	s, stats := newWireSlot(t, slotWireHooks{
		query: func(context.Context, *pgproto3.Backend) error { return errors.New("connection reset") },
	})
	_, err := s.Info(t.Context())
	require.Error(t, err)
	require.EqualValues(t, 2, stats.connections.Load())
	require.EqualValues(t, 2, stats.queries.Load())
}

func TestInfoDoesNotRetryServerSQLError(t *testing.T) {
	s, stats := newWireSlot(t, slotWireHooks{
		query: func(_ context.Context, backend *pgproto3.Backend) error {
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied"})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return backend.Flush()
		},
	})
	_, err := s.Info(t.Context())
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "42501", pgErr.Code)
	require.False(t, s.conn.IsClosed())
	require.EqualValues(t, 1, stats.connections.Load())
	require.EqualValues(t, 1, stats.queries.Load())
}

func TestInfoDeadlineBoundsQueryAndReconnect(t *testing.T) {
	for _, phase := range []string{"query", "reconnect"} {
		for _, callerDeadline := range []bool{false, true} {
			name := phase + "/slot timeout"
			if callerDeadline {
				name = phase + "/shorter caller deadline"
			}
			t.Run(name, func(t *testing.T) {
				s, stats := newWireSlot(t, slotWireHooks{
					connect: func(ctx context.Context, id int) error {
						if phase == "reconnect" && id == 2 {
							<-ctx.Done() // Accept TCP/startup but never finish authentication.
							return ctx.Err()
						}
						return nil
					},
					query: func(ctx context.Context, backend *pgproto3.Backend) error {
						if phase == "query" {
							<-ctx.Done()
							return ctx.Err()
						}
						return sendSlotInfo(backend)
					},
				})
				if phase == "reconnect" {
					require.NoError(t, s.conn.Close(t.Context()))
				}
				s.cfg.InfoTimeout = 100 * time.Millisecond
				callerTimeout := 2 * time.Second
				if callerDeadline {
					s.cfg.InfoTimeout = 2 * time.Second
					callerTimeout = 100 * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(t.Context(), callerTimeout)
				defer cancel()
				start := time.Now()
				_, err := s.Info(ctx)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Less(t, time.Since(start), 750*time.Millisecond)
				if phase == "query" {
					require.EqualValues(t, 1, stats.connections.Load(), "a timed-out request must not reconnect")
				} else {
					require.Zero(t, stats.queries.Load())
					// One failed poll must not permanently cancel the slot itself.
					info, err := s.Info(t.Context())
					require.NoError(t, err)
					require.Equal(t, "test_slot", info.Name)
					require.EqualValues(t, 3, stats.connections.Load())
				}
			})
		}
	}
}

func TestCloseInterruptsSlotInfoRecovery(t *testing.T) {
	for _, phase := range []string{"query", "reconnect", "retry wait"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			var once sync.Once
			signal := func() { once.Do(func() { close(entered) }) }
			s, _ := newWireSlot(t, slotWireHooks{
				connect: func(ctx context.Context, id int) error {
					if id > 1 && phase != "query" {
						signal()
						if phase == "retry wait" {
							return errors.New("connection rejected")
						}
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				},
				query: func(ctx context.Context, _ *pgproto3.Backend) error {
					signal()
					<-ctx.Done()
					return ctx.Err()
				},
			})
			s.cfg.InfoTimeout = time.Minute // Shutdown must interrupt it, not the timer.
			if phase != "query" {
				require.NoError(t, s.conn.Close(t.Context()))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := s.Info(ctx)
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("slot operation did not start")
			}
			if phase == "retry wait" {
				// Let pgconn observe rejection and enter the one-second retry wait.
				time.Sleep(50 * time.Millisecond)
			}
			closed := make(chan struct{})
			go func() {
				s.Close(context.Background())
				close(closed)
			}()
			select {
			case err := <-result:
				require.ErrorIs(t, err, ErrorSlotClosed)
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown did not interrupt the slot operation")
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("Close did not return")
			}
			require.NoError(t, ctx.Err(), "shutdown must not cancel the caller's context")
			require.True(t, s.conn.IsClosed())
		})
	}
}
