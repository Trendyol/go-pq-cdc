package slot

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestInfoDeadlineIncludesAdmissionWait(t *testing.T) {
	entered := make(chan struct{})
	s, _ := newWireSlot(t, slotWireHooks{query: func(ctx context.Context, _ *pgproto3.Backend) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}})
	s.cfg.InfoTimeout = time.Minute
	first := make(chan error, 1)
	go func() {
		_, err := s.Info(t.Context())
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first status query did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := s.Info(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 750*time.Millisecond)
	s.Close(context.Background())
	select {
	case err := <-first:
		require.ErrorIs(t, err, ErrorSlotClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown failed to interrupt the admitted query")
	}
}

func TestMetricsStopsBeforeNextTick(t *testing.T) {
	s := NewSlot("", "", Config{Name: "metrics_stop", SlotActivityCheckerInterval: 60_000}, nil, nil)
	t.Cleanup(func() { s.Close(context.Background()) })
	done := make(chan struct{})
	go func() {
		s.Metrics(context.Background())
		close(done)
	}()
	s.Close(context.Background())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("metrics waited for a stopped ticker instead of observing shutdown")
	}
}

func TestStatusQueryQuotesSlotLiteral(t *testing.T) {
	s := NewSlot("", "", Config{Name: "slot' OR true --", SlotActivityCheckerInterval: 1000}, nil, nil)
	t.Cleanup(func() { s.Close(context.Background()) })
	require.Contains(t, s.statusSQL, "WHERE slot_name = 'slot'' OR true --';")
}
