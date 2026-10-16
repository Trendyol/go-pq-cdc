package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnlineConcurrentNextCancellation(t *testing.T) {
	f := newOnlineFakeTransport()
	entered := make(chan struct{})
	firstDone := make(chan error, 1)
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(ctx context.Context, _ uint32) error { close(entered); <-ctx.Done(); return ctx.Err() }})
	onlineFeedTransaction(f, 1, 100)
	go func() { _, err := r.Next(context.Background()); firstDone <- err }()
	onlineAwait(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() { _, err := r.Next(ctx); secondDone <- err }()
	// Always release the reader if the cancellation assertion fails.
	defer r.Close(context.Background())
	require.ErrorIs(t, onlineAwait(t, secondDone), context.Canceled)
	require.ErrorIs(t, onlineAwait(t, firstDone), context.Canceled)
	onlineAwait(t, r.Done())
}
