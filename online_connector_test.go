package cdc

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

func TestOnlineConstructorContract(t *testing.T) {
	t.Run("caller-input-immutable", func(t *testing.T) {
		cfg := config.Config{Host: "localhost", Username: "u", Password: "p", Database: "d", Publication: publication.Config{Name: "p", Tables: publication.Tables{{Name: "items", ReplicaIdentity: "FULL"}}}, Slot: slot.Config{Name: "s", ProtoVersion: 1}, Snapshot: config.SnapshotConfig{Enabled: true, Mode: config.SnapshotModeInitial, Strategy: config.SnapshotStrategyOnline, Tables: publication.Tables{{Name: "items", ReplicaIdentity: "FULL"}}}}
		var group sync.WaitGroup
		results := make(chan error, 20)
		for i := 0; i < 20; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, err := NewOnlineConnector(context.Background(), cfg, func(context.Context, OnlineEvent) error { return nil })
				results <- err
			}()
		}
		group.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		if cfg.Publication.Tables[0].Schema != "" || cfg.Snapshot.Tables[0].Schema != "" {
			t.Fatal("caller-owned slices changed")
		}
	})
	cfg := config.Config{Host: "localhost", Username: "test", Password: "test", Database: "test", Publication: publication.Config{Name: "p"}, Slot: slot.Config{Name: "s", ProtoVersion: 1}, Snapshot: config.SnapshotConfig{Enabled: true, Mode: config.SnapshotModeInitial, Strategy: config.SnapshotStrategyOnline}}
	handler := func(context.Context, OnlineEvent) error { return nil }
	small := cfg
	small.Snapshot.Online.TransactionBytes = 46
	if _, err := NewOnlineConnector(context.Background(), small, handler); err == nil {
		t.Fatal("impossible BEGIN/COMMIT budget accepted before Run")
	}
	if _, err := NewOnlineConnector(context.Background(), cfg, handler, WithOnlineObserver(func(context.Context, OnlineObservation) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if _, err := NewConnector(context.Background(), cfg, nil); err == nil || !strings.Contains(err.Error(), "NewOnlineConnector") {
		t.Fatalf("legacy constructor accepted online: %v", err)
	}
	if _, err := NewOnlineConnector(context.Background(), cfg, nil); err == nil {
		t.Fatal("nil durable handler accepted")
	}
	c, err := NewOnlineConnector(context.Background(), cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = c.Run(ctx); err == nil {
		t.Fatal("canceled Run succeeded")
	}
	if c.Err() == nil {
		t.Fatal("missing terminal error")
	}
	if err = c.WaitUntilCaptureReady(context.Background()); err == nil {
		t.Fatal("false readiness")
	}
	if err = c.WaitUntilSnapshotComplete(context.Background()); err == nil {
		t.Fatal("false snapshot readiness")
	}
	if err = c.Run(context.Background()); err == nil {
		t.Fatal("second Run accepted")
	}
}
