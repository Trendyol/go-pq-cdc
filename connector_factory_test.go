package cdc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Exact function-value compatibility, not merely compatibility of call sites.
var (
	_ func(context.Context, config.Config, replication.ListenerFunc) (Connector, error)                       = NewConnector
	_ func(context.Context, string, replication.ListenerFunc) (Connector, error)                              = NewConnectorWithConfigFile
	_ func(context.Context, config.Config, OnlineHandler, ...OnlineConnectorOption) (*OnlineConnector, error) = NewOnlineConnector
)

func factoryOnlineConfig() config.Config {
	return config.Config{Host: "localhost", Username: "u", Password: "p", Database: "db",
		Publication: publication.Config{Name: "p"}, Slot: slot.Config{Name: "s", ProtoVersion: 1},
		Snapshot: config.SnapshotConfig{Enabled: true, Mode: config.SnapshotModeInitial, Strategy: config.SnapshotStrategyOnline}}
}

func TestConnectorFactoryRejectsAmbiguousOrWrongHandlers(t *testing.T) {
	handler := func(context.Context, OnlineEvent) error { return nil }
	listener := func(*replication.ListenerContext) {}
	cases := []struct {
		name     string
		online   bool
		options  []ConnectorOption
		contains string
	}{
		{"online requires durable handler", true, nil, "WithOnlineHandler"},
		{"consistent requires listener option", false, nil, "WithLegacyListener"},
		{"online rejects legacy listener", true, []ConnectorOption{WithLegacyListener(listener)}, "legacy listener"},
		{"nil durable handler", true, []ConnectorOption{WithOnlineHandler(nil)}, "durable handler"},
		{"mixed handlers", true, []ConnectorOption{WithLegacyListener(listener), WithOnlineHandler(handler)}, "handler"},
		{"duplicate durable handlers", true, []ConnectorOption{WithOnlineHandler(handler), WithOnlineHandler(handler)}, "handler"},
		{"duplicate legacy listeners", false, []ConnectorOption{WithLegacyListener(listener), WithLegacyListener(listener)}, "handler"},
		{"consistent rejects online handler", false, []ConnectorOption{WithOnlineHandler(handler)}, "online"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := factoryOnlineConfig()
			if !tc.online {
				cfg.Snapshot.Strategy = config.SnapshotStrategyConsistent
			}
			_, err := NewConnectorWithOptions(t.Context(), cfg, tc.options...)
			require.ErrorContains(t, err, tc.contains)
		})
	}
}

func TestConnectorFactoryOnlineConstructionAndCloseBeforeStart(t *testing.T) {
	cfg := factoryOnlineConfig()
	connector, err := NewConnectorWithOptions(t.Context(), cfg, nil, WithOnlineHandler(func(context.Context, OnlineEvent) error {
		t.Error("closing an unopened connector called its handler")
		return nil
	}, WithOnlineObserver(func(context.Context, OnlineObservation) error { return nil })))
	require.NoError(t, err)
	lifecycle, ok := connector.(OnlineConnectorLifecycle)
	require.True(t, ok)
	require.Equal(t, OnlineHealth{}, lifecycle.Health())
	connector.Close()
	connector.Close()
	connector.Start(t.Context())
	select {
	case <-lifecycle.Done():
	case <-time.After(time.Second):
		t.Fatal("Close before Start did not finish the online lifecycle")
	}
	require.ErrorIs(t, lifecycle.Err(), context.Canceled)
	require.ErrorIs(t, connector.WaitUntilReady(t.Context()), context.Canceled)
	require.ErrorIs(t, lifecycle.WaitUntilSnapshotComplete(t.Context()), context.Canceled)
}

func TestConnectorFactoryConcurrentStartCloseCancelsOnlyOwnedContext(t *testing.T) {
	connector, err := NewConnectorWithOptions(t.Context(), factoryOnlineConfig(), WithOnlineHandler(func(context.Context, OnlineEvent) error { return nil }))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			connector.Start(ctx)
			connector.Close()
		}()
	}
	group.Wait()
	lifecycle := connector.(OnlineConnectorLifecycle)
	require.ErrorIs(t, lifecycle.Err(), context.Canceled)
	require.NoError(t, t.Context().Err(), "closing a connector canceled the caller's context")
}

func TestConnectorFactoryReturnsOwnedConfigAndRegistersCollectors(t *testing.T) {
	cfg := factoryOnlineConfig()
	cfg.Publication.Tables = publication.Tables{{Name: "items", ReplicaIdentity: "FULL"}}
	cfg.Snapshot.Tables = publication.Tables{{Name: "items", ReplicaIdentity: "FULL"}}
	cfg.Heartbeat.Table.Columns = []string{"value"}
	connector, err := NewConnectorWithOptions(t.Context(), cfg, WithOnlineHandler(func(context.Context, OnlineEvent) error { return nil }))
	require.NoError(t, err)
	defer connector.Close()
	view := connector.GetConfig()
	view.Publication.Tables[0].Schema = "changed"
	view.Snapshot.Tables[0].Name = "changed"
	view.Snapshot.Strategy = config.SnapshotStrategyConsistent
	view.Heartbeat.Table.Columns[0] = "changed"
	require.Equal(t, []string{"value"}, connector.GetConfig().Heartbeat.Table.Columns)
	require.Equal(t, []string{"value"}, cfg.Heartbeat.Table.Columns)
	require.Equal(t, config.SnapshotStrategyOnline, connector.GetConfig().Snapshot.Strategy)
	require.Equal(t, "public", connector.GetConfig().Publication.Tables[0].Schema)
	require.Equal(t, "items", connector.GetConfig().Snapshot.Tables[0].Name)
	require.Empty(t, cfg.Publication.Tables[0].Schema)
	collector := prometheus.NewGauge(prometheus.GaugeOpts{Name: "factory_test_extra"})
	connector.SetMetricCollectors(collector)
	adapter := connector.(*onlineConnectorAdapter)
	families, err := adapter.registry.Prometheus().Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		found = found || family.GetName() == "factory_test_extra"
	}
	require.True(t, found)
}

func TestConnectorFactoryPreservesLegacyValidation(t *testing.T) {
	cfg := config.Config{}
	_, oldErr := NewConnector(t.Context(), cfg, nil)
	_, factoryErr := NewConnectorWithOptions(t.Context(), cfg, WithLegacyListener(nil))
	require.Error(t, oldErr)
	require.Error(t, factoryErr)
	require.Equal(t, oldErr.Error(), factoryErr.Error())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := NewConnectorWithOptions(ctx, factoryOnlineConfig(), WithOnlineHandler(func(context.Context, OnlineEvent) error { return nil }))
	require.ErrorIs(t, err, context.Canceled)
}
