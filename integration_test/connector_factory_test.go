package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func factoryPort(t *testing.T) int {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close()) }()
	return listener.Addr().(*net.TCPAddr).Port
}

func startFactoryConnector(t *testing.T, connector cdc.Connector) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { connector.Start(t.Context()); close(done) }()
	t.Cleanup(func() {
		connector.Close()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("factory Start cleanup deadline")
		}
	})
	ready, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(ready))
	return done
}

func TestConnectorFactoryConsistentListenerUnchanged(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE legacy_items(id bigint PRIMARY KEY,value text); INSERT INTO legacy_items VALUES(1,'old')")
	cfg := onlineConfig(t, source, "legacy_items")
	cfg.Snapshot.Strategy = config.SnapshotStrategyConsistent
	cfg.Snapshot.Enabled = false
	cfg.Metric.Port = factoryPort(t)
	updates := make(chan *format.Update, 1)
	ackErrors := make(chan error, 1)
	connector, err := cdc.NewConnectorWithOptions(t.Context(), cfg, cdc.WithLegacyListener(func(ctx *replication.ListenerContext) {
		if err := ctx.Ack(); err != nil {
			ackErrors <- err
			return
		}
		if update, ok := ctx.Message.(*format.Update); ok {
			updates <- update
		}
	}))
	require.NoError(t, err)
	startFactoryConnector(t, connector)
	onlineExec(t, conn, "UPDATE legacy_items SET value='new' WHERE id=1")
	select {
	case update := <-updates:
		require.Equal(t, "new", update.NewDecoded["value"])
	case err := <-ackErrors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("legacy callback did not receive the update")
	}
	require.Equal(t, config.SnapshotStrategyConsistent, connector.GetConfig().Snapshot.Strategy)
}

func TestConnectorFactoryOnlineDurableLifecycle(t *testing.T) {
	source, destination := newOnlinePGFixture(t), newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, destination)
	onlineExec(t, conn, "CREATE TABLE factory_items(id bigint PRIMARY KEY,value text); INSERT INTO factory_items VALUES(1,'old'),(2,'deleted')")
	provisionOnline(t, conn, "factory_items")
	cfg := onlineConfig(t, source, "factory_items")
	cfg.Metric.Port = factoryPort(t)
	connector, err := cdc.NewConnectorWithOptions(t.Context(), cfg, cdc.WithOnlineHandler(func(ctx context.Context, event cdc.OnlineEvent) error {
		return target.Apply(ctx, event, onlineTargetOK)
	}))
	require.NoError(t, err)
	connector.SetMetricCollectors(prometheus.NewGauge(prometheus.GaugeOpts{Name: "factory_live_extra"}))
	started := startFactoryConnector(t, connector)
	lifecycle, ok := connector.(cdc.OnlineConnectorLifecycle)
	require.True(t, ok)
	complete, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, lifecycle.WaitUntilSnapshotComplete(complete))
	onlineExec(t, conn, "UPDATE factory_items SET value='new' WHERE id=1; DELETE FROM factory_items WHERE id=2")
	waitOnlineFence(t, conn)
	compareOnlineTable(t, conn, target, "factory_items")
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.Metric.Port)
	onlineEventually(t, func() bool {
		ctx, stop := context.WithTimeout(t.Context(), time.Second)
		defer stop()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		return err == nil && response.StatusCode == http.StatusOK && strings.Contains(string(data), "factory_live_extra") && strings.Contains(string(data), "go_pq_cdc_online_running")
	}, 5*time.Second, 10*time.Millisecond, "factory metrics endpoint")
	connector.Close()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("factory Close did not join Start")
	}
	require.ErrorIs(t, lifecycle.Err(), context.Canceled)
	require.False(t, lifecycle.Health().Running)
	require.ErrorIs(t, connector.WaitUntilReady(t.Context()), context.Canceled)
	require.NoError(t, t.Context().Err())
}

func TestConnectorFactoryOnlineHandlerFailureRetainsSafeACK(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE factory_failure(id bigint PRIMARY KEY,value text); INSERT INTO factory_failure VALUES(1,'old')")
	provisionOnline(t, conn, "factory_failure")
	cfg := onlineConfig(t, source, "factory_failure")
	cfg.Metric.Port = factoryPort(t)
	sentinel := errors.New("destination did not commit")
	connector, err := cdc.NewConnectorWithOptions(t.Context(), cfg, cdc.WithOnlineHandler(func(_ context.Context, event cdc.OnlineEvent) error {
		if event.Operation == cdc.OnlineUpdate {
			return sentinel
		}
		return nil
	}))
	require.NoError(t, err)
	startFactoryConnector(t, connector)
	lifecycle := connector.(cdc.OnlineConnectorLifecycle)
	complete, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, lifecycle.WaitUntilSnapshotComplete(complete))
	onlineExec(t, conn, "UPDATE factory_failure SET value='not-durable' WHERE id=1")
	select {
	case <-lifecycle.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("factory did not expose the handler failure")
	}
	require.ErrorIs(t, lifecycle.Err(), sentinel)
	var unsafe bool
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
	require.False(t, unsafe)
}
