package cdc

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func observableOnlineConnector() *OnlineConnector {
	return &OnlineConnector{cfg: config.Config{Slot: slot.Config{Name: "observed_slot"}}, capture: make(chan struct{}), snapshot: make(chan struct{}), done: make(chan struct{})}
}

func TestOnlineHealthRetainsNotificationsButNotRunningAfterStop(t *testing.T) {
	c := observableOnlineConnector()
	require.Equal(t, OnlineHealth{}, c.Health())
	require.NoError(t, c.beginOnlineRun())
	require.True(t, c.Health().Running)
	close(c.capture)
	require.True(t, c.Health().CaptureReady)
	require.False(t, c.Health().SnapshotComplete)
	close(c.snapshot)
	c.finishOnlineRun(nil)
	require.Equal(t, OnlineHealth{CaptureReady: true, SnapshotComplete: true}, c.Health())
}

func TestOnlineCollectorExportsBoundedCountersWithoutNetwork(t *testing.T) {
	c := observableOnlineConnector()
	c.counters.RecordRead(1234, 250*time.Millisecond)
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(c.MetricsCollector()))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, len(onlineMetricDefinitions))
	values := map[string]float64{}
	helps := map[string]string{}
	for _, family := range families {
		helps[family.GetName()] = family.GetHelp()
		metric := family.GetMetric()[0]
		require.Equal(t, "slot_name", metric.GetLabel()[0].GetName())
		require.Equal(t, "observed_slot", metric.GetLabel()[0].GetValue())
		if metric.Gauge != nil {
			values[family.GetName()] = metric.GetGauge().GetValue()
		} else {
			values[family.GetName()] = metric.GetCounter().GetValue()
		}
	}
	require.InDelta(t, 1, values["go_pq_cdc_online_read_queries_total"], 0.000001)
	require.InDelta(t, 1234, values["go_pq_cdc_online_peak_chunk_raw_bytes"], 0.000001)
	require.InDelta(t, 0.25, values["go_pq_cdc_online_peak_read_latency_seconds"], 0.000001)
	require.Zero(t, values["go_pq_cdc_online_running"])
	require.Zero(t, values["go_pq_cdc_online_last_feedback_timestamp_seconds"])
	require.Equal(t, "Current queued and visibility-gating encoded envelope bytes, not process RSS.", helps["go_pq_cdc_online_buffered_encoded_bytes"])
}
