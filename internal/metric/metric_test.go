package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func replicaLagSeries(m *metric) int {
	ch := make(chan prometheus.Metric, 8)
	m.visibilityReplicaLagBytes.Collect(ch)
	close(ch)
	return len(ch)
}

func replicaCheckSeries(m *metric) int {
	ch := make(chan prometheus.Metric, 8)
	m.visibilityReplicaChecks.Collect(ch)
	close(ch)
	return len(ch)
}

// A standby that left the resolved set must not keep exporting its last lag
// or its check counters.
func TestDeleteVisibilityReplica(t *testing.T) {
	m := NewMetric("test_slot").(*metric)
	m.SetVisibilityReplicaLag("standby1:5432", 16)
	m.SetVisibilityReplicaLag("standby2:5432", 0)
	m.VisibilityReplicaCheck("standby1:5432", true)
	m.VisibilityReplicaCheck("standby1:5432", false)
	m.VisibilityReplicaCheck("standby2:5432", false)
	assert.Equal(t, 2, replicaLagSeries(m))
	assert.Equal(t, 3, replicaCheckSeries(m))

	m.DeleteVisibilityReplica("standby1:5432")
	assert.Equal(t, 1, replicaLagSeries(m))
	assert.Equal(t, 1, replicaCheckSeries(m), "both result series of standby1 are deleted, standby2's stays")
	m.DeleteVisibilityReplica("never-seen:5432")
	assert.Equal(t, 1, replicaLagSeries(m))
	assert.Equal(t, 1, replicaCheckSeries(m))
}

func TestStreamingSpillMetrics(t *testing.T) {
	m := NewMetric("spill_slot")
	reg := NewRegistry(m)

	m.SetStreamingActiveTransactions(3)
	m.SetStreamingBufferBytes(4096)
	m.SetStreamingSpilledBytes(8192)
	m.StreamingSpillBytesAdd(16384)
	m.StreamingSpillTransactionsIncrement()
	m.StreamingAbortedTransactionsIncrement()

	families, err := reg.Prometheus().Gather()
	require.NoError(t, err)

	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, mf := range families {
		byName[mf.GetName()] = mf
	}

	assertGauge := func(name string, want float64) {
		t.Helper()
		mf, ok := byName[name]
		require.True(t, ok, "missing metric %s", name)
		require.Len(t, mf.GetMetric(), 1)
		assert.InDelta(t, want, mf.GetMetric()[0].GetGauge().GetValue(), 0)
		assertHasSlotAndHostLabels(t, mf.GetMetric()[0], "spill_slot")
	}

	assertCounter := func(name string, want float64) {
		t.Helper()
		mf, ok := byName[name]
		require.True(t, ok, "missing metric %s", name)
		require.Len(t, mf.GetMetric(), 1)
		assert.InDelta(t, want, mf.GetMetric()[0].GetCounter().GetValue(), 0)
		assertHasSlotAndHostLabels(t, mf.GetMetric()[0], "spill_slot")
	}

	assertGauge("go_pq_cdc_streaming_active_transactions", 3)
	assertGauge("go_pq_cdc_streaming_buffer_bytes", 4096)
	assertGauge("go_pq_cdc_streaming_spilled_bytes", 8192)
	assertCounter("go_pq_cdc_streaming_spill_bytes_total", 16384)
	assertCounter("go_pq_cdc_streaming_spill_transactions_total", 1)
	assertCounter("go_pq_cdc_streaming_aborted_transactions_total", 1)
}

func assertHasSlotAndHostLabels(t *testing.T, m *dto.Metric, wantSlot string) {
	t.Helper()
	labels := make(map[string]string, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	assert.Equal(t, wantSlot, labels["slot_name"])
	_, hasHost := labels["host"]
	assert.True(t, hasHost, "expected host label")
}
