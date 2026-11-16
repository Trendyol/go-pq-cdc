package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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
