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

// A standby that left the resolved set must not keep exporting its last lag.
func TestDeleteVisibilityReplicaLag(t *testing.T) {
	m := NewMetric("test_slot").(*metric)
	m.SetVisibilityReplicaLag("standby1:5432", 16)
	m.SetVisibilityReplicaLag("standby2:5432", 0)
	assert.Equal(t, 2, replicaLagSeries(m))

	m.DeleteVisibilityReplicaLag("standby1:5432")
	assert.Equal(t, 1, replicaLagSeries(m))
	m.DeleteVisibilityReplicaLag("never-seen:5432")
	assert.Equal(t, 1, replicaLagSeries(m))
}
