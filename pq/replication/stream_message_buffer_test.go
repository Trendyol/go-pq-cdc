package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const messageBufferLengthMetricName = "go_pq_cdc_message_buffer_length"

// messageBufferLengthGauge reads the gauge through the registry, the same path
// a Prometheus scrape takes.
func messageBufferLengthGauge(t *testing.T, m metric.Metric) int {
	t.Helper()
	families, err := metric.NewRegistry(m).Prometheus().Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == messageBufferLengthMetricName {
			require.Len(t, family.GetMetric(), 1)
			return int(family.GetMetric()[0].GetGauge().GetValue())
		}
	}
	require.FailNow(t, "message buffer gauge is not exported")
	return 0
}

// The gauge reads the channel at scrape time, so it follows the fill level up
// to capacity and drops to 0 after the consumer drains it, with no send in
// between to refresh it. The sink is not running here: the test fills the
// channel directly, as a parked sink would.
func TestMessageBufferLengthTracksChannelAtScrape(t *testing.T) {
	m := metric.NewMetric("test_slot")
	s := NewStream("", config.Config{}, m, func(*ListenerContext) {}).(*stream)

	assert.Equal(t, 0, messageBufferLengthGauge(t, m), "empty channel before any message")

	for range 3 {
		s.messageCH <- &Message{}
	}
	assert.Equal(t, 3, messageBufferLengthGauge(t, m), "steady flow")

	for range metric.MessageBufferCapacity - 3 {
		s.messageCH <- &Message{}
	}
	assert.Equal(t, metric.MessageBufferCapacity, messageBufferLengthGauge(t, m),
		"channel full, sink parked on send")

	for range metric.MessageBufferCapacity {
		<-s.messageCH
	}
	assert.Equal(t, 0, messageBufferLengthGauge(t, m), "channel drained, sink idle")
}
