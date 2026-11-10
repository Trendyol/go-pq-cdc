package cdc

import (
	"github.com/prometheus/client_golang/prometheus"
)

// OnlineHealth separates the running lifecycle from capture and snapshot
// notifications. CaptureReady does not certify sink freshness or durability.
type OnlineHealth struct {
	Running          bool `json:"running"`
	CaptureReady     bool `json:"capture_ready"`
	SnapshotComplete bool `json:"snapshot_complete"`
}

func channelSignaled(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

// Health is safe to inspect before, during, and after Run. A host application's
// readiness endpoint must require Running as well as its chosen readiness flag.
func (c *OnlineConnector) Health() OnlineHealth {
	c.mu.Lock()
	defer c.mu.Unlock()
	return OnlineHealth{
		Running:          c.started && !channelSignaled(c.done),
		CaptureReady:     channelSignaled(c.capture),
		SnapshotComplete: channelSignaled(c.snapshot),
	}
}

type onlineMetric struct {
	name, help string
	kind       prometheus.ValueType
}

type onlineCollector struct {
	connector   *OnlineConnector
	descriptors []*prometheus.Desc
}

var onlineMetricDefinitions = []onlineMetric{
	{"running", "Whether the online connector Run lifecycle is active.", prometheus.GaugeValue},
	{"capture_ready", "Whether capture readiness was observed in this instance.", prometheus.GaugeValue},
	{"snapshot_complete", "Whether snapshot completion was observed in this instance.", prometheus.GaugeValue},
	{"read_queries_total", "Snapshot read queries observed in this instance.", prometheus.CounterValue},
	{"peak_chunk_raw_bytes", "Peak retained raw snapshot chunk bytes, not process RSS.", prometheus.GaugeValue},
	{"peak_read_latency_seconds", "Peak snapshot query duration in seconds.", prometheus.GaugeValue},
	{"peak_transaction_encoded_bytes", "Peak encoded transaction bytes in this instance.", prometheus.GaugeValue},
	{"peak_queue_and_assembly_encoded_bytes", "Peak encoded queue and transaction assembly bytes.", prometheus.GaugeValue},
	{"peak_relation_encoded_bytes", "Peak encoded relation-cache bytes.", prometheus.GaugeValue},
	{"buffered_encoded_bytes", "Current queued and visibility-gating encoded envelope bytes, not process RSS.", prometheus.GaugeValue},
	{"feedback_writes_total", "Replication feedback writes reported by this instance.", prometheus.CounterValue},
	{"last_feedback_timestamp_seconds", "Last recorded feedback timestamp, or zero before feedback.", prometheus.GaugeValue},
}

// MetricsCollector exposes the existing bounded Stats and Health snapshots.
// Register it once in the host application's Prometheus registry. No HTTP
// listener or database connection is created. Counters reset with each instance.
func (c *OnlineConnector) MetricsCollector() prometheus.Collector {
	collector := &onlineCollector{connector: c}
	for _, metric := range onlineMetricDefinitions {
		collector.descriptors = append(collector.descriptors, prometheus.NewDesc(
			"go_pq_cdc_online_"+metric.name, metric.help, nil, prometheus.Labels{"slot_name": c.cfg.Slot.Name}))
	}
	return collector
}

func (c *onlineCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, descriptor := range c.descriptors {
		ch <- descriptor
	}
}

func booleanMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (c *onlineCollector) Collect(ch chan<- prometheus.Metric) {
	stats, health := c.connector.Stats(), c.connector.Health()
	feedbackTimestamp := float64(0)
	if !stats.LastFeedback.IsZero() {
		feedbackTimestamp = float64(stats.LastFeedback.UnixNano()) / 1e9
	}
	values := []float64{
		booleanMetric(health.Running), booleanMetric(health.CaptureReady), booleanMetric(health.SnapshotComplete),
		float64(stats.ReadQueries), float64(stats.PeakChunkRawBytes), stats.PeakReadLatency.Seconds(),
		float64(stats.PeakTransactionEncodedBytes), float64(stats.PeakQueueAndAssemblyEncodedBytes),
		float64(stats.PeakRelationEncodedBytes), float64(stats.BufferedEncodedBytes), float64(stats.FeedbackWrites), feedbackTimestamp,
	}
	for i, value := range values {
		ch <- prometheus.MustNewConstMetric(c.descriptors[i], onlineMetricDefinitions[i].kind, value)
	}
}
