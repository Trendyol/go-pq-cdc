package metric

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	cdcNamespace             = "go_pq_cdc"
	replicationSlotSubsystem = "replication_slot"
)

type Metric interface {
	InsertOpIncrement(count int64)
	UpdateOpIncrement(count int64)
	DeleteOpIncrement(count int64)
	SetCDCLatency(latency int64)
	SetProcessLatency(latency int64)
	SetSlotActivity(active bool)
	SetSlotCurrentLSN(lsn float64)
	SetSlotConfirmedFlushLSN(lsn float64)
	SetSlotRetainedWALSize(lsn float64)
	SetSlotLag(lsn float64)

	PrometheusCollectors() []prometheus.Collector
}

type metric struct {
	totalInsert prometheus.Counter
	totalUpdate prometheus.Counter
	totalDelete prometheus.Counter

	cdcLatency            prometheus.Gauge
	processLatency        prometheus.Gauge
	slotActivity          prometheus.Gauge
	slotConfirmedFlushLSN prometheus.Gauge
	slotCurrentLSN        prometheus.Gauge
	slotRetainedWALSize   prometheus.Gauge
	slotLag               prometheus.Gauge
}

//nolint:funlen
func NewMetric(slotName string) Metric {
	hostname, _ := os.Hostname()
	return &metric{
		totalInsert: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "insert",
			Name:      "total",
			Help:      "total number of insert operation message in cdc",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		totalUpdate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "update",
			Name:      "total",
			Help:      "total number of update operation message in cdc",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		totalDelete: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "delete",
			Name:      "total",
			Help:      "total number of delete operation message in cdc",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		cdcLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: "cdc_latency",
			Name:      "current",
			Help:      "latest consumed cdc message latency ms",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		processLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: "process_latency",
			Name:      "current",
			Help:      "latest cdc process latency",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		slotActivity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: replicationSlotSubsystem,
			Name:      "slot_is_active",
			Help:      "whether the replication slot is active or not",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		slotConfirmedFlushLSN: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: replicationSlotSubsystem,
			Name:      "slot_confirmed_flush_lsn",
			Help:      "last lsn confirmed flushed to the replication slot",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		slotCurrentLSN: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: replicationSlotSubsystem,
			Name:      "slot_current_lsn",
			Help:      "current lsn",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		slotRetainedWALSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: replicationSlotSubsystem,
			Name:      "slot_retained_wal_size",
			Help:      "current lsn - restart lsn",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
		slotLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: replicationSlotSubsystem,
			Name:      "slot_lag",
			Help:      "current lsn - confirmed flush lsn",
			ConstLabels: prometheus.Labels{
				"slot_name": slotName,
				"host":      hostname,
			},
		}),
	}
}

func (m *metric) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.totalInsert,
		m.totalUpdate,
		m.totalDelete,
		m.cdcLatency,
		m.processLatency,
		m.slotActivity,
		m.slotCurrentLSN,
		m.slotConfirmedFlushLSN,
		m.slotRetainedWALSize,
		m.slotLag,
	}
}

func (m *metric) InsertOpIncrement(count int64) {
	m.totalInsert.Add(float64(count))
}

func (m *metric) UpdateOpIncrement(count int64) {
	m.totalUpdate.Add(float64(count))
}

func (m *metric) DeleteOpIncrement(count int64) {
	m.totalDelete.Add(float64(count))
}

func (m *metric) SetCDCLatency(latency int64) {
	m.cdcLatency.Set(float64(latency))
}

func (m *metric) SetProcessLatency(latency int64) {
	m.processLatency.Set(float64(latency))
}

func (m *metric) SetSlotActivity(active bool) {
	slotActivity := 0.0
	if active {
		slotActivity = 1.0
	}

	m.slotActivity.Set(slotActivity)
}

func (m *metric) SetSlotCurrentLSN(lsn float64) {
	m.slotCurrentLSN.Set(float64(lsn))
}

func (m *metric) SetSlotConfirmedFlushLSN(lsn float64) {
	m.slotConfirmedFlushLSN.Set(float64(lsn))
}

func (m *metric) SetSlotRetainedWALSize(lsn float64) {
	m.slotRetainedWALSize.Set(float64(lsn))
}

func (m *metric) SetSlotLag(lsn float64) {
	m.slotLag.Set(float64(lsn))
}
