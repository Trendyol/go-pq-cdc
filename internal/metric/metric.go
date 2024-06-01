package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

const cdcNamespace = "go_pq_cdc"

type Metric interface {
	InsertOpIncrement(count int64)
	UpdateOpIncrement(count int64)
	DeleteOpIncrement(count int64)
	SetCDCLatency(latency int64)
	SetProcessLatency(latency int64)

	PrometheusCollectors() []prometheus.Collector
}

type metric struct {
	totalInsert prometheus.Counter
	totalUpdate prometheus.Counter
	totalDelete prometheus.Counter

	cdcLatency     prometheus.Gauge
	processLatency prometheus.Gauge
}

func NewMetric() Metric {
	return &metric{
		totalInsert: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "insert",
			Name:      "total",
			Help:      "total number of insert operation message in cdc",
		}),
		totalUpdate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "update",
			Name:      "total",
			Help:      "total number of update operation message in cdc",
		}),
		totalDelete: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cdcNamespace,
			Subsystem: "delete",
			Name:      "total",
			Help:      "total number of delete operation message in cdc",
		}),
		cdcLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: "cdc_latency",
			Name:      "current",
			Help:      "latest consumed cdc message latency ms",
		}),
		processLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cdcNamespace,
			Subsystem: "process_latency",
			Name:      "current",
			Help:      "latest cdc process latency",
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
