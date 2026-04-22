package metric

import (
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry manages Prometheus metric collector registration.
type Registry interface {
	AddMetricCollectors(metricCollectors ...prometheus.Collector)
	Prometheus() *prometheus.Registry
}

type prometheusRegistry struct {
	registry *prometheus.Registry
}

// NewRegistry creates a new Prometheus registry with default and CDC metric collectors.
func NewRegistry(m Metric) Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewBuildInfoCollector())
	r.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/.*")}),
	))
	r.MustRegister(m.PrometheusCollectors()...)

	return &prometheusRegistry{
		registry: r,
	}
}

func (r *prometheusRegistry) AddMetricCollectors(metricCollectors ...prometheus.Collector) {
	r.registry.MustRegister(metricCollectors...)
}

func (r *prometheusRegistry) Prometheus() *prometheus.Registry {
	return r.registry
}
