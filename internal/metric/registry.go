package metric

import (
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry interface {
	AddMetricCollectors(metricCollectors ...prometheus.Collector)
	Prometheus() *prometheus.Registry
}

type prometheusRegistry struct {
	registry *prometheus.Registry
}

func NewRegistry(m Metric) Registry {
	return NewRegistryWithCollectors(m.PrometheusCollectors()...)
}

// NewRegistryWithCollectors shares runtime collection without adding metrics
// from a backend that is not running.
func NewRegistryWithCollectors(metricCollectors ...prometheus.Collector) Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewBuildInfoCollector())
	r.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/.*")}),
	))
	r.MustRegister(metricCollectors...)

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
