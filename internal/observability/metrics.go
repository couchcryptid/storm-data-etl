package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus counters, histograms, and gauges for the ETL pipeline.
type Metrics struct {
	MessagesConsumed   prometheus.Counter
	MessagesProduced   prometheus.Counter
	TransformErrors    prometheus.Counter
	ProcessingDuration prometheus.Histogram
	PipelineRunning    prometheus.Gauge
}

// NewMetrics creates and registers all pipeline metrics with the default Prometheus registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		MessagesConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "etl",
			Name:      "messages_consumed_total",
			Help:      "Total messages read from the source topic.",
		}),
		MessagesProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "etl",
			Name:      "messages_produced_total",
			Help:      "Total messages written to the sink topic.",
		}),
		TransformErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "etl",
			Name:      "transform_errors_total",
			Help:      "Total transformation failures.",
		}),
		ProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "etl",
			Name:      "processing_duration_seconds",
			Help:      "Duration of a single extract-transform-load cycle.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),
		PipelineRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "etl",
			Name:      "pipeline_running",
			Help:      "1 when the pipeline is active, 0 when shut down.",
		}),
	}

	prometheus.MustRegister(
		m.MessagesConsumed,
		m.MessagesProduced,
		m.TransformErrors,
		m.ProcessingDuration,
		m.PipelineRunning,
	)

	return m
}

// NewMetricsForTesting creates Metrics with a fresh registry to avoid
// "already registered" panics when called from multiple tests.
func NewMetricsForTesting() *Metrics {
	return &Metrics{
		MessagesConsumed:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "etl", Name: "messages_consumed_total"}),
		MessagesProduced:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "etl", Name: "messages_produced_total"}),
		TransformErrors:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: "etl", Name: "transform_errors_total"}),
		ProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "etl", Name: "processing_duration_seconds"}),
		PipelineRunning:    prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "etl", Name: "pipeline_running"}),
	}
}
