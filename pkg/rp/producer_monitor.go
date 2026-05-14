package rp

import (
	"fmt"
	"maps"
	"net"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SanitizeMetricName replaces characters that are invalid in Prometheus metric
// names (dots, hyphens, etc.) with underscores. Service names like
// "ceac.advstats-consumer-backfill" become "ceac_advstats_consumer_backfill".
func SanitizeMetricName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, s)
}

// mergeLabels builds ConstLabels from a sanitized appName ("service" key),
// a client role ("producer" or "consumer"), and any extra caller labels.
func mergeLabels(appName, client string, extra prometheus.Labels) prometheus.Labels {
	m := prometheus.Labels{"service": SanitizeMetricName(appName), "client": client}
	maps.Copy(m, extra)
	return m
}

// ProducerMonitor collects Prometheus metrics for a Kafka/Redpanda producer
// by implementing franz-go kgo hook interfaces. Opt-in: set it in
// ProducerConfig.Monitor and register it with prometheus.MustRegister.
//
// All metrics are named redpanda_<metric> and labeled with service=<appName>
// so a single generic dashboard covers all services.
type ProducerMonitor struct {
	recordsBuffered   prometheus.Counter
	recordsUnbuffered *prometheus.CounterVec
	batchesProduced   *prometheus.CounterVec
	recordsProduced   *prometheus.CounterVec
	bytesProduced     *prometheus.CounterVec

	brokerConnects    *prometheus.CounterVec
	brokerDisconnects *prometheus.CounterVec
	brokerThrottleSec *prometheus.CounterVec
	brokerE2ELatency  *prometheus.HistogramVec
}

// NewProducerMonitor creates a new ProducerMonitor. appName is sanitized and
// injected as a constant "service" label on every metric. Extra labels are
// merged in (e.g. {"priority": "high"} to distinguish multiple deploys).
// Metric names follow the pattern redpanda_<metric>{service="...", ...}.
func NewProducerMonitor(appName string, labels prometheus.Labels) *ProducerMonitor {
	cl := mergeLabels(appName, "producer", labels)

	return &ProducerMonitor{
		recordsBuffered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "records_buffered_total",
			Help:        "Records accepted by Produce (buffered internally).",
			ConstLabels: cl,
		}),
		recordsUnbuffered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "records_unbuffered_total",
			Help:        "Records promised back (success or error).",
			ConstLabels: cl,
		}, []string{"topic", "status"}),
		batchesProduced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "batches_produced_total",
			Help:        "Batches successfully written to brokers.",
			ConstLabels: cl,
		}, []string{"topic"}),
		recordsProduced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "records_produced_total",
			Help:        "Records successfully produced.",
			ConstLabels: cl,
		}, []string{"topic"}),
		bytesProduced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "bytes_produced_total",
			Help:        "Bytes produced (compressed and uncompressed).",
			ConstLabels: cl,
		}, []string{"topic", "kind"}),
		brokerConnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "broker_connects_total",
			Help:        "Broker connection attempts.",
			ConstLabels: cl,
		}, []string{"broker", "status"}),
		brokerDisconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "broker_disconnects_total",
			Help:        "Broker disconnections.",
			ConstLabels: cl,
		}, []string{"broker"}),
		brokerThrottleSec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "broker_throttle_seconds_total",
			Help:        "Cumulative broker throttle time.",
			ConstLabels: cl,
		}, []string{"broker"}),
		brokerE2ELatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "redpanda", Name: "broker_e2e_latency_seconds",
			Help:        "Broker request/response round-trip latency.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: cl,
		}, []string{"broker"}),
	}
}

// Describe implements prometheus.Collector.
func (m *ProducerMonitor) Describe(ch chan<- *prometheus.Desc) {
	m.recordsBuffered.Describe(ch)
	m.recordsUnbuffered.Describe(ch)
	m.batchesProduced.Describe(ch)
	m.recordsProduced.Describe(ch)
	m.bytesProduced.Describe(ch)
	m.brokerConnects.Describe(ch)
	m.brokerDisconnects.Describe(ch)
	m.brokerThrottleSec.Describe(ch)
	m.brokerE2ELatency.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *ProducerMonitor) Collect(ch chan<- prometheus.Metric) {
	m.recordsBuffered.Collect(ch)
	m.recordsUnbuffered.Collect(ch)
	m.batchesProduced.Collect(ch)
	m.recordsProduced.Collect(ch)
	m.bytesProduced.Collect(ch)
	m.brokerConnects.Collect(ch)
	m.brokerDisconnects.Collect(ch)
	m.brokerThrottleSec.Collect(ch)
	m.brokerE2ELatency.Collect(ch)
}

func brokerLabel(meta kgo.BrokerMetadata) string {
	return fmt.Sprintf("%s:%d", meta.Host, meta.Port)
}

// kgo hook: HookProduceRecordBuffered
func (m *ProducerMonitor) OnProduceRecordBuffered(_ *kgo.Record) {
	m.recordsBuffered.Inc()
}

// kgo hook: HookProduceRecordUnbuffered
func (m *ProducerMonitor) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	m.recordsUnbuffered.WithLabelValues(r.Topic, status).Inc()
}

// kgo hook: HookProduceBatchWritten
func (m *ProducerMonitor) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	topic string,
	_ int32,
	metrics kgo.ProduceBatchMetrics,
) {
	m.batchesProduced.WithLabelValues(topic).Inc()
	m.recordsProduced.WithLabelValues(topic).Add(float64(metrics.NumRecords))
	m.bytesProduced.WithLabelValues(topic, "uncompressed").Add(float64(metrics.UncompressedBytes))
	m.bytesProduced.WithLabelValues(topic, "compressed").Add(float64(metrics.CompressedBytes))
}

// kgo hook: HookBrokerConnect
func (m *ProducerMonitor) OnBrokerConnect(
	meta kgo.BrokerMetadata,
	_ time.Duration,
	_ net.Conn,
	err error,
) {
	status := "success"
	if err != nil {
		status = "error"
	}
	m.brokerConnects.WithLabelValues(brokerLabel(meta), status).Inc()
}

// kgo hook: HookBrokerDisconnect
func (m *ProducerMonitor) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	m.brokerDisconnects.WithLabelValues(brokerLabel(meta)).Inc()
}

// kgo hook: HookBrokerThrottle
func (m *ProducerMonitor) OnBrokerThrottle(
	meta kgo.BrokerMetadata,
	throttleInterval time.Duration,
	_ bool,
) {
	m.brokerThrottleSec.WithLabelValues(brokerLabel(meta)).Add(throttleInterval.Seconds())
}

// kgo hook: HookBrokerE2E
func (m *ProducerMonitor) OnBrokerE2E(meta kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	m.brokerE2ELatency.WithLabelValues(brokerLabel(meta)).Observe(e2e.DurationE2E().Seconds())
}
