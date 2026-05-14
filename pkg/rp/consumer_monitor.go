package rp

import (
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerMonitor collects Prometheus metrics for a Kafka/Redpanda consumer
// by implementing franz-go kgo hook interfaces and receiving partition event
// callbacks. Opt-in: set it in ConsumerConfig.Monitor and register it with
// prometheus.MustRegister.
//
// All metrics are named redpanda_<metric> and labeled with service=<appName>
// so a single generic dashboard covers all services.
type ConsumerMonitor struct {
	batchesConsumed *prometheus.CounterVec
	recordsConsumed *prometheus.CounterVec
	bytesConsumed   *prometheus.CounterVec

	partitionsAssigned *prometheus.CounterVec
	partitionsRevoked  *prometheus.CounterVec
	partitionsLost     *prometheus.CounterVec
	rebalances         prometheus.Counter
	groupErrors        prometheus.Counter

	brokerConnects    *prometheus.CounterVec
	brokerDisconnects *prometheus.CounterVec
	brokerThrottleSec *prometheus.CounterVec
	brokerE2ELatency  *prometheus.HistogramVec
}

// NewConsumerMonitor creates a new ConsumerMonitor. appName is sanitized and
// injected as a constant "service" label on every metric. Extra labels are
// merged in (e.g. {"priority": "high"} to distinguish multiple deploys).
// Metric names follow the pattern redpanda_<metric>{service="...", ...}.
func NewConsumerMonitor(appName string, labels prometheus.Labels) *ConsumerMonitor {
	cl := mergeLabels(appName, "consumer", labels)

	return &ConsumerMonitor{
		batchesConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "batches_consumed_total",
			Help:        "Batches fetched from brokers.",
			ConstLabels: cl,
		}, []string{"topic"}),
		recordsConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "records_consumed_total",
			Help:        "Records fetched from brokers.",
			ConstLabels: cl,
		}, []string{"topic"}),
		bytesConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "bytes_consumed_total",
			Help:        "Bytes fetched (compressed and uncompressed).",
			ConstLabels: cl,
		}, []string{"topic", "kind"}),
		partitionsAssigned: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "partitions_assigned_total",
			Help:        "Partition assignment events.",
			ConstLabels: cl,
		}, []string{"topic"}),
		partitionsRevoked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "partitions_revoked_total",
			Help:        "Partition revocation events.",
			ConstLabels: cl,
		}, []string{"topic"}),
		partitionsLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "partitions_lost_total",
			Help:        "Partition lost events.",
			ConstLabels: cl,
		}, []string{"topic"}),
		rebalances: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "rebalances_total",
			Help:        "Total rebalance events (assigned + revoked + lost).",
			ConstLabels: cl,
		}),
		groupErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "redpanda", Name: "group_errors_total",
			Help:        "Consumer group management errors.",
			ConstLabels: cl,
		}),
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
func (m *ConsumerMonitor) Describe(ch chan<- *prometheus.Desc) {
	m.batchesConsumed.Describe(ch)
	m.recordsConsumed.Describe(ch)
	m.bytesConsumed.Describe(ch)
	m.partitionsAssigned.Describe(ch)
	m.partitionsRevoked.Describe(ch)
	m.partitionsLost.Describe(ch)
	m.rebalances.Describe(ch)
	m.groupErrors.Describe(ch)
	m.brokerConnects.Describe(ch)
	m.brokerDisconnects.Describe(ch)
	m.brokerThrottleSec.Describe(ch)
	m.brokerE2ELatency.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *ConsumerMonitor) Collect(ch chan<- prometheus.Metric) {
	m.batchesConsumed.Collect(ch)
	m.recordsConsumed.Collect(ch)
	m.bytesConsumed.Collect(ch)
	m.partitionsAssigned.Collect(ch)
	m.partitionsRevoked.Collect(ch)
	m.partitionsLost.Collect(ch)
	m.rebalances.Collect(ch)
	m.groupErrors.Collect(ch)
	m.brokerConnects.Collect(ch)
	m.brokerDisconnects.Collect(ch)
	m.brokerThrottleSec.Collect(ch)
	m.brokerE2ELatency.Collect(ch)
}

// kgo hook: HookFetchBatchRead
func (m *ConsumerMonitor) OnFetchBatchRead(
	_ kgo.BrokerMetadata,
	topic string,
	_ int32,
	metrics kgo.FetchBatchMetrics,
) {
	m.batchesConsumed.WithLabelValues(topic).Inc()
	m.recordsConsumed.WithLabelValues(topic).Add(float64(metrics.NumRecords))
	m.bytesConsumed.WithLabelValues(topic, "uncompressed").Add(float64(metrics.UncompressedBytes))
	m.bytesConsumed.WithLabelValues(topic, "compressed").Add(float64(metrics.CompressedBytes))
}

// kgo hook: HookGroupManageError
func (m *ConsumerMonitor) OnGroupManageError(_ error) {
	m.groupErrors.Inc()
}

// kgo hook: HookBrokerConnect
func (m *ConsumerMonitor) OnBrokerConnect(
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
func (m *ConsumerMonitor) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	m.brokerDisconnects.WithLabelValues(brokerLabel(meta)).Inc()
}

// kgo hook: HookBrokerThrottle
func (m *ConsumerMonitor) OnBrokerThrottle(
	meta kgo.BrokerMetadata,
	throttleInterval time.Duration,
	_ bool,
) {
	m.brokerThrottleSec.WithLabelValues(brokerLabel(meta)).Add(throttleInterval.Seconds())
}

// kgo hook: HookBrokerE2E
func (m *ConsumerMonitor) OnBrokerE2E(meta kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	m.brokerE2ELatency.WithLabelValues(brokerLabel(meta)).Observe(e2e.DurationE2E().Seconds())
}

// OnAssigned is called from the OnPartitionsAssigned callback in NewConsumer.
func (m *ConsumerMonitor) OnAssigned(assigned map[string][]int32) {
	m.rebalances.Inc()
	for topic, partitions := range assigned {
		m.partitionsAssigned.WithLabelValues(topic).Add(float64(len(partitions)))
	}
}

// OnRevoked is called from the OnPartitionsRevoked callback in NewConsumer.
func (m *ConsumerMonitor) OnRevoked(revoked map[string][]int32) {
	m.rebalances.Inc()
	for topic, partitions := range revoked {
		m.partitionsRevoked.WithLabelValues(topic).Add(float64(len(partitions)))
	}
}

// OnLost is called from the OnPartitionsLost callback in NewConsumer.
func (m *ConsumerMonitor) OnLost(lost map[string][]int32) {
	m.rebalances.Inc()
	for topic, partitions := range lost {
		m.partitionsLost.WithLabelValues(topic).Add(float64(len(partitions)))
	}
}
