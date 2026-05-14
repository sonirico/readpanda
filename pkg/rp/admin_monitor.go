package rp

import "github.com/prometheus/client_golang/prometheus"

// AdminMonitor collects Prometheus metrics for administrative Redpanda/Kafka
// operations (e.g. consumer group lag). Metric naming follows the same
// redpanda_<metric>{service="...", client="admin"} convention used by
// ConsumerMonitor and ProducerMonitor.
type AdminMonitor struct {
	consumerLag *prometheus.GaugeVec
}

// NewAdminMonitor creates an AdminMonitor. appName is sanitized and injected
// as a constant "service" label. Extra labels are merged in.
func NewAdminMonitor(appName string, labels prometheus.Labels) *AdminMonitor {
	cl := mergeLabels(appName, "admin", labels)

	return &AdminMonitor{
		consumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "redpanda",
			Name:        "consumer_lag",
			Help:        "Total consumer group lag (sum of all partitions) per group and topic.",
			ConstLabels: cl,
		}, []string{"consumer_group", "topic"}),
	}
}

// Describe implements prometheus.Collector.
func (m *AdminMonitor) Describe(ch chan<- *prometheus.Desc) {
	m.consumerLag.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *AdminMonitor) Collect(ch chan<- prometheus.Metric) {
	m.consumerLag.Collect(ch)
}

// ResetLag removes all consumer lag gauge series. Call before re-setting
// current values so that groups which stop reporting are not left stale.
func (m *AdminMonitor) ResetLag() {
	m.consumerLag.Reset()
}

// SetLag sets the consumer lag gauge for a given consumer group and topic.
func (m *AdminMonitor) SetLag(group, topic string, lag int64) {
	m.consumerLag.WithLabelValues(group, topic).Set(float64(lag))
}

// ConsumerLagGauge returns the gauge for a consumer group and topic.
// Intended for use in tests with prometheus/testutil.
func (m *AdminMonitor) ConsumerLagGauge(group, topic string) prometheus.Gauge {
	return m.consumerLag.WithLabelValues(group, topic)
}
