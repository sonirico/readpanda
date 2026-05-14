package rp

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func counterValue(c prometheus.Collector) float64 {
	ch := make(chan prometheus.Metric, 1)
	c.Collect(ch)
	m := <-ch
	var d dto.Metric
	_ = m.Write(&d)
	return d.GetCounter().GetValue()
}

func counterVecValue(cv *prometheus.CounterVec, labels ...string) float64 {
	return counterValue(cv.WithLabelValues(labels...))
}

func TestProducerMonitor_RecordBuffered(t *testing.T) {
	m := NewProducerMonitor("test_app", nil)

	m.OnProduceRecordBuffered(&kgo.Record{Topic: "t1"})
	m.OnProduceRecordBuffered(&kgo.Record{Topic: "t2"})

	if got := counterValue(m.recordsBuffered); got != 2 {
		t.Fatalf("recordsBuffered = %v, want 2", got)
	}
}

func TestProducerMonitor_RecordUnbuffered(t *testing.T) {
	m := NewProducerMonitor("test_app", nil)

	m.OnProduceRecordUnbuffered(&kgo.Record{Topic: "t1"}, nil)
	m.OnProduceRecordUnbuffered(&kgo.Record{Topic: "t1"}, nil)
	m.OnProduceRecordUnbuffered(&kgo.Record{Topic: "t1"}, errForTest)

	if got := counterVecValue(m.recordsUnbuffered, "t1", "success"); got != 2 {
		t.Fatalf("success = %v, want 2", got)
	}
	if got := counterVecValue(m.recordsUnbuffered, "t1", "error"); got != 1 {
		t.Fatalf("error = %v, want 1", got)
	}
}

func TestProducerMonitor_BatchWritten(t *testing.T) {
	m := NewProducerMonitor("test_app", nil)

	meta := kgo.BrokerMetadata{Host: "broker1", Port: 9092}
	m.OnProduceBatchWritten(meta, "orders", 0, kgo.ProduceBatchMetrics{
		NumRecords:        10,
		UncompressedBytes: 5000,
		CompressedBytes:   2000,
	})

	if got := counterVecValue(m.batchesProduced, "orders"); got != 1 {
		t.Fatalf("batchesProduced = %v, want 1", got)
	}
	if got := counterVecValue(m.recordsProduced, "orders"); got != 10 {
		t.Fatalf("recordsProduced = %v, want 10", got)
	}
	if got := counterVecValue(m.bytesProduced, "orders", "uncompressed"); got != 5000 {
		t.Fatalf("bytes uncompressed = %v, want 5000", got)
	}
	if got := counterVecValue(m.bytesProduced, "orders", "compressed"); got != 2000 {
		t.Fatalf("bytes compressed = %v, want 2000", got)
	}
}

func TestProducerMonitor_BrokerConnect(t *testing.T) {
	m := NewProducerMonitor("test_app", nil)

	meta := kgo.BrokerMetadata{Host: "host1", Port: 9092}
	m.OnBrokerConnect(meta, 0, nil, nil)
	m.OnBrokerConnect(meta, 0, nil, errForTest)

	if got := counterVecValue(m.brokerConnects, "host1:9092", "success"); got != 1 {
		t.Fatalf("success = %v, want 1", got)
	}
	if got := counterVecValue(m.brokerConnects, "host1:9092", "error"); got != 1 {
		t.Fatalf("error = %v, want 1", got)
	}
}

func TestProducerMonitor_ImplementsCollector(t *testing.T) {
	m := NewProducerMonitor("test_app", nil)
	var _ prometheus.Collector = m
}

// TestDualMonitorRegistration ensures ProducerMonitor and ConsumerMonitor can
// coexist in the same registry without descriptor collisions (the "client"
// label differentiates overlapping broker metrics).
func TestDualMonitorRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"priority": "high"}
	prod := NewProducerMonitor("ceac.advstats-consumer", labels)
	cons := NewConsumerMonitor("ceac.advstats-consumer", labels)

	if err := reg.Register(prod); err != nil {
		t.Fatalf("register producer: %v", err)
	}
	if err := reg.Register(cons); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected at least one metric family")
	}
}

func TestProducerMonitor_ExtraLabels(t *testing.T) {
	m := NewProducerMonitor("my_svc", prometheus.Labels{"priority": "high", "region": "us"})

	m.OnProduceRecordBuffered(&kgo.Record{Topic: "t1"})

	ch := make(chan prometheus.Metric, 1)
	m.recordsBuffered.Collect(ch)
	metric := <-ch

	var d dto.Metric
	if err := metric.Write(&d); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	labels := map[string]string{}
	for _, lp := range d.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	if labels["service"] != "my_svc" {
		t.Errorf("service = %q, want my_svc", labels["service"])
	}
	if labels["priority"] != "high" {
		t.Errorf("priority = %q, want high", labels["priority"])
	}
	if labels["region"] != "us" {
		t.Errorf("region = %q, want us", labels["region"])
	}
}

func TestSanitizeMetricName(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"advstats_consumer", "advstats_consumer"},
		{"ceac.advstats-consumer-backfill", "ceac_advstats_consumer_backfill"},
		{"my.service.v2", "my_service_v2"},
		{"already_clean_123", "already_clean_123"},
	}
	for _, tt := range tests {
		if got := SanitizeMetricName(tt.in); got != tt.want {
			t.Errorf("SanitizeMetricName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestProducerMonitor_ServiceLabel(t *testing.T) {
	m := NewProducerMonitor("ceac.advstats-consumer-backfill", nil)

	// Collect a metric and verify the service label is present and sanitized.
	m.OnProduceRecordBuffered(&kgo.Record{Topic: "t1"})

	ch := make(chan prometheus.Metric, 1)
	m.recordsBuffered.Collect(ch)
	metric := <-ch

	var d dto.Metric
	if err := metric.Write(&d); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	var found bool
	for _, lp := range d.GetLabel() {
		if lp.GetName() == "service" {
			found = true
			if lp.GetValue() != "ceac_advstats_consumer_backfill" {
				t.Errorf("service label = %q, want ceac_advstats_consumer_backfill", lp.GetValue())
			}
		}
	}
	if !found {
		t.Error("service label not found on metric")
	}

	// Verify fqName starts with redpanda_ and has no app name baked in.
	descCh := make(chan *prometheus.Desc, 20)
	m.Describe(descCh)
	close(descCh)
	for desc := range descCh {
		s := desc.String()
		start := strings.Index(s, `"`)
		end := strings.Index(s[start+1:], `"`) + start + 1
		fqName := s[start+1 : end]
		if !strings.HasPrefix(fqName, "redpanda_") {
			t.Errorf("fqName should start with redpanda_: %s", fqName)
		}
		if strings.Contains(fqName, "ceac") {
			t.Errorf("fqName should not contain app name: %s", fqName)
		}
	}
}
