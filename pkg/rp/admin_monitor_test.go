package rp

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestAdminMonitor_SetLag(t *testing.T) {
	m := NewAdminMonitor("test_app", nil)

	m.SetLag("group-a", "topic-1", 42)
	m.SetLag("group-a", "topic-2", 100)
	m.SetLag("group-b", "topic-1", 7)

	if got := testutil.ToFloat64(m.ConsumerLagGauge("group-a", "topic-1")); got != 42 {
		t.Fatalf("group-a/topic-1 lag = %v, want 42", got)
	}
	if got := testutil.ToFloat64(m.ConsumerLagGauge("group-a", "topic-2")); got != 100 {
		t.Fatalf("group-a/topic-2 lag = %v, want 100", got)
	}
	if got := testutil.ToFloat64(m.ConsumerLagGauge("group-b", "topic-1")); got != 7 {
		t.Fatalf("group-b/topic-1 lag = %v, want 7", got)
	}
}

func TestAdminMonitor_SetLagOverwrites(t *testing.T) {
	m := NewAdminMonitor("test_app", nil)

	m.SetLag("g1", "t1", 500)
	m.SetLag("g1", "t1", 10)

	if got := testutil.ToFloat64(m.ConsumerLagGauge("g1", "t1")); got != 10 {
		t.Fatalf("lag after overwrite = %v, want 10", got)
	}
}

func TestAdminMonitor_ConstLabels(t *testing.T) {
	m := NewAdminMonitor("ceac.monitor", prometheus.Labels{"env": "test"})

	m.SetLag("g1", "t1", 1)

	ch := make(chan prometheus.Metric, 1)
	m.ConsumerLagGauge("g1", "t1").Collect(ch)
	metric := <-ch

	var d dto.Metric
	if err := metric.Write(&d); err != nil {
		t.Fatalf("metric.Write: %v", err)
	}

	labels := make(map[string]string)
	for _, lp := range d.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}

	if labels["service"] != "ceac_monitor" {
		t.Fatalf("service = %q, want %q", labels["service"], "ceac_monitor")
	}
	if labels["client"] != "admin" {
		t.Fatalf("client = %q, want %q", labels["client"], "admin")
	}
	if labels["env"] != "test" {
		t.Fatalf("env = %q, want %q", labels["env"], "test")
	}
}

func TestAdminMonitor_ResetLag(t *testing.T) {
	m := NewAdminMonitor("test_app", nil)

	m.SetLag("group-a", "topic-1", 42)
	m.SetLag("group-b", "topic-1", 7)

	if got := testutil.CollectAndCount(m); got == 0 {
		t.Fatal("expected gauges before reset, got 0")
	}

	m.ResetLag()

	if got := testutil.CollectAndCount(m); got != 0 {
		t.Fatalf("expected 0 gauges after ResetLag, got %d", got)
	}
}

func TestAdminMonitor_ImplementsCollector(t *testing.T) {
	m := NewAdminMonitor("test_app", nil)
	var _ prometheus.Collector = m
}
