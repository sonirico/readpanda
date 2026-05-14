package rp

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errForTest = errors.New("test error")

func TestConsumerMonitor_FetchBatchRead(t *testing.T) {
	m := NewConsumerMonitor("test_app", nil)

	meta := kgo.BrokerMetadata{Host: "b1", Port: 9092}
	m.OnFetchBatchRead(meta, "events", 0, kgo.FetchBatchMetrics{
		NumRecords:        25,
		UncompressedBytes: 8000,
		CompressedBytes:   3000,
	})

	if got := counterVecValue(m.batchesConsumed, "events"); got != 1 {
		t.Fatalf("batchesConsumed = %v, want 1", got)
	}
	if got := counterVecValue(m.recordsConsumed, "events"); got != 25 {
		t.Fatalf("recordsConsumed = %v, want 25", got)
	}
	if got := counterVecValue(m.bytesConsumed, "events", "uncompressed"); got != 8000 {
		t.Fatalf("bytes uncompressed = %v, want 8000", got)
	}
	if got := counterVecValue(m.bytesConsumed, "events", "compressed"); got != 3000 {
		t.Fatalf("bytes compressed = %v, want 3000", got)
	}
}

func TestConsumerMonitor_PartitionEvents(t *testing.T) {
	m := NewConsumerMonitor("test_app", nil)

	m.OnAssigned(map[string][]int32{"t1": {0, 1, 2}, "t2": {0}})
	m.OnRevoked(map[string][]int32{"t1": {2}})
	m.OnLost(map[string][]int32{"t1": {0}})

	if got := counterVecValue(m.partitionsAssigned, "t1"); got != 3 {
		t.Fatalf("assigned t1 = %v, want 3", got)
	}
	if got := counterVecValue(m.partitionsAssigned, "t2"); got != 1 {
		t.Fatalf("assigned t2 = %v, want 1", got)
	}
	if got := counterVecValue(m.partitionsRevoked, "t1"); got != 1 {
		t.Fatalf("revoked t1 = %v, want 1", got)
	}
	if got := counterVecValue(m.partitionsLost, "t1"); got != 1 {
		t.Fatalf("lost t1 = %v, want 1", got)
	}
	if got := counterValue(m.rebalances); got != 3 {
		t.Fatalf("rebalances = %v, want 3", got)
	}
}

func TestConsumerMonitor_GroupError(t *testing.T) {
	m := NewConsumerMonitor("test_app", nil)

	m.OnGroupManageError(errForTest)
	m.OnGroupManageError(errForTest)

	if got := counterValue(m.groupErrors); got != 2 {
		t.Fatalf("groupErrors = %v, want 2", got)
	}
}

func TestConsumerMonitor_BrokerConnect(t *testing.T) {
	m := NewConsumerMonitor("test_app", nil)

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

func TestConsumerMonitor_ImplementsCollector(t *testing.T) {
	m := NewConsumerMonitor("test_app", nil)
	var _ prometheus.Collector = m
}
