package rp

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicDetail is the full picture of a single topic, mirroring what the
// Redpanda Web UI shows on the topic page: size on disk, estimated message
// count, cleanup policy, retention, partition layout and the full config map.
type TopicDetail struct {
	Name              string
	Internal          bool
	Partitions        []TopicPartition
	ReplicationFactor int32
	Configs           []TopicConfig

	// EstimatedMessages = sum(endOffset - startOffset) across partitions. It
	// is an approximation: it overcounts when tombstones/compaction have run
	// and ignores transactional aborts.
	EstimatedMessages int64

	// SizeBytes is the logical on-disk size — the leader replica's bytes,
	// summed across partitions. This matches what the Redpanda Cloud Web UI
	// reports for the topic. Use SizeBytesAllReplicas when you need the raw
	// cluster disk usage (×RF).
	SizeBytes int64

	// SizeBytesAllReplicas sums the size of every replica on every broker.
	// Useful for capacity planning at the cluster level. Zero when the
	// broker doesn't expose DescribeLogDirs to the calling principal.
	SizeBytesAllReplicas int64

	// CleanupPolicy / RetentionMs / RetentionBytes are extracted from Configs
	// for convenience — the same values are in Configs too.
	CleanupPolicy  string
	RetentionMs    int64 // -1 means infinite
	RetentionBytes int64 // -1 means infinite
}

// TopicPartition summarises one partition: leader broker, replicas and ISR.
type TopicPartition struct {
	ID          int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
	StartOffset int64
	EndOffset   int64
	SizeBytes   int64
}

// TopicConfig is one config entry as returned by DescribeTopicConfigs.
type TopicConfig struct {
	Key       string
	Value     string
	Source    string
	IsDefault bool
	Sensitive bool
}

// DescribeTopic fetches metadata, offsets, configs and log-dir sizes for a
// single topic and assembles them into a TopicDetail. Partial failures
// (e.g. log dirs not available) degrade gracefully: the corresponding fields
// stay zero rather than aborting the whole call.
func (a *Admin) DescribeTopic(ctx context.Context, name string) (TopicDetail, error) {
	md, err := a.cl.Metadata(ctx, name)
	if err != nil {
		return TopicDetail{}, fmt.Errorf("kadm metadata: %w", err)
	}
	topic, ok := md.Topics[name]
	if !ok {
		return TopicDetail{}, fmt.Errorf("topic %q not found", name)
	}

	detail := TopicDetail{
		Name:     topic.Topic,
		Internal: topic.IsInternal,
	}
	if topic.Err != nil {
		return detail, fmt.Errorf("topic metadata: %w", topic.Err)
	}

	parts := topic.Partitions.Sorted()
	if len(parts) > 0 {
		detail.ReplicationFactor = int32(len(parts[0].Replicas))
	}

	startOffsets, _ := a.cl.ListStartOffsets(ctx, name)
	endOffsets, _ := a.cl.ListEndOffsets(ctx, name)

	partitions := make([]TopicPartition, 0, len(parts))
	for _, p := range parts {
		tp := TopicPartition{
			ID:       p.Partition,
			Leader:   p.Leader,
			Replicas: append([]int32(nil), p.Replicas...),
			ISR:      append([]int32(nil), p.ISR...),
		}
		if o, ok := startOffsets.Lookup(name, p.Partition); ok && o.Err == nil {
			tp.StartOffset = o.Offset
		}
		if o, ok := endOffsets.Lookup(name, p.Partition); ok && o.Err == nil {
			tp.EndOffset = o.Offset
		}
		if tp.EndOffset >= tp.StartOffset {
			detail.EstimatedMessages += tp.EndOffset - tp.StartOffset
		}
		partitions = append(partitions, tp)
	}

	logDirs, err := a.cl.DescribeAllLogDirs(ctx, nil)
	if err == nil {
		allSizes := topicPartitionSizes(logDirs, name)
		leaderSizes := leaderPartitionSizes(logDirs, name, partitions)
		for i := range partitions {
			leaderSize := leaderSizes[partitions[i].ID]
			partitions[i].SizeBytes = leaderSize
			detail.SizeBytes += leaderSize
			detail.SizeBytesAllReplicas += allSizes[partitions[i].ID]
		}
	}
	detail.Partitions = partitions

	cfgs, err := a.cl.DescribeTopicConfigs(ctx, name)
	if err == nil {
		for _, c := range cfgs {
			if c.Err != nil {
				continue
			}
			for _, cfg := range c.Configs {
				value := ""
				if cfg.Value != nil {
					value = *cfg.Value
				}
				entry := TopicConfig{
					Key:       cfg.Key,
					Value:     value,
					Source:    cfg.Source.String(),
					IsDefault: cfg.Source == kmsg.ConfigSourceDefaultConfig,
					Sensitive: cfg.Sensitive,
				}
				detail.Configs = append(detail.Configs, entry)
				switch cfg.Key {
				case "cleanup.policy":
					detail.CleanupPolicy = value
				case "retention.ms":
					detail.RetentionMs, _ = strconv.ParseInt(value, 10, 64)
				case "retention.bytes":
					detail.RetentionBytes, _ = strconv.ParseInt(value, 10, 64)
				}
			}
		}
		sort.Slice(detail.Configs, func(i, j int) bool {
			return detail.Configs[i].Key < detail.Configs[j].Key
		})
	}

	return detail, nil
}

// topicPartitionSizes sums, per partition, the bytes reported by every broker
// (across all log dirs). Counts every replica — useful for raw disk usage.
func topicPartitionSizes(logDirs kadm.DescribedAllLogDirs, topic string) map[int32]int64 {
	out := map[int32]int64{}
	for _, broker := range logDirs {
		for _, dir := range broker {
			if dir.Err != nil {
				continue
			}
			topics, ok := dir.Topics[topic]
			if !ok {
				continue
			}
			for _, p := range topics {
				out[p.Partition] += p.Size
			}
		}
	}
	return out
}

// leaderPartitionSizes returns the per-partition size as reported by each
// partition's leader broker — the "logical" size, ignoring replicas. Matches
// the number the Redpanda Cloud Web UI surfaces.
func leaderPartitionSizes(
	logDirs kadm.DescribedAllLogDirs, topic string, partitions []TopicPartition,
) map[int32]int64 {
	out := map[int32]int64{}
	for _, p := range partitions {
		brokerDirs, ok := logDirs[p.Leader]
		if !ok {
			continue
		}
		for _, dir := range brokerDirs {
			if dir.Err != nil {
				continue
			}
			topics, ok := dir.Topics[topic]
			if !ok {
				continue
			}
			for _, lp := range topics {
				if lp.Partition == p.ID {
					out[p.ID] += lp.Size
				}
			}
		}
	}
	return out
}
