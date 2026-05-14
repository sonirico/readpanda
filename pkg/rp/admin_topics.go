package rp

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TopicInfo summarises a single topic for listing in admin tools.
type TopicInfo struct {
	Name       string
	Partitions int32
	Replicas   int32
	Internal   bool
	Messages   int64
	EndOffsets map[int32]int64
}

// ListTopics returns metadata for all topics in the cluster, sorted by name.
// Messages is the sum of end offsets across all partitions (approximate count
// when no retention/compaction has run, but the best signal kadm exposes
// without per-record scans).
func (a *Admin) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	md, err := a.cl.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kadm metadata: %w", err)
	}

	topics := md.Topics.Sorted()
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Topic)
	}

	endOffsets, err := a.cl.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("kadm end offsets: %w", err)
	}

	out := make([]TopicInfo, 0, len(topics))
	for _, t := range topics {
		info := TopicInfo{
			Name:       t.Topic,
			Partitions: int32(len(t.Partitions)),
			Internal:   t.IsInternal,
			EndOffsets: map[int32]int64{},
		}
		if len(t.Partitions) > 0 {
			info.Replicas = int32(len(t.Partitions.Sorted()[0].Replicas))
		}
		var total int64
		endOffsets.Each(func(o kadm.ListedOffset) {
			if o.Topic != t.Topic || o.Err != nil {
				return
			}
			info.EndOffsets[o.Partition] = o.Offset
			total += o.Offset
		})
		info.Messages = total
		out = append(out, info)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
