package rp

import (
	"context"
	"fmt"
	"sort"
)

// BrokerInfo summarises a broker as the TUI / CLI wants to see it: id,
// host:port, rack, and whether it is the active controller for the cluster.
type BrokerInfo struct {
	NodeID       int32
	Host         string
	Port         int32
	Rack         string
	IsController bool
	LogDirSize   int64 // total bytes across this broker's log dirs; 0 if unavailable
}

// ListBrokers returns every broker the cluster currently exposes, sorted by
// node id. The active controller is flagged via IsController. LogDirSize is
// populated when DescribeLogDirs is available to the calling principal; if it
// isn't, the field stays at zero and the rest of the data is still returned.
func (a *Admin) ListBrokers(ctx context.Context) ([]BrokerInfo, error) {
	md, err := a.cl.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kadm metadata: %w", err)
	}

	out := make([]BrokerInfo, 0, len(md.Brokers))
	for _, b := range md.Brokers {
		info := BrokerInfo{
			NodeID:       b.NodeID,
			Host:         b.Host,
			Port:         b.Port,
			IsController: b.NodeID == md.Controller,
		}
		if b.Rack != nil {
			info.Rack = *b.Rack
		}
		out = append(out, info)
	}

	if logDirs, err := a.cl.DescribeAllLogDirs(ctx, nil); err == nil {
		for i, b := range out {
			var total int64
			brokerDirs, ok := logDirs[b.NodeID]
			if !ok {
				continue
			}
			for _, dir := range brokerDirs {
				if dir.Err != nil {
					continue
				}
				for _, topic := range dir.Topics {
					for _, p := range topic {
						total += p.Size
					}
				}
			}
			out[i].LogDirSize = total
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}
