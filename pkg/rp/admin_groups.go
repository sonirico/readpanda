package rp

import (
	"context"
	"fmt"
	"sort"
)

// GroupInfo summarises a single consumer group for listing.
type GroupInfo struct {
	Name         string
	State        string
	ProtocolType string
	Members      int
}

// ListGroups returns all consumer groups visible to the admin client, sorted
// by name. State and member count come from the DescribeGroups call so callers
// don't have to issue a second round-trip.
func (a *Admin) ListGroups(ctx context.Context) ([]GroupInfo, error) {
	listed, err := a.cl.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("kadm list groups: %w", err)
	}
	names := listed.Groups()
	if len(names) == 0 {
		return nil, nil
	}

	described, err := a.cl.DescribeGroups(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("kadm describe groups: %w", err)
	}

	out := make([]GroupInfo, 0, len(names))
	for _, g := range described.Sorted() {
		info := GroupInfo{
			Name:         g.Group,
			State:        g.State,
			ProtocolType: g.ProtocolType,
			Members:      len(g.Members),
		}
		out = append(out, info)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// AllGroupLags fetches lag for every group visible to the admin client.
func (a *Admin) AllGroupLags(ctx context.Context) ([]GroupTopicLag, error) {
	listed, err := a.cl.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("kadm list groups: %w", err)
	}
	names := listed.Groups()
	if len(names) == 0 {
		return nil, nil
	}
	return a.Lag(ctx, names...)
}
