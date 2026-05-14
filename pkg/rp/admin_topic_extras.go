package rp

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TopicGroupLag pairs a consumer group with its total lag against one topic.
// It is the data structure powering the per-topic "consumers" view.
type TopicGroupLag struct {
	Group string
	Lag   int64
	Err   error
}

// GroupsForTopic lists every consumer group with committed offsets on the
// given topic, summed across partitions. Returned slice is sorted by lag
// descending (busiest groups first).
func (a *Admin) GroupsForTopic(ctx context.Context, topic string) ([]TopicGroupLag, error) {
	lags, err := a.AllGroupLags(ctx)
	if err != nil {
		return nil, err
	}
	byGroup := map[string]*TopicGroupLag{}
	for _, l := range lags {
		if l.Topic != topic && l.Err == nil {
			continue
		}
		entry, ok := byGroup[l.Group]
		if !ok {
			entry = &TopicGroupLag{Group: l.Group}
			byGroup[l.Group] = entry
		}
		if l.Err != nil && entry.Err == nil {
			entry.Err = l.Err
			continue
		}
		entry.Lag += l.Lag
	}

	out := make([]TopicGroupLag, 0, len(byGroup))
	for _, v := range byGroup {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Lag > out[j].Lag })
	return out, nil
}

// TopicACL is one ACL entry on a topic resource.
type TopicACL struct {
	Principal  string
	Host       string
	Operation  string
	Permission string
	Pattern    string
}

// TopicACLs describes ACLs scoped to one topic. Empty slice + nil error means
// the broker returned no rules. Errors from the underlying DescribeACLs call
// are returned verbatim — Redpanda Cloud may not expose ACL APIs to every
// principal.
func (a *Admin) TopicACLs(ctx context.Context, topic string) ([]TopicACL, error) {
	b := kadm.NewACLs().
		Topics(topic).
		Operations().
		ResourcePatternType(kadm.ACLPatternAny).
		Allow().AllowHosts().
		Deny().DenyHosts()
	results, err := a.cl.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("kadm describe acls: %w", err)
	}
	var out []TopicACL
	for _, r := range results {
		if r.Err != nil {
			return nil, fmt.Errorf("acl filter: %w", r.Err)
		}
		for _, d := range r.Described {
			out = append(out, TopicACL{
				Principal:  d.Principal,
				Host:       d.Host,
				Operation:  d.Operation.String(),
				Permission: d.Permission.String(),
				Pattern:    d.Pattern.String(),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Principal != out[j].Principal {
			return out[i].Principal < out[j].Principal
		}
		return out[i].Operation < out[j].Operation
	})
	return out, nil
}
