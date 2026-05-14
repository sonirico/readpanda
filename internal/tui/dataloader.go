package tui

import (
	"context"

	"github.com/sonirico/readpanda/pkg/rp"
)

// dataLoader is the consumer-side interface the TUI uses to talk to a cluster.
// It is deliberately small and unexported: only the methods the views actually
// need. The real implementation is *rp.Admin.
type dataLoader interface {
	ListTopics(ctx context.Context) ([]rp.TopicInfo, error)
	ListGroups(ctx context.Context) ([]rp.GroupInfo, error)
	AllGroupLags(ctx context.Context) ([]rp.GroupTopicLag, error)
	Lag(ctx context.Context, groups ...string) ([]rp.GroupTopicLag, error)
	DescribeTopic(ctx context.Context, name string) (rp.TopicDetail, error)
	GroupsForTopic(ctx context.Context, topic string) ([]rp.TopicGroupLag, error)
	TopicACLs(ctx context.Context, topic string) ([]rp.TopicACL, error)
	ListBrokers(ctx context.Context) ([]rp.BrokerInfo, error)
	Close()
}
