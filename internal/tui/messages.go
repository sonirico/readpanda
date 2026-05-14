package tui

import (
	"github.com/sonirico/readpanda/internal/profile"
	"github.com/sonirico/readpanda/pkg/rp"
)

// Internal tea.Msg types. Each type is small and lives here so views can match
// on them without importing each other.

type topicsLoadedMsg struct {
	topics []rp.TopicInfo
}

type groupsLoadedMsg struct {
	groups []rp.GroupInfo
	lags   map[string]int64
}

type topicDetailLoadedMsg struct {
	detail rp.TopicDetail
}

type topicGroupsLoadedMsg struct {
	groups []rp.TopicGroupLag
}

type topicACLsLoadedMsg struct {
	acls []rp.TopicACL
	err  error
}

type brokersLoadedMsg struct {
	brokers []rp.BrokerInfo
}

type tailRecordMsg struct {
	msg rp.Msg
}

type tailErrorMsg struct {
	err error
}

type errorMsg struct {
	err error
}

type profileSwitchedMsg struct {
	profile profile.Profile
}

type switchViewMsg struct {
	view viewID
}
