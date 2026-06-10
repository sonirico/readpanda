package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sonirico/readpanda/pkg/rp"
)

// detailTab identifies which sub-section of the topic detail is currently
// rendered. Each tab has its own scroll position.
type detailTab int

const (
	tabMessages detailTab = iota
	tabConsumers
	tabPartitions
	tabConfiguration
	tabACL
)

const detailTabCount = 5

// TopicDetailView renders the full topic view: a one-line summary header and
// five tabs (Messages / Consumers / Partitions / Configuration / ACL).
// Messages embeds the live tail consumer; Consumers shows lag per group; ACL
// lists access control entries.
type TopicDetailView struct {
	app      *App
	viewport viewport.Model
	tail     *TailView
	target   string

	detail       rp.TopicDetail
	detailLoaded bool

	groups       []rp.TopicGroupLag
	groupsLoaded bool

	acls       []rp.TopicACL
	aclsLoaded bool
	aclErr     error

	err error

	tab     detailTab
	offsets [detailTabCount]int
	w, h    int
}

func newTopicDetailView(app *App) *TopicDetailView {
	vp := viewport.New(80, 20)
	return &TopicDetailView{
		app:      app,
		viewport: vp,
		tail:     newTailView(app),
	}
}

func (v *TopicDetailView) Title() string {
	if v.target == "" {
		return "Topic"
	}
	return v.target
}

func (v *TopicDetailView) SetSize(w, h int) {
	v.w, v.h = w, h
	if w > 0 {
		v.viewport.Width = w
	}
	// Reserve rows for chrome: tabs (1) + rule (1) + summary (1) + rule (1) = 4.
	// Title is now in the panel border.
	body := h - 4
	if body < 3 {
		body = 3
	}
	v.viewport.Height = body
	v.tail.SetSize(w, body)
	v.refresh()
}

// Init resets per-topic state and eagerly kicks off every async load in
// parallel so switching tabs feels instantaneous afterwards.
func (v *TopicDetailView) Init() tea.Cmd {
	v.detail = rp.TopicDetail{}
	v.detailLoaded = false
	v.groups = nil
	v.groupsLoaded = false
	v.acls = nil
	v.aclsLoaded = false
	v.aclErr = nil
	v.err = nil
	v.tab = tabMessages
	v.offsets = [detailTabCount]int{}
	v.tail.target = v.target
	v.refresh()
	return tea.Batch(
		v.loadDetail(),
		v.loadGroups(),
		v.loadACLs(),
		v.tail.Init(),
	)
}

func (v *TopicDetailView) Update(msg tea.Msg) tea.Cmd {
	// Tail-specific messages must always reach the tail, even when the user
	// is looking at another tab — otherwise the consumer's pump loop stalls
	// and we lose records.
	switch m := msg.(type) {
	case tailRecordMsg, tailErrorMsg:
		return v.tail.Update(m)
	case topicDetailLoadedMsg:
		v.detail = m.detail
		v.detailLoaded = true
		v.err = nil
		v.refresh()
		return nil
	case topicGroupsLoadedMsg:
		v.groups = m.groups
		v.groupsLoaded = true
		v.refresh()
		return nil
	case topicACLsLoadedMsg:
		v.acls = m.acls
		v.aclErr = m.err
		v.aclsLoaded = true
		v.refresh()
		return nil
	case errorMsg:
		v.err = m.err
		v.refresh()
		return nil
	case tea.KeyMsg:
		switch m.String() {
		case "r":
			return v.reloadCurrentTab()
		case "1":
			return v.setTab(tabMessages)
		case "2":
			return v.setTab(tabConsumers)
		case "3":
			return v.setTab(tabPartitions)
		case "4":
			return v.setTab(tabConfiguration)
		case "5":
			return v.setTab(tabACL)
		case "tab", "right", "l":
			return v.setTab(detailTab((int(v.tab) + 1) % detailTabCount))
		case "shift+tab", "left", "h":
			return v.setTab(detailTab((int(v.tab) + detailTabCount - 1) % detailTabCount))
		}
		if v.tab == tabMessages {
			return v.tail.Update(msg)
		}
	}
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

func (v *TopicDetailView) View() string {
	width := v.w
	if width < 1 {
		width = 1
	}
	rule := renderRule(width)
	tabLabels := []string{"1 Messages", "2 Consumers", "3 Partitions", "4 Configuration", "5 ACL"}
	out := renderTabBar(int(v.tab), tabLabels) + "\n" +
		rule + "\n" +
		v.renderSummaryLine() + "\n" +
		rule + "\n"
	if v.tab == tabMessages {
		out += v.tail.View()
	} else {
		out += v.viewport.View()
	}
	return out + "\x1b[0m"
}

// Stop is called by the App when the user leaves the detail view so the
// embedded tail consumer doesn't keep polling in the background.
func (v *TopicDetailView) Stop() {
	v.tail.stop()
}

// renderSummaryLine is the compact summary the user used to see in tab 1.
// Kept always-visible at the top so size / retention / partitions are one
// glance away regardless of the active tab.
func (v *TopicDetailView) renderSummaryLine() string {
	if !v.detailLoaded {
		return lipgloss.NewStyle().Foreground(colorMuted).Render("  loading…")
	}
	d := v.detail
	muted := lipgloss.NewStyle().Foreground(colorMuted)
	sizeStr := fmt.Sprintf("size=%s", humanBytes(d.SizeBytes))
	if d.SizeBytesAllReplicas > 0 && d.SizeBytesAllReplicas != d.SizeBytes {
		sizeStr += fmt.Sprintf(" (%s w/replicas)", humanBytes(d.SizeBytesAllReplicas))
	}
	parts := []string{
		sizeStr,
		fmt.Sprintf("msgs≈%d", d.EstimatedMessages),
		fmt.Sprintf("parts=%d×%d", len(d.Partitions), d.ReplicationFactor),
	}
	if d.CleanupPolicy != "" {
		parts = append(parts, "cleanup="+d.CleanupPolicy)
	}
	parts = append(parts, "retention="+humanRetentionMs(d.RetentionMs))
	return muted.Render("  " + strings.Join(parts, "  "))
}

func (v *TopicDetailView) setTab(t detailTab) tea.Cmd {
	if t == v.tab {
		return nil
	}
	v.offsets[v.tab] = v.viewport.YOffset
	v.tab = t
	v.refresh()
	v.viewport.SetYOffset(v.offsets[v.tab])
	// Force a full repaint. Bubbletea's frame differ otherwise leaves
	// background-coloured cells (e.g. the histogram bars) untouched when the
	// new tab's content at the same row is shorter than the old.
	return tea.ClearScreen
}

func (v *TopicDetailView) reloadCurrentTab() tea.Cmd {
	switch v.tab {
	case tabMessages:
		v.tail.stop()
		return v.tail.Init()
	case tabConsumers:
		v.groupsLoaded = false
		return v.loadGroups()
	case tabPartitions, tabConfiguration:
		v.detailLoaded = false
		return v.loadDetail()
	case tabACL:
		v.aclsLoaded = false
		return v.loadACLs()
	}
	return nil
}

func (v *TopicDetailView) loadDetail() tea.Cmd {
	admin := v.app.admin
	name := v.target
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		d, err := admin.DescribeTopic(ctx, name)
		if err != nil {
			return errorMsg{err: fmt.Errorf("describe %s: %w", name, err)}
		}
		return topicDetailLoadedMsg{detail: d}
	}
}

func (v *TopicDetailView) loadGroups() tea.Cmd {
	admin := v.app.admin
	name := v.target
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		gs, err := admin.GroupsForTopic(ctx, name)
		if err != nil {
			return errorMsg{err: fmt.Errorf("groups for %s: %w", name, err)}
		}
		return topicGroupsLoadedMsg{groups: gs}
	}
}

func (v *TopicDetailView) loadACLs() tea.Cmd {
	admin := v.app.admin
	name := v.target
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		acls, err := admin.TopicACLs(ctx, name)
		// ACL endpoints can return errors that we want to surface inside
		// the tab body instead of as a fatal error banner — many cloud
		// principals lack ACL describe permissions.
		return topicACLsLoadedMsg{acls: acls, err: err}
	}
}

func (v *TopicDetailView) refresh() {
	if v.target == "" {
		v.viewport.SetContent("\n  no topic selected\n")
		return
	}
	if v.err != nil {
		v.viewport.SetContent("\n  error: " + v.err.Error() + "\n")
		return
	}
	switch v.tab {
	case tabMessages:
		// Tail renders itself in View(); nothing to push into the viewport.
		return
	case tabConsumers:
		v.viewport.SetContent(v.renderConsumers())
	case tabPartitions:
		if !v.detailLoaded {
			v.viewport.SetContent("\n  loading partitions…\n")
			return
		}
		v.viewport.SetContent(renderPartitions(v.detail, v.viewport.Width))
	case tabConfiguration:
		if !v.detailLoaded {
			v.viewport.SetContent("\n  loading configuration…\n")
			return
		}
		v.viewport.SetContent(renderConfigs(v.detail, v.viewport.Width))
	case tabACL:
		v.viewport.SetContent(v.renderACLs())
	}
}

func (v *TopicDetailView) renderConsumers() string {
	muted := lipgloss.NewStyle().Foreground(colorMuted)
	if !v.groupsLoaded {
		return "\n  loading consumer groups…\n"
	}
	if len(v.groups) == 0 {
		return "\n  " + muted.Render("no consumer groups currently committing to this topic") + "\n"
	}
	var b strings.Builder
	b.WriteString(muted.Render(fmt.Sprintf("  %-50s %-15s\n", "GROUP", "LAG")))
	for _, g := range v.groups {
		lag := fmt.Sprintf("%d", g.Lag)
		if g.Err != nil {
			lag = "(err: " + g.Err.Error() + ")"
		}
		b.WriteString(fmt.Sprintf("  %-50s %s\n", g.Group, lag))
	}
	return b.String()
}

func (v *TopicDetailView) renderACLs() string {
	muted := lipgloss.NewStyle().Foreground(colorMuted)
	if !v.aclsLoaded {
		return "\n  loading ACLs…\n"
	}
	if v.aclErr != nil {
		return "\n  " + muted.Render("ACL query failed: "+v.aclErr.Error()) + "\n"
	}
	if len(v.acls) == 0 {
		return "\n  " + muted.Render("no ACL rules set on this topic") + "\n"
	}
	var b strings.Builder
	b.WriteString(muted.Render(
		fmt.Sprintf("  %-35s %-15s %-12s %-10s %s\n",
			"PRINCIPAL", "OPERATION", "PERMISSION", "PATTERN", "HOST"),
	))
	for _, a := range v.acls {
		b.WriteString(fmt.Sprintf("  %-35s %-15s %-12s %-10s %s\n",
			a.Principal, a.Operation, a.Permission, a.Pattern, a.Host))
	}
	return b.String()
}

func renderPartitions(d rp.TopicDetail, width int) string {
	muted := lipgloss.NewStyle().Foreground(colorMuted)
	var b strings.Builder

	header := fmt.Sprintf("  %-5s %-7s %-19s %-19s %-13s %-13s %-12s",
		"ID", "LEADER", "REPLICAS", "ISR", "START", "END", "SIZE")
	b.WriteString(muted.Render(header) + "\n")
	for _, p := range d.Partitions {
		isr := intsToString(p.ISR)
		marker := ""
		if len(p.ISR) < len(p.Replicas) {
			marker = lipgloss.NewStyle().
				Foreground(colorErr).
				Render(" ⚠ under-replicated")
		}
		b.WriteString(fmt.Sprintf("  %-5d %-7d %-19s %-19s %-13d %-13d %-12s%s\n",
			p.ID, p.Leader, intsToString(p.Replicas), isr,
			p.StartOffset, p.EndOffset, humanBytes(p.SizeBytes), marker,
		))
	}

	if dist := leaderHistogram(d.Partitions); dist != "" {
		b.WriteString("\n  " + muted.Render("Leader distribution:") + "\n")
		b.WriteString(dist)
	}
	_ = width
	return b.String()
}

func renderConfigs(d rp.TopicDetail, width int) string {
	muted := lipgloss.NewStyle().Foreground(colorMuted)
	var b strings.Builder
	writeRow := func(c rp.TopicConfig) {
		val := c.Value
		if c.Sensitive {
			val = "***"
		}
		marker := ""
		if c.IsDefault {
			marker = muted.Render("  [default]")
		}
		b.WriteString("  " + muted.Render(padRight(c.Key+":", 40)) + val + marker + "\n")
	}
	for _, c := range d.Configs {
		if !c.IsDefault {
			writeRow(c)
		}
	}
	for _, c := range d.Configs {
		if c.IsDefault {
			writeRow(c)
		}
	}
	_ = width
	return b.String()
}

func humanBytes(n int64) string {
	if n <= 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffixes[exp])
}

func humanRetentionMs(ms int64) string {
	switch {
	case ms < 0:
		return "infinite"
	case ms == 0:
		return "—"
	}
	d := time.Duration(ms) * time.Millisecond
	if d >= 24*time.Hour {
		return fmt.Sprintf("~%.1f days", d.Hours()/24)
	}
	return d.String()
}

func intsToString(xs []int32) string {
	if len(xs) == 0 {
		return "—"
	}
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = fmt.Sprintf("%d", x)
	}
	return strings.Join(parts, ",")
}

// leaderHistogram renders a vertical text bar chart of partitions per leader.
// Each row is "  brokerN  ████  count", with bar length scaled to the most
// loaded broker. Helps spot hotspots and bad balance at a glance.
func leaderHistogram(ps []rp.TopicPartition) string {
	type kv struct {
		broker int32
		n      int
	}
	counts := map[int32]int{}
	for _, p := range ps {
		counts[p.Leader]++
	}
	if len(counts) == 0 {
		return ""
	}
	pairs := make([]kv, 0, len(counts))
	maxN := 0
	for b, n := range counts {
		pairs = append(pairs, kv{b, n})
		if n > maxN {
			maxN = n
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].broker < pairs[j].broker })

	const maxBar = 30
	bar := lipgloss.NewStyle().Foreground(colorPrimary)
	muted := lipgloss.NewStyle().Foreground(colorMuted)

	var b strings.Builder
	for _, p := range pairs {
		width := 1
		if maxN > 0 {
			width = (p.n * maxBar) / maxN
			if width == 0 {
				width = 1
			}
		}
		b.WriteString("    ")
		b.WriteString(muted.Render(padRight(fmt.Sprintf("broker%d", p.broker), 10)))
		b.WriteString(bar.Render(strings.Repeat("█", width)))
		b.WriteString(strings.Repeat(" ", maxBar-width+2))
		b.WriteString(fmt.Sprintf("%d\n", p.n))
	}
	return b.String()
}
