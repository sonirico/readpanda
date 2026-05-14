package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/sonirico/readpanda/pkg/rp"
)

// GroupsView lists consumer groups with their total lag.
type GroupsView struct {
	app     *App
	table   table.Model
	groups  []rp.GroupInfo
	lags    map[string]int64
	filter  filter
	errored bool
}

func newGroupsView(app *App) *GroupsView {
	cols := groupsColumns(120)
	tbl := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithHeight(20),
	)
	tbl.SetStyles(tableStyles())
	return &GroupsView{app: app, table: tbl, lags: map[string]int64{}}
}

func (v *GroupsView) SetSize(w, h int) {
	if h > 2 {
		v.table.SetHeight(h - 1)
	}
	if w > 0 {
		v.table.SetWidth(w)
		v.table.SetColumns(groupsColumns(w))
	}
}

func groupsColumns(width int) []table.Column {
	const (
		stateW   = 12
		membersW = 8
		lagW     = 14
		gutter   = 2
	)
	fixed := stateW + membersW + lagW + gutter*4
	name := width - fixed
	if name < 20 {
		name = 20
	}
	return []table.Column{
		{Title: "GROUP", Width: name},
		{Title: "STATE", Width: stateW},
		{Title: "MEMBERS", Width: membersW},
		{Title: "LAG", Width: lagW},
	}
}

func (v *GroupsView) Init() tea.Cmd {
	return v.load()
}

func (v *GroupsView) Update(msg tea.Msg) tea.Cmd {
	switch m := msg.(type) {
	case groupsLoadedMsg:
		v.groups = m.groups
		v.lags = m.lags
		v.errored = false
		v.refreshRows()
		return v.tick()
	case errorMsg:
		v.errored = true
		return nil
	case tickMsg:
		if v.app.current != viewGroups || v.errored {
			return nil
		}
		return v.load()
	case tea.KeyMsg:
		if consumed, applied := v.filter.Handle(m); consumed {
			if applied {
				v.refreshRows()
			}
			return nil
		}
		if m.String() == "r" {
			v.errored = false
			return v.load()
		}
	}

	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

func (v *GroupsView) View() string {
	if len(v.groups) == 0 {
		return "\n  no consumer groups (or still loading)…\n"
	}
	if bar := v.filter.Bar(); bar != "" {
		return bar + "\n" + v.table.View()
	}
	return v.table.View()
}

func (v *GroupsView) filtered() []rp.GroupInfo {
	if v.filter.query == "" {
		return v.groups
	}
	out := make([]rp.GroupInfo, 0, len(v.groups))
	for _, g := range v.groups {
		if v.filter.Match(g.Name) {
			out = append(out, g)
		}
	}
	return out
}

func (v *GroupsView) refreshRows() {
	v.table.SetRows(groupsToRows(v.filtered(), v.lags))
}

func (v *GroupsView) load() tea.Cmd {
	admin := v.app.admin
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		groups, err := admin.ListGroups(ctx)
		if err != nil {
			return errorMsg{err: err}
		}
		lagEntries, err := admin.AllGroupLags(ctx)
		if err != nil {
			return errorMsg{err: err}
		}
		totals := make(map[string]int64, len(groups))
		for _, l := range lagEntries {
			if l.Err != nil {
				continue
			}
			totals[l.Group] += l.Lag
		}
		return groupsLoadedMsg{groups: groups, lags: totals}
	}
}

func (v *GroupsView) tick() tea.Cmd {
	return tea.Tick(5*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

func groupsToRows(groups []rp.GroupInfo, lags map[string]int64) []table.Row {
	rows := make([]table.Row, 0, len(groups))
	for _, g := range groups {
		rows = append(rows, table.Row{
			g.Name,
			g.State,
			fmt.Sprintf("%d", g.Members),
			fmt.Sprintf("%d", lags[g.Name]),
		})
	}
	return rows
}
