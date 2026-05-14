package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
)

// TopicsView lists topics as a collapsible tree keyed on the
// dot-separated naming convention (e.g. chesscom.stats.v1.foo). Leaves are
// individual topics; branches aggregate descendant counts.
type TopicsView struct {
	app     *App
	table   table.Model
	tree    *topicTree
	rows    []*topicNode // current visible flatten
	filter  filter
	errored bool
}

func newTopicsView(app *App) *TopicsView {
	cols := topicsColumns(120)
	tbl := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithHeight(20),
	)
	tbl.SetStyles(tableStyles())
	return &TopicsView{app: app, table: tbl, tree: newTopicTree(nil)}
}

// SetSize resizes the underlying table to fill the available area and grows
// the TOPIC column to consume any width left after the fixed-size columns.
func (v *TopicsView) SetSize(w, h int) {
	if h > 2 {
		v.table.SetHeight(h - 1)
	}
	if w > 0 {
		v.table.SetWidth(w)
		v.table.SetColumns(topicsColumns(w))
	}
}

func topicsColumns(width int) []table.Column {
	const (
		partsW    = 6
		replW     = 5
		messagesW = 14
		internalW = 8
		gutter    = 2
	)
	fixed := partsW + replW + messagesW + internalW + gutter*5
	name := width - fixed
	if name < 20 {
		name = 20
	}
	return []table.Column{
		{Title: "TOPIC", Width: name},
		{Title: "PARTS", Width: partsW},
		{Title: "REPL", Width: replW},
		{Title: "MESSAGES", Width: messagesW},
		{Title: "INTERNAL", Width: internalW},
	}
}

func (v *TopicsView) Init() tea.Cmd {
	return v.load()
}

func (v *TopicsView) Update(msg tea.Msg) tea.Cmd {
	switch m := msg.(type) {
	case topicsLoadedMsg:
		// Preserve expand state on refresh.
		prevExpanded := v.tree.expanded
		v.tree = newTopicTree(m.topics)
		for path, on := range prevExpanded {
			v.tree.expanded[path] = on
		}
		v.errored = false
		v.refreshRows()
		return v.tick()
	case errorMsg:
		v.errored = true
		return nil
	case tickMsg:
		if v.app.current != viewTopics || v.errored {
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
		switch m.String() {
		case "r":
			v.errored = false
			return v.load()
		case "o":
			v.tree.ExpandAll()
			v.refreshRows()
			return tea.ClearScreen
		case "O":
			v.tree.CollapseAll()
			v.refreshRows()
			return tea.ClearScreen
		case "enter":
			if n, ok := v.selected(); ok {
				if n.isLeaf {
					return func() tea.Msg {
						v.app.topicDetail.target = n.info.Name
						return switchViewMsg{view: viewTopicDetail}
					}
				}
				v.tree.Toggle(n.fullPath)
				v.refreshRows()
				return tea.ClearScreen
			}
		}
	}

	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

func (v *TopicsView) View() string {
	if v.tree == nil || len(v.rows) == 0 {
		if v.filter.query != "" {
			return "\n  no topics match \"" + v.filter.query + "\"\n"
		}
		return "\n  loading topics…\n"
	}
	if bar := v.filter.Bar(); bar != "" {
		return bar + "\n" + v.table.View()
	}
	return v.table.View()
}

func (v *TopicsView) selected() (*topicNode, bool) {
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(v.rows) {
		return nil, false
	}
	return v.rows[idx], true
}

func (v *TopicsView) refreshRows() {
	v.rows = v.tree.VisibleRows(v.filter.query)
	v.table.SetRows(treeRows(v.rows, v.tree))
}

func (v *TopicsView) load() tea.Cmd {
	admin := v.app.admin
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		topics, err := admin.ListTopics(ctx)
		if err != nil {
			return errorMsg{err: err}
		}
		return topicsLoadedMsg{topics: topics}
	}
}

func (v *TopicsView) tick() tea.Cmd {
	return tea.Tick(5*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

type tickMsg struct{}

// treeRows converts the flat list of visible nodes into bubbles/table rows,
// indenting the TOPIC column by depth and marking branches with ▸ / ▾.
func treeRows(nodes []*topicNode, t *topicTree) []table.Row {
	rows := make([]table.Row, 0, len(nodes))
	for _, n := range nodes {
		label := strings.Repeat("  ", n.depth-1)
		switch {
		case n.isLeaf:
			label += "• " + n.name
		case t.IsExpanded(n.fullPath):
			label += "▾ " + n.name
		default:
			label += "▸ " + n.name
		}
		if n.isLeaf {
			internal := ""
			if n.info.Internal {
				internal = "✓"
			}
			rows = append(rows, table.Row{
				label,
				fmt.Sprintf("%d", n.info.Partitions),
				fmt.Sprintf("%d", n.info.Replicas),
				fmt.Sprintf("%d", n.info.Messages),
				internal,
			})
			continue
		}
		rows = append(rows, table.Row{
			label,
			"",
			"",
			fmt.Sprintf("%d (%d)", n.messagesSum, n.leafCount),
			"",
		})
	}
	return rows
}
