package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/sonirico/readpanda/pkg/rp"
)

// BrokersView lists every broker in the cluster: id, host:port, rack, the
// active controller flag and total log-dir size when DescribeLogDirs is
// available. Refreshes on the standard 5 s tick.
type BrokersView struct {
	app     *App
	table   table.Model
	brokers []rp.BrokerInfo
	errored bool
}

func newBrokersView(app *App) *BrokersView {
	tbl := table.New(
		table.WithColumns(brokersColumns(120)),
		table.WithFocused(true),
		table.WithHeight(20),
	)
	tbl.SetStyles(tableStyles())
	return &BrokersView{app: app, table: tbl}
}

func (v *BrokersView) SetSize(w, h int) {
	if h > 2 {
		v.table.SetHeight(h - 1)
	}
	if w > 0 {
		v.table.SetWidth(w)
		v.table.SetColumns(brokersColumns(w))
	}
}

func brokersColumns(width int) []table.Column {
	const (
		idW    = 6
		rackW  = 12
		ctrlW  = 11
		sizeW  = 14
		gutter = 2
	)
	fixed := idW + rackW + ctrlW + sizeW + gutter*5
	host := width - fixed
	if host < 20 {
		host = 20
	}
	return []table.Column{
		{Title: "ID", Width: idW},
		{Title: "HOST", Width: host},
		{Title: "RACK", Width: rackW},
		{Title: "CONTROLLER", Width: ctrlW},
		{Title: "LOG SIZE", Width: sizeW},
	}
}

func (v *BrokersView) Init() tea.Cmd {
	return tea.Batch(v.load(), v.tick())
}

func (v *BrokersView) Update(msg tea.Msg) tea.Cmd {
	switch m := msg.(type) {
	case brokersLoadedMsg:
		v.brokers = m.brokers
		v.errored = false
		v.table.SetRows(brokersToRows(m.brokers))
		return v.tick()
	case errorMsg:
		v.errored = true
		return nil
	case tickMsg:
		if v.app.current != viewBrokers || v.errored {
			return nil
		}
		return tea.Batch(v.load(), v.tick())
	case tea.KeyMsg:
		if m.String() == "r" {
			v.errored = false
			return v.load()
		}
	}
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

func (v *BrokersView) View() string {
	if len(v.brokers) == 0 {
		return "\n  loading brokers…\n"
	}
	return v.table.View()
}

func (v *BrokersView) load() tea.Cmd {
	admin := v.app.admin
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		bs, err := admin.ListBrokers(ctx)
		if err != nil {
			return errorMsg{err: err}
		}
		return brokersLoadedMsg{brokers: bs}
	}
}

func (v *BrokersView) tick() tea.Cmd {
	return tea.Tick(5*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

func brokersToRows(bs []rp.BrokerInfo) []table.Row {
	rows := make([]table.Row, 0, len(bs))
	for _, b := range bs {
		controller := ""
		if b.IsController {
			controller = "✓"
		}
		size := "—"
		if b.LogDirSize > 0 {
			size = humanBytes(b.LogDirSize)
		}
		rack := b.Rack
		if rack == "" {
			rack = "—"
		}
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", b.NodeID),
			fmt.Sprintf("%s:%d", b.Host, b.Port),
			rack,
			controller,
			size,
		})
	}
	return rows
}
