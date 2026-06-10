package tui

import (
	"fmt"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/sonirico/readpanda/internal/profile"
)

// ContextsView lets the user switch the active rpk profile, k9s-style.
type ContextsView struct {
	app   *App
	table table.Model
}

func newContextsView(app *App) *ContextsView {
	cols := contextsColumns(120)
	tbl := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithHeight(15),
	)
	tbl.SetStyles(tableStyles())
	return &ContextsView{app: app, table: tbl}
}

func (v *ContextsView) SetSize(w, h int) {
	if h > 2 {
		v.table.SetHeight(h - 1)
	}
	if w > 0 {
		v.table.SetWidth(w)
		v.table.SetColumns(contextsColumns(w))
	}
}

func contextsColumns(width int) []table.Column {
	const (
		activeW = 7
		gutter  = 2
	)
	remaining := width - activeW - gutter*3
	if remaining < 40 {
		remaining = 40
	}
	// Split remaining between PROFILE and BROKERS (40/60 split favours brokers,
	// which tend to be long FQDNs).
	profile := remaining * 2 / 5
	if profile < 20 {
		profile = 20
	}
	brokers := remaining - profile
	if brokers < 20 {
		brokers = 20
	}
	return []table.Column{
		{Title: "ACTIVE", Width: activeW},
		{Title: "PROFILE", Width: profile},
		{Title: "BROKERS", Width: brokers},
	}
}

func (v *ContextsView) Title() string {
	f := v.app.cfg.ProfileFile
	if f != nil && len(f.Profiles) > 0 {
		return fmt.Sprintf("Contexts (%d)", len(f.Profiles))
	}
	return "Contexts"
}

func (v *ContextsView) Init() tea.Cmd {
	v.refresh()
	return nil
}

func (v *ContextsView) Update(msg tea.Msg) tea.Cmd {
	if m, ok := msg.(tea.KeyMsg); ok {
		if m.String() == "enter" {
			if p, ok := v.selected(); ok {
				return func() tea.Msg { return profileSwitchedMsg{profile: p} }
			}
		}
	}
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

func (v *ContextsView) View() string {
	if v.app.cfg.ProfileFile == nil || len(v.app.cfg.ProfileFile.Profiles) == 0 {
		return "\n  no rpk profiles found (~/.config/rpk/rpk.yaml)\n"
	}
	return v.table.View()
}

func (v *ContextsView) selected() (profile.Profile, bool) {
	f := v.app.cfg.ProfileFile
	if f == nil {
		return profile.Profile{}, false
	}
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(f.Profiles) {
		return profile.Profile{}, false
	}
	return f.Profiles[idx], true
}

func (v *ContextsView) refresh() {
	f := v.app.cfg.ProfileFile
	if f == nil {
		return
	}
	rows := make([]table.Row, 0, len(f.Profiles))
	for _, p := range f.Profiles {
		active := ""
		if p.Name == v.app.prof.Name {
			active = "*"
		}
		brokers := ""
		if len(p.Brokers) > 0 {
			brokers = p.Brokers[0]
			if len(p.Brokers) > 1 {
				brokers += fmt.Sprintf(" (+%d)", len(p.Brokers)-1)
			}
		}
		rows = append(rows, table.Row{active, p.Name, brokers})
	}
	v.table.SetRows(rows)
}
