package tui

import (
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// HelpView is a scrollable cheat sheet shown via `?`, `:help`, or `:?`.
type HelpView struct {
	app      *App
	viewport viewport.Model
}

func newHelpView(app *App) *HelpView {
	vp := viewport.New(80, 20)
	vp.SetContent(helpContent())
	return &HelpView{app: app, viewport: vp}
}

func (v *HelpView) SetSize(w, h int) {
	if w > 0 {
		v.viewport.Width = w
	}
	if h > 2 {
		v.viewport.Height = h
	}
}

func (v *HelpView) Title() string { return "Help" }

func (v *HelpView) Init() tea.Cmd { return nil }

func (v *HelpView) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

func (v *HelpView) View() string {
	return v.viewport.View()
}

func helpContent() string {
	bold := lipgloss.NewStyle().Bold(true).Foreground(colorPrimary)
	dim := lipgloss.NewStyle().Foreground(colorMuted)
	section := func(title string) string {
		return "\n" + bold.Render(title) + "\n"
	}
	row := func(k, desc string) string {
		return "  " + bold.Render(padRight(k, 14)) + dim.Render(desc) + "\n"
	}

	var b strings.Builder
	b.WriteString(bold.Render("readpanda - keymap"))
	b.WriteString("\n")

	b.WriteString(section("Navigation"))
	b.WriteString(row(":", "open command bar"))
	b.WriteString(row("/", "filter rows (esc to clear, enter to keep)"))
	b.WriteString(row("up/k dn/j", "move cursor"))
	b.WriteString(row("enter", "drill into selected row"))
	b.WriteString(row("esc", "back to previous view"))
	b.WriteString(row("r", "refresh current view"))
	b.WriteString(row("?", "toggle this help"))
	b.WriteString(row("q / ctrl+c", "quit"))

	b.WriteString(section("Commands (after `:`)"))
	b.WriteString(row("topics, t", "topics view"))
	b.WriteString(row("groups, g", "consumer groups + lag"))
	b.WriteString(row("brokers, b", "brokers + log dir size"))
	b.WriteString(row("ctx", "switch rpk profile"))
	b.WriteString(row("help, ?", "this help"))
	b.WriteString(row("quit, q", "quit"))

	b.WriteString(section("Topics tree"))
	b.WriteString(row("enter", "expand/collapse branch; enter to open topic"))
	b.WriteString(row("o", "expand all"))
	b.WriteString(row("O", "collapse all"))

	b.WriteString(section("Topic detail tabs"))
	b.WriteString(row("1", "messages (live tail)"))
	b.WriteString(row("2", "consumer groups + lag"))
	b.WriteString(row("3", "partitions"))
	b.WriteString(row("4", "configuration"))
	b.WriteString(row("5", "ACL"))
	b.WriteString(row("tab/h,l", "cycle tabs"))

	b.WriteString(section("Tail tab"))
	b.WriteString(row("p", "pause / resume"))
	b.WriteString(row("c", "clear buffer"))

	return b.String()
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}
