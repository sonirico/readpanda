// Package tui is the bubbletea-driven terminal UI for readpanda.
package tui

import (
	"context"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sonirico/readpanda/internal/profile"
	"github.com/sonirico/readpanda/pkg/rp"
)

type viewID int

const (
	viewTopics viewID = iota
	viewGroups
	viewContexts
	viewHelp
	viewTopicDetail
	viewBrokers
)

// AppConfig wires the bits the App needs from the outside world. The factory
// allows the app to recreate the admin client when switching profile contexts.
// RegistryFactory is optional — when nil or when it returns nil, the TUI runs
// without Schema Registry support and falls back to plain JSON/text detection.
type AppConfig struct {
	Profile         profile.Profile
	ProfileFile     *profile.File
	AdminFactory    func(profile.Profile) (*rp.Admin, error)
	RegistryFactory func(profile.Profile) (*rp.SchemaRegistry, error)
}

// App is the root bubbletea Model. It owns the active view, the keymap and the
// data loader, and routes messages either to itself (global concerns: command
// bar, quit, view switching) or to the active view.
type App struct {
	cfg      AppConfig
	admin    dataLoader
	registry *rp.SchemaRegistry
	decoder  *Decoder
	prof     profile.Profile

	keys   KeyMap
	styles Styles

	width  int
	height int

	current     viewID
	prev        viewID
	topics      *TopicsView
	groups      *GroupsView
	contexts    *ContextsView
	help        *HelpView
	topicDetail *TopicDetailView
	brokers     *BrokersView

	cmdMode   bool
	cmdBuf    string
	lastErr   error
	connected bool
}

// NewApp wires the app with an initial admin client.
func NewApp(cfg AppConfig) (*App, error) {
	admin, err := cfg.AdminFactory(cfg.Profile)
	if err != nil {
		return nil, fmt.Errorf("init admin: %w", err)
	}
	reg := buildRegistry(cfg)
	a := &App{
		cfg:       cfg,
		admin:     admin,
		registry:  reg,
		decoder:   NewDecoder(reg),
		prof:      cfg.Profile,
		keys:      newKeyMap(),
		styles:    newStyles(),
		current:   viewTopics,
		connected: true,
	}
	a.topics = newTopicsView(a)
	a.groups = newGroupsView(a)
	a.contexts = newContextsView(a)
	a.help = newHelpView(a)
	a.topicDetail = newTopicDetailView(a)
	a.brokers = newBrokersView(a)
	return a, nil
}

// buildRegistry returns a Schema Registry client when the factory is set and
// the profile has SR configured; nil otherwise. Errors are swallowed —
// readpanda must keep running even if SR is unreachable.
func buildRegistry(cfg AppConfig) *rp.SchemaRegistry {
	if cfg.RegistryFactory == nil {
		return nil
	}
	reg, err := cfg.RegistryFactory(cfg.Profile)
	if err != nil {
		return nil
	}
	return reg
}

// Close releases the admin client. Safe to call multiple times.
func (a *App) Close() {
	if a.admin != nil {
		a.admin.Close()
	}
	if a.topicDetail != nil {
		a.topicDetail.Stop()
	}
}

func (a *App) Init() tea.Cmd {
	return a.topics.Init()
}

func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch m := msg.(type) {
	case tea.WindowSizeMsg:
		a.width, a.height = m.Width, m.Height
		a.resizeViews()
	case tea.KeyMsg:
		if cmd, handled := a.handleKey(m); handled {
			return a, cmd
		}
	case errorMsg:
		a.lastErr = m.err
		a.connected = false
		// Fall through so the current view can react (e.g. stop polling).
	case profileSwitchedMsg:
		return a, a.applyProfileSwitch(m.profile)
	case switchViewMsg:
		return a, a.switchView(m.view)
	}

	switch a.current {
	case viewTopics:
		cmd := a.topics.Update(msg)
		return a, cmd
	case viewGroups:
		cmd := a.groups.Update(msg)
		return a, cmd
	case viewContexts:
		cmd := a.contexts.Update(msg)
		return a, cmd
	case viewHelp:
		cmd := a.help.Update(msg)
		return a, cmd
	case viewTopicDetail:
		cmd := a.topicDetail.Update(msg)
		return a, cmd
	case viewBrokers:
		cmd := a.brokers.Update(msg)
		return a, cmd
	}
	return a, nil
}

func (a *App) View() string {
	body := ""
	switch a.current {
	case viewTopics:
		body = a.topics.View()
	case viewGroups:
		body = a.groups.View()
	case viewContexts:
		body = a.contexts.View()
	case viewHelp:
		body = a.help.View()
	case viewTopicDetail:
		body = a.topicDetail.View()
	case viewBrokers:
		body = a.brokers.View()
	}

	header := a.renderHeader()
	footer := a.renderFooter()
	// Pad the body so the footer always sits at the bottom of the alt screen.
	if a.height > 0 {
		body = padToHeight(body, a.bodyHeight())
	}
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// bodyHeight returns the number of rows available for the active view's body
// (everything between the 1-line header and 1-line footer).
func (a *App) bodyHeight() int {
	h := a.height - 2
	if h < 1 {
		return 1
	}
	return h
}

func (a *App) resizeViews() {
	w, h := a.width, a.bodyHeight()
	a.topics.SetSize(w, h)
	a.groups.SetSize(w, h)
	a.contexts.SetSize(w, h)
	a.help.SetSize(w, h)
	a.topicDetail.SetSize(w, h)
	a.brokers.SetSize(w, h)
}

func padToHeight(s string, h int) string {
	lines := strings.Count(s, "\n") + 1
	if lines >= h {
		return s
	}
	return s + strings.Repeat("\n", h-lines)
}

func (a *App) handleKey(k tea.KeyMsg) (tea.Cmd, bool) {
	if a.cmdMode {
		return a.handleCommandKey(k), true
	}

	switch {
	case keyMatches(k, a.keys.Quit):
		return tea.Quit, true
	case keyMatches(k, a.keys.Command):
		a.cmdMode = true
		a.cmdBuf = ""
		return nil, true
	case keyMatches(k, a.keys.Help):
		return a.toggleHelp(), true
	case keyMatches(k, a.keys.Back):
		switch a.current {
		case viewTopicDetail:
			a.topicDetail.Stop()
			return a.switchView(viewTopics), true
		case viewHelp:
			return a.switchView(a.prev), true
		case viewContexts, viewGroups, viewBrokers:
			return a.switchView(viewTopics), true
		}
	}
	return nil, false
}

func (a *App) toggleHelp() tea.Cmd {
	if a.current == viewHelp {
		return a.switchView(a.prev)
	}
	return a.switchView(viewHelp)
}

func (a *App) handleCommandKey(k tea.KeyMsg) tea.Cmd {
	switch k.String() {
	case "esc":
		a.cmdMode = false
		a.cmdBuf = ""
		return nil
	case "enter":
		cmd := a.runCommand(strings.TrimSpace(a.cmdBuf))
		a.cmdMode = false
		a.cmdBuf = ""
		return cmd
	case "backspace":
		if len(a.cmdBuf) > 0 {
			a.cmdBuf = a.cmdBuf[:len(a.cmdBuf)-1]
		}
		return nil
	}
	if len(k.String()) == 1 {
		a.cmdBuf += k.String()
	}
	return nil
}

func (a *App) runCommand(cmd string) tea.Cmd {
	switch cmd {
	case "topics", "t":
		return a.switchView(viewTopics)
	case "groups", "g":
		return a.switchView(viewGroups)
	case "ctx", "context", "contexts":
		return a.switchView(viewContexts)
	case "brokers", "b":
		return a.switchView(viewBrokers)
	case "help", "?", "h":
		return a.toggleHelp()
	case "quit", "q":
		return tea.Quit
	}
	a.lastErr = fmt.Errorf("unknown command: %s", cmd)
	return nil
}

func (a *App) switchView(v viewID) tea.Cmd {
	if a.current != viewHelp {
		a.prev = a.current
	}
	a.current = v
	switch v {
	case viewTopics:
		return a.topics.Init()
	case viewGroups:
		return a.groups.Init()
	case viewContexts:
		return a.contexts.Init()
	case viewHelp:
		return a.help.Init()
	case viewTopicDetail:
		return a.topicDetail.Init()
	case viewBrokers:
		return a.brokers.Init()
	}
	return nil
}

func (a *App) applyProfileSwitch(p profile.Profile) tea.Cmd {
	if a.admin != nil {
		a.admin.Close()
	}
	cl, err := a.cfg.AdminFactory(p)
	if err != nil {
		a.lastErr = fmt.Errorf("switch profile %q: %w", p.Name, err)
		a.connected = false
		return nil
	}
	a.admin = cl
	a.registry = buildRegistry(AppConfig{
		Profile:         p,
		RegistryFactory: a.cfg.RegistryFactory,
	})
	a.decoder = NewDecoder(a.registry)
	a.prof = p
	a.connected = true
	a.lastErr = nil
	return a.switchView(viewTopics)
}

func (a *App) renderHeader() string {
	title := a.styles.Title.Render("readpanda")
	prof := fmt.Sprintf("profile=%s", a.prof.Name)
	if len(a.prof.Brokers) > 0 {
		prof += " · " + a.prof.Brokers[0]
		if len(a.prof.Brokers) > 1 {
			prof += fmt.Sprintf(" (+%d)", len(a.prof.Brokers)-1)
		}
	}
	status := a.styles.StatusOK.Render("● connected")
	if !a.connected {
		status = a.styles.StatusErr.Render("● error")
	}
	view := a.styles.HeaderKey.Render(viewName(a.current))
	left := a.styles.Header.Render(title + "  " + view)
	right := a.styles.HeaderKey.Render(prof) + "  " + status
	return left + "  " + right
}

func (a *App) renderFooter() string {
	if a.cmdMode {
		return a.styles.CommandBar.Render(":" + a.cmdBuf)
	}
	if a.lastErr != nil {
		return a.styles.Error.Render("err: " + a.lastErr.Error())
	}
	keys := []string{
		": command", "/ filter", "? help", "↵ select", "esc back", "r refresh", "q quit",
	}
	return a.styles.Footer.Render(strings.Join(keys, " · "))
}

func viewName(v viewID) string {
	switch v {
	case viewTopics:
		return "topics"
	case viewGroups:
		return "groups"
	case viewContexts:
		return "contexts"
	case viewHelp:
		return "help"
	case viewTopicDetail:
		return "topic"
	case viewBrokers:
		return "brokers"
	}
	return "?"
}

// Run starts the bubbletea program. The provided context is honored: when it
// is cancelled, the program shuts down cleanly.
func (a *App) Run(ctx context.Context) error {
	p := tea.NewProgram(a, tea.WithAltScreen(), tea.WithContext(ctx))
	_, err := p.Run()
	a.Close()
	if err != nil {
		return fmt.Errorf("tui run: %w", err)
	}
	return nil
}

// keyMatches checks if a key event matches a binding's configured keys.
func keyMatches(msg tea.KeyMsg, b binding) bool {
	for _, k := range b.Keys() {
		if msg.String() == k {
			return true
		}
	}
	return false
}

// binding mirrors the subset of bubbles/key.Binding methods we use, so
// keymap.go can stay decoupled if we ever swap the bindings library.
type binding interface {
	Keys() []string
}
