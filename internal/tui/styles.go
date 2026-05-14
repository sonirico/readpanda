package tui

import (
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

// Redpanda palette. Approximated from the brand's primary coral-red and the
// dark surfaces used across docs / console. Exposed as package vars so views
// can pull from one source of truth.
var (
	colorPrimary   = lipgloss.AdaptiveColor{Light: "#C73E2C", Dark: "#E14F39"}
	colorPrimaryHi = lipgloss.AdaptiveColor{Light: "#E14F39", Dark: "#FF6B47"}
	colorMuted     = lipgloss.AdaptiveColor{Light: "#6B7280", Dark: "#9CA3AF"}
	colorDim       = lipgloss.AdaptiveColor{Light: "#9CA3AF", Dark: "#6B7280"}
	colorOK        = lipgloss.AdaptiveColor{Light: "#10B981", Dark: "#34D399"}
	colorErr       = lipgloss.AdaptiveColor{Light: "#DC2626", Dark: "#F87171"}
	colorBorder    = lipgloss.AdaptiveColor{Light: "#D1D5DB", Dark: "#374151"}
	colorSelFG     = lipgloss.Color("#0B0B0B")
	colorBarBG     = lipgloss.AdaptiveColor{Light: "#E5E7EB", Dark: "#1F1F1F"}
)

// Styles holds the lip gloss styles used across the TUI. Centralising them
// keeps the visual identity in one place and makes it trivial to swap themes.
type Styles struct {
	Header     lipgloss.Style
	HeaderKey  lipgloss.Style
	Footer     lipgloss.Style
	StatusOK   lipgloss.Style
	StatusErr  lipgloss.Style
	CommandBar lipgloss.Style
	Error      lipgloss.Style
	Title      lipgloss.Style

	// Tail view styles — applied per record so headers, key, value and meta
	// are easy to spot when scanning fast traffic.
	TailTs        lipgloss.Style
	TailLoc       lipgloss.Style
	TailMeta      lipgloss.Style
	TailSchema    lipgloss.Style
	TailLabel     lipgloss.Style
	TailHeaderKey lipgloss.Style
	TailKey       lipgloss.Style
	TailValue     lipgloss.Style
	TailErr       lipgloss.Style
	TailSep       lipgloss.Style
}

func newStyles() Styles {
	return Styles{
		Header:     lipgloss.NewStyle().Bold(true).Foreground(colorPrimary).Padding(0, 1),
		HeaderKey:  lipgloss.NewStyle().Foreground(colorMuted),
		Footer:     lipgloss.NewStyle().Foreground(colorMuted).Padding(0, 1),
		StatusOK:   lipgloss.NewStyle().Foreground(colorOK),
		StatusErr:  lipgloss.NewStyle().Foreground(colorErr),
		CommandBar: lipgloss.NewStyle().Background(colorBarBG).Padding(0, 1),
		Error:      lipgloss.NewStyle().Foreground(colorErr).Bold(true),
		Title:      lipgloss.NewStyle().Bold(true).Foreground(colorPrimary),

		TailTs:        lipgloss.NewStyle().Foreground(colorDim),
		TailLoc:       lipgloss.NewStyle().Bold(true).Foreground(colorPrimaryHi),
		TailMeta:      lipgloss.NewStyle().Foreground(colorMuted),
		TailSchema:    lipgloss.NewStyle().Foreground(colorPrimary).Bold(true),
		TailLabel:     lipgloss.NewStyle().Foreground(colorMuted).Bold(true),
		TailHeaderKey: lipgloss.NewStyle().Foreground(colorOK),
		TailKey:       lipgloss.NewStyle().Foreground(colorPrimaryHi).Bold(true),
		TailValue: lipgloss.NewStyle().
			Foreground(lipgloss.AdaptiveColor{Light: "#111827", Dark: "#E5E7EB"}),
		TailErr: lipgloss.NewStyle().Foreground(colorErr),
		TailSep: lipgloss.NewStyle().Foreground(colorBorder),
	}
}

// tableStyles returns the default table.Styles tuned for the Redpanda palette.
// Used by every table view so selected rows look identical across screens.
func tableStyles() table.Styles {
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(colorBorder).
		Foreground(colorPrimaryHi).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(colorSelFG).
		Background(colorPrimary).
		Bold(true)
	return s
}
