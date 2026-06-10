package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// panel renders body inside a rounded box whose content area is contentW
// columns wide. A non-empty title is folded into the top border
// ("-- Title --...--"); an empty title leaves a plain border.
//
// lipgloss Width counts the horizontal padding, so the usable content area is
// Width-2; we set Width to contentW+2 to leave room for the body.
func panel(title, body string, contentW int) string {
	if contentW < 8 {
		contentW = 8
	}
	boxed := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(colorBorder).
		Padding(0, 1).
		Width(contentW + 2).
		Render(body)

	lines := strings.Split(boxed, "\n")
	if len(lines) == 0 {
		return boxed
	}
	if title == "" {
		return strings.Join(lines, "\n")
	}
	border := lipgloss.NewStyle().Foreground(colorBorder)
	titleSt := lipgloss.NewStyle().Bold(true).Foreground(colorPrimary)
	full := lipgloss.Width(lines[0])
	label := " " + title + " "
	fill := full - 3 - lipgloss.Width(label) // "╭─" (2) + "╮" (1)
	if fill < 0 {
		label = " " + truncate(title, maxInt(full-5, 0)) + " "
		fill = maxInt(full-3-lipgloss.Width(label), 0)
	}
	lines[0] = border.Render("╭─") +
		titleSt.Render(label) +
		border.Render(strings.Repeat("─", fill)+"╮")
	return strings.Join(lines, "\n")
}

// renderRule returns a horizontal divider w columns wide in the border colour.
func renderRule(w int) string {
	return lipgloss.NewStyle().Foreground(colorBorder).
		Render(strings.Repeat("─", maxInt(w, 0)))
}

// renderTabBar renders a row of tab labels, highlighting the active index.
func renderTabBar(active int, labels []string) string {
	on := lipgloss.NewStyle().Bold(true).
		Foreground(colorSelFG).Background(colorPrimary).Padding(0, 1)
	off := lipgloss.NewStyle().Foreground(colorMuted).Padding(0, 1)
	parts := make([]string, len(labels))
	for i, l := range labels {
		if i == active {
			parts[i] = on.Render(l)
		} else {
			parts[i] = off.Render(l)
		}
	}
	return strings.Join(parts, "")
}

// fitHeight pads s with blank lines or trims it so it is exactly h lines tall,
// keeping framed content from shifting the boxes around it.
func fitHeight(s string, h int) string {
	if h < 0 {
		h = 0
	}
	lines := strings.Split(s, "\n")
	if len(lines) > h {
		lines = lines[:h]
	}
	for len(lines) < h {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
