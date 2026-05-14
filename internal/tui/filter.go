package tui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// filter is a tiny `/`-driven substring filter, modelled after k9s. The
// embedding view consults Active/Query when computing rows and feeds key
// events through Handle so typing stays in the filter buffer instead of
// scrolling the underlying table.
type filter struct {
	active bool
	query  string
}

// Handle processes a key event when filter mode is active or when the user
// presses `/` to enter it. It returns (consumed, applied):
//   - consumed: the view should NOT forward the key to its table widget;
//   - applied: the query changed so the view should re-render rows.
func (f *filter) Handle(k tea.KeyMsg) (consumed, applied bool) {
	if !f.active {
		if k.String() == "/" {
			f.active = true
			f.query = ""
			return true, true
		}
		return false, false
	}
	switch k.String() {
	case "esc":
		f.active = false
		f.query = ""
		return true, true
	case "enter":
		// Keep the filter applied but exit edit mode so arrow keys move the
		// cursor again. Press `/` then `esc` (or `/` again) to clear.
		f.active = false
		return true, false
	case "backspace":
		if len(f.query) > 0 {
			f.query = f.query[:len(f.query)-1]
			return true, true
		}
		return true, false
	}
	if len(k.String()) == 1 {
		f.query += k.String()
		return true, true
	}
	return true, false
}

// Match reports whether the given name passes the filter (case-insensitive
// substring). An empty query matches everything.
func (f *filter) Match(name string) bool {
	if f.query == "" {
		return true
	}
	return strings.Contains(strings.ToLower(name), strings.ToLower(f.query))
}

// Bar returns a single-line indicator suitable for embedding above the table.
// Returns empty string when the filter is fully cleared.
func (f *filter) Bar() string {
	if !f.active && f.query == "" {
		return ""
	}
	prefix := "/"
	if !f.active {
		prefix = "[filter] "
	}
	return prefix + f.query
}
