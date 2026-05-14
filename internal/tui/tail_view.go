package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/sonirico/readpanda/pkg/rp"
)

// TailView is a live tail of messages from a single topic. It spawns a
// throwaway consumer with a unique consumer group and starts from the end.
type TailView struct {
	app      *App
	viewport viewport.Model
	target   string
	lines    []string
	paused   bool

	mu       sync.Mutex
	cancel   context.CancelFunc
	consumer *rp.TailConsumer
	msgCh    chan rp.Msg
	errCh    chan error
	running  bool
}

func newTailView(app *App) *TailView {
	vp := viewport.New(80, 20)
	return &TailView{app: app, viewport: vp}
}

func (v *TailView) SetSize(w, h int) {
	if w > 0 {
		v.viewport.Width = w
	}
	if h > 2 {
		// One row reserved for the in-view header line ("tail topic · N lines · LIVE").
		v.viewport.Height = h - 1
	}
}

func (v *TailView) Init() tea.Cmd {
	return v.start()
}

func (v *TailView) Update(msg tea.Msg) tea.Cmd {
	switch m := msg.(type) {
	case tailRecordMsg:
		if !v.paused {
			v.appendLine(v.formatMessage(m.msg))
		}
		return v.pump()
	case tailErrorMsg:
		v.appendLine("[ERR] " + m.err.Error())
		return v.pump()
	case tea.KeyMsg:
		switch m.String() {
		case "p":
			v.paused = !v.paused
		case "c":
			v.lines = nil
			v.viewport.SetContent("")
		}
	}
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

func (v *TailView) View() string {
	if v.target == "" {
		return "\n  select a topic from the topics view first\n"
	}
	header := fmt.Sprintf("tail %s · %d lines · %s",
		v.target, len(v.lines), pausedLabel(v.paused))
	return header + "\n" + v.viewport.View()
}

func (v *TailView) appendLine(line string) {
	v.lines = append(v.lines, line)
	if len(v.lines) > 5000 {
		v.lines = v.lines[len(v.lines)-5000:]
	}
	v.viewport.SetContent(strings.Join(v.lines, "\n"))
	v.viewport.GotoBottom()
}

func (v *TailView) start() tea.Cmd {
	v.stop()
	if v.target == "" {
		return nil
	}

	cons, err := rp.NewTailConsumer(rp.TailConfig{
		Brokers:  v.app.prof.Brokers,
		SASLUser: v.app.prof.SASLUser,
		SASLPass: v.app.prof.SASLPass,
		TLS:      v.app.prof.TLS,
		Topic:    v.target,
	})
	if err != nil {
		v.appendLine("[ERR] new tail consumer: " + err.Error())
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	v.mu.Lock()
	v.consumer = cons
	v.cancel = cancel
	v.msgCh = make(chan rp.Msg, 256)
	v.errCh = make(chan error, 1)
	v.running = true
	msgCh, errCh := v.msgCh, v.errCh
	v.mu.Unlock()

	go func() {
		err := cons.Run(ctx, func(_ context.Context, m rp.Msg) error {
			select {
			case msgCh <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		if err != nil && err != context.Canceled {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	return v.pump()
}

func (v *TailView) pump() tea.Cmd {
	v.mu.Lock()
	msgCh, errCh, running := v.msgCh, v.errCh, v.running
	v.mu.Unlock()
	if !running {
		return nil
	}
	return func() tea.Msg {
		select {
		case m, ok := <-msgCh:
			if !ok {
				return nil
			}
			return tailRecordMsg{msg: m}
		case e := <-errCh:
			return tailErrorMsg{err: e}
		}
	}
}

func (v *TailView) stop() {
	v.mu.Lock()
	cancel := v.cancel
	cons := v.consumer
	v.cancel = nil
	v.consumer = nil
	v.running = false
	v.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if cons != nil {
		cons.Close()
	}
}

func (v *TailView) formatMessage(m rp.Msg) string {
	st := v.app.styles
	dec := v.app.decoder.Decode(context.Background(), m.Value)

	keyStr := string(m.Key)
	if !utf8.ValidString(keyStr) {
		keyStr = fmt.Sprintf("%x", m.Key)
	}

	size := len(m.Value) + len(m.Key)
	for _, h := range m.Headers {
		size += len(h.Key) + len(h.Value)
	}

	sep := st.TailMeta.Render(" · ")

	// Meta line: ts · p@offset · size · codec · format [· schema] [· err]
	var meta strings.Builder
	meta.WriteString(st.TailTs.Render(m.Ts.Format("15:04:05.000")))
	meta.WriteString(sep)
	meta.WriteString(st.TailLoc.Render(fmt.Sprintf("p%d@%d", m.Partition, m.Offset)))
	meta.WriteString(sep)
	meta.WriteString(st.TailMeta.Render(humanBytes(int64(size))))
	meta.WriteString(sep)
	meta.WriteString(st.TailMeta.Render("codec=" + m.CompressionCodec))
	meta.WriteString(sep)
	meta.WriteString(st.TailMeta.Render("fmt=" + dec.Format))
	if dec.SchemaID > 0 {
		meta.WriteString(sep)
		label := fmt.Sprintf("schema=id:%d", dec.SchemaID)
		if dec.SchemaSubject != "" {
			label = fmt.Sprintf("schema=%s (id:%d)", dec.SchemaSubject, dec.SchemaID)
		}
		meta.WriteString(st.TailSchema.Render(label))
	}
	if dec.Err != nil {
		meta.WriteString(sep)
		meta.WriteString(st.TailErr.Render("decode-err: " + dec.Err.Error()))
	}

	var b strings.Builder
	sepWidth := v.viewport.Width
	if sepWidth < 1 {
		sepWidth = 80
	}
	b.WriteString(st.TailSep.Render(strings.Repeat("─", sepWidth)))
	b.WriteString("\n")
	b.WriteString(meta.String())
	b.WriteString("\n")
	if len(m.Headers) > 0 {
		b.WriteString(st.TailLabel.Render("  headers  "))
		b.WriteString(renderHeaders(m.Headers, st))
		b.WriteString("\n")
	}
	b.WriteString(st.TailLabel.Render("  key      "))
	b.WriteString(st.TailKey.Render(keyStr))
	b.WriteString("\n")
	b.WriteString(st.TailLabel.Render("  value\n"))
	b.WriteString(indent(st.TailValue.Render(dec.Text), "    "))
	return b.String()
}

func renderHeaders(hs []rp.Header, st Styles) string {
	parts := make([]string, 0, len(hs))
	for _, h := range hs {
		val := string(h.Value)
		if !utf8.ValidString(val) {
			val = fmt.Sprintf("%x", h.Value)
		}
		if len(val) > 80 {
			val = val[:80] + "…"
		}
		parts = append(parts, st.TailHeaderKey.Render(h.Key)+st.TailMeta.Render("=")+val)
	}
	return strings.Join(parts, st.TailMeta.Render(", "))
}

func indent(s, prefix string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}

func pausedLabel(p bool) string {
	if p {
		return "PAUSED (p to resume, c to clear)"
	}
	return "LIVE (p to pause, c to clear)"
}
