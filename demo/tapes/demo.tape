# VHS tape for readpanda
# Generate the gif with:   vhs demo/tapes/demo.tape (run from repo root)
# Requires: https://github.com/charmbracelet/vhs
# Prereq:   `just demo-up` and `go run ./demo/traffic` running

Output docs/gifs/demo.gif

Set Shell        "zsh"
Set FontSize     14
Set Width        1200
Set Height       700
Set TypingSpeed  60ms
Set Theme        "Catppuccin Mocha"

# ── intro ───────────────────────────────────────────────────────────────
Type "go run ./cmd/readpanda --brokers localhost:19092 --sr-url http://localhost:18081"
Enter
Sleep 2.5s

# ── topics tree ─────────────────────────────────────────────────────────
# At this point the tree is rendered with the first level expanded.
# Press `o` to expand everything so the gif shows full structure.
Type "o"
Sleep 1.5s

# ── filter ──────────────────────────────────────────────────────────────
Type "/"
Sleep 400ms
Type "orders"
Sleep 1.5s
Enter
Sleep 800ms

# ── drill into a topic ──────────────────────────────────────────────────
# The filter keeps ancestor branches (shop, orders) visible above the
# matching leaves; move down to select shop.orders.v1 before drilling in.
Down 3
Sleep 400ms
Enter
Sleep 2.5s

# Tab 2: consumers + lag
Type "2"
Sleep 2.5s

# Tab 3: partitions + leader histogram
Type "3"
Sleep 3s

# Tab 4: configuration
Type "4"
Sleep 2.5s

# Tab 1: live tail (Avro/Proto decoded inline to JSON)
Type "1"
Sleep 5s

# back to topics
Escape
Sleep 800ms
Escape
Sleep 800ms

# ── brokers view ────────────────────────────────────────────────────────
Type ":"
Sleep 200ms
Type "brokers"
Enter
Sleep 3s

# ── done ────────────────────────────────────────────────────────────────
Type "q"
Sleep 600ms
