# VHS tape for readpanda
# Generate the gif with:   vhs demo.tape
# Requires: https://github.com/charmbracelet/vhs
# Prereq:   a working `rpk profile use <name>` pointing at a cluster
#           with some traffic; ideally one with Avro/Proto SR topics so
#           the tail tab shows decoded JSON.

Output demo.gif

Set Shell        "zsh"
Set FontSize     14
Set Width        1280
Set Height       720
Set TypingSpeed  60ms
Set Theme        "Catppuccin Mocha"

# ── intro ───────────────────────────────────────────────────────────────
Type "readpanda"
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
Type "advstats"
Sleep 1.5s
Enter
Sleep 800ms

# ── drill into a topic ──────────────────────────────────────────────────
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

# ── context switcher ────────────────────────────────────────────────────
Type ":"
Sleep 200ms
Type "ctx"
Enter
Sleep 2.5s

# ── done ────────────────────────────────────────────────────────────────
Type "q"
Sleep 600ms
