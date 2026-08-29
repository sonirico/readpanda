# readpanda

Terminal UI for Redpanda and Kafka. Topics, consumer groups, lag, live tail
with Avro / JSON-SR / Protobuf decoded inline.

Built because the Redpanda Cloud console eats my laptop.

![demo](docs/gifs/demo.gif)

## Features

### Topics tree
Collapsible, grouped by dot prefix, with partitions, replication and message
count.

![tree](docs/gifs/tree.gif)

### Live tail
Groupless consumer so it doesn't litter your cluster with empty groups. Per
record: size, compression, format hint, headers, key, value. Decoders:
- Plain JSON / UTF-8 / hex preview.
- Confluent Avro via Schema Registry, decoded to JSON (hamba/avro).
- Confluent JSON-SR.
- Confluent Protobuf via Schema Registry, decoded to JSON. Runtime `.proto`
  compile (bufbuild/protocompile), recursive SR reference resolution,
  well-known types bundled, dynamicpb + protojson.

![tail](docs/gifs/tail.gif)

### Consumer groups + lag
Consumer groups view with lag.

![groups](docs/gifs/groups.gif)

### Partitions + config
Partitions (with leader-distribution histogram), full config (non-defaults
first), ACL.

![partitions](docs/gifs/partitions.gif)

### Brokers
Brokers view.

![brokers](docs/gifs/brokers.gif)

## Try it in 2 minutes

Needs Docker.

```
just demo-up
# in another terminal
just demo-traffic
go run ./cmd/readpanda --brokers localhost:19092 --sr-url http://localhost:18081
just demo-down
```

## Install

```
go install github.com/sonirico/readpanda/cmd/readpanda@latest
```

Or from source:

```
git clone https://github.com/sonirico/readpanda
cd readpanda
just install
```

Needs Go 1.26+. `just build` if you only want the binary in `./bin/`.

## Configuration

Reads `rpk` profiles from the usual location
(`~/Library/Application Support/rpk/rpk.yaml` on macOS,
`~/.config/rpk/rpk.yaml` elsewhere). Set up clusters with `rpk` the way you
already do:

```
rpk profile create prod
rpk profile set kafka_api.brokers=broker1:9092,broker2:9092
rpk profile set kafka_api.sasl.user=...
rpk profile set kafka_api.sasl.password=...
rpk profile set kafka_api.tls.enabled=true
rpk profile set schema_registry.addresses=https://sr.example.com:30081
rpk profile use prod
```

If no separate Schema Registry credentials are configured, the kafka SASL
ones are reused for SR HTTP basic auth. rpk does the same.

To bypass `rpk` entirely:

```
readpanda --brokers host:9092 --sasl-user u --sasl-pass p --tls
readpanda --sr-url https://sr.example.com:30081 --sr-user u --sr-pass p
```

The SR pieces also pick up `READPANDA_SR_URL`, `READPANDA_SR_USER` and
`READPANDA_SR_PASS`. Handy if you keep creds in a `.envrc`.

## Keys

Global:

| Key | Action |
|-----|--------|
| `:` | command bar (`topics`, `groups`, `brokers`, `ctx`, `help`, `quit`) |
| `/` | filter rows (esc to clear) |
| `?` | help |
| `r` | refresh (also clears the error-pause state) |
| `enter` | drill in |
| `esc` | back |
| `q` / `ctrl+c` | quit |

Topics tree: `enter` to expand/collapse a branch or open a leaf, `o` expands
all, `O` collapses all.

Topic detail tabs: `1` messages, `2` consumers, `3` partitions, `4` config,
`5` ACL. `tab` cycles forward, `shift+tab` backward.

Tail tab: `p` pause/resume, `c` clear buffer.

## Go version

`.gvmrc` pins Go to `go1.26.3`. To auto-switch on `cd`, add this to your
shell rc:

```
gvmrc_hook() {
    [[ -f .gvmrc && -r .gvmrc ]] && gvm use "$(cat .gvmrc)" >/dev/null
}
autoload -U add-zsh-hook 2>/dev/null && add-zsh-hook chpwd gvmrc_hook
```

Or run `gvm use $(cat .gvmrc)` by hand after `gvm install go1.26.3 -B`.

## Status

Read-only. No produce, no topic creation, no ACL editing yet.

## Roadmap

### Core
- Produce / publish from the TUI (key, value, headers; JSON, Avro and Proto
  via SR for the outgoing payload).
- Create / delete topics, alter partitions, alter configs.
- Inline config editing.
- Reset consumer group offsets (earliest, latest, specific, by timestamp).
- Delete dead consumer groups.
- Tail from a specific offset or timestamp (today it always starts at end).
- Per-record filter inside the tail (`/` regex on key/headers/value).
- Yank record to clipboard.
- Global pause/resume hotkey.

### Decoders and schemas
- SR bearer-token auth for `from_cloud: true` profiles (reuse the JWT in
  `cloud_auth.auth_token`).
- Schemas view (`:schemas`): subjects, versions, source with highlighting,
  resolved references.
- Key decoding. Today keys are treated as strings or UTF-8; should honour the
  Confluent wire format too.

### Metrics and observability
- Per-topic throughput (msg/s, bytes/s).
- Per-group consumption rate and ETA to head.
- Per-partition lag histogram inside group detail.

### Auth / config
- mTLS (client cert/key, custom CA).
- SASL PLAIN and OAUTHBEARER (only SCRAM-SHA-256 today).
- `--sasl-mechanism` override.

### TUI / UX
- Context-aware help pages.
- Non-blocking toasts instead of footer-only errors.
- Remember the last-used profile across runs.
- Forced light theme / colour-blind palette.

### Quality
- Tests for `internal/tui/format.go`, `proto_decode.go` and the admin
  list/describe paths.
- GitHub Actions CI (`go vet`, `golangci-lint`, tests).
- `goreleaser` for darwin/linux x amd64/arm64 binaries.

### Performance and robustness
- Tail backpressure with a real ring buffer and drop counter.
- Automatic consumer reconnection on broker drop.

## Layout

```
cmd/readpanda/   binary
pkg/rp/          franz-go client + Schema Registry (importable lib)
internal/
  profile/       rpk.yaml parser
  tui/           bubbletea views, format/proto decoders
demo/            docker-compose, seed script, traffic generator, vhs tapes
```

`pkg/rp` is a regular library you can import on its own:

```go
import "github.com/sonirico/readpanda/pkg/rp"
```
