# Show HN draft

## Title (80-char limit)

```
Show HN: Readpanda - a k9s-style terminal UI for Redpanda and Kafka clusters
```

Alternates:

- `Show HN: Readpanda - terminal UI for Redpanda with inline Avro/Proto decoding`
- `Show HN: A k9s for Kafka/Redpanda, written in Go + Bubble Tea`

Pick the first one. Save the inline-decode angle for the body.

## Body

```
I built readpanda because Redpanda Cloud's web console reliably maxes out my
laptop on any topic with real traffic: multi-hundred-MB tabs, fans spinning,
the works. I wanted something local, keyboard-driven, and able to decode
messages inline.

readpanda is a Bubble Tea TUI on top of franz-go. Features:

- Topics view as a collapsible tree by dot-separated name (chesscom.stats.v1.*
  collapses to a single chesscom branch with descendant counts).
- Topic detail with 5 tabs: live tail, consumer groups + lag, partitions
  (with leader-distribution histogram), full config, ACL.
- Live tail with per-record size, compression codec, format hint, headers,
  key, and value decoded:
    * Plain JSON / UTF-8 / hex preview.
    * Confluent Schema Registry Avro -> JSON (hamba/avro).
    * Confluent Schema Registry JSON-SR.
    * Confluent Schema Registry Protobuf -> JSON. This was the hardest part:
      runtime .proto compile via bufbuild/protocompile, recursive reference
      resolution against SR (subjects with slashes have to be URL-escaped,
      that bit me), bundled google/protobuf well-known types, then dynamicpb
      + protojson. To my knowledge no other open-source TUI does proto-via-SR
      decoding inline.
- Picks up rpk profiles automatically (~/.config/rpk/rpk.yaml or the macOS
  Application Support equivalent). SR HTTP basic auth falls back to the
  kafka_api SASL credentials, mirroring rpk's own behaviour.
- k9s-style UX: `:` command bar, `/` filter, `?` help, dynamic column widths,
  Redpanda colour palette.

Closest existing tool: kaskade (Python). readpanda is Go + Bubble Tea, with
proto/SR decoding and Redpanda-native bits (rpk profiles, DescribeLogDirs
size that matches the web UI rather than the inflated all-replicas number).

Read-only for now: no produce, no topic creation. The roadmap in the README
covers what is coming next.

Source: https://github.com/sonirico/readpanda
Install: `go install github.com/sonirico/readpanda/cmd/readpanda@latest`
Demo:    [link to gif/asciinema once recorded]

Feedback welcome, especially on the proto decoder edge cases.
```

## Posting checklist

1. Demo GIF embedded at the top of the README before posting.
2. `v0.1.0` (or newer) tag and release notes visible.
3. `go install` works from the tag.
4. README's Status section makes the read-only scope explicit (already does).
5. Post Tue-Thu, 08:00-10:00 ET.
6. Reply to every top-level comment in the first 90 minutes.
7. Do NOT cross-post to r/golang in the same hour. Wait until HN settles.
