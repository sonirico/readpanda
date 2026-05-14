package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/hamba/avro/v2"
	"github.com/sonirico/readpanda/pkg/rp"
)

// decoded is the result of attempting to render one record value as text.
type decoded struct {
	// Format is a short tag like "JSON", "AVRO", "JSON-SR", "PROTOBUF",
	// "TEXT", "BINARY".
	Format string
	// SchemaID is set when the payload is in Confluent wire format.
	SchemaID int
	// SchemaSubject / SchemaType come from the Schema Registry lookup. Empty
	// when there is no SR or the lookup failed.
	SchemaSubject string
	SchemaType    string
	// Text is the human-readable rendering of the payload.
	Text string
	// Err is set when decoding failed; Text falls back to a safe rendering.
	Err error
}

// Decoder bundles every record-decoding strategy readpanda supports: plain
// JSON / UTF-8, Confluent SR-Avro, Confluent SR-JSON and Confluent SR-Proto.
// Construct one per App (it caches schemas and compiled .proto files); pass
// nil where no Schema Registry is configured — Decode will degrade to plain
// detection.
type Decoder struct {
	sr    *rp.SchemaRegistry
	proto *protoDecoder
}

// NewDecoder wires the decoder against the optional Schema Registry. A nil sr
// disables every Confluent-wire-format path.
func NewDecoder(sr *rp.SchemaRegistry) *Decoder {
	d := &Decoder{sr: sr}
	if sr != nil {
		d.proto = newProtoDecoder(sr)
	}
	return d
}

// Decode is the single entry point used by the tail view. See decodeValue.
func (d *Decoder) Decode(ctx context.Context, value []byte) decoded {
	return decodeValue(ctx, value, d)
}

// decodeValue picks the best rendering for the record value:
//   - Confluent wire format → look up schema, decode if we can.
//   - Otherwise → JSON pretty-print, UTF-8 text, or hex preview as fallback.
//
// d may be nil; in that case Confluent payloads are reported by schema id
// only.
func decodeValue(ctx context.Context, value []byte, d *Decoder) decoded {
	if id, payload, ok := rp.ExtractSchemaID(value); ok {
		return decodeConfluent(ctx, id, payload, d)
	}
	return decodePlain(value)
}

func decodeConfluent(
	ctx context.Context, id int, payload []byte, d *Decoder,
) decoded {
	out := decoded{Format: "SR", SchemaID: id}
	if d == nil || d.sr == nil {
		out.Format = "SR (no registry configured)"
		out.Text = hexPreview(payload)
		return out
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	info, err := d.sr.FetchByID(lookupCtx, id)
	if err != nil {
		out.Err = err
		out.Text = hexPreview(payload)
		return out
	}
	out.SchemaSubject = info.Subject
	out.SchemaType = info.Type

	switch info.Type {
	case "AVRO":
		out.Format = "AVRO"
		text, err := decodeAvro(info.Schema, payload)
		if err != nil {
			out.Err = err
			out.Text = hexPreview(payload)
			return out
		}
		out.Text = text
	case "JSON":
		out.Format = "JSON-SR"
		out.Text = prettyJSON(payload)
	case "PROTOBUF":
		out.Format = "PROTOBUF"
		if d.proto == nil {
			out.Err = errors.New("no proto decoder configured")
			out.Text = hexPreview(payload)
			return out
		}
		text, err := d.proto.Decode(ctx, info, payload)
		if err != nil {
			out.Err = err
			out.Text = hexPreview(payload)
			return out
		}
		out.Text = text
	default:
		out.Format = info.Type
		out.Text = hexPreview(payload)
	}
	return out
}

func decodePlain(value []byte) decoded {
	if len(value) == 0 {
		return decoded{Format: "EMPTY", Text: ""}
	}
	if json.Valid(value) {
		return decoded{Format: "JSON", Text: prettyJSON(value)}
	}
	if utf8.Valid(value) {
		return decoded{Format: "TEXT", Text: string(value)}
	}
	return decoded{Format: "BINARY", Text: hexPreview(value)}
}

func decodeAvro(schemaStr string, payload []byte) (string, error) {
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		return "", fmt.Errorf("avro parse: %w", err)
	}
	var v any
	if err := avro.Unmarshal(schema, payload, &v); err != nil {
		return "", fmt.Errorf("avro unmarshal: %w", err)
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("avro->json: %w", err)
	}
	return string(out), nil
}

func prettyJSON(value []byte) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, value, "", "  "); err != nil {
		return string(value)
	}
	return buf.String()
}

func hexPreview(value []byte) string {
	const limit = 256
	if len(value) <= limit {
		return fmt.Sprintf("<%d bytes binary: %x>", len(value), value)
	}
	return fmt.Sprintf("<%d bytes binary, first %d: %x…>", len(value), limit, value[:limit])
}
