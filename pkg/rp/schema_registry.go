package rp

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ConfluentMagicByte is the leading byte of Confluent / Redpanda Schema
// Registry wire-format payloads. The next 4 bytes are a big-endian schema ID.
const ConfluentMagicByte byte = 0x00

// SchemaInfo is the metadata Schema Registry returns for a schema ID.
type SchemaInfo struct {
	ID         int
	Type       string // "AVRO" | "PROTOBUF" | "JSON"
	Subject    string
	Version    int
	Schema     string
	References []SchemaReference
}

// SchemaReference is a named import inside a schema (used by Protobuf
// references and Avro union types that reference other subjects).
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SchemaRegistryConfig configures access to a Confluent-compatible Schema
// Registry. The first address is used; extra addresses are kept for future
// fallback but not consulted today.
type SchemaRegistryConfig struct {
	Addresses []string
	Username  string
	Password  string
	Timeout   time.Duration
}

// SchemaRegistry is a thin HTTP client with in-memory caching. It is safe for
// concurrent use.
type SchemaRegistry struct {
	cfg    SchemaRegistryConfig
	client *http.Client
	mu     sync.RWMutex
	byID   map[int]SchemaInfo
}

// NewSchemaRegistry validates the config and returns a ready-to-use client.
// Returns an error when no addresses are configured.
func NewSchemaRegistry(cfg SchemaRegistryConfig) (*SchemaRegistry, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("schema registry: %w", ErrConfig)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &SchemaRegistry{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		byID:   map[int]SchemaInfo{},
	}, nil
}

// ExtractSchemaID parses the 5-byte Confluent header from value and returns
// the schema ID plus the remaining payload. ok=false means the buffer is not
// in Confluent wire format.
func ExtractSchemaID(value []byte) (id int, payload []byte, ok bool) {
	if len(value) < 5 || value[0] != ConfluentMagicByte {
		return 0, value, false
	}
	return int(binary.BigEndian.Uint32(value[1:5])), value[5:], true
}

// FetchBySubjectVersion looks up a schema by its subject + version. version=-1
// (or 0) means "latest". Used to resolve protobuf schema references — SR
// references identify imports by (subject, version), not by ID.
func (r *SchemaRegistry) FetchBySubjectVersion(
	ctx context.Context, subject string, version int,
) (SchemaInfo, error) {
	v := fmt.Sprintf("%d", version)
	if version <= 0 {
		v = "latest"
	}
	base := strings.TrimRight(r.cfg.Addresses[0], "/")
	// Subject names commonly contain slashes (e.g. "chesscom/chess/v1/foo.proto"
	// for protobuf imports) — they MUST be percent-encoded or the registry
	// interprets them as path segments and returns 404.
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%s", base, url.PathEscape(subject), v)
	return r.fetchURL(ctx, endpoint, -1)
}

// FetchByID looks up a schema by its registry ID, with in-memory caching.
func (r *SchemaRegistry) FetchByID(ctx context.Context, id int) (SchemaInfo, error) {
	r.mu.RLock()
	if s, ok := r.byID[id]; ok {
		r.mu.RUnlock()
		return s, nil
	}
	r.mu.RUnlock()

	info, err := r.fetchByID(ctx, id)
	if err != nil {
		return SchemaInfo{}, err
	}

	r.mu.Lock()
	r.byID[id] = info
	r.mu.Unlock()
	return info, nil
}

func (r *SchemaRegistry) fetchByID(ctx context.Context, id int) (SchemaInfo, error) {
	base := strings.TrimRight(r.cfg.Addresses[0], "/")
	url := fmt.Sprintf("%s/schemas/ids/%d", base, id)
	return r.fetchURL(ctx, url, id)
}

// fetchURL issues a GET against the registry and parses the standard response
// shape. When the caller already knows the id it can pass it in so the
// returned SchemaInfo carries it; otherwise pass -1 and the registry response
// is trusted as-is.
func (r *SchemaRegistry) fetchURL(ctx context.Context, url string, id int) (SchemaInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return SchemaInfo{}, err
	}
	if r.cfg.Username != "" {
		req.SetBasicAuth(r.cfg.Username, r.cfg.Password)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return SchemaInfo{}, fmt.Errorf("schema registry GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return SchemaInfo{}, fmt.Errorf(
			"schema registry status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(body)),
		)
	}

	var body struct {
		ID         int               `json:"id"`
		Schema     string            `json:"schema"`
		SchemaType string            `json:"schemaType"`
		Subject    string            `json:"subject"`
		Version    int               `json:"version"`
		References []SchemaReference `json:"references"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return SchemaInfo{}, fmt.Errorf("schema registry decode: %w", err)
	}
	typ := body.SchemaType
	if typ == "" {
		typ = "AVRO"
	}
	out := SchemaInfo{
		ID:         id,
		Type:       typ,
		Subject:    body.Subject,
		Version:    body.Version,
		Schema:     body.Schema,
		References: body.References,
	}
	if id < 0 {
		out.ID = body.ID
	}
	return out, nil
}

// ErrSchemaTypeUnsupported is returned by DecodeValue when the schema type is
// known (e.g. PROTOBUF) but no decoder is wired in.
var ErrSchemaTypeUnsupported = errors.New("schema type not supported for inline decoding")
