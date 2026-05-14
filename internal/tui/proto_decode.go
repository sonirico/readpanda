package tui

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bufbuild/protocompile"
	"github.com/sonirico/readpanda/pkg/rp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoDecoder compiles Confluent Schema Registry-hosted .proto sources at
// runtime and decodes binary payloads to JSON. Compiled file descriptors are
// cached by SR schema id; resolved schema sources are cached by filename.
type protoDecoder struct {
	sr *rp.SchemaRegistry

	mu        sync.Mutex
	fileCache map[int]protoreflect.FileDescriptor // by SR schema id
}

func newProtoDecoder(sr *rp.SchemaRegistry) *protoDecoder {
	return &protoDecoder{
		sr:        sr,
		fileCache: map[int]protoreflect.FileDescriptor{},
	}
}

// Decode parses Confluent proto wire format (message indexes + payload), looks
// up the right MessageDescriptor inside the compiled file, and renders the
// resulting message as indented JSON.
func (d *protoDecoder) Decode(
	ctx context.Context, info rp.SchemaInfo, rawPayload []byte,
) (string, error) {
	indexes, payload, err := parseProtoMessageIndexes(rawPayload)
	if err != nil {
		return "", fmt.Errorf("parse message indexes: %w", err)
	}

	fd, err := d.fileForSchema(ctx, info)
	if err != nil {
		return "", err
	}

	md, err := messageByIndexes(fd, indexes)
	if err != nil {
		return "", err
	}

	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return "", fmt.Errorf("proto unmarshal: %w", err)
	}
	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("protojson marshal: %w", err)
	}
	return string(out), nil
}

func (d *protoDecoder) fileForSchema(
	ctx context.Context, info rp.SchemaInfo,
) (protoreflect.FileDescriptor, error) {
	d.mu.Lock()
	cached, ok := d.fileCache[info.ID]
	d.mu.Unlock()
	if ok {
		return cached, nil
	}

	// Walk the reference tree, building a map of filename -> .proto source.
	sources := map[string]string{}
	rootName := info.Subject
	if rootName == "" {
		rootName = fmt.Sprintf("schema-%d.proto", info.ID)
	}
	sources[rootName] = info.Schema
	if err := d.collectReferences(ctx, info.References, sources); err != nil {
		return nil, err
	}

	// Wrap our SR-backed resolver with WithStandardImports so the bundled
	// well-known types (google/protobuf/descriptor.proto, timestamp.proto,
	// etc.) are available without going to SR for them — services routinely
	// import them for custom annotations.
	compiler := protocompile.Compiler{
		Resolver:       protocompile.WithStandardImports(&mapResolver{sources: sources}),
		SourceInfoMode: protocompile.SourceInfoNone,
	}
	fds, err := compiler.Compile(ctx, rootName)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", rootName, err)
	}
	fd := fds.FindFileByPath(rootName)
	if fd == nil {
		return nil, fmt.Errorf("file %s not found after compile", rootName)
	}

	d.mu.Lock()
	d.fileCache[info.ID] = fd
	d.mu.Unlock()
	return fd, nil
}

func (d *protoDecoder) collectReferences(
	ctx context.Context, refs []rp.SchemaReference, sources map[string]string,
) error {
	for _, r := range refs {
		if _, ok := sources[r.Name]; ok {
			continue
		}
		info, err := d.sr.FetchBySubjectVersion(ctx, r.Subject, r.Version)
		if err != nil {
			return fmt.Errorf("resolve %s (subject=%s v=%d): %w",
				r.Name, r.Subject, r.Version, err)
		}
		sources[r.Name] = info.Schema
		if err := d.collectReferences(ctx, info.References, sources); err != nil {
			return err
		}
	}
	return nil
}

// mapResolver feeds protocompile's compiler from an in-memory map of filename
// to .proto source string.
type mapResolver struct {
	sources map[string]string
}

func (r *mapResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	src, ok := r.sources[path]
	if !ok {
		return protocompile.SearchResult{}, fmt.Errorf("source not found: %s", path)
	}
	return protocompile.SearchResult{Source: stringReader(src)}, nil
}

func stringReader(s string) io.Reader { return strings.NewReader(s) }

// parseProtoMessageIndexes reads the Confluent message-index prefix that
// follows the 5-byte schema header. Per the Confluent wire spec, the prefix
// is a varint count followed by `count` zig-zag varints. As an optimisation,
// a single 0 byte means "the default first message" (indexes == [0]).
func parseProtoMessageIndexes(p []byte) (indexes []int, payload []byte, err error) {
	if len(p) == 0 {
		return nil, nil, errors.New("empty payload")
	}
	count, off := binary.Varint(p)
	if off <= 0 {
		return nil, nil, errors.New("bad index-count varint")
	}
	if count == 0 {
		return []int{0}, p[off:], nil
	}
	indexes = make([]int, 0, count)
	for i := int64(0); i < count; i++ {
		v, k := binary.Varint(p[off:])
		if k <= 0 {
			return nil, nil, fmt.Errorf("bad index varint at %d", i)
		}
		off += k
		indexes = append(indexes, int(v))
	}
	return indexes, p[off:], nil
}

// messageByIndexes walks the file's message tree following the index path.
// indexes[0] picks a top-level message; subsequent indexes pick nested
// messages within the previously-picked one.
func messageByIndexes(
	fd protoreflect.FileDescriptor, indexes []int,
) (protoreflect.MessageDescriptor, error) {
	if fd.Messages().Len() == 0 {
		return nil, errors.New("file has no messages")
	}
	if len(indexes) == 0 {
		return fd.Messages().Get(0), nil
	}
	msgs := fd.Messages()
	var md protoreflect.MessageDescriptor
	for level, idx := range indexes {
		if idx < 0 || idx >= msgs.Len() {
			return nil, fmt.Errorf(
				"message index %d out of range at depth %d (len=%d)",
				idx, level, msgs.Len(),
			)
		}
		md = msgs.Get(idx)
		msgs = md.Messages()
	}
	return md, nil
}
