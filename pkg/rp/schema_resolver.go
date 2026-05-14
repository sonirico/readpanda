package rp

import "context"

// SchemaResolver resolves a Confluent wire-format schema ID at producer init
// time. The returned [4]byte is the big-endian uint32 schema ID that gets
// embedded in every published message as a 6-byte header
// ([0x00, schemaID[0..3], 0x00, payload...]). Return a zero [4]byte to skip
// header injection.
type SchemaResolver interface {
	Resolve(ctx context.Context) ([4]byte, error)
}
