package rp

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type testSchemaResolver struct {
	id  [4]byte
	err error
}

func (r *testSchemaResolver) Resolve(context.Context) ([4]byte, error) {
	return r.id, r.err
}

func newTestSchemaResolver(id [4]byte, err error) *testSchemaResolver {
	return &testSchemaResolver{id: id, err: err}
}

func TestNewProducer_schema_resolver(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		resolver SchemaResolver
		wantErr  string
	}

	tests := []testCase{
		{
			name:     "nil resolver is accepted",
			resolver: nil,
		},
		{
			name:     "successful resolve is accepted",
			resolver: newTestSchemaResolver(uint32ToSchemaID(42), nil),
		},
		{
			name:     "failed resolve returns error",
			resolver: newTestSchemaResolver([4]byte{}, fmt.Errorf("connection refused")),
			wantErr:  "schema resolver failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewProducer(context.Background(), ProducerConfig{
				Brokers:        []string{"localhost:9092"},
				SchemaResolver: tc.resolver,
			})

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			// NewProducer will fail on Ping since there's no real broker,
			// but that's after the schema resolver check.
			if err != nil {
				require.NotContains(t, err.Error(), "schema resolver")
			}
		})
	}
}
