package rp

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestHTTPSchemaResolver(url, topic string) *HTTPSchemaResolver {
	return &HTTPSchemaResolver{
		URL:    url,
		Key:    "test-key",
		Secret: "test-secret",
		Topic:  topic,
	}
}

func TestHTTPSchemaResolver(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		schemaID   int
		statusCode int
		body       string
		wantErr    string
		wantID     [4]byte
	}

	tests := []testCase{
		{
			name:     "resolves schema ID from registry",
			schemaID: 42,
			wantID:   uint32ToSchemaID(42),
		},
		{
			name:     "resolves large schema ID",
			schemaID: 100_123,
			wantID:   uint32ToSchemaID(100_123),
		},
		{
			name:       "returns error on persistent 500",
			statusCode: http.StatusInternalServerError,
			wantErr:    "schema registry retries exhausted",
		},
		{
			name:       "returns error on 404",
			statusCode: http.StatusNotFound,
			wantErr:    "schema registry retries exhausted",
		},
		{
			name:       "returns error on malformed JSON",
			statusCode: http.StatusOK,
			body:       `not json`,
			wantErr:    "invalid character",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestSchemaServer(tc.statusCode, tc.schemaID, tc.body)
			t.Cleanup(srv.Close)

			resolver := newTestHTTPSchemaResolver(srv.URL, "test-topic")

			id, err := resolver.Resolve(context.Background())

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantID, id)
		})
	}
}

func TestHTTPSchemaResolver_basic_auth(t *testing.T) {
	t.Parallel()

	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		fmt.Fprintf(w, `{"id": 1}`)
	}))
	t.Cleanup(srv.Close)

	resolver := newTestHTTPSchemaResolver(srv.URL, "my-topic")
	resolver.Key = "my-api-key"
	resolver.Secret = "my-api-secret"

	_, err := resolver.Resolve(context.Background())

	require.NoError(t, err)
	require.Equal(t, "my-api-key", gotUser)
	require.Equal(t, "my-api-secret", gotPass)
}

func TestHTTPSchemaResolver_no_auth_when_key_empty(t *testing.T) {
	t.Parallel()

	var gotAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, gotAuth = r.BasicAuth()
		fmt.Fprintf(w, `{"id": 1}`)
	}))
	t.Cleanup(srv.Close)

	resolver := newTestHTTPSchemaResolver(srv.URL, "my-topic")
	resolver.Key = ""
	resolver.Secret = ""

	_, err := resolver.Resolve(context.Background())

	require.NoError(t, err)
	require.False(t, gotAuth)
}

func TestHTTPSchemaResolver_subject_path(t *testing.T) {
	t.Parallel()

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		fmt.Fprintf(w, `{"id": 1}`)
	}))
	t.Cleanup(srv.Close)

	resolver := newTestHTTPSchemaResolver(
		srv.URL,
		"chesscom.ceac.v1.game_state_generated_advstats_backfill",
	)

	_, err := resolver.Resolve(context.Background())

	require.NoError(t, err)
	require.Equal(
		t,
		"/subjects/chesscom.ceac.v1.game_state_generated_advstats_backfill-value/versions/latest",
		gotPath,
	)
}

func TestHTTPSchemaResolver_retries_on_transient_failure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"id": 99}`)
	}))
	t.Cleanup(srv.Close)

	resolver := newTestHTTPSchemaResolver(srv.URL, "test-topic")

	id, err := resolver.Resolve(context.Background())

	require.NoError(t, err)
	require.Equal(t, uint32ToSchemaID(99), id)
	require.GreaterOrEqual(t, attempts.Load(), int32(3))
}

func TestHTTPSchemaResolver_respects_context_cancellation(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"id": 1}`)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resolver := newTestHTTPSchemaResolver(srv.URL, "test-topic")

	_, err := resolver.Resolve(ctx)

	require.Error(t, err)
}

func newTestSchemaServer(statusCode, schemaID int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if statusCode != 0 && statusCode != http.StatusOK {
			w.WriteHeader(statusCode)
			return
		}
		if body != "" {
			fmt.Fprint(w, body)
			return
		}
		fmt.Fprintf(w, `{"id": %d}`, schemaID)
	}))
}

func uint32ToSchemaID(v uint32) [4]byte {
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], v)
	return id
}
