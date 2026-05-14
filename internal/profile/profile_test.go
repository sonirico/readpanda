package profile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadFrom(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name    string
		yaml    string
		wantErr error
		assert  func(t *testing.T, f *File)
	}

	cases := []testCase{
		{
			name: "single profile with SASL+TLS",
			yaml: `
current_profile: prod
profiles:
  - name: prod
    kafka_api:
      brokers:
        - broker1:9092
        - broker2:9092
      sasl:
        user: alice
        password: s3cret
        mechanism: SCRAM-SHA-256
      tls: {}
`,
			assert: func(t *testing.T, f *File) {
				require.Equal(t, "prod", f.CurrentProfile)
				p, err := f.Current()
				require.NoError(t, err)
				require.Equal(t, "prod", p.Name)
				require.Equal(t, []string{"broker1:9092", "broker2:9092"}, p.Brokers)
				require.Equal(t, "alice", p.SASLUser)
				require.Equal(t, "s3cret", p.SASLPass)
				require.Equal(t, "SCRAM-SHA-256", p.SASLMech)
				require.True(t, p.TLS)
			},
		},
		{
			name: "plain local profile no auth",
			yaml: `
current_profile: local
profiles:
  - name: local
    kafka_api:
      brokers: [localhost:9092]
`,
			assert: func(t *testing.T, f *File) {
				p, err := f.Current()
				require.NoError(t, err)
				require.Equal(t, []string{"localhost:9092"}, p.Brokers)
				require.False(t, p.TLS)
				require.Empty(t, p.SASLUser)
			},
		},
		{
			name: "tls explicitly disabled with enabled:false",
			yaml: `
current_profile: x
profiles:
  - name: x
    kafka_api:
      brokers: [a:9092]
      tls:
        enabled: false
`,
			assert: func(t *testing.T, f *File) {
				p, err := f.Current()
				require.NoError(t, err)
				require.False(t, p.TLS)
			},
		},
		{
			name: "multiple profiles, current_profile points to second",
			yaml: `
current_profile: staging
profiles:
  - name: prod
    kafka_api: {brokers: [p:9092]}
  - name: staging
    kafka_api: {brokers: [s:9092]}
`,
			assert: func(t *testing.T, f *File) {
				p, err := f.Current()
				require.NoError(t, err)
				require.Equal(t, "staging", p.Name)
				require.Len(t, f.Profiles, 2)
			},
		},
		{
			name: "unknown current_profile name returns ErrNoProfile",
			yaml: `
current_profile: ghost
profiles:
  - name: prod
    kafka_api: {brokers: [p:9092]}
`,
			wantErr: ErrNoProfile,
			assert: func(t *testing.T, f *File) {
				_, err := f.Current()
				require.ErrorIs(t, err, ErrNoProfile)
			},
		},
		{
			name: "ignores unknown top-level fields",
			yaml: `
version: 6
current_profile: prod
some_future_field: hello
profiles:
  - name: prod
    kafka_api: {brokers: [x:9092]}
    admin_api: {addresses: [y:9644]}
`,
			assert: func(t *testing.T, f *File) {
				p, err := f.Current()
				require.NoError(t, err)
				require.Equal(t, []string{"x:9092"}, p.Brokers)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "rpk.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.yaml), 0o600))

			f, err := LoadFrom(path)
			require.NoError(t, err)

			if tc.assert != nil {
				tc.assert(t, f)
			}
		})
	}
}

func TestLoadFrom_NotFound(t *testing.T) {
	t.Parallel()
	_, err := LoadFrom(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConfigNotFound))
}

func TestFile_Find(t *testing.T) {
	t.Parallel()
	f := &File{Profiles: []Profile{{Name: "a"}, {Name: "b"}}}
	p, err := f.Find("b")
	require.NoError(t, err)
	require.Equal(t, "b", p.Name)

	_, err = f.Find("c")
	require.ErrorIs(t, err, ErrNoProfile)
}
