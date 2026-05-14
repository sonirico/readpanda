// Package profile reads rpk profiles from ~/.config/rpk/rpk.yaml and exposes
// them in a shape the readpanda TUI can consume directly.
//
// We deliberately parse only the fields readpanda needs (kafka_api: brokers,
// sasl, tls). Unknown fields are tolerated so we don't break when rpk evolves
// its schema.
package profile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ErrNoProfile means the rpk config exists but has no usable profile (either
// no current_profile is set or the named profile is missing).
var ErrNoProfile = errors.New("no rpk profile selected")

// ErrConfigNotFound means the rpk config file does not exist on disk. Callers
// can fall back to CLI flags.
var ErrConfigNotFound = errors.New("rpk config not found")

// Profile is the readpanda-friendly view of one rpk profile.
type Profile struct {
	Name     string
	Brokers  []string
	SASLUser string
	SASLPass string
	SASLMech string
	TLS      bool

	// Schema Registry. Empty Addresses means SR is not configured.
	SRAddresses []string
	SRUser      string
	SRPass      string
	SRTLS       bool
}

// File is the parsed rpk.yaml — the full set of profiles plus the active one.
type File struct {
	Path           string
	CurrentProfile string
	Profiles       []Profile
}

// Current returns the active profile from the parsed file.
func (f *File) Current() (Profile, error) {
	if f.CurrentProfile == "" {
		return Profile{}, ErrNoProfile
	}
	for _, p := range f.Profiles {
		if p.Name == f.CurrentProfile {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("profile %q: %w", f.CurrentProfile, ErrNoProfile)
}

// Find returns a profile by name.
func (f *File) Find(name string) (Profile, error) {
	for _, p := range f.Profiles {
		if p.Name == name {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("profile %q: %w", name, ErrNoProfile)
}

// Load reads and parses the rpk config file at the default location
// ($XDG_CONFIG_HOME/rpk/rpk.yaml, falling back to ~/.config/rpk/rpk.yaml).
func Load() (*File, error) {
	path, err := defaultConfigPath()
	if err != nil {
		return nil, err
	}
	return LoadFrom(path)
}

// LoadFrom parses the rpk config file at the given path.
func LoadFrom(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s: %w", path, ErrConfigNotFound)
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	out := &File{
		Path:           path,
		CurrentProfile: raw.CurrentProfile,
	}
	for _, p := range raw.Profiles {
		out.Profiles = append(out.Profiles, p.toProfile())
	}
	return out, nil
}

// defaultConfigPath returns the first rpk config path that exists on disk.
// rpk picks its storage location per platform: XDG/~/.config on Linux,
// ~/Library/Application Support on macOS, %AppData% on Windows. We probe the
// known locations in priority order rather than depending on runtime.GOOS.
func defaultConfigPath() (string, error) {
	candidates, err := candidateConfigPaths()
	if err != nil {
		return "", err
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no rpk config candidate paths")
	}
	return candidates[0], nil
}

func candidateConfigPaths() ([]string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	var out []string
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		out = append(out, filepath.Join(xdg, "rpk", "rpk.yaml"))
	}
	out = append(out,
		filepath.Join(home, "Library", "Application Support", "rpk", "rpk.yaml"),
		filepath.Join(home, ".config", "rpk", "rpk.yaml"),
	)
	if appData := os.Getenv("APPDATA"); appData != "" {
		out = append(out, filepath.Join(appData, "rpk", "rpk.yaml"))
	}
	return out, nil
}

type rawConfig struct {
	CurrentProfile string       `yaml:"current_profile"`
	Profiles       []rawProfile `yaml:"profiles"`
}

type rawProfile struct {
	Name           string            `yaml:"name"`
	KafkaAPI       rawKafkaAPI       `yaml:"kafka_api"`
	SchemaRegistry rawSchemaRegistry `yaml:"schema_registry"`
}

type rawSchemaRegistry struct {
	Addresses []string `yaml:"addresses"`
	TLS       *rawTLS  `yaml:"tls"`
	// rpk stores SR auth either inline or in `basic_auth`; accept both.
	BasicAuth *rawBasicAuth `yaml:"basic_auth"`
	Username  string        `yaml:"username"`
	Password  string        `yaml:"password"`
}

type rawBasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type rawKafkaAPI struct {
	Brokers []string `yaml:"brokers"`
	TLS     *rawTLS  `yaml:"tls"`
	SASL    *rawSASL `yaml:"sasl"`
}

// rawTLS mirrors rpk's TLS block. In rpk, the *presence* of the tls block
// (even empty) signals TLS enabled — there is no explicit `enabled` field in
// upstream rpk schema. We additionally accept `enabled: false` as an opt-out
// for hand-written configs.
type rawTLS struct {
	Enabled *bool `yaml:"enabled"`
}

type rawSASL struct {
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Mechanism string `yaml:"mechanism"`
}

func (p rawProfile) toProfile() Profile {
	out := Profile{
		Name:    p.Name,
		Brokers: append([]string(nil), p.KafkaAPI.Brokers...),
	}
	if p.KafkaAPI.SASL != nil {
		out.SASLUser = p.KafkaAPI.SASL.User
		out.SASLPass = p.KafkaAPI.SASL.Password
		out.SASLMech = p.KafkaAPI.SASL.Mechanism
	}
	if p.KafkaAPI.TLS != nil {
		out.TLS = true
		if p.KafkaAPI.TLS.Enabled != nil && !*p.KafkaAPI.TLS.Enabled {
			out.TLS = false
		}
	}

	out.SRAddresses = append([]string(nil), p.SchemaRegistry.Addresses...)
	if p.SchemaRegistry.BasicAuth != nil {
		out.SRUser = p.SchemaRegistry.BasicAuth.Username
		out.SRPass = p.SchemaRegistry.BasicAuth.Password
	}
	if out.SRUser == "" {
		out.SRUser = p.SchemaRegistry.Username
	}
	if out.SRPass == "" {
		out.SRPass = p.SchemaRegistry.Password
	}
	if p.SchemaRegistry.TLS != nil {
		out.SRTLS = true
		if p.SchemaRegistry.TLS.Enabled != nil && !*p.SchemaRegistry.TLS.Enabled {
			out.SRTLS = false
		}
	}
	return out
}
