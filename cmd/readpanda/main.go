package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sonirico/readpanda/internal/profile"
	"github.com/sonirico/readpanda/internal/tui"
	"github.com/sonirico/readpanda/pkg/rp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "readpanda:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseFlags()
	if err != nil {
		return err
	}

	prof, profFile, err := resolveProfile(cfg)
	if err != nil {
		return err
	}

	app, err := tui.NewApp(tui.AppConfig{
		Profile:         prof,
		ProfileFile:     profFile,
		AdminFactory:    adminFromProfile,
		RegistryFactory: registryFromProfile,
	})
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return app.Run(ctx)
}

type cliConfig struct {
	profileName string
	brokers     string
	saslUser    string
	saslPass    string
	tls         bool
	srURL       string
	srUser      string
	srPass      string
}

func parseFlags() (cliConfig, error) {
	var c cliConfig
	flag.StringVar(
		&c.profileName,
		"profile",
		"",
		"rpk profile name (defaults to current_profile in rpk.yaml)",
	)
	flag.StringVar(&c.brokers, "brokers", "", "comma-separated brokers (overrides profile)")
	flag.StringVar(&c.saslUser, "sasl-user", "", "SASL username (overrides profile)")
	flag.StringVar(&c.saslPass, "sasl-pass", "", "SASL password (overrides profile)")
	flag.BoolVar(&c.tls, "tls", false, "enable TLS (overrides profile)")
	flag.StringVar(
		&c.srURL,
		"sr-url",
		os.Getenv("READPANDA_SR_URL"),
		"Schema Registry URL (env: READPANDA_SR_URL)",
	)
	flag.StringVar(
		&c.srUser,
		"sr-user",
		os.Getenv("READPANDA_SR_USER"),
		"Schema Registry basic-auth username (env: READPANDA_SR_USER)",
	)
	flag.StringVar(
		&c.srPass,
		"sr-pass",
		os.Getenv("READPANDA_SR_PASS"),
		"Schema Registry basic-auth password (env: READPANDA_SR_PASS)",
	)
	flag.Parse()
	return c, nil
}

func resolveProfile(cfg cliConfig) (profile.Profile, *profile.File, error) {
	file, err := profile.Load()
	var prof profile.Profile
	switch {
	case err == nil:
		if cfg.profileName != "" {
			prof, err = file.Find(cfg.profileName)
		} else {
			prof, err = file.Current()
		}
		if err != nil && cfg.brokers == "" {
			return profile.Profile{}, file, fmt.Errorf("resolve profile: %w", err)
		}
	case errors.Is(err, profile.ErrConfigNotFound):
		file = nil
	default:
		return profile.Profile{}, nil, err
	}

	if cfg.brokers != "" {
		prof = profile.Profile{
			Name:     orDefault(cfg.profileName, "cli"),
			Brokers:  strings.Split(cfg.brokers, ","),
			SASLUser: cfg.saslUser,
			SASLPass: cfg.saslPass,
			TLS:      cfg.tls,
		}
	}

	if len(prof.Brokers) == 0 {
		return profile.Profile{}, file,
			errors.New("no brokers configured: set rpk profile or pass --brokers")
	}

	// SR overrides: flags and env vars beat the rpk profile so users with SR
	// creds in a .envrc / direnv setup don't need to edit rpk.yaml.
	if cfg.srURL != "" {
		prof.SRAddresses = []string{cfg.srURL}
	}
	if cfg.srUser != "" {
		prof.SRUser = cfg.srUser
	}
	if cfg.srPass != "" {
		prof.SRPass = cfg.srPass
	}
	return prof, file, nil
}

func adminFromProfile(p profile.Profile) (*rp.Admin, error) {
	return rp.NewAdmin(rp.AdminConfig{
		Brokers:  p.Brokers,
		SASLUser: p.SASLUser,
		SASLPass: p.SASLPass,
		TLS:      p.TLS,
	})
}

func registryFromProfile(p profile.Profile) (*rp.SchemaRegistry, error) {
	if len(p.SRAddresses) == 0 {
		return nil, nil
	}
	// rpk does not expose a separate schema_registry.basic_auth field — it
	// reuses the kafka_api SASL credentials for Schema Registry HTTP basic
	// auth. Mirror that fallback so users get SR working without copy-pasting
	// credentials around.
	user, pass := p.SRUser, p.SRPass
	if user == "" {
		user = p.SASLUser
	}
	if pass == "" {
		pass = p.SASLPass
	}
	return rp.NewSchemaRegistry(rp.SchemaRegistryConfig{
		Addresses: p.SRAddresses,
		Username:  user,
		Password:  pass,
	})
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
