package rp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// AdminConfig holds connection settings for the Admin client.
// Fields mirror ConsumerConfig/ProducerConfig for SASL and TLS.
type AdminConfig struct {
	Brokers  []string
	SASLUser string
	SASLPass string
	TLS      bool
}

// Admin wraps a kadm.Client for administrative Redpanda/Kafka operations
// such as querying consumer group lag.
type Admin struct {
	cl *kadm.Client
}

// NewAdmin creates an Admin backed by a kadm.Client.
func NewAdmin(cfg AdminConfig) (*Admin, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("brokers: %w", ErrConfig)
	}

	// Admin connections are interactive (used by the TUI / one-off tools), not
	// throughput-critical. Fail fast on dial errors instead of spending the
	// default ~30 s burning retries against unreachable brokers — this is what
	// turns a single dead broker into a flood of "i/o timeout" log lines.
	//
	// kgo refuses to accept both Dialer and DialTLSConfig, so when TLS is on
	// we wrap our timeout-aware net.Dialer with a tls.Dialer and pass the
	// composite through kgo.Dialer.
	netDialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	dialFn := netDialer.DialContext
	if cfg.TLS {
		tlsDialer := &tls.Dialer{NetDialer: netDialer, Config: &tls.Config{}}
		dialFn = tlsDialer.DialContext
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.Dialer(dialFn),
		kgo.RequestRetries(0),
		kgo.RetryTimeout(3 * time.Second),
	}

	if cfg.SASLUser != "" && cfg.SASLPass != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPass,
		}.AsSha256Mechanism()))
	}

	kgoCl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo client: %w", err)
	}

	return &Admin{cl: kadm.NewClient(kgoCl)}, nil
}

// GroupTopicLag holds the summed lag across all partitions for one
// consumer-group + topic pair. When Err is non-nil the entry represents a
// per-group failure (describe or offset-fetch) and Topic/Lag are zero-valued.
type GroupTopicLag struct {
	Group string
	Topic string
	Lag   int64
	Err   error
}

// Lag returns the total lag (summed across partitions) for each
// consumer-group / topic combination. Only the requested groups are queried.
// Per-group failures are returned as entries with Err set rather than being
// silently dropped.
func (a *Admin) Lag(ctx context.Context, groups ...string) ([]GroupTopicLag, error) {
	described, err := a.cl.Lag(ctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("kadm lag: %w", err)
	}

	var results []GroupTopicLag
	described.Each(func(gl kadm.DescribedGroupLag) {
		if gl.DescribeErr != nil {
			results = append(results, GroupTopicLag{
				Group: gl.Group,
				Err:   fmt.Errorf("describe group: %w", gl.DescribeErr),
			})
			return
		}
		if gl.FetchErr != nil {
			results = append(results, GroupTopicLag{
				Group: gl.Group,
				Err:   fmt.Errorf("fetch offsets: %w", gl.FetchErr),
			})
			return
		}
		for _, tl := range gl.Lag.TotalByTopic().Sorted() {
			results = append(results, GroupTopicLag{
				Group: gl.Group,
				Topic: tl.Topic,
				Lag:   tl.Lag,
			})
		}
	})
	return results, nil
}

// Close releases the underlying Kafka client resources.
func (a *Admin) Close() {
	a.cl.Close()
}
