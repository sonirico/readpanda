package rp

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// TailConfig configures an ephemeral, groupless tail consumer. It consumes
// directly from every partition of the requested topic — no consumer group is
// created on the broker, so repeated tail sessions don't leave behind empty
// groups for the cluster to garbage-collect.
type TailConfig struct {
	Brokers  []string
	SASLUser string
	SASLPass string
	TLS      bool

	// Topic to tail.
	Topic string

	// FromStart, if true, starts at the oldest available record; otherwise the
	// tail begins at the end (k9s/kcat default).
	FromStart bool
}

// TailConsumer is a groupless, partition-assigned consumer. Close() releases
// the underlying kgo client.
type TailConsumer struct {
	cl        *kgo.Client
	closeOnce sync.Once
}

// NewTailConsumer builds a TailConsumer for one topic.
func NewTailConsumer(cfg TailConfig) (*TailConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("brokers: %w", ErrConfig)
	}
	if cfg.Topic == "" {
		return nil, ErrTopicsRequired
	}

	reset := kgo.NewOffset().AtEnd()
	if cfg.FromStart {
		reset = kgo.NewOffset().AtStart()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(reset),
	}
	if cfg.SASLUser != "" && cfg.SASLPass != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPass,
		}.AsSha256Mechanism()))
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo client: %w", err)
	}
	return &TailConsumer{cl: cl}, nil
}

// Run polls records until ctx is cancelled, invoking handler for each record.
// Errors returned by handler abort the loop. Returning ErrConsumerDone is a
// graceful stop signal.
func (t *TailConsumer) Run(ctx context.Context, handler ConsumerHandler) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := t.cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("poll %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			msg := Msg{
				Topic:            rec.Topic,
				Key:              rec.Key,
				Value:            rec.Value,
				Partition:        rec.Partition,
				Offset:           rec.Offset,
				Ts:               rec.Timestamp,
				CompressionCodec: compressionCodecName(rec.Attrs.CompressionType()),
			}
			for _, h := range rec.Headers {
				msg.Headers = append(msg.Headers, Header{Key: h.Key, Value: h.Value})
			}
			if err := handler(ctx, msg); err != nil {
				if err == ErrConsumerDone {
					return nil
				}
				return err
			}
		}
	}
}

// Close shuts down the underlying client. Safe to call multiple times.
func (t *TailConsumer) Close() {
	t.closeOnce.Do(func() { t.cl.Close() })
}

// compressionCodecName maps Kafka's batch compression code (0..4) to a
// human-readable string. See KIP-31 / KIP-32 for the wire-format definition.
func compressionCodecName(code uint8) string {
	switch code {
	case 0:
		return "none"
	case 1:
		return "gzip"
	case 2:
		return "snappy"
	case 3:
		return "lz4"
	case 4:
		return "zstd"
	}
	return fmt.Sprintf("unknown(%d)", code)
}
