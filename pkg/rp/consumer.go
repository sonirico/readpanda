package rp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultPollRecords = 100
)

// ErrConsumerDone can be returned by a handler to signal graceful stop.
// The consumer will stop polling without treating it as an error.
var ErrConsumerDone = errors.New("consumer done")

// LogLevel mirrors kgo.LogLevel for configuration.
type LogLevel kgo.LogLevel

const (
	LogLevelNone  LogLevel = LogLevel(kgo.LogLevelNone)
	LogLevelError LogLevel = LogLevel(kgo.LogLevelError)
	LogLevelWarn  LogLevel = LogLevel(kgo.LogLevelWarn)
	LogLevelInfo  LogLevel = LogLevel(kgo.LogLevelInfo)
	LogLevelDebug LogLevel = LogLevel(kgo.LogLevelDebug)
)

type (
	ConsumerConfig struct {
		Brokers []string

		ConsumerGroup                string
		ConsumerBlockRebalanceOnPoll bool
		WithLogger                   bool
		WithLogLevel                 LogLevel
		MaxPollRecords               int
		WithLoggingHooks             bool
		WithAppName                  string
		WithVersion                  string

		// SASL authentication
		SASLUser string
		SASLPass string
		TLS      bool

		// DisableCommit skips offset commits (useful for one-off consumers)
		DisableCommit bool

		// DisableAutoCommit disables auto-commit (default: false = auto-commit enabled)
		// When disabled, commits happen synchronously in Subscribe/Poll
		// When enabled, use MarkCommitRecords for async commits
		DisableAutoCommit bool

		// SeekFromEnd, if positive, makes the consumer start reading from
		// (latest - SeekFromEnd) on every startup, ignoring any committed offset.
		SeekFromEnd int

		// StripSchemaHeaders removes Confluent Schema Registry wire format headers
		// (magic byte + 4-byte schema ID) from message values. Default: true
		StripSchemaHeaders *bool

		// OTel enables OpenTelemetry tracing for consume operations.
		OTel *OTelConfig

		// Monitor enables Prometheus metrics via kgo hooks. Nil disables.
		Monitor *ConsumerMonitor
	}

	BasicConsumer struct {
		cfg ConsumerConfig

		topics             []string
		client             *kgo.Client
		tracer             trace.Tracer
		stripSchemaHeaders bool

		closed bool

		closeOnce sync.Once

		// Partition assignment tracking
		partitionsMu     sync.RWMutex
		partitionsReady  bool
		readyCh          chan struct{} // closed when partitions first assigned
		readyChCloseOnce sync.Once
	}

	consumerHandler func(ctx context.Context, m *kgo.Record) error
)

func (c *BasicConsumer) Ping(ctx context.Context) (err error) {
	err = c.client.Ping(ctx)
	return
}

func (c *BasicConsumer) Close() {
	c.safeClose()
}

// Ready returns true when partitions have been assigned and the consumer is ready to poll.
func (c *BasicConsumer) Ready() bool {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	return c.partitionsReady
}

// WaitForReady blocks until partitions are assigned or context is cancelled.
// Only usable for the initial partition assignment, not for rebalancing.
func (c *BasicConsumer) WaitForReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *BasicConsumer) Subscribe(ctx context.Context, handler ConsumerHandler) error {
	if c.closed {
		return ErrConsumerClosed
	}

	return c.start(ctx, func(ctx context.Context, rec *kgo.Record) (err error) {
		// Extract trace context and start span if OTel is enabled
		if c.tracer != nil {
			headers := make(map[string]string)
			for _, h := range rec.Headers {
				headers[h.Key] = string(h.Value)
			}
			ctx = extractTraceContext(ctx, headers)

			var span trace.Span
			ctx, span = startConsumerSpan(ctx, c.tracer, rec.Topic, rec.Partition, rec.Offset)
			defer func() {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				span.End()
			}()
		}

		value := rec.Value
		if c.stripSchemaHeaders {
			value = stripSchemaHeaders(value)
		}

		var hdrs []Header
		for _, h := range rec.Headers {
			hdrs = append(hdrs, Header{Key: h.Key, Value: h.Value})
		}
		err = handler(
			ctx,
			Msg{
				Topic:     rec.Topic,
				Key:       rec.Key,
				Value:     value,
				Headers:   hdrs,
				Partition: rec.Partition,
				Ts:        rec.Timestamp,
			},
		)

		return err
	})
}

func NewConsumer(
	cfg ConsumerConfig,
	topics []string,
) (*BasicConsumer, error) {
	if len(topics) < 1 {
		return nil, ErrTopicsRequired
	}

	// Without a consumer group there is no group coordinator to commit offsets
	if cfg.ConsumerGroup == "" {
		cfg.DisableCommit = true
	}

	if cfg.MaxPollRecords == 0 {
		cfg.MaxPollRecords = defaultPollRecords
	}

	stripHeaders := true
	if cfg.StripSchemaHeaders != nil {
		stripHeaders = *cfg.StripSchemaHeaders
	}

	c := &BasicConsumer{
		cfg:                cfg,
		topics:             topics,
		tracer:             cfg.OTel.tracer(),
		stripSchemaHeaders: stripHeaders,
		readyCh:            make(chan struct{}),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(topics...),
	}

	if cfg.DisableAutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	// SASL authentication
	if cfg.SASLUser != "" && cfg.SASLPass != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPass,
		}.AsSha256Mechanism()))
	}

	// TLS
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}

	if cfg.WithAppName != "" && cfg.WithVersion != "" {
		opts = append(opts, kgo.SoftwareNameAndVersion(cfg.WithAppName, cfg.WithVersion))
	}

	if cfg.SeekFromEnd > 0 {
		opts = append(opts, kgo.ConsumeResetOffset(
			kgo.NewOffset().AtEnd().Relative(-int64(cfg.SeekFromEnd)),
		))
	}

	if cfg.ConsumerBlockRebalanceOnPoll {
		opts = append(opts, kgo.BlockRebalanceOnPoll())
	}

	if cfg.Monitor != nil {
		opts = append(opts, kgo.WithHooks(cfg.Monitor))
	}

	if cfg.ConsumerGroup != "" {
		opts = append(opts, kgo.OnPartitionsAssigned(
			func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
				c.partitionsMu.Lock()
				c.partitionsReady = true
				c.partitionsMu.Unlock()
				c.readyChCloseOnce.Do(func() { close(c.readyCh) })
				if cfg.Monitor != nil {
					cfg.Monitor.OnAssigned(assigned)
				}
				if cfg.WithLoggingHooks {
					log.Debugf("PARTITIONS ASSIGNED => %v", assigned)
				}
			},
		))
		opts = append(opts, kgo.OnPartitionsRevoked(
			func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
				c.partitionsMu.Lock()
				c.partitionsReady = false
				c.partitionsMu.Unlock()
				if cfg.Monitor != nil {
					cfg.Monitor.OnRevoked(revoked)
				}
				if cfg.WithLoggingHooks {
					log.Debugf("PARTITIONS REVOKED => %v", revoked)
				}
			},
		))
		opts = append(opts, kgo.OnPartitionsLost(
			func(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
				c.partitionsMu.Lock()
				c.partitionsReady = false
				c.partitionsMu.Unlock()
				if cfg.Monitor != nil {
					cfg.Monitor.OnLost(lost)
				}
				if cfg.WithLoggingHooks {
					log.Debugf("PARTITIONS LOST => %v", lost)
				}
			},
		))
	} else {
		// Without a consumer group there is no rebalancing, so mark ready immediately
		c.partitionsReady = true
		c.readyChCloseOnce.Do(func() { close(c.readyCh) })
	}

	if cfg.WithLogger {
		level := kgo.LogLevel(cfg.WithLogLevel)
		if cfg.WithLogLevel == LogLevelNone {
			level = kgo.LogLevelInfo
		}

		opts = append(
			opts,
			kgo.WithLogger(kgo.BasicLogger(os.Stderr, level, func() string {
				return "redpanda[consumer][" + time.Now().Format(time.RFC3339) + "]"
			})),
		)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	c.client = client

	return c, nil
}

func (c *BasicConsumer) poll(ctx context.Context, handler consumerHandler) error {
	log.Debugf("polling records: %s -> %d", c.topics, c.cfg.MaxPollRecords)

	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	if fetches.IsClientClosed() {
		log.Error("client is closed")
		return ErrConsumerClosed
	}

	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}

		log.Errorf(
			"failed to fetch records (topic: %s, partition: %d): %v",
			topic,
			partition,
			err,
		)
	})

	var (
		err  error
		recs []*kgo.Record
	)

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if err != nil {
			return
		}

		for i := range p.Records {
			rec := p.Records[i]

			if err2 := handler(ctx, rec); err2 != nil {
				err = fmt.Errorf(
					"topic: %s, partition: %d, offset: %d: %w",
					rec.Topic,
					rec.Partition,
					rec.Offset,
					err2,
				)

				break
			} else {
				recs = append(recs, rec)
			}
		}
	})

	if !c.cfg.DisableCommit {
		log.Infof("committing %d records", len(recs))
		if err2 := c.client.CommitRecords(ctx, recs...); err2 != nil {
			return fmt.Errorf("failed to commit offsets: %w", err2)
		}
		log.Infof("committed %d records", len(recs))
	}
	c.client.AllowRebalance()

	return err
}

func (c *BasicConsumer) start(ctx context.Context, handler consumerHandler) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrConsumerDone) && !errors.Is(err, context.Canceled) {
			log.Errorf("consumer stopping after poll returned error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err = c.poll(ctx, handler); err != nil {
				if errors.Is(err, ErrConsumerDone) {
					return nil // Graceful stop
				}
				return err
			}
		}
	}
}

// stripSchemaHeaders removes Confluent Schema Registry wire format headers if present.
// Format: magic byte (0x00) + 4-byte schema ID + message-indexes byte (0x00) + payload
// Total: 6 bytes header for protobuf messages
func stripSchemaHeaders(data []byte) []byte {
	if len(data) > 6 && data[0] == 0 {
		return data[6:]
	}
	return data
}

// Batch holds a polled set of messages and their associated commit function.
// Commit must always be called — even on processing error — to release the
// rebalance lock. On error, pass a cancelled context so offsets are skipped
// but the lock is still released.
type Batch struct {
	Msgs   []Msg
	Commit func(context.Context) error
}

// PollBatch fetches a batch of records without committing offsets immediately.
func (c *BasicConsumer) PollBatch(ctx context.Context) (Batch, error) {
	if c.closed {
		return Batch{}, ErrConsumerClosed
	}
	recs, err := c.pollRecords(ctx)
	if err != nil {
		c.client.AllowRebalance()
		return Batch{}, err
	}
	return Batch{
		Msgs: c.toMsgs(recs),
		Commit: func(commitCtx context.Context) error {
			defer c.client.AllowRebalance()
			return c.commitRecords(commitCtx, recs)
		},
	}, nil
}

// Poll performs a single poll and returns the records.
// Returns empty slice if no records available (topic is empty).
// This is useful for drain scenarios where you need to detect empty topics.
func (c *BasicConsumer) Poll(ctx context.Context) ([]Msg, error) {
	if c.closed {
		return nil, ErrConsumerClosed
	}
	defer c.client.AllowRebalance()

	recs, err := c.pollRecords(ctx)
	if err != nil {
		return nil, err
	}

	msgs := c.toMsgs(recs)

	if err := c.commitRecords(ctx, recs); err != nil {
		return nil, err
	}

	return msgs, nil
}

// pollRecords fetches a batch of records from Kafka.
func (c *BasicConsumer) pollRecords(ctx context.Context) ([]*kgo.Record, error) {
	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	if fetches.IsClientClosed() {
		return nil, ErrConsumerClosed
	}

	var fetchErr error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		fetchErr = err
	})
	if fetchErr != nil {
		return nil, fetchErr
	}

	return fetches.Records(), nil
}

// toMsgs converts kgo records to Msg slice, stripping schema headers if configured.
func (c *BasicConsumer) toMsgs(recs []*kgo.Record) []Msg {
	msgs := make([]Msg, 0, len(recs))
	for _, rec := range recs {
		value := rec.Value
		if c.stripSchemaHeaders {
			value = stripSchemaHeaders(value)
		}
		var hdrs []Header
		for _, h := range rec.Headers {
			hdrs = append(hdrs, Header{Key: h.Key, Value: h.Value})
		}
		msgs = append(msgs, Msg{
			Topic:     rec.Topic,
			Key:       rec.Key,
			Value:     value,
			Headers:   hdrs,
			Partition: rec.Partition,
			Ts:        rec.Timestamp,
		})
	}
	return msgs
}

// commitRecords handles commit/mark logic based on configuration.
func (c *BasicConsumer) commitRecords(ctx context.Context, recs []*kgo.Record) error {
	if c.cfg.DisableCommit || len(recs) == 0 {
		return nil
	}

	if c.cfg.DisableAutoCommit {
		// Sync commit when auto-commit disabled
		if err := c.client.CommitRecords(ctx, recs...); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	} else {
		// Mark for auto-commit (faster, at-most-once)
		c.client.MarkCommitRecords(recs...)
	}
	return nil
}

func (c *BasicConsumer) safeClose() {
	c.closeOnce.Do(func() {
		c.client.Close()
		c.closed = true
	})
}
