package rp

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type (
	ProducerConfig struct {
		FlushTimeout           time.Duration
		Timeout                time.Duration
		ProduceRequestTimeout  time.Duration
		ConnIdleTimeout        time.Duration
		RequestTimeoutOverhead time.Duration
		RecordDeliveryTimeout  time.Duration
		Linger                 time.Duration
		SessionTimeout         time.Duration
		MaxBufferedRecords     int
		MaxBytes               int32
		Brokers                []string
		WithLogger             bool
		Compression            int
		ProduceSync            bool
		App                    string
		Version                string

		// SASL authentication
		SASLUser string
		SASLPass string
		TLS      bool

		// OTel enables OpenTelemetry tracing for publish operations.
		OTel *OTelConfig

		// Monitor enables Prometheus metrics via kgo hooks. Nil disables.
		Monitor *ProducerMonitor

		// SchemaResolver opts into Confluent wire-format headers.
		// When set, NewProducer calls Resolve at init time (with retries)
		// and prepends the 6-byte header ([0x00, schemaID big-endian, 0x00])
		// to every published message value. If Resolve fails, NewProducer
		// returns an error so the pod restarts instead of silently publishing
		// without wire-format headers. Use HTTPSchemaResolver for the
		// standard Confluent Schema Registry API.
		SchemaResolver SchemaResolver
	}

	BasicProducer struct {
		cli      *kgo.Client
		cfg      ProducerConfig
		tracer   trace.Tracer
		schemaID [4]byte
	}
)

func NewProducer(
	ctx context.Context,
	config ProducerConfig,
) (Producer, error) {
	var opts []kgo.Opt

	if len(config.Brokers) < 1 {
		return nil, fmt.Errorf("brokers: %w", ErrConfig)
	}

	var schemaID [4]byte
	if config.SchemaResolver != nil {
		id, err := config.SchemaResolver.Resolve(ctx)
		if err != nil {
			return nil, fmt.Errorf("rp: schema resolver failed: %w", err)
		}
		schemaID = id
	}

	opts = append(opts, kgo.SeedBrokers(config.Brokers...))

	// SASL authentication
	if config.SASLUser != "" && config.SASLPass != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: config.SASLUser,
			Pass: config.SASLPass,
		}.AsSha256Mechanism()))
	}

	// TLS
	if config.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}

	if config.ProduceRequestTimeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(config.ProduceRequestTimeout))
	}

	if config.ConnIdleTimeout > 0 {
		opts = append(opts, kgo.ConnIdleTimeout(config.ConnIdleTimeout))
	}

	if config.RequestTimeoutOverhead > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(config.RequestTimeoutOverhead))
	}

	if config.RecordDeliveryTimeout > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(config.RecordDeliveryTimeout))
	}

	if config.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(config.SessionTimeout))
	}

	if config.App != "" && config.Version != "" {
		opts = append(opts, kgo.SoftwareNameAndVersion(config.App, config.Version))
	}

	if config.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(config.Linger))
	}

	if config.MaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(config.MaxBytes))
	}

	if config.WithLogger {
		opts = append(
			opts,
			kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
				return "redpanda[BasicProducer]"
			})),
		)
	}

	if config.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(config.MaxBufferedRecords))
	}

	switch config.Compression {
	case CompressionLz4:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case CompressionGzip:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case CompressionZstd:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	case CompressionSnappy:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case CompressionNone:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	}

	if config.Monitor != nil {
		opts = append(opts, kgo.WithHooks(config.Monitor))
	}

	cli, err := kgo.NewClient(opts...)

	if err != nil {
		return nil, err
	}

	if err = cli.Ping(ctx); err != nil {
		return nil, err
	}

	producer := &BasicProducer{
		cli:      cli,
		cfg:      config,
		tracer:   config.OTel.tracer(),
		schemaID: schemaID,
	}

	return producer, nil
}

func (p *BasicProducer) Ping(ctx context.Context) error {
	return p.cli.Ping(ctx)
}

func (p *BasicProducer) PublishAsync(
	ctx context.Context,
	msg Msg,
	onPublish func(Msg, error),
) (err error) {
	return p.publish(ctx, msg, onPublish)
}

func (p *BasicProducer) Publish(ctx context.Context, msg Msg) (err error) {
	return p.publish(ctx, msg, nil)
}

func (p *BasicProducer) publish(
	ctx context.Context,
	msg Msg,
	onPublished func(Msg, error),
) (err error) {
	isAsync := onPublished != nil

	// Start OTel span if tracing is enabled
	var span trace.Span
	if p.tracer != nil {
		ctx, span = startProducerSpan(ctx, p.tracer, msg.Topic, isAsync)
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

	// Build headers: caller-supplied first, then trace context (if enabled).
	var headers []kgo.RecordHeader
	for _, h := range msg.Headers {
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if p.tracer != nil {
		traceHeaders := injectTraceContext(ctx)
		for k, v := range traceHeaders {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}

	value := msg.Value
	if p.schemaID != [4]byte{} {
		value = prependSchemaHeader(msg.Value, p.schemaID)
	}

	if isAsync {
		// Async Publish
		rec := &kgo.Record{
			Topic:   msg.Topic,
			Key:     msg.Key,
			Value:   value,
			Headers: headers,
		}

		p.cli.Produce(ctx, rec, func(record *kgo.Record, err error) {
			if err != nil {
				log.WithFields(log.Fields{
					"topic": msg.Topic,
					"key":   string(msg.Key),
				}).WithError(err).Error("rp: publish async error")
			}
			var outHeaders []Header
			for _, h := range record.Headers {
				outHeaders = append(outHeaders, Header{Key: h.Key, Value: h.Value})
			}
			onPublished(Msg{
				Topic:     record.Topic,
				Key:       record.Key,
				Value:     record.Value,
				Headers:   outHeaders,
				Ts:        record.Timestamp,
				Partition: record.Partition,
			}, err)
		})
	} else {
		if err = p.cli.ProduceSync(ctx, &kgo.Record{
			Topic:   msg.Topic,
			Key:     msg.Key,
			Value:   value,
			Headers: headers,
		}).FirstErr(); err != nil {
			log.WithFields(log.Fields{
				"topic": msg.Topic,
				"key":   string(msg.Key),
			}).WithError(err).Error("rp: publish sync error")
		}
	}

	return err
}

func (p *BasicProducer) Flush(ctx context.Context) error {
	return p.cli.Flush(ctx)
}

func (p *BasicProducer) Close() {
	p.cli.Close()
}

func prependSchemaHeader(payload []byte, schemaID [4]byte) []byte {
	header := []byte{0, schemaID[0], schemaID[1], schemaID[2], schemaID[3], 0}
	return append(header, payload...)
}

func (c ProducerConfig) GetFlushTimeout() time.Duration {
	if c.FlushTimeout != 0 {
		return c.FlushTimeout
	}

	if c.Timeout != 0 {
		return c.Timeout
	}

	return time.Minute
}
