// Package rp provides a Redpanda/Kafka client using franz-go.
// Adapted from github.com/sonirico/vago/rp with logrus logging instead of lol,
// and without Elastic APM (can add OpenTelemetry later).
package rp

import (
	"context"
	"errors"
	"time"
)

var (
	ErrConfig                 = errors.New("missing required config")
	ErrLoggerRequired         = errors.New("logger is required")
	ErrConsumerClosed         = errors.New("consumer is closed")
	ErrConsumerAlreadyCreated = errors.New("consumer is already created")
	ErrTopicsRequired         = errors.New("topics are required for consuming")
	ErrPingFailed             = errors.New("ping failed")
)

// Compression types for producer batches.
const (
	CompressionSnappy int = iota
	CompressionGzip
	CompressionLz4
	CompressionZstd
	CompressionNone
)

// Header is a Kafka record header. Producer-side Headers on Msg are merged
// with any auto-injected headers (e.g. trace context) at publish time.
type Header struct {
	Key   string
	Value []byte
}

// Msg represents a message to be published to or consumed from Redpanda/Kafka.
// On the consume path Offset and CompressionCodec are populated; on produce
// they are ignored.
type Msg struct {
	Topic            string
	Key              []byte
	Value            []byte
	Headers          []Header
	Ts               time.Time
	Partition        int32
	Offset           int64
	CompressionCodec string
}

// Producer defines the interface for publishing messages to Redpanda/Kafka.
type Producer interface {
	Publish(ctx context.Context, msg Msg) error
	PublishAsync(ctx context.Context, msg Msg, fn func(Msg, error)) error
	Flush(ctx context.Context) error
	Ping(ctx context.Context) error
	Close()
}

// Consumer defines the interface for consuming messages from Redpanda/Kafka.
type Consumer interface {
	Subscribe(ctx context.Context, h ConsumerHandler) error
	Ping(ctx context.Context) error
	Close()
}

// ConsumerHandler is a callback function for processing consumed messages.
type ConsumerHandler func(ctx context.Context, m Msg) error

// NoCallback is a no-op callback for async publishing.
var NoCallback = func(Msg, error) {}
