package rp

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/sonirico/readpanda/pkg/rp"
)

// OTelConfig holds OpenTelemetry configuration.
type OTelConfig struct {
	// Enabled controls whether tracing is enabled.
	Enabled bool
	// ServiceName is used as the span name prefix.
	ServiceName string
	// TracerProvider is the OTel tracer provider to use.
	// If nil, the global provider is used.
	TracerProvider trace.TracerProvider
}

// tracer returns a tracer for this package.
func (c *OTelConfig) tracer() trace.Tracer {
	if c == nil || !c.Enabled {
		return nil
	}

	tp := c.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}

	return tp.Tracer(instrumentationName)
}

// headerCarrier implements propagation.TextMapCarrier for kafka headers.
type headerCarrier map[string]string

func (h headerCarrier) Get(key string) string {
	return h[key]
}

func (h headerCarrier) Set(key, value string) {
	h[key] = value
}

func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// injectTraceContext injects the trace context from ctx into headers.
func injectTraceContext(ctx context.Context) map[string]string {
	carrier := make(headerCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// extractTraceContext extracts trace context from headers into a new context.
func extractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	carrier := headerCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// startProducerSpan starts a span for a produce operation.
func startProducerSpan(
	ctx context.Context,
	tracer trace.Tracer,
	topic string,
	isAsync bool,
) (context.Context, trace.Span) {
	if tracer == nil {
		return ctx, nil
	}

	opType := "publish"
	if isAsync {
		opType = "publish_async"
	}

	spanName := topic + " " + opType

	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", opType),
		),
	)

	return ctx, span
}

// startConsumerSpan starts a span for a consume operation.
func startConsumerSpan(
	ctx context.Context,
	tracer trace.Tracer,
	topic string,
	partition int32,
	offset int64,
) (context.Context, trace.Span) {
	if tracer == nil {
		return ctx, nil
	}

	spanName := topic + " process"

	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "process"),
			attribute.Int("messaging.kafka.partition", int(partition)),
			attribute.Int64("messaging.kafka.offset", offset),
		),
	)

	return ctx, span
}
