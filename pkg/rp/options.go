package rp

type (
	Option interface {
		Apply(*Transport)
	}

	ConfigureFunc func(*Transport)
)

func (c ConfigureFunc) Apply(rp *Transport) {
	c(rp)
}

func WithPublishSyncEnabled() Option {
	return ConfigureFunc(func(t *Transport) {
		t.cfg.producerPublishSync = true
	})
}

func WithOnPublishAsync(fn func(Msg, error)) Option {
	return ConfigureFunc(func(t *Transport) {
		t.cfg.producerOnPublishAsync = fn
	})
}

func WithInternalLogger() Option {
	return ConfigureFunc(func(t *Transport) {
		t.cfg.WithInternalLogger = true
	})
}

func WithInternalLogLevel(level LogLevel) Option {
	return ConfigureFunc(func(t *Transport) {
		t.cfg.InternalLogLevel = level
	})
}

// WithOTel enables OpenTelemetry tracing for producer and consumer operations.
func WithOTel(cfg *OTelConfig) Option {
	return ConfigureFunc(func(t *Transport) {
		t.cfg.OTel = cfg
	})
}
