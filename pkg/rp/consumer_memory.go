package rp

import (
	"context"
	"sync"
)

// MemoryConsumer is an in-memory PollConsumer for testing purposes.
// Use SetMessages to configure what Poll returns next.
type MemoryConsumer struct {
	mu       sync.Mutex
	messages []Msg
	ready    bool
}

// NewMemoryConsumer creates a new MemoryConsumer that is ready by default.
func NewMemoryConsumer() *MemoryConsumer {
	return &MemoryConsumer{ready: true}
}

// SetMessages configures the messages that the next Poll call will return.
// After Poll is called, the messages are consumed (cleared).
func (c *MemoryConsumer) SetMessages(msgs []Msg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = msgs
}

// Poll returns the configured messages and clears them.
func (c *MemoryConsumer) Poll(_ context.Context) ([]Msg, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := c.messages
	c.messages = nil
	return msgs, nil
}

// PollBatch returns the configured messages as a Batch with a no-op commit, and clears them.
func (c *MemoryConsumer) PollBatch(_ context.Context) (Batch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := c.messages
	c.messages = nil
	return Batch{
		Msgs:   msgs,
		Commit: func(_ context.Context) error { return nil },
	}, nil
}

// Ready returns whether the consumer is ready.
func (c *MemoryConsumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

// WaitForReady returns immediately (always ready in tests).
func (c *MemoryConsumer) WaitForReady(_ context.Context) error {
	return nil
}

// Close is a no-op for the memory consumer.
func (c *MemoryConsumer) Close() {}
