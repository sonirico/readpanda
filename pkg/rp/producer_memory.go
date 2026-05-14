package rp

import (
	"context"
	"sync"
)

// MemoryProducer is an in-memory producer for testing purposes.
// By default, callbacks are executed immediately. Use HoldCallbacks(true)
// to defer callback execution until FlushCallbacks() is called.
type MemoryProducer struct {
	data []Msg

	mutex sync.Mutex

	Pinged  bool
	PingErr error

	// holdCallbacks when true, callbacks are stored instead of executed immediately
	holdCallbacks    bool
	pendingCallbacks []func()
}

func (p *MemoryProducer) Ping(ctx context.Context) error {
	p.Pinged = true
	return p.PingErr
}

func (p *MemoryProducer) Publish(ctx context.Context, msg Msg) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.data = append(p.data, msg)

	return nil
}

func (p *MemoryProducer) PublishAsync(ctx context.Context, msg Msg, fn func(Msg, error)) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.data = append(p.data, msg)

	if fn != nil {
		if p.holdCallbacks {
			// Capture msg for deferred execution
			m := msg
			p.pendingCallbacks = append(p.pendingCallbacks, func() { fn(m, nil) })
		} else {
			fn(msg, nil)
		}
	}

	return nil
}

func (p *MemoryProducer) Flush(ctx context.Context) error {
	return nil
}

func (p *MemoryProducer) Close() {
}

func (p *MemoryProducer) Data() []Msg {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.data
}

func (p *MemoryProducer) Clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.data = nil
}

// HoldCallbacks sets whether callbacks should be held for later execution.
// When true, PublishAsync stores callbacks instead of executing them immediately.
// Call FlushCallbacks() to execute all pending callbacks.
func (p *MemoryProducer) HoldCallbacks(hold bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.holdCallbacks = hold
}

// FlushCallbacks executes all pending callbacks synchronously and clears the queue.
func (p *MemoryProducer) FlushCallbacks() {
	p.mutex.Lock()
	cbs := p.pendingCallbacks
	p.pendingCallbacks = nil
	p.mutex.Unlock()

	for _, cb := range cbs {
		cb()
	}
}

// PendingCallbacks returns the number of callbacks waiting to be executed.
func (p *MemoryProducer) PendingCallbacks() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.pendingCallbacks)
}

func NewMemoryProducer() *MemoryProducer {
	return &MemoryProducer{}
}
