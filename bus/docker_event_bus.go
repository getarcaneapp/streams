// Package bus provides typed in-process pub/sub fan-out primitives.
package bus

import (
	"sync"

	"github.com/moby/moby/api/types/events"
)

const defaultSubscriberBuffer = 16

// Option configures a DockerEventBus.
type Option func(*DockerEventBus)

// SubscribeOption configures one subscription.
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	buffer int
}

// WithDroppedEventCallback observes messages that could not be delivered to a
// full subscriber channel.
func WithDroppedEventCallback(callback func(events.Message)) Option {
	return func(b *DockerEventBus) {
		b.onDrop = callback
	}
}

// WithSubscriberBuffer sets a subscription channel buffer size.
func WithSubscriberBuffer(size int) SubscribeOption {
	return func(cfg *subscribeConfig) {
		if size >= 0 {
			cfg.buffer = size
		}
	}
}

// DockerEventBus is an in-process fan-out point for Docker daemon events.
type DockerEventBus struct {
	mu     sync.RWMutex
	subs   map[events.Type]map[chan events.Message]struct{}
	closed bool
	onDrop func(events.Message)
}

// NewDockerEventBus creates an empty Docker event bus.
func NewDockerEventBus(opts ...Option) *DockerEventBus {
	b := &DockerEventBus{
		subs: make(map[events.Type]map[chan events.Message]struct{}),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Subscribe registers for messages of eventType. Delivery is non-blocking.
func (b *DockerEventBus) Subscribe(eventType events.Type, opts ...SubscribeOption) (<-chan events.Message, func()) {
	cfg := subscribeConfig{buffer: defaultSubscriberBuffer}
	for _, opt := range opts {
		opt(&cfg)
	}
	ch := make(chan events.Message, cfg.buffer)
	if b == nil {
		close(ch)
		return ch, func() {}
	}

	b.mu.Lock()
	if b.closed {
		close(ch)
		b.mu.Unlock()
		return ch, func() {}
	}
	if b.subs[eventType] == nil {
		b.subs[eventType] = make(map[chan events.Message]struct{})
	}
	b.subs[eventType][ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			if subs := b.subs[eventType]; subs != nil {
				if _, ok := subs[ch]; ok {
					delete(subs, ch)
					close(ch)
				}
				if len(subs) == 0 {
					delete(b.subs, eventType)
				}
			}
			b.mu.Unlock()
		})
	}
}

// Publish fans out msg to subscribers of msg.Type.
func (b *DockerEventBus) Publish(msg events.Message) {
	if b == nil {
		return
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}
	dropped := 0
	for ch := range b.subs[msg.Type] {
		select {
		case ch <- msg:
		default:
			dropped++
		}
	}
	onDrop := b.onDrop
	b.mu.RUnlock()

	if onDrop != nil {
		for range dropped {
			onDrop(msg)
		}
	}
}

// Close closes every subscription. It is safe to call more than once.
func (b *DockerEventBus) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for eventType, subs := range b.subs {
		for ch := range subs {
			close(ch)
			delete(subs, ch)
		}
		delete(b.subs, eventType)
	}
}
