// Package bus provides typed in-process pub/sub fan-out primitives.
package bus

import (
	"sync"
	"sync/atomic"

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
	deliveryMu sync.RWMutex
	state      atomic.Pointer[dockerEventBusState]
	onDrop     func(events.Message)
}

type dockerEventBusState struct {
	closed      bool
	subscribers map[events.Type][]chan events.Message
}

// NewDockerEventBus creates an empty Docker event bus.
func NewDockerEventBus(opts ...Option) *DockerEventBus {
	b := &DockerEventBus{}
	b.state.Store(newDockerEventBusStateInternal())
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

	for {
		current := b.loadStateInternal()
		if current.closed {
			close(ch)
			return ch, func() {}
		}

		next := current.withSubscriptionInternal(eventType, ch)
		if b.state.CompareAndSwap(current, next) {
			break
		}
	}

	return ch, func() {
		b.unsubscribeInternal(eventType, ch)
	}
}

// Publish fans out msg to subscribers of msg.Type.
func (b *DockerEventBus) Publish(msg events.Message) {
	if b == nil {
		return
	}

	state := b.state.Load()
	if state == nil || state.closed || len(state.subscribers[msg.Type]) == 0 {
		return
	}

	b.deliveryMu.RLock()
	state = b.state.Load()
	if state == nil || state.closed {
		b.deliveryMu.RUnlock()
		return
	}
	dropped := 0
	for _, subscriber := range state.subscribers[msg.Type] {
		select {
		case subscriber <- msg:
		default:
			dropped++
		}
	}
	onDrop := b.onDrop
	b.deliveryMu.RUnlock()

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

	for {
		current := b.loadStateInternal()
		if current.closed {
			return
		}

		closed := &dockerEventBusState{closed: true}
		if !b.state.CompareAndSwap(current, closed) {
			continue
		}

		b.deliveryMu.Lock()
		for _, subscribers := range current.subscribers {
			for _, subscriber := range subscribers {
				close(subscriber)
			}
		}
		b.deliveryMu.Unlock()
		return
	}
}

func newDockerEventBusStateInternal() *dockerEventBusState {
	return &dockerEventBusState{subscribers: make(map[events.Type][]chan events.Message)}
}

func (b *DockerEventBus) loadStateInternal() *dockerEventBusState {
	for {
		state := b.state.Load()
		if state != nil {
			return state
		}

		initial := newDockerEventBusStateInternal()
		if b.state.CompareAndSwap(nil, initial) {
			return initial
		}
	}
}

func (s *dockerEventBusState) withSubscriptionInternal(eventType events.Type, subscription chan events.Message) *dockerEventBusState {
	subscribers := cloneDockerEventSubscribersInternal(s.subscribers)
	subscribers[eventType] = append(append([]chan events.Message(nil), subscribers[eventType]...), subscription)
	return &dockerEventBusState{subscribers: subscribers}
}

func (s *dockerEventBusState) withoutSubscriptionInternal(eventType events.Type, subscription chan events.Message) (*dockerEventBusState, bool) {
	current := s.subscribers[eventType]
	index := -1
	for i, candidate := range current {
		if candidate == subscription {
			index = i
			break
		}
	}
	if index < 0 {
		return s, false
	}

	subscribers := cloneDockerEventSubscribersInternal(s.subscribers)
	if len(current) == 1 {
		delete(subscribers, eventType)
	} else {
		next := make([]chan events.Message, 0, len(current)-1)
		next = append(next, current[:index]...)
		next = append(next, current[index+1:]...)
		subscribers[eventType] = next
	}

	return &dockerEventBusState{closed: s.closed, subscribers: subscribers}, true
}

func cloneDockerEventSubscribersInternal(current map[events.Type][]chan events.Message) map[events.Type][]chan events.Message {
	next := make(map[events.Type][]chan events.Message, len(current))
	for eventType, subscriptions := range current {
		next[eventType] = subscriptions
	}
	return next
}

func (b *DockerEventBus) unsubscribeInternal(eventType events.Type, subscription chan events.Message) {
	for {
		current := b.loadStateInternal()
		next, found := current.withoutSubscriptionInternal(eventType, subscription)
		if !found {
			return
		}
		if b.state.CompareAndSwap(current, next) {
			b.deliveryMu.Lock()
			close(subscription)
			b.deliveryMu.Unlock()
			return
		}
	}
}
