// Package logs captures slog output into a bounded in-memory ring buffer and
// fans it out to live subscribers.
package logs

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRingCapacity     = 1000
	defaultSubscriberBuffer = 256
)

// Entry is a single captured log record.
type Entry struct {
	Time    time.Time      `json:"time"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// Option configures a Broadcaster.
type Option func(*Broadcaster)

// WithSubscriberBuffer sets the per-subscriber channel buffer.
func WithSubscriberBuffer(size int) Option {
	return func(b *Broadcaster) {
		if size >= 0 {
			b.subscriberBuffer = size
		}
	}
}

// Broadcaster keeps a bounded ring buffer of recent log entries and fans new
// entries out to active subscribers.
type Broadcaster struct {
	historyMu        sync.Mutex
	deliveryMu       sync.RWMutex
	buf              []Entry
	start            int
	size             int
	capN             int
	subscriberBuffer int
	subscribers      atomic.Pointer[logSubscriberSnapshot]
}

type logSubscriberSnapshot struct {
	subscribers []chan Entry
}

// New returns a Broadcaster retaining up to capacity recent entries.
func New(capacity int, opts ...Option) *Broadcaster {
	if capacity <= 0 {
		capacity = defaultRingCapacity
	}
	b := &Broadcaster{
		buf:              make([]Entry, capacity),
		capN:             capacity,
		subscriberBuffer: defaultSubscriberBuffer,
	}
	b.subscribers.Store(&logSubscriberSnapshot{})
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Append records an entry in the ring buffer and delivers it to subscribers.
func (b *Broadcaster) Append(e Entry) {
	b.historyMu.Lock()

	if b.size < b.capN {
		b.buf[(b.start+b.size)%b.capN] = e
		b.size++
	} else {
		b.buf[b.start] = e
		b.start = (b.start + 1) % b.capN
	}
	b.historyMu.Unlock()

	snapshot := b.subscribers.Load()
	if snapshot == nil || len(snapshot.subscribers) == 0 {
		return
	}

	b.deliveryMu.RLock()
	snapshot = b.subscribers.Load()
	for _, subscription := range snapshot.subscribers {
		select {
		case subscription <- e:
		default:
		}
	}
	b.deliveryMu.RUnlock()
}

// Recent returns buffered entries in chronological order.
func (b *Broadcaster) Recent() []Entry {
	b.historyMu.Lock()
	defer b.historyMu.Unlock()

	out := make([]Entry, b.size)
	for i := range b.size {
		out[i] = b.buf[(b.start+i)%b.capN]
	}
	return out
}

// Subscribe registers a live subscriber.
func (b *Broadcaster) Subscribe() (<-chan Entry, func()) {
	ch := make(chan Entry, b.subscriberBuffer)
	for {
		current := b.loadSubscribersInternal()
		nextSubscribers := append(append([]chan Entry(nil), current.subscribers...), ch)
		next := &logSubscriberSnapshot{subscribers: nextSubscribers}
		if b.subscribers.CompareAndSwap(current, next) {
			break
		}
	}

	cancel := func() {
		b.unsubscribeInternal(ch)
	}
	return ch, cancel
}

func (b *Broadcaster) loadSubscribersInternal() *logSubscriberSnapshot {
	for {
		current := b.subscribers.Load()
		if current != nil {
			return current
		}

		initial := &logSubscriberSnapshot{}
		if b.subscribers.CompareAndSwap(nil, initial) {
			return initial
		}
	}
}

func (b *Broadcaster) unsubscribeInternal(subscription chan Entry) {
	for {
		current := b.loadSubscribersInternal()
		index := -1
		for i, candidate := range current.subscribers {
			if candidate == subscription {
				index = i
				break
			}
		}
		if index < 0 {
			return
		}

		nextSubscribers := make([]chan Entry, 0, len(current.subscribers)-1)
		nextSubscribers = append(nextSubscribers, current.subscribers[:index]...)
		nextSubscribers = append(nextSubscribers, current.subscribers[index+1:]...)
		next := &logSubscriberSnapshot{subscribers: nextSubscribers}
		if b.subscribers.CompareAndSwap(current, next) {
			b.deliveryMu.Lock()
			close(subscription)
			b.deliveryMu.Unlock()
			return
		}
	}
}

// slogHandler wraps a base slog.Handler and appends handled records to a
// Broadcaster.
type slogHandler struct {
	base  slog.Handler
	b     *Broadcaster
	attrs []boundAttrs
	group []string
}

type boundAttrs struct {
	groups []string
	attrs  []slog.Attr
}

// NewSlogHandler returns a slog.Handler that tees records to base and to b.
func NewSlogHandler(base slog.Handler, b *Broadcaster) slog.Handler {
	return &slogHandler{base: base, b: b}
}

func (h *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h *slogHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	for _, bound := range h.attrs {
		addAttrsToMapInternal(attrs, bound.groups, bound.attrs)
	}
	recordAttrs := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		recordAttrs = append(recordAttrs, a)
		return true
	})
	addAttrsToMapInternal(attrs, h.group, recordAttrs)
	if len(attrs) == 0 {
		attrs = nil
	}

	h.b.Append(Entry{
		Time:    r.Time,
		Level:   r.Level.String(),
		Message: r.Message,
		Attrs:   attrs,
	})
	return h.base.Handle(ctx, r)
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := h.cloneInternal()
	copiedAttrs := append([]slog.Attr(nil), attrs...)
	next.attrs = append(next.attrs, boundAttrs{
		groups: append([]string(nil), h.group...),
		attrs:  copiedAttrs,
	})
	next.base = h.base.WithAttrs(attrs)
	return next
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	next := h.cloneInternal()
	if name != "" {
		next.group = append(next.group, name)
	}
	next.base = h.base.WithGroup(name)
	return next
}

func (h *slogHandler) cloneInternal() *slogHandler {
	next := &slogHandler{
		base:  h.base,
		b:     h.b,
		group: append([]string(nil), h.group...),
	}
	if len(h.attrs) > 0 {
		next.attrs = make([]boundAttrs, 0, len(h.attrs))
		for _, bound := range h.attrs {
			next.attrs = append(next.attrs, boundAttrs{
				groups: append([]string(nil), bound.groups...),
				attrs:  append([]slog.Attr(nil), bound.attrs...),
			})
		}
	}
	return next
}

func addAttrsToMapInternal(dst map[string]any, groups []string, attrs []slog.Attr) {
	target := dst
	for _, group := range groups {
		existing, _ := target[group].(map[string]any)
		if existing == nil {
			existing = make(map[string]any)
			target[group] = existing
		}
		target = existing
	}
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}
		target[attr.Key] = normalizeAttrValueInternal(attr.Value)
	}
}

func normalizeAttrValueInternal(value slog.Value) any {
	value = value.Resolve()

	switch value.Kind() {
	case slog.KindAny:
		return normalizeAnyValueInternal(value.Any())
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindGroup:
		return normalizeGroupAttrsInternal(value.Group())
	case slog.KindInt64:
		return value.Int64()
	case slog.KindLogValuer:
		return value.String()
	case slog.KindString:
		return value.String()
	case slog.KindTime:
		return value.Time()
	case slog.KindUint64:
		return value.Uint64()
	default:
		return value.String()
	}
}

func normalizeAnyValueInternal(value any) any {
	switch typed := value.(type) {
	case error:
		return typed.Error()
	case slog.Attr:
		return map[string]any{typed.Key: normalizeAttrValueInternal(typed.Value)}
	case []slog.Attr:
		return normalizeGroupAttrsInternal(typed)
	case slog.Value:
		return normalizeAttrValueInternal(typed)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeAnyValueInternal(item)
		}
		return out
	}

	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Kind() == reflect.Slice {
		out := make([]any, rv.Len())
		for i := range rv.Len() {
			out[i] = normalizeAnyValueInternal(rv.Index(i).Interface())
		}
		return out
	}

	return value
}

func normalizeGroupAttrsInternal(attrs []slog.Attr) map[string]any {
	out := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}
		out[attr.Key] = normalizeAttrValueInternal(attr.Value)
	}
	return out
}
