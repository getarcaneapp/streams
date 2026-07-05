package bus

import (
	"sync/atomic"
	"testing"

	"github.com/moby/moby/api/types/events"
)

func TestPublishCannotPanicAfterUnsubscribe(t *testing.T) {
	b := NewDockerEventBus()
	ch, unsubscribe := b.Subscribe(events.ImageEventType)
	unsubscribe()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("subscription channel remained open")
		}
	default:
		t.Fatal("subscription channel was not closed")
	}

	b.Publish(events.Message{Type: events.ImageEventType})
}

func TestPublishReportsDroppedEvents(t *testing.T) {
	var dropped atomic.Int64
	b := NewDockerEventBus(WithDroppedEventCallback(func(events.Message) {
		dropped.Add(1)
	}))
	ch, unsubscribe := b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))
	defer unsubscribe()

	b.Publish(events.Message{Type: events.ImageEventType, Action: "one"})
	b.Publish(events.Message{Type: events.ImageEventType, Action: "two"})

	if got := dropped.Load(); got != 1 {
		t.Fatalf("dropped callback count = %d, want 1", got)
	}

	<-ch
}

func TestCloseIsIdempotent(t *testing.T) {
	b := NewDockerEventBus()
	ch, _ := b.Subscribe(events.ImageEventType)

	b.Close()
	b.Close()

	_, ok := <-ch
	if ok {
		t.Fatal("subscription channel remained open after Close")
	}
}
