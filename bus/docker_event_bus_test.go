package bus

import (
	"sync"
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

func TestPublishReportsEachDroppedSubscriber(t *testing.T) {
	var dropped atomic.Int64
	b := NewDockerEventBus(WithDroppedEventCallback(func(events.Message) {
		dropped.Add(1)
	}))
	channels := make([]<-chan events.Message, 3)
	for i := range channels {
		ch, unsubscribe := b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))
		channels[i] = ch
		defer unsubscribe()
	}

	b.Publish(events.Message{Type: events.ImageEventType, Action: "one"})
	b.Publish(events.Message{Type: events.ImageEventType, Action: "two"})

	if got := dropped.Load(); got != int64(len(channels)) {
		t.Fatalf("dropped callback count = %d, want %d", got, len(channels))
	}
	for _, ch := range channels {
		<-ch
	}
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

func TestPublishOnlyDeliversMatchingEventType(t *testing.T) {
	b := NewDockerEventBus()
	imageEvents, unsubscribeImages := b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))
	defer unsubscribeImages()
	containerEvents, unsubscribeContainers := b.Subscribe(events.ContainerEventType, WithSubscriberBuffer(1))
	defer unsubscribeContainers()

	b.Publish(events.Message{Type: events.ImageEventType, Action: "pull"})

	select {
	case message := <-imageEvents:
		if message.Action != "pull" {
			t.Fatalf("image event action = %q, want pull", message.Action)
		}
	default:
		t.Fatal("image subscriber did not receive matching event")
	}
	select {
	case message := <-containerEvents:
		t.Fatalf("container subscriber received image event: %#v", message)
	default:
	}
}

func TestSubscribeAfterCloseReturnsClosedChannel(t *testing.T) {
	b := NewDockerEventBus()
	b.Close()

	ch, unsubscribe := b.Subscribe(events.ImageEventType)
	unsubscribe()

	if _, ok := <-ch; ok {
		t.Fatal("subscription created after Close remained open")
	}
}

func TestDroppedEventCallbackCanCloseBus(t *testing.T) {
	var dropped atomic.Int64
	var b *DockerEventBus
	b = NewDockerEventBus(WithDroppedEventCallback(func(events.Message) {
		dropped.Add(1)
		b.Close()
	}))
	ch, unsubscribe := b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))
	defer unsubscribe()

	b.Publish(events.Message{Type: events.ImageEventType, Action: "one"})
	b.Publish(events.Message{Type: events.ImageEventType, Action: "two"})

	if got := dropped.Load(); got != 1 {
		t.Fatalf("dropped callback count = %d, want 1", got)
	}
	if message := <-ch; message.Action != "one" {
		t.Fatalf("buffered event action = %q, want one", message.Action)
	}
	if _, ok := <-ch; ok {
		t.Fatal("subscription channel remained open after callback closed bus")
	}
}

func TestConcurrentPublishAndUnsubscribe(t *testing.T) {
	b := NewDockerEventBus()
	const subscriberCount = 64
	channels := make([]<-chan events.Message, subscriberCount)
	unsubscribes := make([]func(), subscriberCount)
	for i := range subscriberCount {
		channels[i], unsubscribes[i] = b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 1000 {
				b.Publish(events.Message{Type: events.ImageEventType})
			}
		}()
	}
	for _, unsubscribe := range unsubscribes {
		wg.Add(1)
		go func(unsubscribe func()) {
			defer wg.Done()
			<-start
			unsubscribe()
			unsubscribe()
		}(unsubscribe)
	}

	close(start)
	wg.Wait()
	for _, ch := range channels {
		for range ch {
		}
	}
}

func TestConcurrentPublishAndClose(t *testing.T) {
	b := NewDockerEventBus()
	ch, _ := b.Subscribe(events.ImageEventType, WithSubscriberBuffer(1))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 1000 {
				b.Publish(events.Message{Type: events.ImageEventType})
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		b.Close()
	}()

	close(start)
	wg.Wait()
	b.Publish(events.Message{Type: events.ImageEventType})
	for range ch {
	}
}

func TestConcurrentSubscribeAndClose(t *testing.T) {
	b := NewDockerEventBus()
	const subscriberCount = 64
	channels := make([]<-chan events.Message, subscriberCount)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range subscriberCount {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			channels[index], _ = b.Subscribe(events.ImageEventType)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		b.Close()
	}()

	close(start)
	wg.Wait()
	for _, ch := range channels {
		if _, ok := <-ch; ok {
			t.Fatal("concurrent subscription remained open after Close")
		}
	}
}

func BenchmarkDockerEventBusPublish(b *testing.B) {
	b.Run("no-subscribers", func(b *testing.B) {
		benchmarkDockerEventBusPublish(b, 0)
	})
	b.Run("sixteen-subscribers", func(b *testing.B) {
		benchmarkDockerEventBusPublish(b, 16)
	})
}

func benchmarkDockerEventBusPublish(b *testing.B, subscriberCount int) {
	eventBus := NewDockerEventBus()
	defer eventBus.Close()

	for range subscriberCount {
		_, _ = eventBus.Subscribe(events.ImageEventType, WithSubscriberBuffer(0))
	}

	message := events.Message{Type: events.ImageEventType, Action: "benchmark"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		eventBus.Publish(message)
	}
}
