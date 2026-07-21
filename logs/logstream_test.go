package logs

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSlogHandlerNormalizesGroupedAttrsInternal(t *testing.T) {
	b := New(10)
	h := NewSlogHandler(slog.NewTextHandler(io.Discard, nil), b)
	record := slog.NewRecord(time.Unix(1, 0).UTC(), slog.LevelInfo, "Incoming request", 0)
	record.AddAttrs(
		slog.Group("request",
			slog.String("method", "GET"),
			slog.String("path", "/api/diagnostics/logs"),
			slog.Group("headers", slog.String("accept", "application/json")),
		),
		slog.Any("ids", []string{"one", "two"}),
	)

	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	entries := b.Recent()
	if len(entries) != 1 {
		t.Fatalf("Recent returned %d entries, want 1", len(entries))
	}

	wantRequest := map[string]any{
		"method": "GET",
		"path":   "/api/diagnostics/logs",
		"headers": map[string]any{
			"accept": "application/json",
		},
	}
	if !reflect.DeepEqual(entries[0].Attrs["request"], wantRequest) {
		t.Fatalf("request attr = %#v, want %#v", entries[0].Attrs["request"], wantRequest)
	}
	if !reflect.DeepEqual(entries[0].Attrs["ids"], []any{"one", "two"}) {
		t.Fatalf("ids attr = %#v, want []any", entries[0].Attrs["ids"])
	}
}

func TestSlogHandlerNormalizesErrorsInternal(t *testing.T) {
	b := New(10)
	h := NewSlogHandler(slog.NewTextHandler(io.Discard, nil), b)
	record := slog.NewRecord(time.Unix(1, 0).UTC(), slog.LevelError, "request failed", 0)
	record.AddAttrs(slog.Any("error", fmt.Errorf("request: %w", errors.New("connection refused"))))

	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	entries := b.Recent()
	if len(entries) != 1 {
		t.Fatalf("Recent returned %d entries, want 1", len(entries))
	}
	if entries[0].Attrs["error"] != "request: connection refused" {
		t.Fatalf("error attr = %#v, want string", entries[0].Attrs["error"])
	}
	if _, err := json.Marshal(entries); err != nil {
		t.Fatalf("marshal entries: %v", err)
	}
}

func TestSlogHandlerPreservesBoundAttrsAndGroups(t *testing.T) {
	b := New(10)
	h := NewSlogHandler(slog.NewTextHandler(io.Discard, nil), b).
		WithAttrs([]slog.Attr{slog.String("component", "diagnostics")}).
		WithGroup("request").
		WithAttrs([]slog.Attr{slog.String("method", "GET")})

	record := slog.NewRecord(time.Unix(1, 0).UTC(), slog.LevelInfo, "served", 0)
	record.AddAttrs(slog.String("path", "/logs"))

	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	entries := b.Recent()
	if len(entries) != 1 {
		t.Fatalf("Recent returned %d entries, want 1", len(entries))
	}
	if entries[0].Attrs["component"] != "diagnostics" {
		t.Fatalf("component attr = %#v, want diagnostics", entries[0].Attrs["component"])
	}
	wantRequest := map[string]any{"method": "GET", "path": "/logs"}
	if !reflect.DeepEqual(entries[0].Attrs["request"], wantRequest) {
		t.Fatalf("request attr = %#v, want %#v", entries[0].Attrs["request"], wantRequest)
	}
}

func TestSubscribeUsesConfiguredBuffer(t *testing.T) {
	b := New(10, WithSubscriberBuffer(1))
	ch, cancel := b.Subscribe()
	defer cancel()

	b.Append(Entry{Message: "one"})
	b.Append(Entry{Message: "two"})

	entry := <-ch
	if entry.Message != "one" {
		t.Fatalf("first delivered entry = %q, want one", entry.Message)
	}
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra entry despite full subscriber buffer: %#v", extra)
	default:
	}
}

func TestRecentCapsAndPreservesOrder(t *testing.T) {
	b := New(3)
	for _, message := range []string{"one", "two", "three", "four", "five"} {
		b.Append(Entry{Message: message})
	}

	entries := b.Recent()
	if len(entries) != 3 {
		t.Fatalf("Recent returned %d entries, want 3", len(entries))
	}
	for i, want := range []string{"three", "four", "five"} {
		if entries[i].Message != want {
			t.Fatalf("entry[%d].Message = %q, want %q", i, entries[i].Message, want)
		}
	}
}

func TestCancelClosesSubscription(t *testing.T) {
	b := New(10)
	ch, cancel := b.Subscribe()
	cancel()
	cancel()

	if _, ok := <-ch; ok {
		t.Fatal("subscription channel remained open after cancel")
	}
}

func TestConcurrentAppendCancelAndRecent(t *testing.T) {
	b := New(32, WithSubscriberBuffer(1))
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 500 {
				b.Append(Entry{Message: "concurrent"})
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 500 {
				if entries := b.Recent(); len(entries) > 32 {
					t.Errorf("Recent returned %d entries, capacity is 32", len(entries))
					return
				}
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 250 {
				ch, cancel := b.Subscribe()
				cancel()
				for range ch {
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	if entries := b.Recent(); len(entries) != 32 {
		t.Fatalf("Recent returned %d entries after concurrent appends, want 32", len(entries))
	}
}

func BenchmarkBroadcasterAppend(b *testing.B) {
	b.Run("no-subscribers", func(b *testing.B) {
		benchmarkBroadcasterAppend(b, 0)
	})
	b.Run("sixteen-subscribers", func(b *testing.B) {
		benchmarkBroadcasterAppend(b, 16)
	})
}

func benchmarkBroadcasterAppend(b *testing.B, subscriberCount int) {
	broadcaster := New(1000, WithSubscriberBuffer(0))
	for range subscriberCount {
		_, _ = broadcaster.Subscribe()
	}

	entry := Entry{Message: "benchmark"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		broadcaster.Append(entry)
	}
}
