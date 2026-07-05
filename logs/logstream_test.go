package logs

import (
	"context"
	"io"
	"log/slog"
	"reflect"
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
