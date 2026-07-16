package agg

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	err := Run(context.Background(), Config[string]{})
	if err == nil {
		t.Fatal("Run returned nil error for invalid config")
	}
}

func TestRunReturnsEncodeError(t *testing.T) {
	err := Run(context.Background(), Config[string]{
		Writer: failingWriter{},
		Flush:  func() {},
		Producers: []Producer[string]{
			func(ctx context.Context, events chan<- string) {
				Send(ctx, events, "event")
			},
		},
	})
	if err == nil {
		t.Fatal("Run returned nil error after encoder failure")
	}
}

func TestRunAllowsNoHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out bytes.Buffer
	err := Run(ctx, Config[string]{
		Writer: &out,
		Flush:  cancel,
		Producers: []Producer[string]{
			func(ctx context.Context, events chan<- string) {
				Send(ctx, events, "event")
			},
		},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := out.String(); got != "\"event\"\n" {
		t.Fatalf("encoded output = %q, want event JSON line", got)
	}
}

func TestReconcilePollersWaitsForReplacementToExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	calls := 0
	items := [][]string{{"v1"}, {"v2"}}

	go ReconcilePollersByKey(ctx,
		func(context.Context) ([]string, error) {
			if calls >= len(items) {
				cancel()
				return nil, nil
			}
			out := items[calls]
			calls++
			return out, nil
		},
		func(string) string { return "same" },
		func(version string) string { return version },
		10*time.Millisecond,
		"test stream",
		func(ctx context.Context, item string) {
			started <- item
			if item == "v1" {
				<-releaseFirst
			}
		},
	)

	first := <-started
	if first != "v1" {
		t.Fatalf("first poller = %q, want v1", first)
	}

	select {
	case replacement := <-started:
		t.Fatalf("replacement started before first exited: %q", replacement)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFirst)

	select {
	case replacement := <-started:
		if replacement != "v2" {
			t.Fatalf("replacement = %q, want v2", replacement)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not start after first exited")
	}
}
