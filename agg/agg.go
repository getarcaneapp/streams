// Package agg provides JSON-lines fan-in helpers for aggregated streams.
package agg

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

const defaultEventBuffer = 0

// RemoteEnvironmentLister lists the remote environment IDs an aggregated stream
// should cover.
type RemoteEnvironmentLister interface {
	ListRemoteEnvironmentIDs(ctx context.Context) ([]string, error)
}

// Producer sends events until ctx is canceled.
type Producer[T any] func(ctx context.Context, events chan<- T)

// Config configures Run.
type Config[T any] struct {
	Writer            io.Writer
	Flush             func()
	Buffer            int
	HeartbeatInterval time.Duration
	MakeHeartbeat     func() T
	Producers         []Producer[T]
	Logger            *slog.Logger
}

// InvalidConfigError describes an invalid Run configuration.
type InvalidConfigError struct {
	Field  string
	Reason string
}

func (e *InvalidConfigError) Error() string {
	if e == nil {
		return "invalid stream config"
	}
	return fmt.Sprintf("invalid stream config %s: %s", e.Field, e.Reason)
}

// Run drives a JSON-lines aggregated stream. It returns when ctx is canceled or
// encoding fails.
func Run[T any](ctx context.Context, cfg Config[T]) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	events := make(chan T, cfg.Buffer)
	var wg sync.WaitGroup
	wg.Add(len(cfg.Producers))
	for _, producer := range cfg.Producers {
		go func(producer Producer[T]) {
			defer wg.Done()
			producer(streamCtx, events)
		}(producer)
	}
	defer wg.Wait()
	defer cancel()

	var heartbeat <-chan time.Time
	var ticker *time.Ticker
	if cfg.MakeHeartbeat != nil {
		ticker = time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		heartbeat = ticker.C
	}

	for {
		select {
		case <-streamCtx.Done():
			return nil
		case event := <-events:
			if err := json.MarshalWrite(cfg.Writer, event); err != nil {
				return fmt.Errorf("encode stream event: %w", err)
			}
			if _, err := io.WriteString(cfg.Writer, "\n"); err != nil {
				return fmt.Errorf("terminate stream event: %w", err)
			}
			cfg.Flush()
		case <-heartbeat:
			if err := json.MarshalWrite(cfg.Writer, cfg.MakeHeartbeat()); err != nil {
				return fmt.Errorf("encode stream heartbeat: %w", err)
			}
			if _, err := io.WriteString(cfg.Writer, "\n"); err != nil {
				return fmt.Errorf("terminate stream heartbeat: %w", err)
			}
			cfg.Flush()
		}
	}
}

func (cfg Config[T]) validate() error {
	if cfg.Writer == nil {
		return &InvalidConfigError{Field: "Writer", Reason: "is required"}
	}
	if cfg.Flush == nil {
		return &InvalidConfigError{Field: "Flush", Reason: "is required"}
	}
	if cfg.Buffer < 0 {
		return &InvalidConfigError{Field: "Buffer", Reason: "must be non-negative"}
	}
	if cfg.MakeHeartbeat != nil && cfg.HeartbeatInterval <= 0 {
		return &InvalidConfigError{Field: "HeartbeatInterval", Reason: "must be positive when MakeHeartbeat is set"}
	}
	return nil
}

// Send forwards an event to the stream's event channel, giving up when ctx is
// done so producers can never block forever.
func Send[T any](ctx context.Context, events chan<- T, event T) bool {
	select {
	case events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

// ReconcileEnvironmentPollers keeps one poller goroutine per enabled remote
// environment.
func ReconcileEnvironmentPollers(
	ctx context.Context,
	lister RemoteEnvironmentLister,
	reconcileInterval time.Duration,
	streamLabel string,
	runPoller func(ctx context.Context, environmentID string),
) {
	ReconcilePollersByKey(ctx,
		func(ctx context.Context) ([]string, error) {
			return lister.ListRemoteEnvironmentIDs(ctx)
		},
		func(environmentID string) string {
			return environmentID
		},
		nil,
		reconcileInterval,
		streamLabel,
		runPoller,
	)
}

// ReconcilePollersByKey keeps one poller goroutine per listed item, keyed by a
// stable item ID.
func ReconcilePollersByKey[T any](
	ctx context.Context,
	listItems func(context.Context) ([]T, error),
	keyFunc func(T) string,
	versionFunc func(T) string,
	reconcileInterval time.Duration,
	streamLabel string,
	runPoller func(ctx context.Context, item T),
) {
	if reconcileInterval <= 0 {
		reconcileInterval = time.Minute
	}
	logger := slog.Default()

	type poller struct {
		cancel  context.CancelFunc
		done    chan struct{}
		version string
	}

	pollers := make(map[string]poller)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer func() {
		for _, activePoller := range pollers {
			activePoller.cancel()
		}
	}()

	startPoller := func(key string, version string, item T) {
		pollCtx, cancelPoll := context.WithCancel(ctx)
		done := make(chan struct{})
		pollers[key] = poller{cancel: cancelPoll, done: done, version: version}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			runPoller(pollCtx, item)
		}()
	}

	stopPoller := func(activePoller poller, wait bool) {
		activePoller.cancel()
		if wait {
			select {
			case <-activePoller.done:
			case <-ctx.Done():
				<-activePoller.done
			}
		}
	}

	reconcile := func() {
		items, err := listItems(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.WarnContext(ctx, "failed to list environments for "+streamLabel, "error", err)
			}
			return
		}

		current := make(map[string]struct{}, len(items))
		for _, item := range items {
			key := keyFunc(item)
			if key == "" {
				continue
			}
			version := ""
			if versionFunc != nil {
				version = versionFunc(item)
			}
			current[key] = struct{}{}

			if existingPoller, ok := pollers[key]; ok && existingPoller.version == version {
				continue
			}

			if existingPoller, ok := pollers[key]; ok {
				stopPoller(existingPoller, true)
				delete(pollers, key)
			}

			startPoller(key, version, item)
		}

		for id, activePoller := range pollers {
			if _, ok := current[id]; !ok {
				stopPoller(activePoller, false)
				delete(pollers, id)
			}
		}
	}

	reconcile()

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

var _ = defaultEventBuffer
