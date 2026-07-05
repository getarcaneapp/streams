package stats

import (
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

func TestBuildSample(t *testing.T) {
	stats := container.StatsResponse{
		CPUStats: container.CPUStats{
			CPUUsage:    container.CPUUsage{TotalUsage: 250},
			SystemUsage: 500,
		},
		PreCPUStats: container.CPUStats{
			CPUUsage:    container.CPUUsage{TotalUsage: 50},
			SystemUsage: 100,
		},
		MemoryStats: container.MemoryStats{
			Usage: 700,
			Limit: 1000,
			Stats: map[string]uint64{"inactive_file": 100},
		},
	}

	sample := BuildSample(stats)

	if sample.CPUTenths != 500 {
		t.Fatalf("CPUTenths = %d, want 500", sample.CPUTenths)
	}
	if sample.MemoryTenths != 600 {
		t.Fatalf("MemoryTenths = %d, want 600", sample.MemoryTenths)
	}
	if sample.MemoryUsageBytes != 600 {
		t.Fatalf("MemoryUsageBytes = %d, want 600", sample.MemoryUsageBytes)
	}
}

func TestStoreRecordCapsAndPreservesOrder(t *testing.T) {
	store := NewStore(WithCapacity(3))
	baseTime := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)

	for i := range 5 {
		store.Record(
			"container-1",
			StatsHistorySample{
				CPUTenths:        uint16(i),
				MemoryTenths:     uint16(i + 100),
				MemoryUsageBytes: uint64(i + 1000),
			},
			false,
			baseTime.Add(time.Duration(i)*time.Second),
		)
	}

	snapshot := store.History("container-1")
	if len(snapshot) != 3 {
		t.Fatalf("history length = %d, want 3", len(snapshot))
	}

	for i, sample := range snapshot {
		expected := i + 2
		if sample.CPUTenths != uint16(expected) {
			t.Fatalf("sample[%d].CPUTenths = %d, want %d", i, sample.CPUTenths, expected)
		}
		if sample.MemoryTenths != uint16(expected+100) {
			t.Fatalf("sample[%d].MemoryTenths = %d, want %d", i, sample.MemoryTenths, expected+100)
		}
		if sample.MemoryUsageBytes != uint64(expected+1000) {
			t.Fatalf("sample[%d].MemoryUsageBytes = %d, want %d", i, sample.MemoryUsageBytes, expected+1000)
		}
	}
}

func TestStoreRecordPrunesExpiredContainers(t *testing.T) {
	store := NewStore(WithTTL(10*time.Minute), WithPruneInterval(time.Minute))
	baseTime := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)

	store.Record("stale-container", StatsHistorySample{CPUTenths: 111, MemoryTenths: 222, MemoryUsageBytes: 333}, false, baseTime)
	store.Record(
		"fresh-container",
		StatsHistorySample{CPUTenths: 333, MemoryTenths: 444, MemoryUsageBytes: 555},
		false,
		baseTime.Add(9*time.Minute),
	)

	store.Prune(baseTime.Add(11 * time.Minute))

	if stale := store.History("stale-container"); stale != nil {
		t.Fatalf("stale history = %#v, want nil", stale)
	}
	freshSnapshot := store.History("fresh-container")
	if len(freshSnapshot) != 1 {
		t.Fatalf("fresh history length = %d, want 1", len(freshSnapshot))
	}
	if freshSnapshot[0].CPUTenths != 333 {
		t.Fatalf("fresh CPUTenths = %d, want 333", freshSnapshot[0].CPUTenths)
	}
	if freshSnapshot[0].MemoryTenths != 444 {
		t.Fatalf("fresh MemoryTenths = %d, want 444", freshSnapshot[0].MemoryTenths)
	}
	if freshSnapshot[0].MemoryUsageBytes != 555 {
		t.Fatalf("fresh MemoryUsageBytes = %d, want 555", freshSnapshot[0].MemoryUsageBytes)
	}
}

func TestRemoveEvictsHistory(t *testing.T) {
	store := NewStore()
	store.Record("container-1", StatsHistorySample{CPUTenths: 1}, false, time.Now())
	store.Remove("container-1")
	if history := store.History("container-1"); history != nil {
		t.Fatalf("history after remove = %#v, want nil", history)
	}
}
