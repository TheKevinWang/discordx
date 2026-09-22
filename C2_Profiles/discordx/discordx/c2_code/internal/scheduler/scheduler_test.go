package scheduler_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/scheduler"
)

const (
	listenerA = "11111111-1111-4111-8111-111111111111"
	listenerB = "22222222-2222-4222-8222-222222222222"
)

func newScheduler(t *testing.T, capacity int) *scheduler.Scheduler {
	t.Helper()
	value, err := scheduler.New(scheduler.Config{
		MaxListeners: 10, StandardDepth: capacity, SocksDepth: capacity,
		CleanupDepth: capacity, RecoveryDepth: capacity,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return value
}

func enqueue(t *testing.T, value *scheduler.Scheduler, listener string, lane scheduler.Lane, payload string) {
	t.Helper()
	if err := value.Enqueue(context.Background(), scheduler.Item{ListenerID: listener, Lane: lane, Value: payload}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
}

func TestQueueBoundsBlockUntilContextAndReportSaturation(t *testing.T) {
	value := newScheduler(t, 1)
	enqueue(t, value, listenerA, scheduler.Standard, "first")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := value.Enqueue(ctx, scheduler.Item{ListenerID: listenerA, Lane: scheduler.Standard, Value: "blocked"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked Enqueue() error = %v", err)
	}
	status := value.Status(listenerA)
	if status.StandardDepth != 1 || status.SaturationCount == 0 || status.AccumulatedWait <= 0 {
		t.Fatalf("Status() = %#v", status)
	}
	if _, err := value.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := value.Enqueue(context.Background(), scheduler.Item{ListenerID: listenerA, Lane: scheduler.Standard, Value: "after"}); err != nil {
		t.Fatalf("Enqueue() after dequeue error = %v", err)
	}
}

func TestStandardTrafficIsFairAcrossListeners(t *testing.T) {
	value := newScheduler(t, 10)
	for index := 0; index < 3; index++ {
		enqueue(t, value, listenerA, scheduler.Standard, "a")
		enqueue(t, value, listenerB, scheduler.Standard, "b")
	}
	for index, want := range []string{listenerA, listenerB, listenerA, listenerB, listenerA, listenerB} {
		item, err := value.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if item.ListenerID != want {
			t.Fatalf("item %d listener = %s, want %s", index, item.ListenerID, want)
		}
	}
}

func TestSocksUsesAtMostOneQuarterWhileStandardIsQueued(t *testing.T) {
	value := newScheduler(t, 32)
	for index := 0; index < 20; index++ {
		enqueue(t, value, listenerA, scheduler.Standard, "standard")
		enqueue(t, value, listenerB, scheduler.Socks, "socks")
	}
	socks := 0
	standard := 0
	for range 20 {
		item, err := value.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if item.Lane == scheduler.Socks {
			socks++
		} else if item.Lane == scheduler.Standard {
			standard++
		}
	}
	if socks > 5 || standard < 15 {
		t.Fatalf("20 opportunities delivered standard=%d socks=%d", standard, socks)
	}
}

func TestRecoveryAndCleanupHaveReservedProgress(t *testing.T) {
	value := newScheduler(t, 10)
	enqueue(t, value, listenerA, scheduler.Recovery, "recover")
	enqueue(t, value, listenerB, scheduler.Cleanup, "clean")
	first, err := value.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := value.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lanes := map[scheduler.Lane]bool{first.Lane: true, second.Lane: true}
	if !lanes[scheduler.Recovery] || !lanes[scheduler.Cleanup] {
		t.Fatalf("reserved lanes did not progress: %#v %#v", first, second)
	}
}

func TestEmptyNextHonorsCancellationWithoutCreatingGoroutines(t *testing.T) {
	value := newScheduler(t, 10)
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := value.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v", err)
	}
	if after := runtime.NumGoroutine(); after != before {
		t.Fatalf("goroutine count changed from %d to %d", before, after)
	}
}

func TestInvalidLaneListenerAndCapacityAreRejected(t *testing.T) {
	if _, err := scheduler.New(scheduler.Config{}); err == nil {
		t.Fatal("zero scheduler configuration accepted")
	}
	value := newScheduler(t, 1)
	for _, item := range []scheduler.Item{
		{ListenerID: "not-a-uuid", Lane: scheduler.Standard},
		{ListenerID: listenerA, Lane: scheduler.Lane("unknown")},
	} {
		if err := value.Enqueue(context.Background(), item); err == nil {
			t.Fatalf("invalid item accepted: %#v", item)
		}
	}
}
