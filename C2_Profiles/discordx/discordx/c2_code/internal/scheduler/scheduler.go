// Package scheduler provides bounded per-listener queues and fair selection
// for one bot worker. Blocking operations wait on one shared notification
// channel and do not create per-item goroutines or timers.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

type Lane string

const (
	Standard Lane = "standard"
	Socks    Lane = "socks"
	Cleanup  Lane = "cleanup"
	Recovery Lane = "recovery"
)

type Item struct {
	ListenerID string
	Lane       Lane
	Value      any
}

type Config struct {
	MaxListeners  int
	StandardDepth int
	SocksDepth    int
	CleanupDepth  int
	RecoveryDepth int
}

type ListenerStatus struct {
	ListenerID      string        `json:"listener_id"`
	StandardDepth   int           `json:"standard_depth"`
	SocksDepth      int           `json:"socks_depth"`
	CleanupDepth    int           `json:"cleanup_depth"`
	RecoveryDepth   int           `json:"recovery_depth"`
	SaturationCount uint64        `json:"saturation_count"`
	AccumulatedWait time.Duration `json:"accumulated_wait"`
}

type listenerQueues struct {
	items      map[Lane][]Item
	saturation uint64
	wait       time.Duration
}

type Scheduler struct {
	mu                sync.Mutex
	config            Config
	listeners         map[string]*listenerQueues
	order             []string
	cursor            int
	normalCount       int
	opportunities     uint64
	maintenanceToggle bool
	notify            chan struct{}
}

func New(cfg Config) (*Scheduler, error) {
	if cfg.MaxListeners < 1 || cfg.MaxListeners > 1000 ||
		cfg.StandardDepth < 1 || cfg.StandardDepth > 4096 ||
		cfg.SocksDepth < 1 || cfg.SocksDepth > 4096 ||
		cfg.CleanupDepth < 1 || cfg.CleanupDepth > 16_384 ||
		cfg.RecoveryDepth < 1 || cfg.RecoveryDepth > 5000 {
		return nil, errors.New("scheduler bounds are invalid")
	}
	return &Scheduler{
		config: cfg, listeners: make(map[string]*listenerQueues),
		order: make([]string, 0, cfg.MaxListeners), notify: make(chan struct{}, 1),
	}, nil
}

func (scheduler *Scheduler) Enqueue(ctx context.Context, item Item) error {
	if !route.IsCanonicalUUID(item.ListenerID) || !validLane(item.Lane) {
		return errors.New("scheduler item route or lane is invalid")
	}
	started := time.Now()
	saturated := false
	for {
		scheduler.mu.Lock()
		queues, ok := scheduler.listeners[item.ListenerID]
		if !ok {
			if len(scheduler.listeners) >= scheduler.config.MaxListeners {
				scheduler.mu.Unlock()
				return errors.New("scheduler listener limit is reached")
			}
			queues = &listenerQueues{items: map[Lane][]Item{
				Standard: {}, Socks: {}, Cleanup: {}, Recovery: {},
			}}
			scheduler.listeners[item.ListenerID] = queues
			scheduler.order = append(scheduler.order, item.ListenerID)
		}
		if len(queues.items[item.Lane]) < scheduler.capacity(item.Lane) {
			queues.items[item.Lane] = append(queues.items[item.Lane], item)
			if saturated {
				queues.wait += time.Since(started)
			}
			scheduler.signal()
			scheduler.mu.Unlock()
			return nil
		}
		if !saturated {
			queues.saturation++
			saturated = true
		}
		scheduler.mu.Unlock()

		select {
		case <-ctx.Done():
			scheduler.mu.Lock()
			if queues, ok := scheduler.listeners[item.ListenerID]; ok && saturated {
				queues.wait += time.Since(started)
			}
			scheduler.mu.Unlock()
			return ctx.Err()
		case <-scheduler.notify:
		}
	}
}

func (scheduler *Scheduler) Next(ctx context.Context) (Item, error) {
	for {
		scheduler.mu.Lock()
		if item, ok := scheduler.nextLocked(); ok {
			scheduler.signal()
			scheduler.mu.Unlock()
			return item, nil
		}
		scheduler.mu.Unlock()
		select {
		case <-ctx.Done():
			return Item{}, ctx.Err()
		case <-scheduler.notify:
		}
	}
}

func (scheduler *Scheduler) nextLocked() (Item, bool) {
	hasStandard := scheduler.hasLane(Standard)
	hasSocks := scheduler.hasLane(Socks)
	hasMaintenance := scheduler.hasLane(Recovery) || scheduler.hasLane(Cleanup)
	if hasMaintenance && scheduler.opportunities%8 == 7 {
		if item, ok := scheduler.popMaintenance(); ok {
			scheduler.opportunities++
			return item, true
		}
	}
	if hasStandard {
		if hasSocks && scheduler.normalCount >= 3 {
			if item, ok := scheduler.popLane(Socks); ok {
				scheduler.normalCount = 0
				scheduler.opportunities++
				return item, true
			}
		}
		if item, ok := scheduler.popLane(Standard); ok {
			scheduler.normalCount++
			scheduler.opportunities++
			return item, true
		}
	}
	if hasSocks {
		if item, ok := scheduler.popLane(Socks); ok {
			scheduler.normalCount = 0
			scheduler.opportunities++
			return item, true
		}
	}
	if item, ok := scheduler.popMaintenance(); ok {
		scheduler.opportunities++
		return item, true
	}
	return Item{}, false
}

func (scheduler *Scheduler) popMaintenance() (Item, bool) {
	first, second := Recovery, Cleanup
	if scheduler.maintenanceToggle {
		first, second = second, first
	}
	if item, ok := scheduler.popLane(first); ok {
		scheduler.maintenanceToggle = !scheduler.maintenanceToggle
		return item, true
	}
	if item, ok := scheduler.popLane(second); ok {
		scheduler.maintenanceToggle = !scheduler.maintenanceToggle
		return item, true
	}
	return Item{}, false
}

func (scheduler *Scheduler) popLane(lane Lane) (Item, bool) {
	if len(scheduler.order) == 0 {
		return Item{}, false
	}
	for offset := 0; offset < len(scheduler.order); offset++ {
		index := (scheduler.cursor + offset) % len(scheduler.order)
		queues := scheduler.listeners[scheduler.order[index]]
		items := queues.items[lane]
		if len(items) == 0 {
			continue
		}
		item := items[0]
		items[0] = Item{}
		queues.items[lane] = items[1:]
		scheduler.cursor = (index + 1) % len(scheduler.order)
		return item, true
	}
	return Item{}, false
}

func (scheduler *Scheduler) hasLane(lane Lane) bool {
	for _, listenerID := range scheduler.order {
		if len(scheduler.listeners[listenerID].items[lane]) > 0 {
			return true
		}
	}
	return false
}

func (scheduler *Scheduler) Status(listenerID string) ListenerStatus {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	status := ListenerStatus{ListenerID: listenerID}
	if queues, ok := scheduler.listeners[listenerID]; ok {
		status.StandardDepth = len(queues.items[Standard])
		status.SocksDepth = len(queues.items[Socks])
		status.CleanupDepth = len(queues.items[Cleanup])
		status.RecoveryDepth = len(queues.items[Recovery])
		status.SaturationCount = queues.saturation
		status.AccumulatedWait = queues.wait
	}
	return status
}

func (scheduler *Scheduler) RemoveListener(listenerID string) error {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	queues, ok := scheduler.listeners[listenerID]
	if !ok {
		return nil
	}
	for _, lane := range []Lane{Standard, Socks, Cleanup, Recovery} {
		if len(queues.items[lane]) != 0 {
			return errors.New("cannot remove listener with queued work")
		}
	}
	delete(scheduler.listeners, listenerID)
	for index, current := range scheduler.order {
		if current == listenerID {
			scheduler.order = append(scheduler.order[:index], scheduler.order[index+1:]...)
			if len(scheduler.order) == 0 {
				scheduler.cursor = 0
			} else if scheduler.cursor >= len(scheduler.order) {
				scheduler.cursor %= len(scheduler.order)
			}
			break
		}
	}
	return nil
}

func (scheduler *Scheduler) capacity(lane Lane) int {
	switch lane {
	case Standard:
		return scheduler.config.StandardDepth
	case Socks:
		return scheduler.config.SocksDepth
	case Cleanup:
		return scheduler.config.CleanupDepth
	case Recovery:
		return scheduler.config.RecoveryDepth
	default:
		return 0
	}
}

func validLane(lane Lane) bool {
	return lane == Standard || lane == Socks || lane == Cleanup || lane == Recovery
}

func (scheduler *Scheduler) signal() {
	select {
	case scheduler.notify <- struct{}{}:
	default:
	}
}
