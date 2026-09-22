package worker

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

var (
	ErrRecoveryBound  = errors.New("recovery message bound reached")
	ErrRecoveryTime   = errors.New("recovery time budget reached")
	ErrLiveBufferFull = errors.New("Gateway live-event buffer is full")
)

type CursorStore interface {
	Cursor(context.Context, state.ChannelKey) (string, error)
}

type HistoryProvider interface {
	MessagesAfter(context.Context, string, string, int) ([]discord.Message, error)
}

type MessageProcessor interface {
	Process(context.Context, string, discord.Message) (Result, error)
}

type RecoveryConfig struct {
	PageSize    int
	MaxMessages int
	TimeBudget  time.Duration
	Now         func() time.Time
}

type RecoveryStats struct {
	Pages      int
	Observed   int
	Accepted   int
	Duplicates int
	Rejected   int
}

type Recoverer struct {
	store     CursorStore
	provider  HistoryProvider
	processor MessageProcessor
	config    RecoveryConfig
}

func NewRecoverer(store CursorStore, provider HistoryProvider, processor MessageProcessor, cfg RecoveryConfig) (*Recoverer, error) {
	if store == nil || provider == nil || processor == nil || cfg.PageSize < 1 || cfg.PageSize > 100 ||
		cfg.MaxMessages < 1 || cfg.MaxMessages > 5000 || cfg.TimeBudget <= 0 {
		return nil, errors.New("recovery configuration is invalid")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Recoverer{store: store, provider: provider, processor: processor, config: cfg}, nil
}

func (recoverer *Recoverer) CatchUp(ctx context.Context, channel state.ChannelKey) (RecoveryStats, error) {
	started := recoverer.config.Now()
	stats := RecoveryStats{}
	after, err := recoverer.store.Cursor(ctx, channel)
	if err != nil {
		return stats, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if recoverer.config.Now().Sub(started) >= recoverer.config.TimeBudget {
			return stats, ErrRecoveryTime
		}
		remaining := recoverer.config.MaxMessages - stats.Observed
		if remaining <= 0 {
			return stats, ErrRecoveryBound
		}
		limit := recoverer.config.PageSize
		if remaining < limit {
			limit = remaining
		}
		messages, err := recoverer.provider.MessagesAfter(ctx, channel.ChannelID, after, limit)
		if err != nil {
			return stats, err
		}
		stats.Pages++
		sort.Slice(messages, func(i, j int) bool { return snowflakeLess(messages[i].ID, messages[j].ID) })
		if len(messages) == 0 {
			return stats, nil
		}
		previous := after
		for _, message := range messages {
			stats.Observed++
			result, processErr := recoverer.processor.Process(ctx, channel.ProviderID, message)
			switch {
			case processErr == nil:
				if result.Duplicate {
					stats.Duplicates++
				} else if result.Accepted {
					stats.Accepted++
				}
			case errors.Is(processErr, ErrInvalidDocument):
				stats.Rejected++
			case errors.Is(processErr, ErrCleanupPending):
				stats.Accepted++
			default:
				return stats, processErr
			}
		}
		after, err = recoverer.store.Cursor(ctx, channel)
		if err != nil {
			return stats, err
		}
		if after == previous {
			return stats, errors.New("recovery cursor made no progress")
		}
		if len(messages) < limit {
			return stats, nil
		}
	}
}

func snowflakeLess(left, right string) bool {
	if len(left) == len(right) {
		return left < right
	}
	return len(left) < len(right)
}

type LiveEvent struct {
	ProviderID string
	Message    discord.Message
}

type LiveBuffer struct {
	mu       sync.Mutex
	capacity int
	events   []LiveEvent
}

func NewLiveBuffer(capacity int) (*LiveBuffer, error) {
	if capacity < 1 || capacity > 2048 {
		return nil, errors.New("Gateway live-event buffer capacity is invalid")
	}
	return &LiveBuffer{capacity: capacity, events: make([]LiveEvent, 0, capacity)}, nil
}

func (buffer *LiveBuffer) Push(event LiveEvent) error {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if len(buffer.events) >= buffer.capacity {
		return ErrLiveBufferFull
	}
	buffer.events = append(buffer.events, event)
	return nil
}

func (buffer *LiveBuffer) Drain() []LiveEvent {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	events := append([]LiveEvent(nil), buffer.events...)
	clear(buffer.events)
	buffer.events = buffer.events[:0]
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].ProviderID != events[j].ProviderID {
			return events[i].ProviderID < events[j].ProviderID
		}
		if events[i].Message.ChannelID != events[j].Message.ChannelID {
			return events[i].Message.ChannelID < events[j].Message.ChannelID
		}
		return snowflakeLess(events[i].Message.ID, events[j].Message.ID)
	})
	return events
}

func (buffer *LiveBuffer) Len() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return len(buffer.events)
}
