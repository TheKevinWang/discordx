package worker_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

type historyProvider struct {
	messages []discord.Message
	afters   []string
}

func (provider *historyProvider) MessagesAfter(_ context.Context, channel, after string, limit int) ([]discord.Message, error) {
	provider.afters = append(provider.afters, after)
	result := make([]discord.Message, 0, limit)
	for _, message := range provider.messages {
		if message.ChannelID == channel && (len(message.ID) > len(after) || len(message.ID) == len(after) && message.ID > after) {
			result = append(result, message)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

type advancingProcessor struct {
	store   *state.Store
	channel state.ChannelKey
	seen    []string
	failOn  string
}

func (processor *advancingProcessor) Process(ctx context.Context, _ string, message discord.Message) (worker.Result, error) {
	processor.seen = append(processor.seen, message.ID)
	if message.ID == processor.failOn {
		return worker.Result{}, errors.New("transient ingress failure")
	}
	if err := processor.store.AdvanceCursor(ctx, processor.channel, message.ID); err != nil {
		return worker.Result{}, err
	}
	return worker.Result{Accepted: true}, nil
}

func recoveryStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"), 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestRecoveryPagesForwardFromDurableCursorInSnowflakeOrder(t *testing.T) {
	store := recoveryStore(t)
	channel := state.ChannelKey{ProviderID: "provider-a", ListenerID: listenerID, ChannelID: channelID}
	if err := store.AdvanceCursor(context.Background(), channel, "200000000000000001"); err != nil {
		t.Fatal(err)
	}
	provider := &historyProvider{messages: []discord.Message{
		{ID: "200000000000000002", ChannelID: channelID},
		{ID: "200000000000000003", ChannelID: channelID},
		{ID: "200000000000000004", ChannelID: channelID},
	}}
	processor := &advancingProcessor{store: store, channel: channel}
	recoverer, err := worker.NewRecoverer(store, provider, processor, worker.RecoveryConfig{
		PageSize: 2, MaxMessages: 10, TimeBudget: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := recoverer.CatchUp(context.Background(), channel)
	if err != nil || stats.Pages != 2 || stats.Accepted != 3 {
		t.Fatalf("CatchUp() = %#v, %v", stats, err)
	}
	if len(provider.afters) != 2 || provider.afters[0] != "200000000000000001" || provider.afters[1] != "200000000000000003" {
		t.Fatalf("after cursors = %#v", provider.afters)
	}
}

func TestRecoveryStopsBeforeAdvancingPastTransientFailure(t *testing.T) {
	store := recoveryStore(t)
	channel := state.ChannelKey{ProviderID: "provider-a", ListenerID: listenerID, ChannelID: channelID}
	provider := &historyProvider{messages: []discord.Message{
		{ID: "200000000000000001", ChannelID: channelID},
		{ID: "200000000000000002", ChannelID: channelID},
		{ID: "200000000000000003", ChannelID: channelID},
	}}
	processor := &advancingProcessor{store: store, channel: channel, failOn: "200000000000000002"}
	recoverer, _ := worker.NewRecoverer(store, provider, processor, worker.RecoveryConfig{PageSize: 3, MaxMessages: 10, TimeBudget: time.Minute})
	if _, err := recoverer.CatchUp(context.Background(), channel); err == nil {
		t.Fatal("CatchUp() succeeded despite transient failure")
	}
	if cursor, _ := store.Cursor(context.Background(), channel); cursor != "200000000000000001" {
		t.Fatalf("cursor advanced past failure: %q", cursor)
	}
}

func TestLiveBufferIsBoundedAndDrainsInChannelSnowflakeOrder(t *testing.T) {
	buffer, err := worker.NewLiveBuffer(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"200000000000000002", "200000000000000001"} {
		if err := buffer.Push(worker.LiveEvent{ProviderID: "provider-a", Message: discord.Message{ID: id, ChannelID: channelID}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := buffer.Push(worker.LiveEvent{ProviderID: "provider-a", Message: discord.Message{ID: "200000000000000003", ChannelID: channelID}}); !errors.Is(err, worker.ErrLiveBufferFull) {
		t.Fatalf("overflow error = %v", err)
	}
	events := buffer.Drain()
	if len(events) != 2 || events[0].Message.ID != "200000000000000001" || events[1].Message.ID != "200000000000000002" || buffer.Len() != 0 {
		t.Fatalf("Drain() = %#v, len=%d", events, buffer.Len())
	}
}

func TestRecoveryHonorsTimeAndMessageBounds(t *testing.T) {
	store := recoveryStore(t)
	channel := state.ChannelKey{ProviderID: "provider-a", ListenerID: listenerID, ChannelID: channelID}
	provider := &historyProvider{messages: []discord.Message{{ID: "200000000000000001", ChannelID: channelID}, {ID: "200000000000000002", ChannelID: channelID}}}
	processor := &advancingProcessor{store: store, channel: channel}
	recoverer, _ := worker.NewRecoverer(store, provider, processor, worker.RecoveryConfig{PageSize: 1, MaxMessages: 1, TimeBudget: time.Minute})
	if _, err := recoverer.CatchUp(context.Background(), channel); !errors.Is(err, worker.ErrRecoveryBound) {
		t.Fatalf("message-bound error = %v", err)
	}

	now := time.Unix(100, 0)
	calls := 0
	recoverer, _ = worker.NewRecoverer(store, provider, processor, worker.RecoveryConfig{
		PageSize: 1, MaxMessages: 10, TimeBudget: time.Second,
		Now: func() time.Time { calls++; return now.Add(time.Duration(calls) * time.Second) },
	})
	if _, err := recoverer.CatchUp(context.Background(), channel); !errors.Is(err, worker.ErrRecoveryTime) {
		t.Fatalf("time-bound error = %v", err)
	}
}
