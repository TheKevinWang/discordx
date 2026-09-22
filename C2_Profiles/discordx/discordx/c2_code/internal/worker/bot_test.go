package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

type memoryCursor struct {
	mu     sync.Mutex
	values map[state.ChannelKey]string
}

func (store *memoryCursor) Cursor(_ context.Context, channel state.ChannelKey) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.values[channel], nil
}

func (store *memoryCursor) advance(channel state.ChannelKey, messageID string) {
	store.mu.Lock()
	store.values[channel] = messageID
	store.mu.Unlock()
}

type runtimeProvider struct {
	mu       sync.Mutex
	messages map[string][]discord.Message
	gateway  []discord.Message
	calls    map[string]int
	started  chan struct{}
	done     chan struct{}
}

func (provider *runtimeProvider) MessagesAfter(_ context.Context, channel, after string, limit int) ([]discord.Message, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls[channel]++
	result := make([]discord.Message, 0, limit)
	for _, message := range provider.messages[channel] {
		if message.ID > after {
			result = append(result, message)
		}
	}
	return result, nil
}

func (provider *runtimeProvider) ListenGateway(ctx context.Context, handle func(discord.Message) error) error {
	if provider.started != nil {
		close(provider.started)
	}
	for _, message := range provider.gateway {
		if err := handle(message); err != nil {
			return err
		}
	}
	<-ctx.Done()
	if provider.done != nil {
		close(provider.done)
	}
	return ctx.Err()
}

type runtimeProcessor struct {
	mu       sync.Mutex
	store    *memoryCursor
	provider string
	bindings map[string]string
	seen     []discord.Message
}

func (processor *runtimeProcessor) Process(_ context.Context, providerID string, message discord.Message) (worker.Result, error) {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	listener, ok := processor.bindings[message.ChannelID]
	if !ok || providerID != processor.provider {
		return worker.Result{}, errors.New("wrong route")
	}
	processor.seen = append(processor.seen, message)
	processor.store.advance(state.ChannelKey{ProviderID: providerID, ListenerID: listener, ChannelID: message.ChannelID}, message.ID)
	return worker.Result{Accepted: true}, nil
}

func (processor *runtimeProcessor) count(channel string) int {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	count := 0
	for _, message := range processor.seen {
		if message.ChannelID == channel {
			count++
		}
	}
	return count
}

func TestBotRuntimeUsesOneGatewayAndOnePollingDeadlineLoopForMixedListeners(t *testing.T) {
	providerID := "provider-a"
	store := &memoryCursor{values: make(map[state.ChannelKey]string)}
	provider := &runtimeProvider{
		messages: map[string][]discord.Message{
			"100000000000000002": {{ID: "200000000000000002", ChannelID: "100000000000000002"}},
		},
		gateway: []discord.Message{
			{ID: "200000000000000001", ChannelID: channelID},
			{ID: "200000000000000099", ChannelID: "100000000000000002"},
		},
		calls: make(map[string]int),
	}
	processor := &runtimeProcessor{
		store: store, provider: providerID,
		bindings: map[string]string{channelID: listenerID, "100000000000000002": secondListenerID},
	}
	runtime, err := worker.NewBotRuntime(worker.BotSpec{
		Provider: configProvider(providerID), Token: "token",
		Listeners: []worker.ListenerBinding{
			{ListenerID: listenerID, GenerationID: generationID, TaskChannel: channelID, IngressMode: "gateway"},
			{ListenerID: secondListenerID, GenerationID: "55555555-5555-4555-8555-555555555555", TaskChannel: "100000000000000002", IngressMode: "polling", PollEvery: 5 * time.Millisecond},
		},
	}, store, provider, processor)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (processor.count(channelID) < 1 || processor.count("100000000000000002") < 1) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if processor.count(channelID) != 1 {
		t.Fatalf("Gateway channel deliveries = %d", processor.count(channelID))
	}
	if processor.count("100000000000000002") != 1 {
		t.Fatalf("polling channel deliveries = %d; polling-only Gateway event was not ignored", processor.count("100000000000000002"))
	}
}

func TestBotRuntimeStopsWhenParentContextIsCanceled(t *testing.T) {
	store := &memoryCursor{values: make(map[state.ChannelKey]string)}
	provider := &runtimeProvider{
		messages: make(map[string][]discord.Message), calls: make(map[string]int),
		started: make(chan struct{}), done: make(chan struct{}),
	}
	processor := &runtimeProcessor{
		store: store, provider: "provider-a", bindings: map[string]string{channelID: listenerID},
	}
	runtime, err := worker.NewBotRuntime(worker.BotSpec{
		Provider: configProvider("provider-a"), Token: "token",
		Listeners: []worker.ListenerBinding{{
			ListenerID: listenerID, GenerationID: generationID, TaskChannel: channelID,
			IngressMode: "gateway", PollEvery: 15 * time.Second,
		}},
	}, store, provider, processor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("Gateway worker did not start")
	}
	cancel()
	select {
	case <-provider.done:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not stop the Gateway worker")
	}
}

func configProvider(id string) config.Provider {
	return config.Provider{ID: id, Kind: "discord", APIBaseURL: "https://discord.com/api", GatewayBaseURL: "wss://gateway.discord.gg", CDNBaseURL: "https://cdn.discordapp.com", APIVersion: 10}
}
