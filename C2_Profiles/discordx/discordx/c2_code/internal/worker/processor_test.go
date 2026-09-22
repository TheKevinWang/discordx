package worker_test

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

const (
	listenerID   = "11111111-1111-4111-8111-111111111111"
	generationID = "22222222-2222-4222-8222-222222222222"
	clientID     = "33333333-3333-4333-8333-333333333333"
	channelID    = "100000000000000001"
	messageID    = "200000000000000001"
)

type fakeProvider struct {
	attachment []byte
	downloads  int
	deletes    int
	deleteErr  error
}

func (provider *fakeProvider) DownloadAttachment(context.Context, discord.Attachment) ([]byte, error) {
	provider.downloads++
	return append([]byte(nil), provider.attachment...), nil
}

func (provider *fakeProvider) Delete(context.Context, string, string) error {
	provider.deletes++
	return provider.deleteErr
}

type fakeBridge struct {
	ingresses []worker.Ingress
	accept    bool
}

func (bridge *fakeBridge) SendIngress(_ context.Context, ingress worker.Ingress) (worker.Receipt, error) {
	bridge.ingresses = append(bridge.ingresses, ingress)
	return worker.Receipt{IngressID: ingress.ID, Accepted: bridge.accept}, nil
}

func testRegistry(t *testing.T, wire config.Wire) (*registry.Manager, registry.Generation) {
	t.Helper()
	manager := registry.NewManager()
	generation := registry.Generation{
		ID: generationID, State: registry.Active, DiscordToken: "token-canary",
		BotUserID: "900000000000000001", TaskChannelID: channelID,
		Provider: config.Provider{Kind: "discord"}, Wire: wire,
	}
	snapshot := registry.Snapshot{
		ProfileName: "discordx", Revision: 1,
		Listeners: []registry.Listener{{
			ID: listenerID, Name: "one", OperationID: 7, Enabled: true,
			ActiveGenerationID: generationID, Ingress: config.Ingress{},
			Egress: config.EgressProxy{Mode: "direct"}, Generations: []registry.Generation{generation},
		}},
	}
	if _, err := manager.ApplySnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	_, normalized, ok := manager.Generation(listenerID, generationID)
	if !ok {
		t.Fatal("generation was not published")
	}
	return manager, normalized
}

func testProcessor(t *testing.T, manager *registry.Manager, provider *fakeProvider, bridge *fakeBridge) (*worker.Processor, *state.Store) {
	t.Helper()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"), 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	processor, err := worker.NewProcessor(manager, store, provider, bridge)
	if err != nil {
		t.Fatal(err)
	}
	return processor, store
}

func TestFixedIngressReceiptDedupeCursorAndCleanup(t *testing.T) {
	manager, generation := testRegistry(t, config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "none", KeyMode: "single", UseBase64: false,
	})
	protocol, err := envelope.NewProtocol(generation.Wire, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(clientID), []byte("agent-body")...)
	document, err := protocol.Encode(envelope.Message{
		Body: body, SenderID: clientID, ToServer: true, Format: envelope.RawV1,
	}, envelope.AgentToServer)
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeProvider{attachment: []byte(document)}
	bridge := &fakeBridge{accept: true}
	processor, store := testProcessor(t, manager, provider, bridge)
	message := discord.Message{ID: messageID, ChannelID: channelID, Attachments: []discord.Attachment{{URL: "https://cdn.invalid/a"}}}
	result, err := processor.Process(context.Background(), generation.Provider.ID, message)
	if err != nil || !result.Accepted || result.Duplicate {
		t.Fatalf("Process() = %#v, %v", result, err)
	}
	if len(bridge.ingresses) != 1 || string(bridge.ingresses[0].Message) != string(body) || bridge.ingresses[0].Base64Message != nil ||
		bridge.ingresses[0].TrackingID != "dx2:"+listenerID+":"+generationID+":f:"+clientID {
		t.Fatalf("ingress = %#v", bridge.ingresses)
	}
	if provider.downloads != 1 || provider.deletes != 1 {
		t.Fatalf("provider calls = downloads:%d deletes:%d", provider.downloads, provider.deletes)
	}
	result, err = processor.Process(context.Background(), generation.Provider.ID, message)
	if err != nil || !result.Duplicate || len(bridge.ingresses) != 1 {
		t.Fatalf("duplicate Process() = %#v, %v; ingress count %d", result, err, len(bridge.ingresses))
	}
	cursor, err := store.Cursor(context.Background(), state.ChannelKey{ProviderID: generation.Provider.ID, ListenerID: listenerID, ChannelID: channelID})
	if err != nil || cursor != messageID {
		t.Fatalf("Cursor() = %q, %v", cursor, err)
	}
}

func TestLegacyIngressUsesBase64PayloadAndLegacyRoute(t *testing.T) {
	manager, generation := testRegistry(t, config.Wire{Protocol: "legacy"})
	body := base64.StdEncoding.EncodeToString(append([]byte(clientID), []byte("legacy-body")...))
	document := `{"message":"` + body + `","sender_id":"legacy route","to_server":true}`
	provider := &fakeProvider{}
	bridge := &fakeBridge{accept: true}
	processor, _ := testProcessor(t, manager, provider, bridge)
	result, err := processor.Process(context.Background(), generation.Provider.ID, discord.Message{
		ID: messageID, ChannelID: channelID, Content: document,
	})
	if err != nil || !result.Accepted {
		t.Fatalf("Process() = %#v, %v", result, err)
	}
	if len(bridge.ingresses) != 1 || string(bridge.ingresses[0].Base64Message) != body || bridge.ingresses[0].Message != nil ||
		bridge.ingresses[0].TrackingID != "dx2:"+listenerID+":"+generationID+":l:bGVnYWN5IHJvdXRl" {
		t.Fatalf("legacy ingress = %#v", bridge.ingresses)
	}
}

func TestMalformedIsFinalizedWithoutForwardingOrRetry(t *testing.T) {
	manager, generation := testRegistry(t, config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64", Protection: "none", KeyMode: "single",
	})
	provider := &fakeProvider{}
	bridge := &fakeBridge{accept: true}
	processor, _ := testProcessor(t, manager, provider, bridge)
	message := discord.Message{ID: messageID, ChannelID: channelID, Content: "malformed-content-canary"}
	if _, err := processor.Process(context.Background(), generation.Provider.ID, message); !errors.Is(err, worker.ErrInvalidDocument) {
		t.Fatalf("Process() error = %v", err)
	}
	result, err := processor.Process(context.Background(), generation.Provider.ID, message)
	if err != nil || !result.Duplicate || len(bridge.ingresses) != 0 {
		t.Fatalf("second Process() = %#v, %v, ingresses=%d", result, err, len(bridge.ingresses))
	}
}

func TestReceiptPrecedesCursorAndCleanupFailureIsDurable(t *testing.T) {
	manager, generation := testRegistry(t, config.Wire{Protocol: "legacy"})
	body := base64.StdEncoding.EncodeToString(append([]byte(clientID), []byte("legacy-body")...))
	document := `{"message":"` + body + `","sender_id":"legacy route","to_server":true}`
	provider := &fakeProvider{deleteErr: errors.New("delete failed")}
	bridge := &fakeBridge{accept: false}
	processor, store := testProcessor(t, manager, provider, bridge)
	message := discord.Message{ID: messageID, ChannelID: channelID, Content: document}
	if _, err := processor.Process(context.Background(), generation.Provider.ID, message); !errors.Is(err, worker.ErrIngressRejected) {
		t.Fatalf("rejected Process() error = %v", err)
	}
	channel := state.ChannelKey{ProviderID: generation.Provider.ID, ListenerID: listenerID, ChannelID: channelID}
	if cursor, _ := store.Cursor(context.Background(), channel); cursor != "" {
		t.Fatalf("cursor advanced before receipt: %q", cursor)
	}
	bridge.accept = true
	result, err := processor.Recover(context.Background(), generation.Provider.ID, message)
	if !errors.Is(err, worker.ErrCleanupPending) || !result.Accepted || !result.CleanupPending {
		t.Fatalf("Recover() = %#v, %v", result, err)
	}
	if cursor, _ := store.Cursor(context.Background(), channel); cursor != messageID {
		t.Fatalf("cursor after receipt = %q", cursor)
	}
	pending, err := store.Pending(context.Background(), channel, 10)
	if err != nil || len(pending) != 1 || pending[0].State != state.CleanupPending {
		t.Fatalf("Pending() = %#v, %v", pending, err)
	}
}

func TestPendingObservationResumesAfterRestartInsteadOfBecomingDuplicate(t *testing.T) {
	manager, generation := testRegistry(t, config.Wire{Protocol: "legacy"})
	body := base64.StdEncoding.EncodeToString(append([]byte(clientID), []byte("legacy-body")...))
	document := `{"message":"` + body + `","sender_id":"legacy route","to_server":true}`
	provider := &fakeProvider{}
	bridge := &fakeBridge{accept: false}
	processor, _ := testProcessor(t, manager, provider, bridge)
	message := discord.Message{ID: messageID, ChannelID: channelID, Content: document}
	if _, err := processor.Process(context.Background(), generation.Provider.ID, message); !errors.Is(err, worker.ErrIngressRejected) {
		t.Fatalf("initial Process() error = %v", err)
	}
	bridge.accept = true
	result, err := processor.Process(context.Background(), generation.Provider.ID, message)
	if err != nil || !result.Accepted || result.Duplicate || len(bridge.ingresses) != 2 {
		t.Fatalf("resumed Process() = %#v, %v; ingresses=%d", result, err, len(bridge.ingresses))
	}
}
