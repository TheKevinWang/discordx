package registry_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
)

const (
	listenerA   = "11111111-1111-4111-8111-111111111111"
	listenerB   = "22222222-2222-4222-8222-222222222222"
	generationA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	generationB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	clientA     = "aaaaaaaa-1111-4111-8111-111111111111"
	clientB     = "bbbbbbbb-2222-4222-8222-222222222222"
)

func validListener(id, generation, token, botUserID, taskChannel string) registry.Listener {
	return registry.Listener{
		ID: id, Name: "listener-" + id[:4], OperationID: 7, Enabled: true,
		ActiveGenerationID: generation,
		Ingress: config.Ingress{
			Mode: "gateway", PollStrategy: "adaptive", PollIntervalSeconds: 15,
			PollMinIntervalSeconds: 2, PollBaseIntervalSeconds: 15,
			PollMaxIntervalSeconds: 300, PollBuildWarmSeconds: 900,
			ReconciliationIntervalSeconds: 300,
		},
		Egress: config.EgressProxy{Mode: "direct"},
		Generations: []registry.Generation{{
			ID: generation, State: registry.Active, DiscordToken: token,
			BotUserID: botUserID, TaskChannelID: taskChannel,
			Provider: config.Provider{Kind: "discord"},
			Wire:     config.Wire{Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64", Protection: "none", KeyMode: "single"},
		}},
	}
}

func validSnapshot(revision uint64) registry.Snapshot {
	return registry.Snapshot{
		ProfileName: "discordx", Revision: revision, MaxListeners: 1000, MaxBotWorkers: 1000,
		Listeners: []registry.Listener{
			validListener(listenerA, generationA, "token-a-canary", "900000000000000001", "100000000000000001"),
			validListener(listenerB, generationB, "token-b-canary", "900000000000000002", "100000000000000002"),
		},
	}
}

func scaleSnapshot(listenerCount int, explicitLimits bool) registry.Snapshot {
	snapshot := registry.Snapshot{
		ProfileName: "discordx", Revision: 1,
		Listeners: make([]registry.Listener, 0, listenerCount),
	}
	if explicitLimits {
		snapshot.MaxListeners = listenerCount
		snapshot.MaxBotWorkers = listenerCount
	}
	for index := 1; index <= listenerCount; index++ {
		listenerID := fmt.Sprintf("%08x-0000-4000-8000-%012x", index, index)
		generationIndex := index + listenerCount
		generationID := fmt.Sprintf("%08x-0000-4000-8000-%012x", generationIndex, generationIndex)
		snapshot.Listeners = append(snapshot.Listeners, validListener(
			listenerID,
			generationID,
			fmt.Sprintf("synthetic-token-%d", index),
			fmt.Sprintf("9%017d", index),
			fmt.Sprintf("8%017d", index),
		))
		snapshot.Listeners[index-1].Name = fmt.Sprintf("scale-listener-%d", index)
	}
	return snapshot
}

func TestExplicitScaleLimitsMayExceedTheDefaultWithoutRemovingTheHardCeiling(t *testing.T) {
	manager := registry.NewManager()
	result, err := manager.ApplySnapshot(context.Background(), scaleSnapshot(1001, true))
	if err != nil {
		t.Fatalf("explicit 1001-listener snapshot error = %v", err)
	}
	if !result.Applied || result.StartedWorkers != 1001 {
		t.Fatalf("explicit 1001-listener result = %#v", result)
	}
}

func TestOmittedScaleLimitsRetainTheOneThousandListenerDefault(t *testing.T) {
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), scaleSnapshot(1001, false)); err == nil {
		t.Fatal("snapshot above the default 1000-listener ceiling unexpectedly succeeded")
	}
}

func TestExplicitScaleLimitsCannotExceedTheAbsoluteHardCeiling(t *testing.T) {
	snapshot := validSnapshot(1)
	snapshot.MaxListeners = 4097
	snapshot.MaxBotWorkers = 4097
	if _, err := registry.NewManager().ApplySnapshot(context.Background(), snapshot); err == nil {
		t.Fatal("snapshot above the absolute scale ceiling unexpectedly succeeded")
	}
}

func TestApplySnapshotPublishesAtomicallyAndRoutesOnlyExactGenerations(t *testing.T) {
	manager := registry.NewManager()
	result, err := manager.ApplySnapshot(context.Background(), validSnapshot(1))
	if err != nil || !result.Applied || result.Revision != 1 || result.StartedWorkers != 2 {
		t.Fatalf("ApplySnapshot() = %#v, %v", result, err)
	}

	routeA := "dx2:" + listenerA + ":" + generationA + ":f:" + clientA
	resolved, err := manager.ResolveTrackingRoute(routeA)
	if err != nil {
		t.Fatalf("ResolveTrackingRoute() error = %v", err)
	}
	if resolved.ListenerID != listenerA || resolved.GenerationID != generationA || resolved.TaskChannelID != "100000000000000001" {
		t.Fatalf("ResolveTrackingRoute() = %#v", resolved)
	}

	for _, invalid := range []string{
		"dx2:" + listenerA + ":" + generationB + ":f:" + clientA,
		"dx2:" + listenerB + ":" + generationA + ":f:" + clientB,
		"dx2:" + listenerA + ":" + generationA + ":l:" + base64.RawURLEncoding.EncodeToString([]byte("legacy")),
	} {
		if _, err := manager.ResolveTrackingRoute(invalid); err == nil {
			t.Errorf("ResolveTrackingRoute(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestInvalidSnapshotLeavesPreviousRevisionAndRoutesRunning(t *testing.T) {
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), validSnapshot(10)); err != nil {
		t.Fatalf("initial ApplySnapshot() error = %v", err)
	}
	invalid := validSnapshot(11)
	invalid.Listeners[1].Generations[0].TaskChannelID = invalid.Listeners[0].Generations[0].TaskChannelID
	if _, err := manager.ApplySnapshot(context.Background(), invalid); err == nil {
		t.Fatal("channel collision snapshot unexpectedly applied")
	}
	if got := manager.Revision(); got != 10 {
		t.Fatalf("Revision() = %d, want 10", got)
	}
	if _, err := manager.ResolveTrackingRoute("dx2:" + listenerA + ":" + generationA + ":f:" + clientA); err != nil {
		t.Fatalf("previous route stopped after rejected apply: %v", err)
	}
}

func TestOlderAndDuplicateRevisionsAreNoOpsAndUnchangedWorkersAreReused(t *testing.T) {
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), validSnapshot(4)); err != nil {
		t.Fatalf("initial ApplySnapshot() error = %v", err)
	}
	for _, revision := range []uint64{4, 3} {
		result, err := manager.ApplySnapshot(context.Background(), validSnapshot(revision))
		if err != nil || result.Applied || result.Revision != 4 {
			t.Fatalf("ApplySnapshot(revision=%d) = %#v, %v", revision, result, err)
		}
	}
	updated := validSnapshot(5)
	updated.Listeners[0].Name = "renamed"
	result, err := manager.ApplySnapshot(context.Background(), updated)
	if err != nil || !result.Applied || result.ReusedWorkers != 2 || result.StartedWorkers != 0 || result.StoppedWorkers != 0 {
		t.Fatalf("rename ApplySnapshot() = %#v, %v", result, err)
	}
}

func TestServerEgressChangeRestartsOnlyTheAffectedWorker(t *testing.T) {
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), validSnapshot(1)); err != nil {
		t.Fatalf("initial ApplySnapshot() error = %v", err)
	}
	updated := validSnapshot(2)
	updated.Listeners[0].Egress = config.EgressProxy{Mode: "http", URL: "http://proxy.internal:8080"}
	result, err := manager.ApplySnapshot(context.Background(), updated)
	if err != nil {
		t.Fatalf("updated ApplySnapshot() error = %v", err)
	}
	if result.ReusedWorkers != 1 || result.StartedWorkers != 1 || result.StoppedWorkers != 1 {
		t.Fatalf("egress-change ApplySnapshot() = %#v", result)
	}
}

func TestResolveChannelPreservesListenerGenerationAndLane(t *testing.T) {
	snapshot := validSnapshot(1)
	snapshot.Listeners[0].Generations[0].SocksChannelID = "100000000000000003"
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	provider, err := (config.Provider{Kind: "discord"}).Normalize(false)
	if err != nil {
		t.Fatalf("Provider.Normalize() error = %v", err)
	}
	standard, ok := manager.ResolveChannel(provider.ID, "100000000000000001")
	if !ok || standard.ListenerID != listenerA || standard.GenerationID != generationA || standard.Lane != "standard" {
		t.Fatalf("standard ResolveChannel() = %#v, %t", standard, ok)
	}
	socks, ok := manager.ResolveChannel(provider.ID, "100000000000000003")
	if !ok || socks.ListenerID != listenerA || socks.GenerationID != generationA || socks.Lane != "socks" {
		t.Fatalf("SOCKS ResolveChannel() = %#v, %t", socks, ok)
	}
}

func TestRetainedGenerationRemainsOutboundRoutableWithoutOwningActiveIngressChannel(t *testing.T) {
	snapshot := validSnapshot(1)
	retained := snapshot.Listeners[0].Generations[0]
	retained.ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	retained.State = registry.Draining
	snapshot.Listeners[0].Generations = append(snapshot.Listeners[0].Generations, retained)
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	provider, _ := (config.Provider{Kind: "discord"}).Normalize(false)
	resolved, ok := manager.ResolveChannel(provider.ID, "100000000000000001")
	if !ok || resolved.GenerationID != generationA {
		t.Fatalf("active ingress route = %#v, %t", resolved, ok)
	}
	oldRoute := "dx2:" + listenerA + ":" + retained.ID + ":f:" + clientA
	resolved, err := manager.ResolveTrackingRoute(oldRoute)
	if err != nil || resolved.GenerationID != retained.ID {
		t.Fatalf("retained outbound route = %#v, %v", resolved, err)
	}
}

func TestSharedBotRequiresOneEgressPolicyButCanOwnDistinctChannels(t *testing.T) {
	snapshot := validSnapshot(1)
	snapshot.Listeners[1].Generations[0].DiscordToken = snapshot.Listeners[0].Generations[0].DiscordToken
	snapshot.Listeners[1].Generations[0].BotUserID = snapshot.Listeners[0].Generations[0].BotUserID
	manager := registry.NewManager()
	result, err := manager.ApplySnapshot(context.Background(), snapshot)
	if err != nil || result.StartedWorkers != 1 {
		t.Fatalf("shared-bot ApplySnapshot() = %#v, %v", result, err)
	}

	conflict := validSnapshot(2)
	conflict.Listeners[1].Generations[0].DiscordToken = conflict.Listeners[0].Generations[0].DiscordToken
	conflict.Listeners[1].Generations[0].BotUserID = conflict.Listeners[0].Generations[0].BotUserID
	conflict.Listeners[1].Egress = config.EgressProxy{Mode: "http", URL: "http://proxy.internal:8080"}
	if _, err := manager.ApplySnapshot(context.Background(), conflict); err == nil {
		t.Fatal("shared bot with conflicting proxies unexpectedly applied")
	}
}

func TestMigrationAliasesAreExplicitUniqueAndWireMatched(t *testing.T) {
	snapshot := validSnapshot(1)
	snapshot.MigrationAliases = registry.MigrationAliases{
		DTE1: map[string]registry.GenerationRef{"abcdef0123456789": {ListenerID: listenerA, GenerationID: generationA}},
	}
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	resolved, err := manager.ResolveTrackingRoute("dte1:abcdef0123456789:" + clientA)
	if err != nil || resolved.ListenerID != listenerA || resolved.GenerationID != generationA {
		t.Fatalf("ResolveTrackingRoute(dte1) = %#v, %v", resolved, err)
	}
	if _, err := manager.ResolveTrackingRoute("dte1:0000000000000000:" + clientA); err == nil {
		t.Fatal("unknown dte1 alias unexpectedly resolved")
	}
}

func TestStatusAndErrorsNeverContainCredentialOrTransportKey(t *testing.T) {
	snapshot := validSnapshot(1)
	secretKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	snapshot.Listeners[0].Generations[0].Wire.Protection = "aes256-hmac-v1"
	snapshot.Listeners[0].Generations[0].Wire.KeyMode = "directional"
	snapshot.Listeners[0].Generations[0].Wire.Key = secretKey
	snapshot.Listeners[0].Egress = config.EgressProxy{
		Mode: "http", URL: "http://proxy.internal:8080",
		Username: "proxy-user-canary", Password: "proxy-password-canary",
	}
	manager := registry.NewManager()
	if _, err := manager.ApplySnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	status, err := json.Marshal(manager.Status())
	if err != nil {
		t.Fatalf("json.Marshal(Status()) error = %v", err)
	}
	text := string(status)
	for _, secret := range []string{"token-a-canary", "token-b-canary", secretKey, "proxy-user-canary", "proxy-password-canary"} {
		if strings.Contains(text, secret) {
			t.Fatalf("status leaked secret %q: %s", secret, text)
		}
	}
}
