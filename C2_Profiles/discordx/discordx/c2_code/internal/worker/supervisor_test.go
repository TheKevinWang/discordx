package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

const secondListenerID = "44444444-4444-4444-8444-444444444444"

type fakeRuntime struct {
	starts int
	drains int
	fail   bool
}

func (runtime *fakeRuntime) Start(context.Context) error {
	runtime.starts++
	if runtime.fail {
		return errors.New("token-canary must never escape")
	}
	return nil
}

func (runtime *fakeRuntime) Drain(context.Context) error {
	runtime.drains++
	return nil
}

type fakeFactory struct {
	runtimes []*fakeRuntime
	byBot    map[string][]*fakeRuntime
	failBot  string
}

func (factory *fakeFactory) Build(spec worker.BotSpec) (worker.Runtime, error) {
	runtime := &fakeRuntime{fail: spec.BotUserID == factory.failBot}
	factory.runtimes = append(factory.runtimes, runtime)
	if factory.byBot == nil {
		factory.byBot = make(map[string][]*fakeRuntime)
	}
	factory.byBot[spec.BotUserID] = append(factory.byBot[spec.BotUserID], runtime)
	return runtime, nil
}

func botSpec(bot, token, listener, generation, channel, mode string) worker.BotSpec {
	return worker.BotSpec{
		Provider: config.Provider{Kind: "discord"}, Egress: config.EgressProxy{Mode: "direct"},
		Token: token, BotUserID: bot,
		Listeners: []worker.ListenerBinding{{
			ListenerID: listener, GenerationID: generation, TaskChannel: channel, IngressMode: mode,
		}},
	}
}

func TestSupervisorReusesUnchangedWorkerAndRestartsOnlyChangedWorker(t *testing.T) {
	factory := &fakeFactory{}
	supervisor, err := worker.NewSupervisor(factory, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first := botSpec("900000000000000001", "token-a", listenerID, generationID, channelID, "gateway")
	second := botSpec("900000000000000002", "token-b", secondListenerID, "55555555-5555-4555-8555-555555555555", "100000000000000002", "polling")
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{first, second}); err != nil {
		t.Fatal(err)
	}
	if len(factory.runtimes) != 2 {
		t.Fatalf("initial runtime count = %d", len(factory.runtimes))
	}
	firstRuntime := factory.byBot["900000000000000001"][0]
	secondRuntime := factory.byBot["900000000000000002"][0]
	second.Listeners[0].GenerationID = "66666666-6666-4666-8666-666666666666"
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{first, second}); err != nil {
		t.Fatal(err)
	}
	if len(factory.runtimes) != 3 || firstRuntime.drains != 0 || secondRuntime.drains != 1 {
		t.Fatalf("targeted reconcile runtimes=%d drains=%d/%d", len(factory.runtimes), firstRuntime.drains, secondRuntime.drains)
	}
}

func TestFailedBotDoesNotStopUnrelatedWorkerAndStatusIsRedacted(t *testing.T) {
	factory := &fakeFactory{failBot: "900000000000000002"}
	supervisor, _ := worker.NewSupervisor(factory, time.Second)
	first := botSpec("900000000000000001", "token-canary-a", listenerID, generationID, channelID, "gateway")
	second := botSpec("900000000000000002", "token-canary-b", secondListenerID, "55555555-5555-4555-8555-555555555555", "100000000000000002", "polling")
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{first, second}); err != nil {
		t.Fatal(err)
	}
	statuses := supervisor.Status()
	unrelated := factory.byBot["900000000000000001"][0]
	if len(statuses) != 2 || unrelated.drains != 0 {
		t.Fatalf("statuses=%#v unrelated drains=%d", statuses, unrelated.drains)
	}
	encoded, _ := json.Marshal(statuses)
	if strings.Contains(string(encoded), "token-canary") || strings.Contains(string(encoded), "worker_start_failed: token") {
		t.Fatalf("status leaked a secret: %s", encoded)
	}
	states := map[string]int{}
	for _, status := range statuses {
		states[status.State]++
	}
	if states["active"] != 1 || states["degraded"] != 1 {
		t.Fatalf("worker states = %#v", states)
	}
}

func TestSharedBotSpecUsesOneWorkerAndMixedIngressEnablesGateway(t *testing.T) {
	factory := &fakeFactory{}
	supervisor, _ := worker.NewSupervisor(factory, time.Second)
	shared := botSpec("900000000000000001", "token-a", listenerID, generationID, channelID, "gateway")
	shared.Listeners = append(shared.Listeners, worker.ListenerBinding{
		ListenerID: secondListenerID, GenerationID: "55555555-5555-4555-8555-555555555555",
		TaskChannel: "100000000000000002", IngressMode: "polling",
	})
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{shared}); err != nil {
		t.Fatal(err)
	}
	statuses := supervisor.Status()
	if len(factory.runtimes) != 1 || len(statuses) != 1 || len(statuses[0].ListenerIDs) != 2 || !statuses[0].GatewayEnabled {
		t.Fatalf("shared worker status = %#v, runtimes=%d", statuses, len(factory.runtimes))
	}
}

func TestRejectedDesiredSetLeavesPublishedWorkersUnchanged(t *testing.T) {
	factory := &fakeFactory{}
	supervisor, _ := worker.NewSupervisor(factory, time.Second)
	valid := botSpec("900000000000000001", "token-a", listenerID, generationID, channelID, "gateway")
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{valid}); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Listeners[0].TaskChannel = "not-a-snowflake"
	if err := supervisor.Reconcile(context.Background(), []worker.BotSpec{invalid}); err == nil {
		t.Fatal("invalid reconcile succeeded")
	}
	if len(supervisor.Status()) != 1 || factory.runtimes[0].drains != 0 {
		t.Fatalf("invalid desired set changed workers: %#v", supervisor.Status())
	}
}
