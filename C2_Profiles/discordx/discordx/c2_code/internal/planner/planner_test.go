package planner_test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/planner"
)

const (
	listenerID = "11111111-1111-4111-8111-111111111111"
	channelID  = "100000000000000001"
)

func adaptiveConfig() config.Ingress {
	return config.Ingress{
		Mode: "polling", PollStrategy: "adaptive", PollIntervalSeconds: 15,
		PollMinIntervalSeconds: 2, PollBaseIntervalSeconds: 15,
		PollMaxIntervalSeconds: 300, PollBuildWarmSeconds: 900,
		ReconciliationIntervalSeconds: 300,
	}
}

func configuredPlanner(t *testing.T, perListener, global int) *planner.Planner {
	t.Helper()
	value := planner.New(perListener, global)
	if err := value.Configure(listenerID, channelID, adaptiveConfig()); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	return value
}

func TestMissingActivitySnapshotUsesFixedFallback(t *testing.T) {
	value := configuredPlanner(t, 100, 1000)
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	decision, err := value.NextPoll(listenerID, channelID, now)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Reason != planner.FixedFallback || !decision.Deadline.Equal(now.Add(15*time.Second)) {
		t.Fatalf("NextPoll() = %#v", decision)
	}
}

func TestCallbackWindowUsesNuwaPositiveJitterAndResponseCadence(t *testing.T) {
	value := configuredPlanner(t, 100, 1000)
	last := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	if err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{
		Sequence: 1,
		Callbacks: []planner.CallbackHint{{
			ListenerID: listenerID, CallbackID: "opaque-a", Active: true,
			LastCheckin: last, IntervalSeconds: 60, JitterPercent: 25,
			MessageChecks: 10, TimeBetweenChecksSeconds: 10,
		}},
	}); err != nil {
		t.Fatalf("ApplyActivitySnapshot() error = %v", err)
	}
	before, err := value.NextPoll(listenerID, channelID, last)
	if err != nil {
		t.Fatal(err)
	}
	if before.Reason != planner.AgentDue || !before.Deadline.Equal(last.Add(60*time.Second)) {
		t.Fatalf("pre-window NextPoll() = %#v", before)
	}
	inWindow := last.Add(61 * time.Second)
	during, err := value.NextPoll(listenerID, channelID, inWindow)
	if err != nil {
		t.Fatal(err)
	}
	if during.Reason != planner.AgentDue || !during.Deadline.Equal(inWindow.Add(15*time.Second)) {
		t.Fatalf("in-window NextPoll() = %#v", during)
	}
}

func TestLocalActivityBacklogAndBuildWarmSignalsAreBounded(t *testing.T) {
	value := configuredPlanner(t, 100, 1000)
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	if err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	value.Observe(planner.Observation{ListenerID: listenerID, At: now, PayloadBuilt: true})
	warm, _ := value.NextPoll(listenerID, channelID, now)
	if warm.Reason != planner.BuildWarm || !warm.Deadline.Equal(now.Add(15*time.Second)) {
		t.Fatalf("build-warm NextPoll() = %#v", warm)
	}
	afterWarm := now.Add(901 * time.Second)
	cold, _ := value.NextPoll(listenerID, channelID, afterWarm)
	if cold.Reason != planner.ColdProbe || cold.Deadline.After(afterWarm.Add(300*time.Second)) {
		t.Fatalf("post-warm NextPoll() = %#v", cold)
	}
	value.Observe(planner.Observation{ListenerID: listenerID, At: afterWarm, FoundMessages: true})
	hot, _ := value.NextPoll(listenerID, channelID, afterWarm)
	if hot.Reason != planner.RecentActivity || !hot.Deadline.Equal(afterWarm.Add(2*time.Second)) {
		t.Fatalf("recent-activity NextPoll() = %#v", hot)
	}
	value.Observe(planner.Observation{ListenerID: listenerID, At: afterWarm, MorePages: true})
	backlog, _ := value.NextPoll(listenerID, channelID, afterWarm)
	if backlog.Reason != planner.Backlog || !backlog.Deadline.Equal(afterWarm) {
		t.Fatalf("backlog NextPoll() = %#v", backlog)
	}
}

func TestSequenceGapAndHintLimitsDegradeToFixedFallback(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	value := configuredPlanner(t, 1, 1)
	if err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{
		Sequence:  10,
		Callbacks: []planner.CallbackHint{{ListenerID: listenerID, CallbackID: "a", Active: true, LastCheckin: now, IntervalSeconds: 60, MessageChecks: 10, TimeBetweenChecksSeconds: 10}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := value.ApplyActivityHint(planner.ActivityHint{
		Sequence: 12,
		Callback: planner.CallbackHint{ListenerID: listenerID, CallbackID: "a", Active: true, LastCheckin: now, IntervalSeconds: 30, MessageChecks: 10, TimeBetweenChecksSeconds: 10},
	}); !errors.Is(err, planner.ErrSequenceGap) {
		t.Fatalf("gap error = %v", err)
	}
	decision, _ := value.NextPoll(listenerID, channelID, now)
	if decision.Reason != planner.FixedFallback {
		t.Fatalf("gap did not select fixed fallback: %#v", decision)
	}

	value = configuredPlanner(t, 1, 1)
	err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{
		Sequence: 1,
		Callbacks: []planner.CallbackHint{
			{ListenerID: listenerID, CallbackID: "a", Active: true, LastCheckin: now, IntervalSeconds: 60, MessageChecks: 10, TimeBetweenChecksSeconds: 10},
			{ListenerID: listenerID, CallbackID: "b", Active: true, LastCheckin: now, IntervalSeconds: 60, MessageChecks: 10, TimeBetweenChecksSeconds: 10},
		},
	})
	if !errors.Is(err, planner.ErrHintLimit) {
		t.Fatalf("limit error = %v", err)
	}
	decision, _ = value.NextPoll(listenerID, channelID, now)
	if decision.Reason != planner.FixedFallback {
		t.Fatalf("limit did not select fixed fallback: %#v", decision)
	}
}

func TestPlannerUsesNoPerCallbackGoroutines(t *testing.T) {
	value := configuredPlanner(t, 10_000, 10_000)
	now := time.Now().UTC()
	hints := make([]planner.CallbackHint, 10_000)
	for index := range hints {
		hints[index] = planner.CallbackHint{
			ListenerID: listenerID, CallbackID: string(rune(index + 1)), Active: true,
			LastCheckin: now, IntervalSeconds: 60 + index%10,
			MessageChecks: 10, TimeBetweenChecksSeconds: 10,
		}
	}
	before := runtime.NumGoroutine()
	if err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{Sequence: 1, Callbacks: hints}); err != nil {
		t.Fatal(err)
	}
	after := runtime.NumGoroutine()
	if after != before {
		t.Fatalf("goroutine count changed from %d to %d", before, after)
	}
	if got := value.HintCount(listenerID); got != len(hints) {
		t.Fatalf("HintCount() = %d", got)
	}
	if got := value.DeadlineCount(listenerID); got != len(hints) {
		t.Fatalf("DeadlineCount() = %d", got)
	}
}

func TestRepeatedHintUpdatesKeepOneBoundedHeapEntry(t *testing.T) {
	value := configuredPlanner(t, 10, 10)
	now := time.Now().UTC()
	callback := planner.CallbackHint{
		ListenerID: listenerID, CallbackID: "same", Active: true,
		LastCheckin: now, IntervalSeconds: 60,
		MessageChecks: 10, TimeBetweenChecksSeconds: 10,
	}
	if err := value.ApplyActivitySnapshot(planner.ActivitySnapshot{Sequence: 1, Callbacks: []planner.CallbackHint{callback}}); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(2); sequence <= 1001; sequence++ {
		callback.IntervalSeconds = 60 + int(sequence%10)
		if err := value.ApplyActivityHint(planner.ActivityHint{Sequence: sequence, Callback: callback}); err != nil {
			t.Fatal(err)
		}
	}
	if got := value.HintCount(listenerID); got != 1 {
		t.Fatalf("HintCount() = %d", got)
	}
	if got := value.DeadlineCount(listenerID); got != 1 {
		t.Fatalf("DeadlineCount() = %d, heap grew with updates", got)
	}
}

func TestResponseWindowValidationRejectsAgentWaitShorterThanTwoMinimumPolls(t *testing.T) {
	ingress := adaptiveConfig()
	if err := planner.ValidateResponseWindow(ingress, 2, 3); err == nil {
		t.Fatal("short response window accepted")
	}
	if err := planner.ValidateResponseWindow(ingress, 10, 10); err != nil {
		t.Fatalf("valid response window rejected: %v", err)
	}
}
