package main

import (
	"testing"
	"time"
)

const testCallbackID = "33333333-3333-4333-8333-333333333333"

func TestSyntheticEchoTaskProducesCallbackSpecificResult(t *testing.T) {
	task := callbackPacket{
		Kind: "task", CallbackID: testCallbackID, TaskID: "task-1",
		Command: "echo", Argument: "callback:" + testCallbackID,
	}
	result, err := executeSyntheticTask(task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != "result" || result.CallbackID != task.CallbackID || result.TaskID != task.TaskID ||
		result.Output != task.Argument {
		t.Fatalf("executeSyntheticTask() = %#v", result)
	}
}

func TestSyntheticTaskRejectsArbitraryCommands(t *testing.T) {
	_, err := executeSyntheticTask(callbackPacket{
		Kind: "task", CallbackID: testCallbackID, TaskID: "task-1",
		Command: "shell", Argument: "whoami",
	})
	if err == nil {
		t.Fatal("executeSyntheticTask accepted an arbitrary command")
	}
}

func TestCallbackPacketBodyRoundTripsAndRequiresMatchingUUIDPrefix(t *testing.T) {
	want := callbackPacket{Kind: "poll", CallbackID: testCallbackID}
	body, err := encodeCallbackPacket(testCallbackID, want)
	if err != nil {
		t.Fatal(err)
	}
	clientID, got, err := decodeCallbackPacket(body)
	if err != nil {
		t.Fatal(err)
	}
	if clientID != testCallbackID || got != want {
		t.Fatalf("decodeCallbackPacket() = %q, %#v", clientID, got)
	}
	body[0] = '4'
	if _, _, err := decodeCallbackPacket(body); err == nil {
		t.Fatal("decodeCallbackPacket accepted a mismatched client prefix")
	}
}

func TestCallbackPlansUseDeterministicJitteredPollPhases(t *testing.T) {
	entries := makeScaleEntries(10)
	plans, err := makeCallbackPlans(entries, 1000, syntheticCommandEcho, 60*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	repeatedEntries := makeScaleEntries(10)
	repeated, err := makeCallbackPlans(repeatedEntries, 1000, syntheticCommandEcho, 60*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	minimumInterval := 48 * time.Second
	maximumInterval := 72 * time.Second
	maximumDue := time.Duration(0)
	distinctDue := make(map[time.Duration]struct{})
	for index, plan := range plans {
		if plan.EffectiveInterval < minimumInterval || plan.EffectiveInterval > maximumInterval {
			t.Fatalf("plan %d interval = %s", index, plan.EffectiveInterval)
		}
		if plan.PollDueAfter < 0 || plan.PollDueAfter >= plan.EffectiveInterval {
			t.Fatalf("plan %d due = %s within %s", index, plan.PollDueAfter, plan.EffectiveInterval)
		}
		if repeated[index].EffectiveInterval != plan.EffectiveInterval || repeated[index].PollDueAfter != plan.PollDueAfter {
			t.Fatalf("plan %d schedule was not deterministic", index)
		}
		if plan.PollDueAfter > maximumDue {
			maximumDue = plan.PollDueAfter
		}
		distinctDue[plan.PollDueAfter] = struct{}{}
	}
	if maximumDue < 60*time.Second || len(distinctDue) < 900 {
		t.Fatalf("schedule is not broadly staggered: max=%s distinct=%d", maximumDue, len(distinctDue))
	}
}

func TestCallbackReleaseBarrierIsOneShot(t *testing.T) {
	clone := &discordClone{pollRelease: make(chan struct{})}
	if err := clone.ReleaseCallbacks(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-clone.pollRelease:
	default:
		t.Fatal("callback release barrier remained closed")
	}
	if err := clone.ReleaseCallbacks(); err == nil {
		t.Fatal("callback release barrier was released twice")
	}
}
