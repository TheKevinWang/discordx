package main

import (
	"testing"
	"time"
)

func TestSyntheticMythicQueuesEveryCommandBeforePolling(t *testing.T) {
	entries := makeScaleEntries(3)
	plans, err := makeCallbackPlans(entries, 9, syntheticCommandEcho, time.Minute, 20)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newSyntheticMythic(plans)
	if err != nil {
		t.Fatal(err)
	}
	counters := server.Counters()
	if counters.CommandsQueued != int64(len(plans)) || counters.CallbackPolls != 0 || counters.CommandsIssued != 0 {
		t.Fatalf("Counters() before release = %#v", counters)
	}
	for _, plan := range plans {
		lifecycle := server.callbacks[plan.CallbackID]
		clientID, packet, decodeErr := decodeCallbackPacket(lifecycle.taskBody)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if clientID != plan.CallbackID || packet.Kind != "task" || packet.TaskID != plan.TaskID ||
			packet.Command != syntheticCommandEcho || packet.Argument != plan.Argument {
			t.Fatalf("queued task for %s = %#v", plan.CallbackID, packet)
		}
	}
}
