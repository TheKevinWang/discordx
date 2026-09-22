package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/planner"
)

func TestDecodeSnapshotRejectsUnknownTrailingAndOversizedInput(t *testing.T) {
	valid, err := decodeSnapshot([]byte(`{"profile_name":"discordx","revision":1,"listeners":[]}`))
	if err != nil || valid.Revision != 1 {
		t.Fatalf("valid snapshot = %#v, %v", valid, err)
	}
	for name, value := range map[string][]byte{
		"empty":    nil,
		"unknown":  []byte(`{"profile_name":"discordx","revision":1,"listeners":[],"secret_extra":true}`),
		"trailing": []byte(`{"profile_name":"discordx","revision":1,"listeners":[]} true`),
		"oversize": bytes.Repeat([]byte("x"), maximumSnapshotBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSnapshot(value); err == nil {
				t.Fatal("decodeSnapshot accepted invalid input")
			}
		})
	}
}

func TestDecodeActivitySnapshotIsStrictAndRequiresSequence(t *testing.T) {
	snapshot, err := decodeActivitySnapshot([]byte(`{"sequence":1,"callbacks":[]}`))
	if err != nil || snapshot.Sequence != 1 {
		t.Fatalf("valid activity snapshot = %#v, %v", snapshot, err)
	}
	for _, value := range [][]byte{
		[]byte(`{"sequence":0,"callbacks":[]}`),
		[]byte(`{"sequence":1,"callbacks":[],"unknown":true}`),
		[]byte(`{"sequence":1,"callbacks":[]} false`),
	} {
		if _, err := decodeActivitySnapshot(value); err == nil {
			t.Fatalf("invalid activity snapshot was accepted: %s", value)
		}
	}
}

func TestPartitionActivitySnapshotsRoutesEachHintOnceAndIncludesIdleGroups(t *testing.T) {
	groupA := &botGroup{configuration: []groupConfiguration{{ListenerID: "11111111-1111-4111-8111-111111111111"}}}
	groupB := &botGroup{configuration: []groupConfiguration{{ListenerID: "22222222-2222-4222-8222-222222222222"}}}
	groupIdle := &botGroup{configuration: []groupConfiguration{{ListenerID: "33333333-3333-4333-8333-333333333333"}}}
	groups := map[string]*botGroup{"a": groupA, "b": groupB, "idle": groupIdle}
	snapshot := planner.ActivitySnapshot{Sequence: 7, Callbacks: []planner.CallbackHint{
		{ListenerID: groupA.configuration[0].ListenerID, CallbackID: "callback-a", Active: true, LastCheckin: time.Now()},
		{ListenerID: groupB.configuration[0].ListenerID, CallbackID: "callback-b", Active: true, LastCheckin: time.Now()},
		{ListenerID: "44444444-4444-4444-8444-444444444444", CallbackID: "unknown", Active: true, LastCheckin: time.Now()},
	}}
	partitioned := partitionActivitySnapshots(snapshot, groups)
	if len(partitioned) != 3 {
		t.Fatalf("partition count = %d, want 3", len(partitioned))
	}
	if got := partitioned[groupA]; got.Sequence != 7 || len(got.Callbacks) != 1 || got.Callbacks[0].CallbackID != "callback-a" {
		t.Fatalf("group A snapshot = %#v", got)
	}
	if got := partitioned[groupB]; got.Sequence != 7 || len(got.Callbacks) != 1 || got.Callbacks[0].CallbackID != "callback-b" {
		t.Fatalf("group B snapshot = %#v", got)
	}
	if got := partitioned[groupIdle]; got.Sequence != 7 || len(got.Callbacks) != 0 {
		t.Fatalf("idle group snapshot = %#v", got)
	}
}
