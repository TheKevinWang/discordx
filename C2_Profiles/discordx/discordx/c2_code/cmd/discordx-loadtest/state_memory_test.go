package main

import (
	"context"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

func TestMemoryCallbackStorePreservesJournalAndMonotonicCursor(t *testing.T) {
	store := newMemoryCallbackStore()
	key := state.MessageKey{ChannelKey: state.ChannelKey{
		ProviderID: "provider", ListenerID: "11111111-1111-4111-8111-111111111111",
		ChannelID: "100000000000000001",
	}, MessageID: "200000000000000002"}
	claimed, err := store.Claim(context.Background(), key)
	if err != nil || !claimed {
		t.Fatalf("Claim() = %t, %v", claimed, err)
	}
	if claimed, err = store.Claim(context.Background(), key); err != nil || claimed {
		t.Fatalf("duplicate Claim() = %t, %v", claimed, err)
	}
	if err := store.MarkAccepted(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceCursor(context.Background(), key.ChannelKey, key.MessageID); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceCursor(context.Background(), key.ChannelKey, "200000000000000001"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFinalized(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	current, exists, err := store.Lookup(context.Background(), key)
	if err != nil || !exists || current != state.Finalized {
		t.Fatalf("Lookup() = %q, %t, %v", current, exists, err)
	}
	cursor, err := store.Cursor(context.Background(), key.ChannelKey)
	if err != nil || cursor != key.MessageID {
		t.Fatalf("Cursor() = %q, %v", cursor, err)
	}
}
