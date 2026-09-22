package state_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

func channel() state.ChannelKey {
	return state.ChannelKey{
		ProviderID: "provider-a", ListenerID: "11111111-1111-4111-8111-111111111111",
		ChannelID: "100000000000000001",
	}
}

func message(id string) state.MessageKey {
	return state.MessageKey{ChannelKey: channel(), MessageID: id}
}

func openStore(t *testing.T, limit int) (*state.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "discordx-state.db")
	store, err := state.Open(path, limit)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func TestClaimIsDurableAtomicAndContainsNoMessageContent(t *testing.T) {
	store, path := openStore(t, 10)
	ctx := context.Background()
	const messageID = "200000000000000001"
	var winners atomic.Int32
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			claimed, err := store.Claim(ctx, message(messageID))
			if err != nil {
				t.Errorf("Claim() error = %v", err)
				return
			}
			if claimed {
				winners.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("Claim() winners = %d, want 1", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.Open(path, 10)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if claimed, err := reopened.Claim(ctx, message(messageID)); err != nil || claimed {
		t.Fatalf("durable duplicate Claim() = %t, %v", claimed, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("database permissions = %o, want owner-only", info.Mode().Perm())
	}
}

func TestStateTransitionsPendingRecoveryAndMonotonicCursor(t *testing.T) {
	store, _ := openStore(t, 10)
	ctx := context.Background()
	first := message("200000000000000001")
	second := message("200000000000000010")
	for _, key := range []state.MessageKey{first, second} {
		if claimed, err := store.Claim(ctx, key); err != nil || !claimed {
			t.Fatalf("Claim(%s) = %t, %v", key.MessageID, claimed, err)
		}
	}
	if err := store.MarkAccepted(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCleanupPending(ctx, first); err != nil {
		t.Fatal(err)
	}
	if current, ok, err := store.Lookup(ctx, first); err != nil || !ok || current != state.CleanupPending {
		t.Fatalf("Lookup() = %q, %t, %v", current, ok, err)
	}
	if err := store.MarkFinalized(ctx, first); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Pending(ctx, channel(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].MessageID != second.MessageID || pending[0].State != state.Pending {
		t.Fatalf("Pending() = %#v", pending)
	}
	if err := store.AdvanceCursor(ctx, channel(), second.MessageID); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceCursor(ctx, channel(), first.MessageID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Cursor(ctx, channel()); err != nil || got != second.MessageID {
		t.Fatalf("Cursor() = %q, %v", got, err)
	}
}

func TestJournalEvictsOnlyFinalizedRowsAndFailsClosedWhenFull(t *testing.T) {
	store, _ := openStore(t, 2)
	ctx := context.Background()
	first := message("200000000000000001")
	second := message("200000000000000002")
	third := message("200000000000000003")
	for _, key := range []state.MessageKey{first, second} {
		if claimed, err := store.Claim(ctx, key); err != nil || !claimed {
			t.Fatalf("Claim() = %t, %v", claimed, err)
		}
	}
	if claimed, err := store.Claim(ctx, third); !errors.Is(err, state.ErrJournalFull) || claimed {
		t.Fatalf("full Claim() = %t, %v", claimed, err)
	}
	if err := store.MarkAccepted(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFinalized(ctx, first); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.Claim(ctx, third); err != nil || !claimed {
		t.Fatalf("Claim() after finalized eviction = %t, %v", claimed, err)
	}
	if claimed, err := store.Claim(ctx, first); !errors.Is(err, state.ErrJournalFull) || claimed {
		t.Fatalf("evicted old ID should fail closed while active journal is full: %t, %v", claimed, err)
	}
}

func TestInvalidTransitionsAndSecretsAreRejectedOrAbsent(t *testing.T) {
	store, path := openStore(t, 10)
	ctx := context.Background()
	key := message("200000000000000001")
	if err := store.MarkAccepted(ctx, key); !errors.Is(err, state.ErrUnknownMessage) {
		t.Fatalf("MarkAccepted(unknown) error = %v", err)
	}
	if claimed, err := store.Claim(ctx, key); err != nil || !claimed {
		t.Fatalf("Claim() = %t, %v", claimed, err)
	}
	if err := store.MarkFinalized(ctx, key); err == nil {
		t.Fatal("pending message finalized without acceptance")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"discord-token-canary", "transport-key-canary", "agent-output-canary"} {
		if stringsContains(contents, secret) {
			t.Fatalf("database contains forbidden secret/content canary %q", secret)
		}
	}
}

func TestRejectedMessageCanBeFinalizedWithoutBeingAccepted(t *testing.T) {
	store, _ := openStore(t, 10)
	ctx := context.Background()
	key := message("200000000000000001")
	if claimed, err := store.Claim(ctx, key); err != nil || !claimed {
		t.Fatalf("Claim() = %t, %v", claimed, err)
	}
	if err := store.MarkRejected(ctx, key); err != nil {
		t.Fatalf("MarkRejected() error = %v", err)
	}
	if pending, err := store.Pending(ctx, channel(), 10); err != nil || len(pending) != 0 {
		t.Fatalf("Pending() = %#v, %v", pending, err)
	}
}

func stringsContains(value []byte, text string) bool {
	for index := 0; index+len(text) <= len(value); index++ {
		if string(value[index:index+len(text)]) == text {
			return true
		}
	}
	return false
}
