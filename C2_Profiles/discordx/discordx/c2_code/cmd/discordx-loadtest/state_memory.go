package main

import (
	"context"
	"sync"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

// memoryCallbackStore preserves the production journal state machine while
// removing SQLite/fsync contention from the callback and transport capacity
// tier. The optional sqlite backend still uses state.Store unchanged.
type memoryCallbackStore struct {
	mu       sync.Mutex
	messages map[state.MessageKey]state.MessageState
	cursors  map[state.ChannelKey]string
}

func newMemoryCallbackStore() *memoryCallbackStore {
	return &memoryCallbackStore{
		messages: make(map[state.MessageKey]state.MessageState),
		cursors:  make(map[state.ChannelKey]string),
	}
}

func (store *memoryCallbackStore) Claim(_ context.Context, key state.MessageKey) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.messages[key]; exists {
		return false, nil
	}
	store.messages[key] = state.Pending
	return true, nil
}

func (store *memoryCallbackStore) MarkAccepted(_ context.Context, key state.MessageKey) error {
	return store.transition(key, state.Accepted, state.Pending)
}

func (store *memoryCallbackStore) MarkCleanupPending(_ context.Context, key state.MessageKey) error {
	return store.transition(key, state.CleanupPending, state.Accepted)
}

func (store *memoryCallbackStore) MarkFinalized(_ context.Context, key state.MessageKey) error {
	return store.transition(key, state.Finalized, state.Accepted, state.CleanupPending)
}

func (store *memoryCallbackStore) MarkRejected(_ context.Context, key state.MessageKey) error {
	return store.transition(key, state.Finalized, state.Pending)
}

func (store *memoryCallbackStore) Lookup(_ context.Context, key state.MessageKey) (state.MessageState, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.messages[key]
	return current, exists, nil
}

func (store *memoryCallbackStore) AdvanceCursor(_ context.Context, channel state.ChannelKey, messageID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.cursors[channel]
	if len(messageID) > len(current) || len(messageID) == len(current) && messageID > current {
		store.cursors[channel] = messageID
	}
	return nil
}

func (store *memoryCallbackStore) Cursor(_ context.Context, channel state.ChannelKey) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.cursors[channel], nil
}

func (store *memoryCallbackStore) transition(key state.MessageKey, next state.MessageState, allowed ...state.MessageState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.messages[key]
	if !exists {
		return state.ErrUnknownMessage
	}
	for _, candidate := range allowed {
		if current == candidate {
			store.messages[key] = next
			return nil
		}
	}
	return state.ErrInvalidTransition
}
