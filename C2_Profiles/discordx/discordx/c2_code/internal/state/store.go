// Package state persists non-secret Discord message cursors and delivery
// journal state. Tokens, keys, envelopes, and agent output have no fields in
// this schema.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
	_ "modernc.org/sqlite"
)

var (
	ErrJournalFull       = errors.New("message journal is full")
	ErrUnknownMessage    = errors.New("message is not present in the journal")
	ErrInvalidTransition = errors.New("invalid message journal transition")
)

type MessageState string

const (
	Pending        MessageState = "pending"
	Accepted       MessageState = "accepted"
	CleanupPending MessageState = "cleanup_pending"
	Finalized      MessageState = "finalized"
)

type ChannelKey struct {
	ProviderID string
	ListenerID string
	ChannelID  string
}

type MessageKey struct {
	ChannelKey
	MessageID string
}

type Record struct {
	MessageKey
	State MessageState
}

type Store struct {
	database     *sql.DB
	journalLimit int
}

func Open(path string, journalLimit int) (*Store, error) {
	if strings.TrimSpace(path) == "" || journalLimit < 1 || journalLimit > 100_000 {
		return nil, errors.New("invalid recovery-store configuration")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create recovery-store directory: %w", err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open recovery store: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	store := &Store{database: database, journalLimit: journalLimit}
	if err := store.initialize(context.Background()); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("secure recovery store: %w", err)
	}
	return store, nil
}

func (store *Store) initialize(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS channel_cursor (
            provider_id TEXT NOT NULL,
            listener_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            message_id TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (provider_id, listener_id, channel_id)
        ) STRICT`,
		`CREATE TABLE IF NOT EXISTS message_journal (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id TEXT NOT NULL,
            listener_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'cleanup_pending', 'finalized')),
            UNIQUE (provider_id, listener_id, channel_id, message_id)
        ) STRICT`,
		`CREATE INDEX IF NOT EXISTS message_journal_channel_sequence
            ON message_journal (provider_id, listener_id, channel_id, sequence)`,
	}
	for _, statement := range statements {
		if _, err := store.database.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize recovery store: %w", err)
		}
	}
	return nil
}

func (store *Store) Close() error {
	if store == nil || store.database == nil {
		return nil
	}
	return store.database.Close()
}

func (store *Store) Claim(ctx context.Context, key MessageKey) (bool, error) {
	if err := validateMessageKey(key); err != nil {
		return false, err
	}
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = transaction.Rollback() }()

	var existing string
	err = transaction.QueryRowContext(ctx, `
        SELECT state FROM message_journal
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND message_id = ?`,
		key.ProviderID, key.ListenerID, key.ChannelID, key.MessageID).Scan(&existing)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	var count int
	if err := transaction.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM message_journal
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ?`,
		key.ProviderID, key.ListenerID, key.ChannelID).Scan(&count); err != nil {
		return false, err
	}
	if count >= store.journalLimit {
		result, err := transaction.ExecContext(ctx, `
            DELETE FROM message_journal WHERE sequence = (
                SELECT sequence FROM message_journal
                WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND state = 'finalized'
                ORDER BY sequence LIMIT 1
            )`, key.ProviderID, key.ListenerID, key.ChannelID)
		if err != nil {
			return false, err
		}
		removed, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if removed == 0 {
			return false, ErrJournalFull
		}
	}
	if _, err := transaction.ExecContext(ctx, `
        INSERT INTO message_journal (provider_id, listener_id, channel_id, message_id, state)
        VALUES (?, ?, ?, ?, 'pending')`,
		key.ProviderID, key.ListenerID, key.ChannelID, key.MessageID); err != nil {
		return false, err
	}
	if err := transaction.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (store *Store) MarkAccepted(ctx context.Context, key MessageKey) error {
	return store.transition(ctx, key, Accepted, Pending)
}

func (store *Store) MarkCleanupPending(ctx context.Context, key MessageKey) error {
	return store.transition(ctx, key, CleanupPending, Accepted)
}

func (store *Store) MarkFinalized(ctx context.Context, key MessageKey) error {
	return store.transition(ctx, key, Finalized, Accepted, CleanupPending)
}

// MarkRejected finalizes a claimed message that is permanently malformed or
// routed to an incompatible envelope. It is deliberately distinct from
// MarkFinalized so accepted-delivery transitions remain explicit.
func (store *Store) MarkRejected(ctx context.Context, key MessageKey) error {
	return store.transition(ctx, key, Finalized, Pending)
}

func (store *Store) transition(ctx context.Context, key MessageKey, next MessageState, allowed ...MessageState) error {
	if err := validateMessageKey(key); err != nil {
		return err
	}
	placeholders := make([]string, len(allowed))
	arguments := []any{next, key.ProviderID, key.ListenerID, key.ChannelID, key.MessageID}
	for index, current := range allowed {
		placeholders[index] = "?"
		arguments = append(arguments, current)
	}
	result, err := store.database.ExecContext(ctx, `
        UPDATE message_journal SET state = ?
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND message_id = ?
          AND state IN (`+strings.Join(placeholders, ",")+`)`, arguments...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	var existing string
	err = store.database.QueryRowContext(ctx, `
        SELECT state FROM message_journal
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND message_id = ?`,
		key.ProviderID, key.ListenerID, key.ChannelID, key.MessageID).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrUnknownMessage
	}
	if err != nil {
		return err
	}
	return ErrInvalidTransition
}

func (store *Store) Pending(ctx context.Context, channel ChannelKey, limit int) ([]Record, error) {
	if err := validateChannelKey(channel); err != nil || limit < 1 || limit > 5000 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("pending recovery limit is invalid")
	}
	rows, err := store.database.QueryContext(ctx, `
        SELECT message_id, state FROM message_journal
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND state != 'finalized'
        ORDER BY LENGTH(message_id), message_id LIMIT ?`,
		channel.ProviderID, channel.ListenerID, channel.ChannelID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]Record, 0)
	for rows.Next() {
		var record Record
		record.ChannelKey = channel
		if err := rows.Scan(&record.MessageID, &record.State); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (store *Store) Lookup(ctx context.Context, key MessageKey) (MessageState, bool, error) {
	if err := validateMessageKey(key); err != nil {
		return "", false, err
	}
	var current MessageState
	err := store.database.QueryRowContext(ctx, `
        SELECT state FROM message_journal
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ? AND message_id = ?`,
		key.ProviderID, key.ListenerID, key.ChannelID, key.MessageID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return current, true, nil
}

func (store *Store) AdvanceCursor(ctx context.Context, channel ChannelKey, messageID string) error {
	if err := validateChannelKey(channel); err != nil {
		return err
	}
	if !config.ValidSnowflake(messageID) {
		return errors.New("cursor message ID is invalid")
	}
	_, err := store.database.ExecContext(ctx, `
        INSERT INTO channel_cursor (provider_id, listener_id, channel_id, message_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(provider_id, listener_id, channel_id) DO UPDATE SET message_id =
            CASE WHEN LENGTH(excluded.message_id) > LENGTH(channel_cursor.message_id)
                   OR (LENGTH(excluded.message_id) = LENGTH(channel_cursor.message_id)
                       AND excluded.message_id > channel_cursor.message_id)
                 THEN excluded.message_id ELSE channel_cursor.message_id END`,
		channel.ProviderID, channel.ListenerID, channel.ChannelID, messageID)
	return err
}

func (store *Store) Cursor(ctx context.Context, channel ChannelKey) (string, error) {
	if err := validateChannelKey(channel); err != nil {
		return "", err
	}
	var messageID string
	err := store.database.QueryRowContext(ctx, `
        SELECT message_id FROM channel_cursor
        WHERE provider_id = ? AND listener_id = ? AND channel_id = ?`,
		channel.ProviderID, channel.ListenerID, channel.ChannelID).Scan(&messageID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return messageID, err
}

func validateChannelKey(key ChannelKey) error {
	if strings.TrimSpace(key.ProviderID) == "" || !route.IsCanonicalUUID(key.ListenerID) || !config.ValidSnowflake(key.ChannelID) {
		return errors.New("recovery channel key is invalid")
	}
	return nil
}

func validateMessageKey(key MessageKey) error {
	if err := validateChannelKey(key.ChannelKey); err != nil {
		return err
	}
	if !config.ValidSnowflake(key.MessageID) {
		return errors.New("recovery message ID is invalid")
	}
	return nil
}
