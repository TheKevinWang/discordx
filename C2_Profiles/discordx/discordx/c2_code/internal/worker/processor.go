// Package worker joins channel routing, envelope decoding, durable delivery
// state, Mythic receipts, and Discord cleanup without trying candidate
// generations on the message hot path.
package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

var (
	ErrUnknownChannel  = errors.New("ingress channel is not registered")
	ErrGenerationGone  = errors.New("ingress generation is not available")
	ErrInvalidDocument = errors.New("ingress document is invalid")
	ErrIngressRejected = errors.New("Mythic rejected ingress")
	ErrCleanupPending  = errors.New("Discord cleanup is pending")
)

type Registry interface {
	ResolveChannel(providerID, channelID string) (registry.ResolvedRoute, bool)
	Generation(listenerID, generationID string) (registry.Listener, registry.Generation, bool)
}

type StateStore interface {
	Claim(context.Context, state.MessageKey) (bool, error)
	MarkAccepted(context.Context, state.MessageKey) error
	MarkCleanupPending(context.Context, state.MessageKey) error
	MarkFinalized(context.Context, state.MessageKey) error
	MarkRejected(context.Context, state.MessageKey) error
	AdvanceCursor(context.Context, state.ChannelKey, string) error
	Lookup(context.Context, state.MessageKey) (state.MessageState, bool, error)
}

type Provider interface {
	DownloadAttachment(context.Context, discord.Attachment) ([]byte, error)
	Delete(context.Context, string, string) error
}

type Lane string

const (
	Standard Lane = "standard"
	Socks    Lane = "socks"
)

type Ingress struct {
	ID            string
	TrackingID    string
	Message       []byte
	Base64Message []byte
	Lane          Lane
}

type Receipt struct {
	IngressID string
	Accepted  bool
}

type Bridge interface {
	SendIngress(context.Context, Ingress) (Receipt, error)
}

type Result struct {
	Duplicate      bool
	Accepted       bool
	CleanupPending bool
}

type Processor struct {
	registry Registry
	store    StateStore
	provider Provider
	bridge   Bridge
}

func NewProcessor(registry Registry, store StateStore, provider Provider, bridge Bridge) (*Processor, error) {
	if registry == nil || store == nil || provider == nil || bridge == nil {
		return nil, errors.New("worker processor dependencies are required")
	}
	return &Processor{registry: registry, store: store, provider: provider, bridge: bridge}, nil
}

func (processor *Processor) Process(ctx context.Context, providerID string, message discord.Message) (Result, error) {
	resolved, ok := processor.registry.ResolveChannel(providerID, message.ChannelID)
	if !ok {
		return Result{}, ErrUnknownChannel
	}
	key := state.MessageKey{ChannelKey: state.ChannelKey{
		ProviderID: providerID, ListenerID: resolved.ListenerID, ChannelID: message.ChannelID,
	}, MessageID: message.ID}
	claimed, err := processor.store.Claim(ctx, key)
	if err != nil {
		return Result{}, err
	}
	if !claimed {
		current, exists, err := processor.store.Lookup(ctx, key)
		if err != nil {
			return Result{}, err
		}
		if !exists || current == state.Finalized {
			return Result{Duplicate: true}, nil
		}
		if current == state.Accepted || current == state.CleanupPending {
			return processor.finishCleanup(ctx, key, message)
		}
	}
	return processor.processClaimed(ctx, key, resolved, message)
}

// Recover resumes a message already present in the pending journal. Callers
// obtain these keys from StateStore.Pending during bounded cursor catch-up.
func (processor *Processor) Recover(ctx context.Context, providerID string, message discord.Message) (Result, error) {
	resolved, ok := processor.registry.ResolveChannel(providerID, message.ChannelID)
	if !ok {
		return Result{}, ErrUnknownChannel
	}
	key := state.MessageKey{ChannelKey: state.ChannelKey{
		ProviderID: providerID, ListenerID: resolved.ListenerID, ChannelID: message.ChannelID,
	}, MessageID: message.ID}
	return processor.processClaimed(ctx, key, resolved, message)
}

func (processor *Processor) processClaimed(ctx context.Context, key state.MessageKey, resolved registry.ResolvedRoute, message discord.Message) (Result, error) {
	_, generation, ok := processor.registry.Generation(resolved.ListenerID, resolved.GenerationID)
	if !ok {
		return Result{}, ErrGenerationGone
	}
	document, err := processor.document(ctx, message)
	if err != nil {
		return Result{}, err
	}
	ingress, err := decodeIngress(generation, resolved, document)
	if err != nil {
		if finalizeErr := processor.reject(ctx, key); finalizeErr != nil {
			return Result{}, finalizeErr
		}
		return Result{}, ErrInvalidDocument
	}
	ingress.ID = fmt.Sprintf("dxi:%s:%s:%s", resolved.ListenerID, message.ChannelID, message.ID)
	receipt, err := processor.bridge.SendIngress(ctx, ingress)
	if err != nil {
		return Result{}, err
	}
	if !receipt.Accepted || receipt.IngressID != ingress.ID {
		return Result{}, ErrIngressRejected
	}
	if err := processor.store.MarkAccepted(ctx, key); err != nil {
		return Result{}, err
	}
	if err := processor.store.AdvanceCursor(ctx, key.ChannelKey, key.MessageID); err != nil {
		return Result{}, err
	}
	return processor.finishCleanup(ctx, key, message)
}

func (processor *Processor) finishCleanup(ctx context.Context, key state.MessageKey, message discord.Message) (Result, error) {
	if err := processor.provider.Delete(ctx, message.ChannelID, message.ID); err != nil {
		current, _, lookupErr := processor.store.Lookup(ctx, key)
		if lookupErr != nil {
			return Result{}, lookupErr
		}
		if current == state.Accepted {
			if transitionErr := processor.store.MarkCleanupPending(ctx, key); transitionErr != nil {
				return Result{}, transitionErr
			}
		}
		return Result{Accepted: true, CleanupPending: true}, ErrCleanupPending
	}
	if err := processor.store.MarkFinalized(ctx, key); err != nil {
		return Result{}, err
	}
	return Result{Accepted: true}, nil
}

func (processor *Processor) document(ctx context.Context, message discord.Message) (string, error) {
	if len(message.Attachments) > 1 {
		return "", ErrInvalidDocument
	}
	if len(message.Attachments) == 0 {
		return message.Content, nil
	}
	value, err := processor.provider.DownloadAttachment(ctx, message.Attachments[0])
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func decodeIngress(generation registry.Generation, resolved registry.ResolvedRoute, document string) (Ingress, error) {
	lane := Standard
	if resolved.Lane == "socks" {
		lane = Socks
	}
	if generation.Wire.Protocol == "legacy" {
		decoded, err := envelope.DecodeLegacyRequest(document)
		if err != nil {
			return Ingress{}, err
		}
		tracking := (route.DX2{
			ListenerID: resolved.ListenerID, GenerationID: resolved.GenerationID,
			Kind: route.Legacy, LegacySender: decoded.TrackingID,
		}).String()
		return selectPayload(decoded.Body, decoded.Format, tracking, lane)
	}
	protocol, err := envelope.NewProtocol(generation.Wire, nil)
	if err != nil {
		return Ingress{}, err
	}
	decoded, err := protocol.Decode(document, envelope.AgentToServer)
	if err != nil {
		return Ingress{}, err
	}
	tracking := (route.DX2{
		ListenerID: resolved.ListenerID, GenerationID: resolved.GenerationID,
		Kind: route.Fixed, ClientID: decoded.SenderID,
	}).String()
	return selectPayload(decoded.Body, decoded.Format, tracking, lane)
}

func selectPayload(body []byte, format envelope.MessageFormat, tracking string, lane Lane) (Ingress, error) {
	result := Ingress{TrackingID: tracking, Lane: lane}
	switch format {
	case envelope.Legacy:
		result.Base64Message = append([]byte(nil), body...)
	case envelope.RawV1:
		result.Message = append([]byte(nil), body...)
	default:
		return Ingress{}, ErrInvalidDocument
	}
	return result, nil
}

func (processor *Processor) reject(ctx context.Context, key state.MessageKey) error {
	if err := processor.store.MarkRejected(ctx, key); err != nil {
		return err
	}
	return processor.store.AdvanceCursor(ctx, key.ChannelKey, key.MessageID)
}
