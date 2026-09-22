package worker

import (
	"encoding/base64"
	"errors"
	"unicode/utf16"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
)

var ErrOutboundRoute = errors.New("outbound tracking route is invalid or unavailable")

type OutboundRegistry interface {
	ResolveTrackingRoute(string) (registry.ResolvedRoute, error)
	Generation(listenerID, generationID string) (registry.Listener, registry.Generation, bool)
}

type Delivery struct {
	OutboundID   string
	ListenerID   string
	GenerationID string
	ChannelID    string
	Lane         Lane
	Document     discord.OutboundDocument
}

type OutboundEncoder struct {
	registry OutboundRegistry
}

func NewOutboundEncoder(registry OutboundRegistry) (*OutboundEncoder, error) {
	if registry == nil {
		return nil, errors.New("outbound registry is required")
	}
	return &OutboundEncoder{registry: registry}, nil
}

func (encoder *OutboundEncoder) Encode(outbound mythic.Outbound) (Delivery, error) {
	if outbound.ID == "" || len(outbound.Message) == 0 || (outbound.Lane != mythic.Standard && outbound.Lane != mythic.Socks) {
		return Delivery{}, ErrOutboundRoute
	}
	resolved, err := encoder.registry.ResolveTrackingRoute(outbound.TrackingID)
	if err != nil {
		return Delivery{}, ErrOutboundRoute
	}
	_, generation, ok := encoder.registry.Generation(resolved.ListenerID, resolved.GenerationID)
	if !ok {
		return Delivery{}, ErrOutboundRoute
	}
	channelID := resolved.TaskChannelID
	lane := Standard
	if outbound.Lane == mythic.Socks {
		if resolved.SocksChannelID == "" || generation.Wire.Protocol != "fixed" || generation.Wire.EnvelopeFormat != "binary-v1" {
			return Delivery{}, ErrOutboundRoute
		}
		channelID = resolved.SocksChannelID
		lane = Socks
	}
	var document string
	attachmentName := ""
	if generation.Wire.Protocol == "legacy" {
		if lane != Standard || resolved.LegacySender == "" {
			return Delivery{}, ErrOutboundRoute
		}
		document, err = envelope.EncodeLegacyResponse(outbound.Message, resolved.LegacySender, generation.ID)
		attachmentName = outbound.TrackingID
	} else {
		body := append([]byte(nil), outbound.Message...)
		format := envelope.RawV1
		if generation.Wire.UseBase64 {
			framed := append([]byte(resolved.ClientID), outbound.Message...)
			body = []byte(base64.StdEncoding.EncodeToString(framed))
			format = envelope.Legacy
		} else if len(body) <= len(resolved.ClientID) || string(body[:len(resolved.ClientID)]) != resolved.ClientID {
			return Delivery{}, ErrOutboundRoute
		}
		protocol, protocolErr := envelope.NewProtocol(generation.Wire, nil)
		if protocolErr != nil {
			return Delivery{}, ErrOutboundRoute
		}
		document, err = protocol.Encode(envelope.Message{
			Body: body, SenderID: generation.ID, ToServer: false,
			ClientID: resolved.ClientID, Format: format,
		}, envelope.ServerToAgent)
		attachmentName = "message.txt"
	}
	if err != nil {
		return Delivery{}, ErrOutboundRoute
	}
	boundary := 1900
	if generation.Wire.Protocol == "legacy" {
		boundary = 1950
	}
	outboundDocument := discord.OutboundDocument{Content: document}
	if utf16Units(document) > boundary {
		outboundDocument = discord.OutboundDocument{AttachmentName: attachmentName, Attachment: []byte(document)}
	}
	return Delivery{
		OutboundID: outbound.ID, ListenerID: resolved.ListenerID, GenerationID: resolved.GenerationID,
		ChannelID: channelID, Lane: lane, Document: outboundDocument,
	}, nil
}

func utf16Units(value string) int {
	return len(utf16.Encode([]rune(value)))
}
