package worker_test

import (
	"context"
	"strings"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

func outboundRegistry(t *testing.T, wire config.Wire, socks string) (*registry.Manager, registry.Generation) {
	t.Helper()
	manager := registry.NewManager()
	generation := registry.Generation{
		ID: generationID, State: registry.Active, DiscordToken: "token", BotUserID: "900000000000000001",
		TaskChannelID: channelID, SocksChannelID: socks, Provider: config.Provider{Kind: "discord"}, Wire: wire,
	}
	_, err := manager.ApplySnapshot(context.Background(), registry.Snapshot{
		ProfileName: "discordx", Revision: 1,
		Listeners: []registry.Listener{{
			ID: listenerID, Name: "listener", OperationID: 1, Enabled: true,
			ActiveGenerationID: generationID, Ingress: config.Ingress{}, Egress: config.EgressProxy{Mode: "direct"},
			Generations: []registry.Generation{generation},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, normalized, _ := manager.Generation(listenerID, generationID)
	return manager, normalized
}

func TestFixedRawOutboundRequiresMatchingFrameRouteAndSelectsSocksChannel(t *testing.T) {
	manager, generation := outboundRegistry(t, config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64", Protection: "none", KeyMode: "single",
	}, "100000000000000002")
	encoder, _ := worker.NewOutboundEncoder(manager)
	tracking := "dx2:" + listenerID + ":" + generationID + ":f:" + clientID
	delivery, err := encoder.Encode(mythic.Outbound{
		ID: "outbound-1", TrackingID: tracking, Message: append([]byte(clientID), []byte("response")...), Lane: mythic.Socks,
	})
	if err != nil || delivery.ChannelID != "100000000000000002" || delivery.Lane != worker.Socks || delivery.Document.Content == "" {
		t.Fatalf("Encode() = %#v, %v", delivery, err)
	}
	protocol, _ := envelope.NewProtocol(generation.Wire, nil)
	decoded, err := protocol.Decode(delivery.Document.Content, envelope.ServerToAgent)
	if err != nil || decoded.ClientID != clientID || decoded.Format != envelope.RawV1 {
		t.Fatalf("decoded delivery = %#v, %v", decoded, err)
	}
	if _, err := encoder.Encode(mythic.Outbound{ID: "bad", TrackingID: tracking, Message: []byte("wrong-route"), Lane: mythic.Standard}); err == nil {
		t.Fatal("raw outbound without matching UUID prefix was accepted")
	}
}

func TestFixedHistoricalBase64FramesResponseAndUsesAttachmentBoundary(t *testing.T) {
	manager, generation := outboundRegistry(t, config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64", Protection: "none", KeyMode: "single", UseBase64: true,
	}, "")
	encoder, _ := worker.NewOutboundEncoder(manager)
	tracking := "dx2:" + listenerID + ":" + generationID + ":f:" + clientID
	delivery, err := encoder.Encode(mythic.Outbound{ID: "outbound-1", TrackingID: tracking, Message: []byte(strings.Repeat("x", 2000)), Lane: mythic.Standard})
	if err != nil || delivery.Document.Content != "" || delivery.Document.AttachmentName != "message.txt" || len(delivery.Document.Attachment) == 0 {
		t.Fatalf("Encode() = %#v, %v", delivery, err)
	}
	protocol, _ := envelope.NewProtocol(generation.Wire, nil)
	decoded, err := protocol.Decode(string(delivery.Document.Attachment), envelope.ServerToAgent)
	if err != nil || decoded.Format != envelope.Legacy || decoded.ClientID != clientID {
		t.Fatalf("decoded historical delivery = %#v, %v", decoded, err)
	}
}

func TestLegacyOutboundPreservesTrackingFilenameAndRejectsSocks(t *testing.T) {
	manager, _ := outboundRegistry(t, config.Wire{Protocol: "legacy"}, "")
	managerWithAlias := manager
	// A dx2 legacy route is resolved directly and does not require the migration alias.
	tracking := "dx2:" + listenerID + ":" + generationID + ":l:bGVnYWN5IHJvdXRl"
	encoder, _ := worker.NewOutboundEncoder(managerWithAlias)
	delivery, err := encoder.Encode(mythic.Outbound{ID: "outbound-1", TrackingID: tracking, Message: []byte(strings.Repeat("x", 2000)), Lane: mythic.Standard})
	if err != nil || delivery.Document.AttachmentName != tracking || len(delivery.Document.Attachment) == 0 {
		t.Fatalf("Encode() = %#v, %v", delivery, err)
	}
	if _, err := encoder.Encode(mythic.Outbound{ID: "outbound-2", TrackingID: tracking, Message: []byte("x"), Lane: mythic.Socks}); err == nil {
		t.Fatal("legacy SOCKS outbound was accepted")
	}
}

func TestForgedGenerationRouteFailsClosed(t *testing.T) {
	manager, _ := outboundRegistry(t, config.Wire{Protocol: "legacy"}, "")
	encoder, _ := worker.NewOutboundEncoder(manager)
	forged := "dx2:" + listenerID + ":99999999-9999-4999-8999-999999999999:l:bGVnYWN5IHJvdXRl"
	if _, err := encoder.Encode(mythic.Outbound{ID: "forged", TrackingID: forged, Message: []byte("body"), Lane: mythic.Standard}); err == nil {
		t.Fatal("forged generation route was accepted")
	}
}
