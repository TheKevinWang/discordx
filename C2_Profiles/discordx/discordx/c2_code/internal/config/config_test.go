package config_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
)

func TestProviderNormalizationScopesIdentityByEndpointSet(t *testing.T) {
	discord, err := (config.Provider{Kind: "discord"}).Normalize(false)
	if err != nil {
		t.Fatalf("Normalize(discord) error = %v", err)
	}
	if discord.APIBaseURL != "https://discord.com/api" ||
		discord.GatewayBaseURL != "wss://gateway.discord.gg" ||
		discord.CDNBaseURL != "https://cdn.discordapp.com" || discord.APIVersion != 10 {
		t.Fatalf("Normalize(discord) = %#v", discord)
	}

	spacebarA, err := (config.Provider{
		Kind: "spacebar", APIBaseURL: "http://spacebar-a:3001/api/",
		GatewayBaseURL: "ws://spacebar-a:3001/", CDNBaseURL: "http://spacebar-a:3001/",
		APIVersion: 10, TestOnlyAllowInsecureTransport: true,
	}).Normalize(true)
	if err != nil {
		t.Fatalf("Normalize(spacebar A) error = %v", err)
	}
	spacebarB, err := (config.Provider{
		Kind: "spacebar", APIBaseURL: "http://spacebar-b:3001/api",
		GatewayBaseURL: "ws://spacebar-b:3001", CDNBaseURL: "http://spacebar-b:3001",
		APIVersion: 10, TestOnlyAllowInsecureTransport: true,
	}).Normalize(true)
	if err != nil {
		t.Fatalf("Normalize(spacebar B) error = %v", err)
	}
	if spacebarA.ID == spacebarB.ID || spacebarA.ID == discord.ID {
		t.Fatalf("provider IDs collided: discord=%q A=%q B=%q", discord.ID, spacebarA.ID, spacebarB.ID)
	}
	if spacebarA.APIBaseURL != "http://spacebar-a:3001/api" ||
		spacebarA.GatewayBaseURL != "ws://spacebar-a:3001" ||
		spacebarA.CDNBaseURL != "http://spacebar-a:3001" {
		t.Fatalf("spacebar origins were not canonical: %#v", spacebarA)
	}
}

func TestProviderRejectsCredentialedCrossPurposeOrInsecureOrigins(t *testing.T) {
	tests := []config.Provider{
		{Kind: "discord", APIBaseURL: "https://example.test/api"},
		{Kind: "spacebar", APIBaseURL: "http://user:pass@spacebar:3001/api", GatewayBaseURL: "ws://spacebar:3001", CDNBaseURL: "http://spacebar:3001", APIVersion: 10, TestOnlyAllowInsecureTransport: true},
		{Kind: "spacebar", APIBaseURL: "http://spacebar:3001/api?token=x", GatewayBaseURL: "ws://spacebar:3001", CDNBaseURL: "http://spacebar:3001", APIVersion: 10, TestOnlyAllowInsecureTransport: true},
		{Kind: "spacebar", APIBaseURL: "http://spacebar:3001/api", GatewayBaseURL: "ws://spacebar:3001", CDNBaseURL: "http://spacebar:3001", APIVersion: 10, TestOnlyAllowInsecureTransport: true},
	}
	for index, provider := range tests {
		testMode := index != len(tests)-1
		if _, err := provider.Normalize(testMode); err == nil {
			t.Errorf("provider %d unexpectedly normalized", index)
		}
	}
}

func TestProxyValidationIsFailClosedAndRedacted(t *testing.T) {
	valid, err := (config.EgressProxy{
		Mode: "socks5", URL: "socks5://proxy.internal:1080",
		Username: "proxy-user-canary", Password: "proxy-password-canary", RemoteDNS: true,
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got := valid.SafeSummary(); strings.Contains(got, "canary") || !strings.Contains(got, "proxy.internal") {
		t.Fatalf("SafeSummary() leaked or omitted proxy host: %q", got)
	}

	for _, proxy := range []config.EgressProxy{
		{Mode: "direct", URL: "http://proxy.internal:8080"},
		{Mode: "http", URL: "http://user:pass@proxy.internal:8080"},
		{Mode: "socks5", URL: "socks5://proxy.internal:1080", RemoteDNS: false},
		{Mode: "unknown"},
	} {
		if _, err := proxy.Normalize(); err == nil {
			t.Errorf("proxy %#v unexpectedly normalized", proxy)
		}
	}
}

func TestWireAndIngressValidationEnforceBoundsWithoutLeakingSecrets(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	wire := config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "aes256-hmac-v1", KeyMode: "directional", Key: key,
	}
	if _, err := wire.Normalize(); err != nil {
		t.Fatalf("Wire.Normalize() error = %v", err)
	}
	badWire := wire
	badWire.Key = "transport-key-canary"
	if _, err := badWire.Normalize(); err == nil || strings.Contains(err.Error(), badWire.Key) {
		t.Fatalf("bad key error was absent or leaked secret: %v", err)
	}

	valid := config.Ingress{
		Mode: "polling", PollStrategy: "adaptive", PollIntervalSeconds: 15,
		PollMinIntervalSeconds: 2, PollBaseIntervalSeconds: 15,
		PollMaxIntervalSeconds: 300, PollBuildWarmSeconds: 900,
		ReconciliationIntervalSeconds: 300,
	}
	if _, err := valid.Normalize(); err != nil {
		t.Fatalf("Ingress.Normalize() error = %v", err)
	}
	invalid := valid
	invalid.PollMinIntervalSeconds = 20
	invalid.PollBaseIntervalSeconds = 10
	if _, err := invalid.Normalize(); err == nil {
		t.Fatal("invalid adaptive ordering unexpectedly normalized")
	}
}
