package c2functions

import (
	"encoding/base64"
	"strings"
	"testing"

	c2structs "github.com/MythicMeta/MythicContainer/c2_structs"
)

func findDiscordxParameter(t *testing.T, name string) c2structs.C2Parameter {
	t.Helper()
	for _, parameter := range discordxParameters {
		if parameter.Name == name {
			return parameter
		}
	}
	t.Fatalf("Discordx parameter %q was not registered", name)
	return c2structs.C2Parameter{}
}

func validDiscordxParametersForTest() c2structs.C2Parameters {
	return c2structs.C2Parameters{Name: "discordx", Parameters: map[string]interface{}{
		"discord_token": "discord-token-secret-canary", "bot_channel": "123456789012345678",
		"socks_channel": "234567890123456789", "listener_id": "11111111-1111-4111-8111-111111111111",
		"discord_provider_kind": "discord", "discord_api_version": "10",
		"discord_test_only_allow_insecure_transport": false,
		"provider_api_origin":                        "https://discord.com", "provider_gateway_origin": "",
		"provider_cdn_origin":      "https://cdn.discordapp.com",
		"server_egress_proxy_mode": "direct", "server_egress_proxy_url": "",
		"server_egress_proxy_username": "", "server_egress_proxy_password": "",
		"server_egress_proxy_remote_dns": true,
		"server_poll_interval_seconds":   "15", "server_poll_min_interval_seconds": "2",
		"server_poll_base_interval_seconds": "15", "server_poll_max_interval_seconds": "300",
		"server_poll_build_warm_seconds": "900", "server_reconciliation_interval_seconds": "300",
		"wire_protocol": "fixed", "transport_envelope_format": "binary-v1",
		"transport_presentation": "base64", "transport_protection": "chacha20-v1",
		"transport_key_mode": "directional",
		"transport_key":      base64.StdEncoding.EncodeToString(make([]byte, 32)),
	}}
}

func TestGoProfileRegistersMultiListenerParameters(t *testing.T) {
	if discordxDefinition.Name != "discordx" || discordxDefinition.ServerBinaryPath == "" || discordxDefinition.SemVer != version {
		t.Fatalf("Discordx definition = %#v", discordxDefinition)
	}
	for _, name := range []string{
		"listener_id", "listener_enabled", "discord_provider_kind", "server_ingress_mode",
		"server_egress_proxy_mode", "transport_envelope_format", "transport_key",
	} {
		_ = findDiscordxParameter(t, name)
	}
	if key := findDiscordxParameter(t, "transport_key"); !key.Randomize || key.VerifierRegex == "" {
		t.Fatalf("transport key definition = %#v", key)
	}
}

func TestGoConfigCheckIsReadOnlyAndRedactsSecrets(t *testing.T) {
	parameters := validDiscordxParametersForTest()
	response := discordxDefinition.ConfigCheckFunction(c2structs.C2ConfigCheckMessage{C2Parameters: parameters})
	if !response.Success || response.RestartInternalServer {
		t.Fatalf("config check response = %#v", response)
	}
	if strings.Contains(response.Message+response.Error, "discord-token-secret-canary") ||
		strings.Contains(response.Message+response.Error, parameters.Parameters["transport_key"].(string)) {
		t.Fatal("config check exposed a secret")
	}
	parameters.Parameters["provider_api_origin"] = "https://attacker.invalid"
	response = discordxDefinition.ConfigCheckFunction(c2structs.C2ConfigCheckMessage{C2Parameters: parameters})
	if response.Success || strings.Contains(response.Error, "discord-token-secret-canary") {
		t.Fatalf("invalid provider response = %#v", response)
	}
}

func TestGoConfigCheckDiscardsAutoMaterializedKeyForUnprotectedTransport(t *testing.T) {
	parameters := validDiscordxParametersForTest()
	parameters.Parameters["transport_protection"] = "none"
	parameters.Parameters["transport_key_mode"] = "single"
	response := discordxDefinition.ConfigCheckFunction(c2structs.C2ConfigCheckMessage{C2Parameters: parameters})
	if !response.Success {
		t.Fatalf("unprotected config check response = %#v", response)
	}
}
