package c2functions

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	c2structs "github.com/MythicMeta/MythicContainer/c2_structs"
	"github.com/google/uuid"
)

const version = "2.0.0"

var discordxDefinition = c2structs.C2Profile{
	Name:             "discordx",
	Author:           "@tr41nwr3ck & @checkymander",
	Description:      "Discord-compatible multi-listener transport",
	IsP2p:            false,
	IsServerRouted:   true,
	SemVer:           version,
	ServerBinaryPath: filepath.Join(string(filepath.Separator), "usr", "local", "bin", "discordx-server"),
	ConfigCheckFunction: func(message c2structs.C2ConfigCheckMessage) c2structs.C2ConfigCheckMessageResponse {
		if err := validateConfig(message.C2Parameters); err != nil {
			return c2structs.C2ConfigCheckMessageResponse{Success: false, Error: err.Error()}
		}
		return c2structs.C2ConfigCheckMessageResponse{
			Success: true, RestartInternalServer: false,
			Message: "Discordx listener is structurally valid; the shared runtime registry remains unchanged",
		}
	},
}

func parameter(name, description string, defaultValue interface{}, required bool) c2structs.C2Parameter {
	return c2structs.C2Parameter{
		Name: name, Description: description, DefaultValue: defaultValue,
		ParameterType: c2structs.C2_PARAMETER_TYPE_STRING, Required: required,
	}
}

func choice(name, description, defaultValue string, choices []string) c2structs.C2Parameter {
	return c2structs.C2Parameter{
		Name: name, Description: description, DefaultValue: defaultValue,
		ParameterType: c2structs.C2_PARAMETER_TYPE_CHOOSE_ONE, Required: false, Choices: choices,
	}
}

func boolean(name, description string, defaultValue bool) c2structs.C2Parameter {
	return c2structs.C2Parameter{
		Name: name, Description: description, DefaultValue: defaultValue,
		ParameterType: c2structs.C2_PARAMETER_TYPE_BOOLEAN, Required: false,
	}
}

var discordxParameters = []c2structs.C2Parameter{
	parameter("discord_token", "Write-only bot token used by this logical listener", "", true),
	parameter("bot_channel", "Discord task channel snowflake", "", true),
	parameter("socks_channel", "Optional distinct SOCKS channel snowflake", "", false),
	{
		Name: "listener_id", Description: "Immutable canonical listener UUID; blank lets Mythic assign one",
		DefaultValue: "", ParameterType: c2structs.C2_PARAMETER_TYPE_STRING, Required: false,
		VerifierRegex: "^$|^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
	},
	boolean("listener_enabled", "Enable this logical listener in the shared runtime", true),
	choice("discord_provider_kind", "Discord service provider", "discord", []string{"discord", "spacebar", "mock"}),
	choice("discord_api_version", "Discord-compatible REST API version", "10", []string{"10"}),
	boolean("discord_test_only_allow_insecure_transport", "Allow explicit HTTP/WS origins in compatibility labs", false),
	parameter("provider_api_origin", "Discord-compatible API root origin", "https://discord.com", false),
	parameter("provider_gateway_origin", "Optional explicit Gateway root origin", "", false),
	parameter("provider_cdn_origin", "Expected attachment CDN root origin", "https://cdn.discordapp.com", false),
	choice("server_egress_proxy_mode", "Server-side bot egress route", "direct", []string{"direct", "http", "socks5"}),
	parameter("server_egress_proxy_url", "Credential-free server proxy root URL", "", false),
	parameter("server_egress_proxy_username", "Optional write-only server proxy username", "", false),
	parameter("server_egress_proxy_password", "Optional write-only server proxy password", "", false),
	boolean("server_egress_proxy_remote_dns", "Resolve provider hostnames through a SOCKS5 proxy", true),
	choice("server_ingress_mode", "Server discovery path", "gateway", []string{"gateway", "polling"}),
	choice("server_poll_strategy", "Polling cadence planner", "adaptive", []string{"adaptive", "fixed"}),
	parameter("server_poll_interval_seconds", "Fixed/fallback polling interval (2-300 seconds)", "15", false),
	parameter("server_poll_min_interval_seconds", "Adaptive hot interval (2-60 seconds)", "2", false),
	parameter("server_poll_base_interval_seconds", "Adaptive base interval (5-300 seconds)", "15", false),
	parameter("server_poll_max_interval_seconds", "Adaptive cold interval (15-3600 seconds)", "300", false),
	parameter("server_poll_build_warm_seconds", "Post-build adaptive warm duration (0-3600 seconds)", "900", false),
	parameter("server_reconciliation_interval_seconds", "Gateway reconciliation interval (0 or 60-3600 seconds)", "300", false),
	choice("wire_protocol", "Discord wire protocol", "fixed", []string{"fixed", "legacy"}),
	choice("transport_envelope_format", "Fixed channel envelope", "binary-v1", []string{"json-v1", "binary-v1"}),
	choice("transport_presentation", "Discord text presentation", "base64", []string{"plain", "base64", "decimal", "emoji"}),
	choice("transport_protection", "Fixed outer protection", "chacha20-v1", []string{"none", "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1"}),
	choice("transport_key_mode", "Outer key directionality", "directional", []string{"single", "directional"}),
	{
		Name: "transport_key", Description: "Write-only canonical Base64 32-byte outer listener key",
		DefaultValue: "", Randomize: true, FormatString: "[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=",
		ParameterType: c2structs.C2_PARAMETER_TYPE_STRING, Required: false,
		VerifierRegex: "^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$",
	},
	boolean("use_base64", "Use the historical Base64 UUID inner envelope", false),
	parameter("message_checks", "Response checks before declaring failure", "10", false),
	parameter("time_between_checks", "Seconds between response checks", "10", false),
	parameter("callback_interval", "Payload callback interval in seconds", "60", false),
	parameter("callback_jitter", "Payload callback jitter percentage", "10", false),
	choice("encrypted_exchange_check", "Perform the payload type's supported staging exchange", "F", []string{"T", "F"}),
	{
		Name: "AESPSK", Description: "Inner Mythic message protection, independent of outer transport protection",
		DefaultValue: "none", ParameterType: c2structs.C2_PARAMETER_TYPE_CHOOSE_ONE,
		Required: false, IsCryptoType: true, Choices: []string{"aes256_hmac", "none"},
	},
	parameter("user_agent", "Payload HTTP user agent", "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko", false),
	{Name: "proxy_host", Description: "Payload proxy host", DefaultValue: "", ParameterType: c2structs.C2_PARAMETER_TYPE_STRING, Required: false, VerifierRegex: "^$|^(http|https)://[a-zA-Z0-9]+"},
	{Name: "proxy_port", Description: "Payload proxy port", DefaultValue: "", ParameterType: c2structs.C2_PARAMETER_TYPE_STRING, Required: false, VerifierRegex: "^$|^[0-9]+$"},
	parameter("proxy_user", "Payload proxy username", "", false),
	parameter("proxy_pass", "Payload proxy password", "", false),
	{Name: "killdate", Description: "Kill date", DefaultValue: 365, ParameterType: c2structs.C2_PARAMETER_TYPE_DATE, Required: false},
}

func Initialize() {
	c2structs.AllC2Data.Get("discordx").AddC2Definition(discordxDefinition)
	c2structs.AllC2Data.Get("discordx").AddParameters(discordxParameters)
}

func stringValue(parameters c2structs.C2Parameters, name, fallback string) (string, error) {
	if _, exists := parameters.Parameters[name]; !exists {
		return fallback, nil
	}
	value, err := parameters.GetStringArg(name)
	if err != nil {
		return "", fmt.Errorf("%s is invalid", name)
	}
	return strings.TrimSpace(value), nil
}

func boolValue(parameters c2structs.C2Parameters, name string, fallback bool) (bool, error) {
	if _, exists := parameters.Parameters[name]; !exists {
		return fallback, nil
	}
	value, err := parameters.GetBooleanArg(name)
	if err != nil {
		return false, fmt.Errorf("%s is invalid", name)
	}
	return value, nil
}

func intValue(parameters c2structs.C2Parameters, name string, fallback, minimum, maximum int) (int, error) {
	text, err := stringValue(parameters, name, strconv.Itoa(fallback))
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s is outside its supported range", name)
	}
	return value, nil
}

func validateConfig(parameters c2structs.C2Parameters) error {
	token, err := stringValue(parameters, "discord_token", "")
	if err != nil || token == "" {
		return errors.New("discord_token is required")
	}
	taskChannel, err := stringValue(parameters, "bot_channel", "")
	if err != nil || !validSnowflake(taskChannel) {
		return errors.New("bot_channel must be a valid Discord snowflake")
	}
	socksChannel, err := stringValue(parameters, "socks_channel", "")
	if err != nil || (socksChannel != "" && (!validSnowflake(socksChannel) || socksChannel == taskChannel)) {
		return errors.New("socks_channel must be empty or a distinct Discord snowflake")
	}
	listenerID, err := stringValue(parameters, "listener_id", "")
	if err != nil {
		return err
	}
	if listenerID != "" {
		parsed, parseErr := uuid.Parse(listenerID)
		if parseErr != nil || parsed.String() != listenerID || parsed.Version() != 4 {
			return errors.New("listener_id must be a canonical UUIDv4")
		}
	}
	if _, err := boolValue(parameters, "listener_enabled", true); err != nil {
		return err
	}
	providerKind, err := stringValue(parameters, "discord_provider_kind", "discord")
	if err != nil {
		return err
	}
	apiVersion, err := stringValue(parameters, "discord_api_version", "10")
	if err != nil || apiVersion != "10" {
		return errors.New("Discord API version is unsupported")
	}
	insecure, err := boolValue(parameters, "discord_test_only_allow_insecure_transport", false)
	if err != nil {
		return err
	}
	apiOrigin, err := stringValue(parameters, "provider_api_origin", "https://discord.com")
	if err != nil {
		return err
	}
	gatewayOrigin, err := stringValue(parameters, "provider_gateway_origin", "")
	if err != nil {
		return err
	}
	cdnOrigin, err := stringValue(parameters, "provider_cdn_origin", "https://cdn.discordapp.com")
	if err != nil {
		return err
	}
	if err := validateProvider(providerKind, apiOrigin, gatewayOrigin, cdnOrigin, insecure); err != nil {
		return err
	}
	if err := validateEgress(parameters); err != nil {
		return err
	}
	ingressMode, err := stringValue(parameters, "server_ingress_mode", "gateway")
	if err != nil || (ingressMode != "gateway" && ingressMode != "polling") {
		return errors.New("server ingress mode is unsupported")
	}
	pollStrategy, err := stringValue(parameters, "server_poll_strategy", "adaptive")
	if err != nil || (pollStrategy != "adaptive" && pollStrategy != "fixed") {
		return errors.New("server polling strategy is unsupported")
	}
	minimum, err := intValue(parameters, "server_poll_min_interval_seconds", 2, 2, 60)
	if err != nil {
		return err
	}
	base, err := intValue(parameters, "server_poll_base_interval_seconds", 15, 5, 300)
	if err != nil {
		return err
	}
	maximum, err := intValue(parameters, "server_poll_max_interval_seconds", 300, 15, 3600)
	if err != nil || minimum > base || base > maximum {
		return errors.New("adaptive polling requires minimum <= base <= maximum")
	}
	if _, err := intValue(parameters, "server_poll_interval_seconds", 15, 2, 300); err != nil {
		return err
	}
	if _, err := intValue(parameters, "server_poll_build_warm_seconds", 900, 0, 3600); err != nil {
		return err
	}
	reconciliation, err := intValue(parameters, "server_reconciliation_interval_seconds", 300, 0, 3600)
	if err != nil || (reconciliation != 0 && reconciliation < 60) {
		return errors.New("server reconciliation interval must be zero or 60-3600 seconds")
	}
	messageChecks, err := intValue(parameters, "message_checks", 10, 2, 10000)
	if err != nil {
		return err
	}
	betweenChecks, err := intValue(parameters, "time_between_checks", 10, 1, 3600)
	if err != nil {
		return err
	}
	if ingressMode == "polling" && (messageChecks-1)*betweenChecks < 2*minimum {
		return errors.New("payload response window must cover two minimum polling intervals")
	}
	return validateWire(parameters, socksChannel != "")
}

func validateProvider(kind, api, gateway, cdn string, insecure bool) error {
	kind = strings.ToLower(kind)
	if kind == "discord" {
		if strings.TrimSuffix(api, "/") != "https://discord.com" ||
			(gateway != "" && strings.TrimSuffix(gateway, "/") != "wss://gateway.discord.gg") ||
			strings.TrimSuffix(cdn, "/") != "https://cdn.discordapp.com" || insecure {
			return errors.New("official Discord provider endpoints are fixed")
		}
		return nil
	}
	if kind != "spacebar" && kind != "mock" {
		return errors.New("Discord provider kind is unsupported")
	}
	for name, raw := range map[string]string{"API": api, "Gateway": gateway, "CDN": cdn} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil ||
			parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return fmt.Errorf("custom provider %s origin is invalid", name)
		}
		if (name == "Gateway" && parsed.Scheme != "ws" && parsed.Scheme != "wss") ||
			(name != "Gateway" && parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("custom provider %s scheme is invalid", name)
		}
		if (parsed.Scheme == "http" || parsed.Scheme == "ws") && !insecure {
			return errors.New("insecure provider origins require test-only opt-in")
		}
	}
	return nil
}

func validateEgress(parameters c2structs.C2Parameters) error {
	mode, err := stringValue(parameters, "server_egress_proxy_mode", "direct")
	if err != nil {
		return err
	}
	proxyURL, err := stringValue(parameters, "server_egress_proxy_url", "")
	if err != nil {
		return err
	}
	username, err := stringValue(parameters, "server_egress_proxy_username", "")
	if err != nil {
		return err
	}
	password, err := stringValue(parameters, "server_egress_proxy_password", "")
	if err != nil {
		return err
	}
	remoteDNS, err := boolValue(parameters, "server_egress_proxy_remote_dns", true)
	if err != nil {
		return err
	}
	if mode == "direct" {
		if proxyURL != "" || username != "" || password != "" {
			return errors.New("direct server egress forbids proxy settings")
		}
		return nil
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return errors.New("server proxy URL must be a credential-free root URL")
	}
	if mode == "http" && parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("HTTP proxy mode requires an HTTP or HTTPS URL")
	}
	if mode == "socks5" && (parsed.Scheme != "socks5" || !remoteDNS) {
		return errors.New("SOCKS5 proxy mode requires a SOCKS5 URL and remote DNS")
	}
	if mode != "http" && mode != "socks5" {
		return errors.New("server proxy mode is unsupported")
	}
	return nil
}

func validateWire(parameters c2structs.C2Parameters, socks bool) error {
	protocol, _ := stringValue(parameters, "wire_protocol", "fixed")
	format, _ := stringValue(parameters, "transport_envelope_format", "binary-v1")
	presentation, _ := stringValue(parameters, "transport_presentation", "base64")
	protection, _ := stringValue(parameters, "transport_protection", "chacha20-v1")
	keyMode, _ := stringValue(parameters, "transport_key_mode", "directional")
	key, _ := stringValue(parameters, "transport_key", "")
	if protocol == "legacy" {
		if format != "json-v1" || presentation != "plain" || protection != "none" || keyMode != "single" || key != "" || socks {
			return errors.New("legacy wire settings are not canonical")
		}
		return nil
	}
	if protocol != "fixed" || (format != "json-v1" && format != "binary-v1") ||
		(presentation != "plain" && presentation != "base64" && presentation != "decimal" && presentation != "emoji") ||
		(keyMode != "single" && keyMode != "directional") {
		return errors.New("fixed wire settings are unsupported")
	}
	if socks && format != "binary-v1" {
		return errors.New("SOCKS requires fixed binary-v1 transport")
	}
	if presentation == "plain" && (format != "json-v1" || protection != "none") {
		return errors.New("plain presentation requires unprotected json-v1")
	}
	if protection == "none" {
		// Mythic randomizes this profile parameter before it evaluates the
		// selected protection mode. Unprotected payloads deliberately discard
		// that auto-materialized value, matching the Nuwa builder, and never
		// embed or publish it.
		return nil
	}
	if protection != "xor-obfuscation-v1" && protection != "chacha20-v1" && protection != "aes256-hmac-v1" {
		return errors.New("transport protection is unsupported")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(key)
	if err != nil || len(decoded) != 32 || base64.StdEncoding.EncodeToString(decoded) != key {
		return errors.New("transport key must be canonical Base64 for 32 bytes")
	}
	return nil
}

func validSnowflake(value string) bool {
	if value == "" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed != 0
}
