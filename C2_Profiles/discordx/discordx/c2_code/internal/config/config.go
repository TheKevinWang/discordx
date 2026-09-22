// Package config validates listener values before they can enter the live
// registry. Validation errors intentionally omit all secret values.
package config

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	officialAPI     = "https://discord.com/api"
	officialGateway = "wss://gateway.discord.gg"
	officialCDN     = "https://cdn.discordapp.com"
)

type Provider struct {
	ID                             string `json:"id,omitempty"`
	Kind                           string `json:"kind"`
	APIBaseURL                     string `json:"api_base_url"`
	GatewayBaseURL                 string `json:"gateway_base_url"`
	CDNBaseURL                     string `json:"cdn_base_url"`
	APIVersion                     int    `json:"api_version"`
	TestOnlyAllowInsecureTransport bool   `json:"test_only_allow_insecure_transport"`
}

func (provider Provider) Normalize(testMode bool) (Provider, error) {
	provider.ID = ""
	provider.Kind = strings.ToLower(strings.TrimSpace(provider.Kind))
	if provider.Kind == "" {
		provider.Kind = "discord"
	}
	if provider.APIVersion == 0 {
		provider.APIVersion = 10
	}
	if provider.APIVersion != 10 {
		return Provider{}, errors.New("provider API version is unsupported")
	}

	switch provider.Kind {
	case "discord":
		if !emptyOrEqualOrigin(provider.APIBaseURL, officialAPI) ||
			!emptyOrEqualOrigin(provider.GatewayBaseURL, officialGateway) ||
			!emptyOrEqualOrigin(provider.CDNBaseURL, officialCDN) ||
			provider.TestOnlyAllowInsecureTransport {
			return Provider{}, errors.New("official Discord provider endpoints are fixed")
		}
		provider.APIBaseURL = officialAPI
		provider.GatewayBaseURL = officialGateway
		provider.CDNBaseURL = officialCDN
	case "spacebar", "mock":
		api, apiInsecure, err := normalizeOrigin(provider.APIBaseURL, true, "http", "https")
		if err != nil {
			return Provider{}, errors.New("provider API origin is invalid")
		}
		gateway, gatewayInsecure, err := normalizeOrigin(provider.GatewayBaseURL, false, "ws", "wss")
		if err != nil {
			return Provider{}, errors.New("provider Gateway origin is invalid")
		}
		cdn, cdnInsecure, err := normalizeOrigin(provider.CDNBaseURL, false, "http", "https")
		if err != nil {
			return Provider{}, errors.New("provider CDN origin is invalid")
		}
		if apiInsecure || gatewayInsecure || cdnInsecure {
			if !testMode || !provider.TestOnlyAllowInsecureTransport {
				return Provider{}, errors.New("insecure provider transport requires explicit test mode")
			}
		}
		provider.APIBaseURL = api
		provider.GatewayBaseURL = gateway
		provider.CDNBaseURL = cdn
	default:
		return Provider{}, errors.New("provider kind is unsupported")
	}

	digest := sha256.Sum256([]byte(strings.Join([]string{
		provider.Kind,
		provider.APIBaseURL,
		provider.GatewayBaseURL,
		provider.CDNBaseURL,
		strconv.Itoa(provider.APIVersion),
	}, "\x00")))
	provider.ID = hex.EncodeToString(digest[:])
	return provider, nil
}

func emptyOrEqualOrigin(value, expected string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	normalized, _, err := normalizeOrigin(value, strings.HasSuffix(expected, "/api"), "http", "https", "ws", "wss")
	return err == nil && normalized == expected
}

func normalizeOrigin(raw string, allowAPIPath bool, schemes ...string) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || parsed.Opaque != "" {
		return "", false, errors.New("invalid origin")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	allowed := false
	for _, scheme := range schemes {
		if parsed.Scheme == scheme {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", false, errors.New("invalid origin scheme")
	}
	if parsed.RawPath != "" {
		return "", false, errors.New("escaped origin path is not allowed")
	}
	path := strings.TrimSuffix(parsed.Path, "/")
	if allowAPIPath {
		if path != "" && path != "/api" {
			return "", false, errors.New("invalid API origin path")
		}
		if path == "" {
			path = "/api"
		}
	} else if path != "" {
		return "", false, errors.New("origin path is not allowed")
	}
	parsed.Path = path
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String(), parsed.Scheme == "http" || parsed.Scheme == "ws", nil
}

type EgressProxy struct {
	Mode      string `json:"proxy_mode"`
	URL       string `json:"proxy_url,omitempty"`
	Username  string `json:"proxy_username,omitempty"`
	Password  string `json:"proxy_password,omitempty"`
	RemoteDNS bool   `json:"remote_dns"`
	Hostname  string `json:"-"`
}

func (proxy EgressProxy) Normalize() (EgressProxy, error) {
	proxy.Mode = strings.ToLower(strings.TrimSpace(proxy.Mode))
	if proxy.Mode == "" {
		proxy.Mode = "direct"
	}
	proxy.URL = strings.TrimSpace(proxy.URL)
	proxy.Hostname = ""
	if proxy.Mode == "direct" {
		if proxy.URL != "" || proxy.Username != "" || proxy.Password != "" {
			return EgressProxy{}, errors.New("direct server egress forbids proxy settings")
		}
		proxy.RemoteDNS = false
		return proxy, nil
	}
	parsed, err := url.Parse(proxy.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" || parsed.RawPath != "" {
		return EgressProxy{}, errors.New("server proxy URL is invalid")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	switch proxy.Mode {
	case "http":
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return EgressProxy{}, errors.New("HTTP proxy mode requires an HTTP or HTTPS URL")
		}
	case "socks5":
		if parsed.Scheme != "socks5" {
			return EgressProxy{}, errors.New("SOCKS5 proxy mode requires a SOCKS5 URL")
		}
		if !proxy.RemoteDNS {
			return EgressProxy{}, errors.New("SOCKS5 proxy mode requires remote DNS")
		}
	default:
		return EgressProxy{}, errors.New("server proxy mode is unsupported")
	}
	proxy.URL = parsed.String()
	proxy.Hostname = parsed.Hostname()
	return proxy, nil
}

func (proxy EgressProxy) SafeSummary() string {
	if proxy.Mode == "direct" || proxy.Mode == "" {
		return "direct"
	}
	hostname := proxy.Hostname
	if hostname == "" {
		if parsed, err := url.Parse(proxy.URL); err == nil {
			hostname = parsed.Hostname()
		}
	}
	return fmt.Sprintf("%s://%s", proxy.Mode, hostname)
}

type Ingress struct {
	Mode                          string `json:"mode"`
	PollStrategy                  string `json:"poll_strategy"`
	PollIntervalSeconds           int    `json:"poll_interval_seconds"`
	PollMinIntervalSeconds        int    `json:"poll_min_interval_seconds"`
	PollBaseIntervalSeconds       int    `json:"poll_base_interval_seconds"`
	PollMaxIntervalSeconds        int    `json:"poll_max_interval_seconds"`
	PollBuildWarmSeconds          int    `json:"poll_build_warm_seconds"`
	ReconciliationIntervalSeconds int    `json:"reconciliation_interval_seconds"`
}

func (ingress Ingress) Normalize() (Ingress, error) {
	ingress.Mode = strings.ToLower(strings.TrimSpace(ingress.Mode))
	if ingress.Mode == "" {
		ingress.Mode = "gateway"
	}
	if ingress.Mode != "gateway" && ingress.Mode != "polling" {
		return Ingress{}, errors.New("server ingress mode is unsupported")
	}
	ingress.PollStrategy = strings.ToLower(strings.TrimSpace(ingress.PollStrategy))
	if ingress.PollStrategy == "" {
		ingress.PollStrategy = "adaptive"
	}
	if ingress.PollStrategy != "adaptive" && ingress.PollStrategy != "fixed" {
		return Ingress{}, errors.New("server poll strategy is unsupported")
	}
	if ingress.PollIntervalSeconds == 0 {
		ingress.PollIntervalSeconds = 15
	}
	if ingress.PollMinIntervalSeconds == 0 {
		ingress.PollMinIntervalSeconds = 2
	}
	if ingress.PollBaseIntervalSeconds == 0 {
		ingress.PollBaseIntervalSeconds = 15
	}
	if ingress.PollMaxIntervalSeconds == 0 {
		ingress.PollMaxIntervalSeconds = 300
	}
	if ingress.PollIntervalSeconds < 2 || ingress.PollIntervalSeconds > 300 {
		return Ingress{}, errors.New("fixed polling interval is out of range")
	}
	if ingress.PollMinIntervalSeconds < 2 || ingress.PollMinIntervalSeconds > 60 ||
		ingress.PollBaseIntervalSeconds < 5 || ingress.PollBaseIntervalSeconds > 300 ||
		ingress.PollMaxIntervalSeconds < 15 || ingress.PollMaxIntervalSeconds > 3600 ||
		ingress.PollMinIntervalSeconds > ingress.PollBaseIntervalSeconds ||
		ingress.PollBaseIntervalSeconds > ingress.PollMaxIntervalSeconds {
		return Ingress{}, errors.New("adaptive polling intervals are invalid")
	}
	if ingress.PollBuildWarmSeconds < 0 || ingress.PollBuildWarmSeconds > 3600 {
		return Ingress{}, errors.New("poll build-warm duration is out of range")
	}
	if ingress.ReconciliationIntervalSeconds != 0 &&
		(ingress.ReconciliationIntervalSeconds < 60 || ingress.ReconciliationIntervalSeconds > 3600) {
		return Ingress{}, errors.New("Gateway reconciliation interval is out of range")
	}
	return ingress, nil
}

type Wire struct {
	Protocol       string `json:"protocol"`
	EnvelopeFormat string `json:"envelope_format"`
	Presentation   string `json:"presentation"`
	Protection     string `json:"protection"`
	KeyMode        string `json:"key_mode"`
	Key            string `json:"key,omitempty"`
	UseBase64      bool   `json:"use_base64"`
}

func (wire Wire) Normalize() (Wire, error) {
	wire.Protocol = strings.ToLower(strings.TrimSpace(wire.Protocol))
	if wire.Protocol == "" {
		wire.Protocol = "fixed"
	}
	if wire.Protocol == "legacy" {
		if (wire.EnvelopeFormat != "" && wire.EnvelopeFormat != "json-v1") ||
			(wire.Presentation != "" && wire.Presentation != "plain") ||
			(wire.Protection != "" && wire.Protection != "none") ||
			(wire.KeyMode != "" && wire.KeyMode != "single") || wire.Key != "" {
			return Wire{}, errors.New("legacy wire configuration contains fixed-envelope settings")
		}
		wire.EnvelopeFormat = "json-v1"
		wire.Presentation = "plain"
		wire.Protection = "none"
		wire.KeyMode = "single"
		wire.UseBase64 = true
		return wire, nil
	}
	if wire.Protocol != "fixed" {
		return Wire{}, errors.New("wire protocol is unsupported")
	}
	wire.EnvelopeFormat = strings.ToLower(strings.TrimSpace(wire.EnvelopeFormat))
	wire.Presentation = strings.ToLower(strings.TrimSpace(wire.Presentation))
	wire.Protection = strings.ToLower(strings.TrimSpace(wire.Protection))
	wire.KeyMode = strings.ToLower(strings.TrimSpace(wire.KeyMode))
	if wire.EnvelopeFormat == "" {
		wire.EnvelopeFormat = "binary-v1"
	}
	if wire.Presentation == "" {
		wire.Presentation = "base64"
	}
	if wire.Protection == "" {
		wire.Protection = "none"
	}
	if wire.KeyMode == "" {
		wire.KeyMode = "single"
	}
	if wire.EnvelopeFormat != "json-v1" && wire.EnvelopeFormat != "binary-v1" {
		return Wire{}, errors.New("transport envelope format is unsupported")
	}
	switch wire.Presentation {
	case "plain", "base64", "decimal", "emoji":
	default:
		return Wire{}, errors.New("transport presentation is unsupported")
	}
	if wire.KeyMode != "single" && wire.KeyMode != "directional" {
		return Wire{}, errors.New("transport key mode is unsupported")
	}
	switch wire.Protection {
	case "none":
		if wire.Key != "" {
			return Wire{}, errors.New("unprotected transport must not include a key")
		}
	case "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1":
		decoded, err := base64.StdEncoding.Strict().DecodeString(wire.Key)
		if err != nil || len(decoded) != 32 || base64.StdEncoding.EncodeToString(decoded) != wire.Key {
			return Wire{}, errors.New("transport key must be canonical Base64 for 32 bytes")
		}
	default:
		return Wire{}, errors.New("transport protection is unsupported")
	}
	if wire.Presentation == "plain" && (wire.EnvelopeFormat != "json-v1" || wire.Protection != "none") {
		return Wire{}, errors.New("plain presentation requires unprotected json-v1")
	}
	return wire, nil
}

func (wire Wire) SafeSummary() string {
	return strings.Join([]string{wire.Protocol, wire.EnvelopeFormat, wire.Presentation, wire.Protection, wire.KeyMode}, "/")
}

func (wire Wire) DiagnosticFingerprint() string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		wire.Protocol, wire.EnvelopeFormat, wire.Presentation, wire.Protection,
		wire.KeyMode, strconv.FormatBool(wire.UseBase64),
	}, "\x00")))
	return hex.EncodeToString(digest[:8])
}

func ValidSnowflake(value string) bool {
	if value == "" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed != 0
}
