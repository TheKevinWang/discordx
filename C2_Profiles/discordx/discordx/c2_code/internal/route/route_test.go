package route_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

const (
	listenerID   = "11111111-1111-4111-8111-111111111111"
	generationID = "22222222-2222-4222-8222-222222222222"
	clientID     = "33333333-3333-4333-8333-333333333333"
)

func TestDX2FixedRoundTripIsExactAndCanonical(t *testing.T) {
	want := "dx2:" + listenerID + ":" + generationID + ":f:" + clientID
	parsed, err := route.ParseDX2(want)
	if err != nil {
		t.Fatalf("ParseDX2() error = %v", err)
	}
	if parsed.ListenerID != listenerID || parsed.GenerationID != generationID ||
		parsed.Kind != route.Fixed || parsed.ClientID != clientID || parsed.LegacySender != "" {
		t.Fatalf("ParseDX2() = %#v", parsed)
	}
	if got := parsed.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestDX2LegacyRoundTripUsesUnpaddedBase64URL(t *testing.T) {
	sender := "legacy:sender/with unicode ☃"
	wantTail := base64.RawURLEncoding.EncodeToString([]byte(sender))
	want := "dx2:" + listenerID + ":" + generationID + ":l:" + wantTail
	parsed, err := route.ParseDX2(want)
	if err != nil {
		t.Fatalf("ParseDX2() error = %v", err)
	}
	if parsed.Kind != route.Legacy || parsed.LegacySender != sender || parsed.ClientID != "" {
		t.Fatalf("ParseDX2() = %#v", parsed)
	}
	if got := parsed.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestDX2RejectsMalformedAmbiguousAndOversizedRoutes(t *testing.T) {
	validLegacy := base64.RawURLEncoding.EncodeToString([]byte("legacy"))
	tests := []string{
		"",
		"dx2:" + listenerID + ":" + generationID + ":f",
		"dx2:" + listenerID + ":" + generationID + ":f:" + clientID + ":extra",
		"dx2:" + strings.ToUpper("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") + ":" + generationID + ":f:" + clientID,
		"dx2:" + listenerID + ":" + generationID + ":f:" + strings.ToUpper("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		"dx2:" + listenerID + ":" + generationID + ":x:" + clientID,
		"dx2:" + listenerID + ":" + generationID + ":l:" + validLegacy + "=",
		"dx2:" + listenerID + ":" + generationID + ":l:" + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", 257))),
		"dx2:" + listenerID + ":" + generationID + ":l:_w",
	}
	for _, value := range tests {
		if _, err := route.ParseDX2(value); err == nil {
			t.Errorf("ParseDX2(%q) unexpectedly succeeded", value)
		}
	}
}

func TestDTE1ParserAcceptsOnlyFingerprintAndCanonicalClientUUID(t *testing.T) {
	parsed, err := route.ParseDTE1("dte1:abcdef0123456789:" + clientID)
	if err != nil {
		t.Fatalf("ParseDTE1() error = %v", err)
	}
	if parsed.Fingerprint != "abcdef0123456789" || parsed.ClientID != clientID {
		t.Fatalf("ParseDTE1() = %#v", parsed)
	}
	for _, value := range []string{
		"dte1::" + clientID,
		"dte1:ABCDEF0123456789:" + clientID,
		"dte1:abcdef0123456789:" + strings.ToUpper("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		"dte1:abcdef0123456789:" + clientID + ":extra",
	} {
		if _, err := route.ParseDTE1(value); err == nil {
			t.Errorf("ParseDTE1(%q) unexpectedly succeeded", value)
		}
	}
}
