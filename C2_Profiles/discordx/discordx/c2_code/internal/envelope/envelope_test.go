package envelope

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
)

const (
	requestRoute  = "00000000-0000-0000-0000-000000000000"
	responseRoute = "11111111-1111-1111-1111-111111111111"
	testKey       = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
)

type fixedEntropy struct{ value []byte }

func (entropy fixedEntropy) Bytes(count int) ([]byte, error) {
	if len(entropy.value) != count {
		return nil, errors.New("unexpected entropy request")
	}
	return append([]byte(nil), entropy.value...), nil
}

func intPointer(value int) *int    { return &value }
func boolPointer(value bool) *bool { return &value }

func request(body []byte, format MessageFormat) Message {
	return Message{
		Body: body, SenderID: requestRoute, ToServer: true, Format: format,
		ID: intPointer(1), Final: boolPointer(true),
	}
}

func response(body []byte, format MessageFormat) Message {
	return Message{
		Body: body, SenderID: requestRoute, ToServer: false, ClientID: responseRoute,
		Format: format, ID: intPointer(1), Final: boolPointer(true),
	}
}

func TestFrozenCSharpFixtureHashAndFormatBytesDoNotDrift(t *testing.T) {
	fixturePath := "../../tests/discordx.Tests/Fixtures/discord-dual-envelope-v1-vectors.json"
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(fixtureBytes)
	if got := hex.EncodeToString(hash[:]); got != "65bfef9b021207489b06f56f427ca1b65403dac25b429fce2c7e98021c698206" {
		t.Fatalf("frozen .NET fixture changed: %s", got)
	}
	var fixture struct {
		Vectors []struct {
			Name           string `json:"name"`
			Format         string `json:"format"`
			Direction      string `json:"direction"`
			UseBase64      bool   `json:"use_base64"`
			BodyBase64     string `json:"body_base64"`
			EnvelopeBase64 string `json:"envelope_base64"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, vector := range fixture.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			body, err := base64.StdEncoding.DecodeString(vector.BodyBase64)
			if err != nil {
				t.Fatal(err)
			}
			want, err := base64.StdEncoding.DecodeString(vector.EnvelopeBase64)
			if err != nil {
				t.Fatal(err)
			}
			direction := AgentToServer
			message := request(body, RawV1)
			if vector.UseBase64 {
				message.Format = Legacy
			}
			if vector.Direction == "server-to-agent" {
				direction = ServerToAgent
				message = response(body, message.Format)
			}
			var got []byte
			switch vector.Format {
			case "json-v1":
				got, err = serializeJSON(message, direction)
			case "binary-v1":
				got, err = serializeBinary(message, direction, message.Format)
			default:
				t.Fatalf("unknown fixture format %q", vector.Format)
			}
			if err != nil {
				t.Fatalf("serialize error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("serialized bytes drifted\n got: %s\nwant: %s", base64.StdEncoding.EncodeToString(got), vector.EnvelopeBase64)
			}
			var decoded Message
			if vector.Format == "json-v1" {
				decoded, err = deserializeJSON(want, direction)
			} else {
				decoded, err = deserializeBinary(want, direction, message.Format)
			}
			if err != nil || !bytes.Equal(decoded.Body, body) {
				t.Fatalf("deserialize = %#v, %v", decoded, err)
			}
		})
	}
}

func TestProtocolRoundTripsAllPresentationsProtectionsAndKeyModes(t *testing.T) {
	for _, format := range []string{"json-v1", "binary-v1"} {
		for _, presentation := range []string{"base64", "decimal", "emoji"} {
			for _, protection := range []string{"none", "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1"} {
				for _, keyMode := range []string{"single", "directional"} {
					name := strings.Join([]string{format, presentation, protection, keyMode}, "/")
					t.Run(name, func(t *testing.T) {
						key := testKey
						if protection == "none" {
							key = ""
						}
						entropySize := map[string]int{"none": 0, "xor-obfuscation-v1": 8, "chacha20-v1": 12, "aes256-hmac-v1": 16}[protection]
						protocol, err := NewProtocol(config.Wire{
							Protocol: "fixed", EnvelopeFormat: format, Presentation: presentation,
							Protection: protection, KeyMode: keyMode, Key: key,
						}, fixedEntropy{value: bytes.Repeat([]byte{0x42}, entropySize)})
						if err != nil {
							t.Fatal(err)
						}
						body := append([]byte(requestRoute), []byte("{\"action\":\"checkin\"}")...)
						document, err := protocol.Encode(request(body, RawV1), AgentToServer)
						if err != nil {
							t.Fatal(err)
						}
						decoded, err := protocol.Decode(document, AgentToServer)
						if err != nil || !bytes.Equal(decoded.Body, body) || decoded.SenderID != requestRoute {
							t.Fatalf("round trip = %#v, %v", decoded, err)
						}
					})
				}
			}
		}
	}
}

func TestPlainIsLimitedToUnprotectedJSONAndRejectsFallback(t *testing.T) {
	valid, err := NewProtocol(config.Wire{
		Protocol: "fixed", EnvelopeFormat: "json-v1", Presentation: "plain",
		Protection: "none", KeyMode: "single",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(requestRoute + "{\"snow\":\"☃\"}")
	document, err := valid.Encode(request(body, RawV1), AgentToServer)
	if err != nil || !strings.HasPrefix(document, "{\"message\":") || strings.Contains(document, "envelope_version") {
		t.Fatalf("plain JSON document = %q, %v", document, err)
	}
	if _, err := valid.Decode(document, ServerToAgent); err == nil {
		t.Fatal("wrong direction was accepted")
	}
	for _, wire := range []config.Wire{
		{Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "plain", Protection: "none", KeyMode: "single"},
		{Protocol: "fixed", EnvelopeFormat: "json-v1", Presentation: "plain", Protection: "chacha20-v1", KeyMode: "single", Key: testKey},
	} {
		if _, err := NewProtocol(wire, nil); err == nil {
			t.Fatalf("invalid plain composition accepted: %#v", wire)
		}
	}
}

func TestBinaryBase64XORMatchesCurrentCSharpVector(t *testing.T) {
	inner, _ := hex.DecodeString("090106616374696f6e060b6765745f7461736b696e67")
	body := append([]byte(requestRoute), inner...)
	protocol, err := NewProtocol(config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "xor-obfuscation-v1", KeyMode: "single", Key: testKey,
	}, fixedEntropy{value: bytes.Repeat([]byte{0xa5}, 8)})
	if err != nil {
		t.Fatal(err)
	}
	document, err := protocol.Encode(request(body, RawV1), AgentToServer)
	if err != nil {
		t.Fatal(err)
	}
	want := "paWlpaWlpaWjk5KdnJ+emZiGmoWEh5uBgIOCkIyPjomVi4qVlJeWkZCTkp2cn56ZmJuahYSahoGAg5+NjI+OlIiLipWJl5aRkJOSnZyfnpmYoquz1dTC2N/dtLbb2srmzNrJzs3JwQ=="
	if document != want {
		t.Fatalf("XOR document drifted\n got: %s\nwant: %s", document, want)
	}
}

func TestStrictJSONAndBinaryValidationRejectsAmbiguity(t *testing.T) {
	jsonProtocol, err := NewProtocol(config.Wire{
		Protocol: "fixed", EnvelopeFormat: "json-v1", Presentation: "plain",
		Protection: "none", KeyMode: "single",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := requestRoute + "{}"
	for _, invalid := range []string{
		`{"message":"` + body + `","message":"` + body + `","sender_id":"` + requestRoute + `","to_server":true}`,
		`{"message":"` + body + `","sender_id":"` + requestRoute + `","to_server":true,"unknown":1}`,
		`{"message":"` + body + `","sender_id":"` + requestRoute + `","to_server":true,"envelope_version":1}`,
	} {
		if _, err := jsonProtocol.Decode(invalid, AgentToServer); err == nil {
			t.Fatalf("invalid JSON envelope accepted: %s", invalid)
		}
	}

	binaryProtocol, err := NewProtocol(config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "none", KeyMode: "single",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := binaryProtocol.Encode(request([]byte(body), RawV1), AgentToServer)
	if err != nil {
		t.Fatal(err)
	}
	packet, _ := base64.StdEncoding.DecodeString(valid)
	packet[0] |= 0x80
	if _, err := binaryProtocol.Decode(base64.StdEncoding.EncodeToString(packet), AgentToServer); err == nil {
		t.Fatal("reserved binary flag was accepted")
	}
	if _, err := binaryProtocol.Decode(base64.StdEncoding.EncodeToString(make([]byte, 37)), AgentToServer); err == nil {
		t.Fatal("short binary frame was accepted")
	}
}

func TestAESAuthenticatesDirectionAndTampering(t *testing.T) {
	protocol, err := NewProtocol(config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "aes256-hmac-v1", KeyMode: "directional", Key: testKey,
	}, fixedEntropy{value: bytes.Repeat([]byte{0x24}, 16)})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(requestRoute + "payload")
	document, err := protocol.Encode(request(body, RawV1), AgentToServer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.Decode(document, ServerToAgent); err == nil {
		t.Fatal("AES packet accepted in the wrong direction")
	}
	packet, _ := base64.StdEncoding.DecodeString(document)
	packet[len(packet)/2] ^= 1
	if _, err := protocol.Decode(base64.StdEncoding.EncodeToString(packet), AgentToServer); err == nil {
		t.Fatal("tampered AES packet was accepted")
	}
}

func TestChaCha20MatchesRFC8439BlockVector(t *testing.T) {
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(index)
	}
	nonce, _ := hex.DecodeString("000000090000004a00000000")
	want, _ := hex.DecodeString("10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4ed2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e")
	got, err := chacha20XOR(make([]byte, 64), key, nonce, 1)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ChaCha20 block = %x, %v", got, err)
	}
}

func TestPresentationsAreCanonicalAndBoundariesAreStrict(t *testing.T) {
	source := []byte{0x00, 0x7b, 0xff}
	for _, name := range []string{"base64", "decimal", "emoji"} {
		encoded, err := present(name, source)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := unpresent(name, encoded)
		if err != nil || !bytes.Equal(decoded, source) {
			t.Fatalf("%s round trip = %x, %v", name, decoded, err)
		}
	}
	for _, invalid := range []struct{ name, value string }{
		{"base64", "e30"}, {"base64", "e30=\n"}, {"decimal", "256"},
		{"decimal", "12"}, {"emoji", "😁"}, {"emoji", "💩😁"},
	} {
		if _, err := unpresent(invalid.name, invalid.value); err == nil {
			t.Fatalf("%s accepted %q", invalid.name, invalid.value)
		}
	}
}

func TestLegacyWireCompatibility(t *testing.T) {
	document := `{"message":"payload","sender_id":"legacy-route","to_server":true,"message_format":"raw-v1","ignored":"compat"}`
	decoded, err := DecodeLegacyRequest(document)
	if err != nil || string(decoded.Body) != "payload" || decoded.TrackingID != "legacy-route" || decoded.Format != RawV1 {
		t.Fatalf("DecodeLegacyRequest() = %#v, %v", decoded, err)
	}
	encoded, err := EncodeLegacyResponse([]byte("hello"), "client", "server")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"message":"hello","sender_id":"server","to_server":false,"client_id":"client"}`
	if encoded != want {
		t.Fatalf("EncodeLegacyResponse() = %s, want %s", encoded, want)
	}
}
