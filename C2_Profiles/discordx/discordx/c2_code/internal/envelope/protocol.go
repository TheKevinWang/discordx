package envelope

import (
	"encoding/base64"
	"unicode"
	"unicode/utf8"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
)

type Protocol struct {
	wire      config.Wire
	masterKey []byte
	entropy   Entropy
}

func NewProtocol(wire config.Wire, entropy Entropy) (*Protocol, error) {
	normalized, err := wire.Normalize()
	if err != nil || normalized.Protocol != "fixed" {
		return nil, ErrInvalid
	}
	if entropy == nil {
		entropy = cryptoEntropy{}
	}
	var key []byte
	if normalized.Protection != "none" {
		key, err = base64.StdEncoding.Strict().DecodeString(normalized.Key)
		if err != nil || len(key) != 32 {
			return nil, ErrInvalid
		}
	}
	return &Protocol{wire: normalized, masterKey: key, entropy: entropy}, nil
}

func (protocol *Protocol) Encode(message Message, direction Direction) (string, error) {
	var envelope []byte
	var err error
	switch protocol.wire.EnvelopeFormat {
	case "json-v1":
		envelope, err = serializeJSON(message, direction)
	case "binary-v1":
		selected := RawV1
		if protocol.wire.UseBase64 {
			selected = Legacy
		}
		envelope, err = serializeBinary(message, direction, selected)
	default:
		err = ErrInvalid
	}
	if err != nil || len(envelope) > MaximumEnvelopeBytes {
		return "", ErrInvalid
	}
	packet, err := protect(protocol.wire.Protection, protocol.wire.KeyMode, protocol.masterKey, envelope, direction, protocol.entropy)
	if err != nil || len(packet) > MaximumEnvelopeBytes+protectionOverhead(protocol.wire.Protection) {
		return "", ErrInvalid
	}
	document, err := present(protocol.wire.Presentation, packet)
	if err != nil || validateDocument(document) != nil {
		return "", ErrInvalid
	}
	return document, nil
}

func (protocol *Protocol) Decode(document string, direction Direction) (Message, error) {
	if validateDocument(document) != nil {
		return Message{}, ErrInvalid
	}
	packet, err := unpresent(protocol.wire.Presentation, document)
	if err != nil || len(packet) > MaximumEnvelopeBytes+protectionOverhead(protocol.wire.Protection) {
		return Message{}, ErrInvalid
	}
	envelope, err := unprotect(protocol.wire.Protection, protocol.wire.KeyMode, protocol.masterKey, packet, direction)
	if err != nil || len(envelope) > MaximumEnvelopeBytes {
		return Message{}, ErrInvalid
	}
	switch protocol.wire.EnvelopeFormat {
	case "json-v1":
		return deserializeJSON(envelope, direction)
	case "binary-v1":
		selected := RawV1
		if protocol.wire.UseBase64 {
			selected = Legacy
		}
		return deserializeBinary(envelope, direction, selected)
	default:
		return Message{}, ErrInvalid
	}
}

func validateDocument(document string) error {
	if !utf8.ValidString(document) || len([]byte(document)) > MaximumDocumentUTF8Bytes {
		return ErrInvalid
	}
	if document == "" {
		return nil
	}
	first, _ := utf8.DecodeRuneInString(document)
	last, _ := utf8.DecodeLastRuneInString(document)
	if unicode.IsSpace(first) || unicode.IsSpace(last) {
		return ErrInvalid
	}
	for _, character := range document {
		if character == 0 || unicode.IsControl(character) {
			return ErrInvalid
		}
	}
	return nil
}
