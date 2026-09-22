package envelope

import (
	"encoding/json"
	"unicode/utf8"
)

type LegacyRequest struct {
	Body       []byte
	TrackingID string
	Format     MessageFormat
}

type legacyWireMessage struct {
	Message       *string `json:"message"`
	SenderID      *string `json:"sender_id"`
	ToServer      bool    `json:"to_server"`
	ClientID      *string `json:"client_id,omitempty"`
	MessageFormat *string `json:"message_format,omitempty"`
}

func DecodeLegacyRequest(document string) (LegacyRequest, error) {
	if !utf8.ValidString(document) || len([]byte(document)) > MaximumDocumentUTF8Bytes {
		return LegacyRequest{}, ErrInvalid
	}
	var wire legacyWireMessage
	if err := json.Unmarshal([]byte(document), &wire); err != nil || wire.Message == nil || wire.SenderID == nil || !wire.ToServer || *wire.SenderID == "" {
		return LegacyRequest{}, ErrInvalid
	}
	format := Legacy
	if wire.MessageFormat != nil && *wire.MessageFormat != "" {
		if *wire.MessageFormat != string(RawV1) {
			return LegacyRequest{}, ErrInvalid
		}
		format = RawV1
	}
	return LegacyRequest{Body: []byte(*wire.Message), TrackingID: *wire.SenderID, Format: format}, nil
}

func EncodeLegacyResponse(message []byte, clientID, serverID string) (string, error) {
	body := string(message)
	wire := legacyWireMessage{Message: &body, SenderID: &serverID, ToServer: false, ClientID: &clientID}
	value, err := json.Marshal(wire)
	if err != nil {
		return "", ErrInvalid
	}
	return string(value), nil
}
