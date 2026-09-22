package envelope

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

type jsonWireMessage struct {
	Message       *string `json:"message"`
	SenderID      *string `json:"sender_id"`
	ToServer      *bool   `json:"to_server"`
	ClientID      *string `json:"client_id,omitempty"`
	MessageFormat *string `json:"message_format,omitempty"`
	ID            *int    `json:"id,omitempty"`
	Final         *bool   `json:"final,omitempty"`
}

func serializeJSON(message Message, direction Direction) ([]byte, error) {
	if err := validateMessage(message, direction, true); err != nil || !utf8.Valid(message.Body) {
		return nil, ErrInvalid
	}
	body := string(message.Body)
	sender := message.SenderID
	toServer := message.ToServer
	wire := jsonWireMessage{Message: &body, SenderID: &sender, ToServer: &toServer, ID: message.ID, Final: message.Final}
	if message.ClientID != "" {
		client := message.ClientID
		wire.ClientID = &client
	}
	if message.Format == RawV1 {
		format := string(RawV1)
		wire.MessageFormat = &format
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(wire); err != nil {
		return nil, ErrInvalid
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func deserializeJSON(value []byte, direction Direction) (Message, error) {
	if !utf8.Valid(value) || rejectDuplicateObjectFields(value) != nil {
		return Message{}, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var wire jsonWireMessage
	if err := decoder.Decode(&wire); err != nil {
		return Message{}, ErrInvalid
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || wire.Message == nil || wire.SenderID == nil || wire.ToServer == nil {
		return Message{}, ErrInvalid
	}
	message := Message{Body: []byte(*wire.Message), SenderID: *wire.SenderID, ToServer: *wire.ToServer, ID: wire.ID, Final: wire.Final, Format: Legacy}
	if wire.ClientID != nil {
		message.ClientID = *wire.ClientID
	}
	if wire.MessageFormat != nil {
		if *wire.MessageFormat != string(RawV1) {
			return Message{}, ErrInvalid
		}
		message.Format = RawV1
	}
	if err := validateMessage(message, direction, true); err != nil {
		return Message{}, ErrInvalid
	}
	return message, nil
}

func serializeBinary(message Message, direction Direction, selected MessageFormat) ([]byte, error) {
	if message.Format != selected || validateMessage(message, direction, false) != nil {
		return nil, ErrInvalid
	}
	routeID := message.SenderID
	flags := byte(0)
	if direction == AgentToServer {
		flags |= 0x01
	} else {
		routeID = message.ClientID
	}
	if selected == RawV1 {
		flags |= 0x02
	}
	result := make([]byte, binaryHeaderLength+len(message.Body))
	result[0] = flags
	copy(result[1:binaryHeaderLength], routeID)
	copy(result[binaryHeaderLength:], message.Body)
	return result, nil
}

func deserializeBinary(value []byte, direction Direction, selected MessageFormat) (Message, error) {
	if len(value) <= binaryHeaderLength || value[0]&^byte(0x03) != 0 {
		return Message{}, ErrInvalid
	}
	toServer := value[0]&0x01 != 0
	if toServer != (direction == AgentToServer) {
		return Message{}, ErrInvalid
	}
	format := Legacy
	if value[0]&0x02 != 0 {
		format = RawV1
	}
	if format != selected {
		return Message{}, ErrInvalid
	}
	routeID := string(value[1:binaryHeaderLength])
	if !route.IsCanonicalUUID(routeID) {
		return Message{}, ErrInvalid
	}
	one := 1
	final := true
	message := Message{
		Body: append([]byte(nil), value[binaryHeaderLength:]...), SenderID: routeID,
		ToServer: toServer, Format: format, ID: &one, Final: &final,
	}
	if !toServer {
		message.ClientID = routeID
	}
	if validateMessage(message, direction, false) != nil {
		return Message{}, ErrInvalid
	}
	return message, nil
}

func validateMessage(message Message, direction Direction, requireSender bool) error {
	toServer := direction == AgentToServer
	if direction != AgentToServer && direction != ServerToAgent || message.ToServer != toServer {
		return ErrInvalid
	}
	if requireSender && !route.IsCanonicalUUID(message.SenderID) {
		return ErrInvalid
	}
	routeID := message.SenderID
	if toServer {
		if message.ClientID != "" || !route.IsCanonicalUUID(message.SenderID) {
			return ErrInvalid
		}
	} else {
		if !route.IsCanonicalUUID(message.ClientID) {
			return ErrInvalid
		}
		routeID = message.ClientID
	}
	if message.ID != nil && *message.ID != 1 || message.Final != nil && !*message.Final {
		return ErrInvalid
	}
	return validateBody(message.Body, routeID, message.Format)
}

func validateBody(body []byte, routeID string, format MessageFormat) error {
	if len(body) == 0 {
		return ErrInvalid
	}
	decoded := body
	switch format {
	case Legacy:
		if !utf8.Valid(body) {
			return ErrInvalid
		}
		var err error
		decoded, err = base64.StdEncoding.Strict().DecodeString(string(body))
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != string(body) {
			return ErrInvalid
		}
	case RawV1:
	default:
		return ErrInvalid
	}
	if len(decoded) < 36 || !bytes.Equal(decoded[:36], []byte(routeID)) {
		return ErrInvalid
	}
	return nil
}

func rejectDuplicateObjectFields(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return ErrInvalid
	}
	seen := make(map[string]struct{}, 7)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return ErrInvalid
		}
		name, ok := token.(string)
		if !ok {
			return ErrInvalid
		}
		if _, exists := seen[name]; exists {
			return ErrInvalid
		}
		seen[name] = struct{}{}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return ErrInvalid
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return ErrInvalid
	}
	return nil
}
