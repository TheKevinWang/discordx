package envelope

import "errors"

const (
	MaximumDocumentUTF8Bytes = 2_097_152
	MaximumEnvelopeBytes     = 524_288
	binaryHeaderLength       = 37
)

var (
	ErrInvalid = errors.New("transport envelope rejected")
	ErrEntropy = errors.New("transport entropy source failed")
)

type Direction uint8

const (
	AgentToServer Direction = iota + 1
	ServerToAgent
)

func (direction Direction) label() ([]byte, error) {
	switch direction {
	case AgentToServer:
		return []byte("agent-to-server"), nil
	case ServerToAgent:
		return []byte("server-to-agent"), nil
	default:
		return nil, ErrInvalid
	}
}

type MessageFormat string

const (
	Legacy MessageFormat = "legacy"
	RawV1  MessageFormat = "raw-v1"
)

type Message struct {
	Body     []byte
	SenderID string
	ToServer bool
	ClientID string
	Format   MessageFormat
	ID       *int
	Final    *bool
}

type Entropy interface {
	Bytes(count int) ([]byte, error)
}
