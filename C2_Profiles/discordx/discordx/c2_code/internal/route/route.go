// Package route implements the internal, non-Discord tracking routes used by
// the one-to-many Mythic Push C2 stream.
package route

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxLegacySenderBytes = 256

type Kind string

const (
	Fixed  Kind = "f"
	Legacy Kind = "l"
)

type DX2 struct {
	ListenerID   string
	GenerationID string
	Kind         Kind
	ClientID     string
	LegacySender string
}

func (value DX2) String() string {
	tail := value.ClientID
	if value.Kind == Legacy {
		tail = base64.RawURLEncoding.EncodeToString([]byte(value.LegacySender))
	}
	return strings.Join([]string{"dx2", value.ListenerID, value.GenerationID, string(value.Kind), tail}, ":")
}

func ParseDX2(value string) (DX2, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 5 || parts[0] != "dx2" {
		return DX2{}, errors.New("invalid dx2 tracking route")
	}
	if !IsCanonicalUUID(parts[1]) || !IsCanonicalUUID(parts[2]) {
		return DX2{}, errors.New("invalid dx2 listener or generation")
	}
	result := DX2{ListenerID: parts[1], GenerationID: parts[2], Kind: Kind(parts[3])}
	switch result.Kind {
	case Fixed:
		if !IsCanonicalUUID(parts[4]) {
			return DX2{}, errors.New("invalid dx2 fixed client route")
		}
		result.ClientID = parts[4]
	case Legacy:
		if parts[4] == "" || strings.Contains(parts[4], "=") {
			return DX2{}, errors.New("invalid dx2 legacy route")
		}
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(parts[4])
		if err != nil || len(decoded) == 0 || len(decoded) > maxLegacySenderBytes || !utf8.Valid(decoded) ||
			base64.RawURLEncoding.EncodeToString(decoded) != parts[4] {
			return DX2{}, errors.New("invalid dx2 legacy route")
		}
		result.LegacySender = string(decoded)
	default:
		return DX2{}, errors.New("invalid dx2 route kind")
	}
	return result, nil
}

type DTE1 struct {
	Fingerprint string
	ClientID    string
}

func ParseDTE1(value string) (DTE1, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 3 || parts[0] != "dte1" || !isLowerHex(parts[1], 16) || !IsCanonicalUUID(parts[2]) {
		return DTE1{}, errors.New("invalid dte1 tracking route")
	}
	return DTE1{Fingerprint: parts[1], ClientID: parts[2]}, nil
}

func IsCanonicalUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index := range value {
		character := value[index]
		switch index {
		case 8, 13, 18, 23:
			if character != '-' {
				return false
			}
		default:
			if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
				return false
			}
		}
	}
	return true
}

func RequireCanonicalUUID(value, field string) error {
	if !IsCanonicalUUID(value) {
		return fmt.Errorf("%s must be a canonical lowercase UUID", field)
	}
	return nil
}

func isLowerHex(value string, exactLength int) bool {
	if len(value) != exactLength {
		return false
	}
	for index := range value {
		character := value[index]
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}
