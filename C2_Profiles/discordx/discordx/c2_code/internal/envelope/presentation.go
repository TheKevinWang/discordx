package envelope

import (
	"encoding/base64"
	"strings"
	"unicode/utf8"
)

var emojiAlphabet = [...]string{
	"😁", "😂", "😅", "😳", "🥺", "🙄", "🤗", "😫",
	"🥰", "😏", "🤭", "😉", "😃", "😨", "😰", "🤑",
}

func present(name string, packet []byte) (string, error) {
	switch name {
	case "plain":
		if !utf8.Valid(packet) {
			return "", ErrInvalid
		}
		return string(packet), nil
	case "base64":
		return base64.StdEncoding.EncodeToString(packet), nil
	case "decimal":
		if len(packet) > MaximumDocumentUTF8Bytes/3 {
			return "", ErrInvalid
		}
		result := make([]byte, len(packet)*3)
		for index, current := range packet {
			result[index*3] = '0' + current/100
			result[index*3+1] = '0' + (current%100)/10
			result[index*3+2] = '0' + current%10
		}
		return string(result), nil
	case "emoji":
		if len(packet) > MaximumDocumentUTF8Bytes/8 {
			return "", ErrInvalid
		}
		var builder strings.Builder
		builder.Grow(len(packet) * 8)
		for _, current := range packet {
			builder.WriteString(emojiAlphabet[current>>4])
			builder.WriteString(emojiAlphabet[current&15])
		}
		return builder.String(), nil
	default:
		return "", ErrInvalid
	}
}

func unpresent(name, document string) ([]byte, error) {
	switch name {
	case "plain":
		if !utf8.ValidString(document) {
			return nil, ErrInvalid
		}
		return []byte(document), nil
	case "base64":
		decoded, err := base64.StdEncoding.Strict().DecodeString(document)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != document {
			return nil, ErrInvalid
		}
		return decoded, nil
	case "decimal":
		if len(document)%3 != 0 {
			return nil, ErrInvalid
		}
		result := make([]byte, len(document)/3)
		for index := 0; index < len(document); index += 3 {
			a, b, c := document[index], document[index+1], document[index+2]
			if a < '0' || a > '9' || b < '0' || b > '9' || c < '0' || c > '9' {
				return nil, ErrInvalid
			}
			value := int(a-'0')*100 + int(b-'0')*10 + int(c-'0')
			if value > 255 {
				return nil, ErrInvalid
			}
			result[index/3] = byte(value)
		}
		return result, nil
	case "emoji":
		nibbles := make([]byte, 0, len(document)/4)
		for len(document) > 0 {
			matched := false
			for index, token := range emojiAlphabet {
				if strings.HasPrefix(document, token) {
					nibbles = append(nibbles, byte(index))
					document = document[len(token):]
					matched = true
					break
				}
			}
			if !matched {
				return nil, ErrInvalid
			}
		}
		if len(nibbles)%2 != 0 {
			return nil, ErrInvalid
		}
		result := make([]byte, len(nibbles)/2)
		for index := 0; index < len(nibbles); index += 2 {
			result[index/2] = nibbles[index]<<4 | nibbles[index+1]
		}
		return result, nil
	default:
		return nil, ErrInvalid
	}
}
