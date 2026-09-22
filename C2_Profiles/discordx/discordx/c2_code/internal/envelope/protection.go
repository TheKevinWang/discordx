package envelope

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
)

type cryptoEntropy struct{}

func (cryptoEntropy) Bytes(count int) ([]byte, error) {
	value := make([]byte, count)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return nil, ErrEntropy
	}
	return value, nil
}

func derive(root []byte, label string) []byte {
	hash := sha256.New()
	_, _ = hash.Write(root)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(label))
	return hash.Sum(nil)
}

func directionKey(master []byte, keyMode string, direction Direction) ([]byte, error) {
	if len(master) != 32 {
		return nil, ErrInvalid
	}
	if keyMode == "single" {
		return append([]byte(nil), master...), nil
	}
	label, err := direction.label()
	if err != nil {
		return nil, err
	}
	return derive(master, string(label)), nil
}

func protect(name, keyMode string, master, plain []byte, direction Direction, entropy Entropy) ([]byte, error) {
	if name == "none" {
		if direction != AgentToServer && direction != ServerToAgent {
			return nil, ErrInvalid
		}
		return append([]byte(nil), plain...), nil
	}
	root, err := directionKey(master, keyMode, direction)
	if err != nil {
		return nil, ErrInvalid
	}
	switch name {
	case "xor-obfuscation-v1":
		salt, err := entropy.Bytes(8)
		if err != nil || len(salt) != 8 {
			return nil, ErrEntropy
		}
		return append(append([]byte(nil), salt...), xorBody(plain, root, salt)...), nil
	case "chacha20-v1":
		nonce, err := entropy.Bytes(12)
		if err != nil || len(nonce) != 12 {
			return nil, ErrEntropy
		}
		ciphertext, err := chacha20XOR(plain, root, nonce, 1)
		if err != nil {
			return nil, err
		}
		return append(append([]byte(nil), nonce...), ciphertext...), nil
	case "aes256-hmac-v1":
		iv, err := entropy.Bytes(aes.BlockSize)
		if err != nil || len(iv) != aes.BlockSize {
			return nil, ErrEntropy
		}
		encKey := derive(root, "aes256-hmac-v1/encryption")
		macKey := derive(root, "aes256-hmac-v1/authentication")
		padded := pkcs7Pad(plain)
		block, err := aes.NewCipher(encKey)
		if err != nil {
			return nil, ErrInvalid
		}
		ciphertext := make([]byte, len(padded))
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
		label, _ := direction.label()
		mac := hmac.New(sha256.New, macKey)
		_, _ = mac.Write(label)
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write(iv)
		_, _ = mac.Write(ciphertext)
		packet := append(append(append([]byte(nil), iv...), ciphertext...), mac.Sum(nil)...)
		return packet, nil
	default:
		return nil, ErrInvalid
	}
}

func unprotect(name, keyMode string, master, packet []byte, direction Direction) ([]byte, error) {
	if name == "none" {
		if direction != AgentToServer && direction != ServerToAgent {
			return nil, ErrInvalid
		}
		return append([]byte(nil), packet...), nil
	}
	root, err := directionKey(master, keyMode, direction)
	if err != nil {
		return nil, ErrInvalid
	}
	switch name {
	case "xor-obfuscation-v1":
		if len(packet) < 8 {
			return nil, ErrInvalid
		}
		return xorBody(packet[8:], root, packet[:8]), nil
	case "chacha20-v1":
		if len(packet) < 12 {
			return nil, ErrInvalid
		}
		return chacha20XOR(packet[12:], root, packet[:12], 1)
	case "aes256-hmac-v1":
		if len(packet) < aes.BlockSize+aes.BlockSize+sha256.Size || (len(packet)-aes.BlockSize-sha256.Size)%aes.BlockSize != 0 {
			return nil, ErrInvalid
		}
		iv := packet[:aes.BlockSize]
		ciphertext := packet[aes.BlockSize : len(packet)-sha256.Size]
		tag := packet[len(packet)-sha256.Size:]
		encKey := derive(root, "aes256-hmac-v1/encryption")
		macKey := derive(root, "aes256-hmac-v1/authentication")
		label, _ := direction.label()
		mac := hmac.New(sha256.New, macKey)
		_, _ = mac.Write(label)
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write(iv)
		_, _ = mac.Write(ciphertext)
		if subtle.ConstantTimeCompare(tag, mac.Sum(nil)) != 1 {
			return nil, ErrInvalid
		}
		block, err := aes.NewCipher(encKey)
		if err != nil {
			return nil, ErrInvalid
		}
		plain := make([]byte, len(ciphertext))
		cipher.NewCBCDecrypter(block, iv).CryptBlocks(plain, ciphertext)
		return pkcs7Unpad(plain)
	default:
		return nil, ErrInvalid
	}
}

func protectionOverhead(name string) int {
	switch name {
	case "none":
		return 0
	case "xor-obfuscation-v1":
		return 8
	case "chacha20-v1":
		return 12
	case "aes256-hmac-v1":
		return 64
	default:
		return -1
	}
}

func xorBody(value, key, salt []byte) []byte {
	result := make([]byte, len(value))
	for index, current := range value {
		saltByte := salt[index%len(salt)]
		result[index] = current ^ key[(index+int(saltByte))%len(key)] ^ saltByte
	}
	return result
}

func pkcs7Pad(value []byte) []byte {
	padding := aes.BlockSize - len(value)%aes.BlockSize
	result := make([]byte, len(value)+padding)
	copy(result, value)
	for index := len(value); index < len(result); index++ {
		result[index] = byte(padding)
	}
	return result
}

func pkcs7Unpad(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value)%aes.BlockSize != 0 {
		return nil, ErrInvalid
	}
	padding := int(value[len(value)-1])
	if padding == 0 || padding > aes.BlockSize || padding > len(value) {
		return nil, ErrInvalid
	}
	var invalid byte
	for _, current := range value[len(value)-padding:] {
		invalid |= current ^ byte(padding)
	}
	if invalid != 0 {
		return nil, ErrInvalid
	}
	return append([]byte(nil), value[:len(value)-padding]...), nil
}

func chacha20XOR(value, key, nonce []byte, counter uint32) ([]byte, error) {
	if len(key) != 32 || len(nonce) != 12 {
		return nil, ErrInvalid
	}
	blocks := (uint64(len(value)) + 63) / 64
	if blocks > uint64(math.MaxUint32-counter)+1 {
		return nil, ErrInvalid
	}
	result := make([]byte, len(value))
	for offset := 0; offset < len(value); offset += 64 {
		keystream := chacha20Block(key, nonce, counter)
		count := len(value) - offset
		if count > 64 {
			count = 64
		}
		for index := 0; index < count; index++ {
			result[offset+index] = value[offset+index] ^ keystream[index]
		}
		counter++
	}
	return result, nil
}

func chacha20Block(key, nonce []byte, counter uint32) [64]byte {
	state := [16]uint32{0x61707865, 0x3320646e, 0x79622d32, 0x6b206574}
	for index := 0; index < 8; index++ {
		state[4+index] = binary.LittleEndian.Uint32(key[index*4:])
	}
	state[12] = counter
	state[13] = binary.LittleEndian.Uint32(nonce[0:4])
	state[14] = binary.LittleEndian.Uint32(nonce[4:8])
	state[15] = binary.LittleEndian.Uint32(nonce[8:12])
	working := state
	for round := 0; round < 10; round++ {
		quarterRound(&working, 0, 4, 8, 12)
		quarterRound(&working, 1, 5, 9, 13)
		quarterRound(&working, 2, 6, 10, 14)
		quarterRound(&working, 3, 7, 11, 15)
		quarterRound(&working, 0, 5, 10, 15)
		quarterRound(&working, 1, 6, 11, 12)
		quarterRound(&working, 2, 7, 8, 13)
		quarterRound(&working, 3, 4, 9, 14)
	}
	var output [64]byte
	for index := range working {
		binary.LittleEndian.PutUint32(output[index*4:], working[index]+state[index])
	}
	return output
}

func quarterRound(state *[16]uint32, a, b, c, d int) {
	state[a] += state[b]
	state[d] = bits.RotateLeft32(state[d]^state[a], 16)
	state[c] += state[d]
	state[b] = bits.RotateLeft32(state[b]^state[c], 12)
	state[a] += state[b]
	state[d] = bits.RotateLeft32(state[d]^state[a], 8)
	state[c] += state[d]
	state[b] = bits.RotateLeft32(state[b]^state[c], 7)
}
