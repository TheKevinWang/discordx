package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

const syntheticCommandEcho = "echo"

type callbackPacket struct {
	Kind       string `json:"kind"`
	CallbackID string `json:"callback_id"`
	TaskID     string `json:"task_id,omitempty"`
	Command    string `json:"command,omitempty"`
	Argument   string `json:"argument,omitempty"`
	Output     string `json:"output,omitempty"`
}

type callbackPlan struct {
	CallbackID        string
	ListenerID        string
	GenerationID      string
	TaskID            string
	Argument          string
	PollDocument      string
	Protocol          *envelope.Protocol
	EffectiveInterval time.Duration
	PollDueAfter      time.Duration
}

func encodeCallbackPacket(clientID string, packet callbackPacket) ([]byte, error) {
	if !route.IsCanonicalUUID(clientID) || packet.CallbackID != clientID || validateCallbackPacket(packet) != nil {
		return nil, errors.New("synthetic callback packet is invalid")
	}
	encoded, err := json.Marshal(packet)
	if err != nil {
		return nil, errors.New("synthetic callback packet encoding failed")
	}
	body := make([]byte, 0, len(clientID)+len(encoded))
	body = append(body, clientID...)
	body = append(body, encoded...)
	return body, nil
}

func decodeCallbackPacket(body []byte) (string, callbackPacket, error) {
	if len(body) <= 36 {
		return "", callbackPacket{}, errors.New("synthetic callback packet is too short")
	}
	clientID := string(body[:36])
	if !route.IsCanonicalUUID(clientID) {
		return "", callbackPacket{}, errors.New("synthetic callback client ID is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(body[36:]))
	decoder.DisallowUnknownFields()
	var packet callbackPacket
	if err := decoder.Decode(&packet); err != nil {
		return "", callbackPacket{}, errors.New("synthetic callback packet JSON is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || packet.CallbackID != clientID || validateCallbackPacket(packet) != nil {
		return "", callbackPacket{}, errors.New("synthetic callback packet fields are invalid")
	}
	return clientID, packet, nil
}

func validateCallbackPacket(packet callbackPacket) error {
	if !route.IsCanonicalUUID(packet.CallbackID) {
		return errors.New("synthetic callback ID is invalid")
	}
	switch packet.Kind {
	case "poll":
		if packet.TaskID != "" || packet.Command != "" || packet.Argument != "" || packet.Output != "" {
			return errors.New("synthetic poll contains task fields")
		}
	case "task":
		if !validSyntheticTaskID(packet.TaskID) || packet.Command != syntheticCommandEcho || packet.Argument == "" || packet.Output != "" {
			return errors.New("synthetic task fields are invalid")
		}
	case "result":
		if !validSyntheticTaskID(packet.TaskID) || packet.Command != "" || packet.Argument != "" || packet.Output == "" {
			return errors.New("synthetic result fields are invalid")
		}
	default:
		return errors.New("synthetic callback packet kind is invalid")
	}
	return nil
}

func validSyntheticTaskID(value string) bool {
	return strings.HasPrefix(value, "task-") && len(value) > len("task-") && len(value) <= 64 && !strings.ContainsAny(value, " \t\r\n")
}

func executeSyntheticTask(task callbackPacket) (callbackPacket, error) {
	if task.Kind != "task" || validateCallbackPacket(task) != nil {
		return callbackPacket{}, errors.New("synthetic callback task is invalid")
	}
	if task.Command != syntheticCommandEcho {
		return callbackPacket{}, errors.New("synthetic callback command is not allow-listed")
	}
	return callbackPacket{
		Kind: "result", CallbackID: task.CallbackID, TaskID: task.TaskID, Output: task.Argument,
	}, nil
}

func makeCallbackPlans(entries []scaleEntry, count int, command string, pollInterval time.Duration, jitterPercent int) ([]callbackPlan, error) {
	if len(entries) == 0 || count < 1 || command != syntheticCommandEcho || pollInterval < time.Second ||
		pollInterval > 2*time.Minute || jitterPercent < 0 || jitterPercent > 50 {
		return nil, errors.New("synthetic callback plan configuration is invalid")
	}
	plans := make([]callbackPlan, 0, count)
	for index := 0; index < count; index++ {
		entryIndex := index % len(entries)
		clientID := syntheticUUID(index + 1 + 2*maximumLoadBots)
		protocol, err := envelope.NewProtocol(scaleWire(), nil)
		if err != nil {
			return nil, err
		}
		packet := callbackPacket{Kind: "poll", CallbackID: clientID}
		body, err := encodeCallbackPacket(clientID, packet)
		if err != nil {
			return nil, err
		}
		document, err := protocol.Encode(envelope.Message{
			Body: body, SenderID: clientID, ToServer: true, Format: envelope.RawV1,
		}, envelope.AgentToServer)
		if err != nil {
			return nil, errors.New("synthetic callback poll encoding failed")
		}
		effectiveInterval, pollDueAfter := callbackSchedule(clientID, pollInterval, jitterPercent)
		plan := callbackPlan{
			CallbackID: clientID, ListenerID: entries[entryIndex].ListenerID,
			GenerationID: entries[entryIndex].GenerationID,
			TaskID:       fmt.Sprintf("task-%06d", index+1), Argument: "callback:" + clientID,
			PollDocument: document, Protocol: protocol,
			EffectiveInterval: effectiveInterval, PollDueAfter: pollDueAfter,
		}
		entries[entryIndex].Identity.Callbacks = append(entries[entryIndex].Identity.Callbacks, plan)
		plans = append(plans, plan)
	}
	return plans, nil
}

func callbackSchedule(clientID string, pollInterval time.Duration, jitterPercent int) (time.Duration, time.Duration) {
	jitterRange := int64(pollInterval) * int64(jitterPercent) / 100
	adjustment := int64(0)
	if jitterRange > 0 {
		adjustment = int64(stableHash64("interval", clientID)%uint64(2*jitterRange+1)) - jitterRange
	}
	effective := pollInterval + time.Duration(adjustment)
	due := time.Duration(stableHash64("phase", clientID) % uint64(effective))
	return effective, due
}

func stableHash64(label, value string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(label))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(value))
	return hash.Sum64()
}
