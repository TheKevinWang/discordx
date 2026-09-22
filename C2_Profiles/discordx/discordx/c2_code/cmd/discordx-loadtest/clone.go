package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/envelope"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type syntheticIdentity struct {
	Token     string
	BotID     string
	ChannelID string
	Wire      config.Wire
	Callbacks []callbackPlan
}

type cloneCounters struct {
	CredentialRequests int64 `json:"credential_requests"`
	HistoryRequests    int64 `json:"history_requests"`
	MessagesPosted     int64 `json:"messages_posted"`
	MessagesDeleted    int64 `json:"messages_deleted"`
	GatewayConnections int64 `json:"gateway_connections"`
	GatewayMessages    int64 `json:"gateway_messages"`
	PollsEmitted       int64 `json:"polls_emitted"`
	FirstPollOffsetMS  int64 `json:"first_poll_offset_ms"`
	LastPollOffsetMS   int64 `json:"last_poll_offset_ms"`
	CommandsExecuted   int64 `json:"commands_executed"`
	ActiveGateways     int64 `json:"active_gateways"`
	RouteErrors        int64 `json:"route_errors"`
}

type gatewaySession struct {
	connection *websocket.Conn
	identity   syntheticIdentity

	writeMu  sync.Mutex
	sequence int64
}

type discordClone struct {
	ctx        context.Context
	cancel     context.CancelFunc
	server     *httptest.Server
	identities map[string]syntheticIdentity
	channels   map[string]string
	callbacks  map[string]callbackPlan

	credentialRequests atomic.Int64
	historyRequests    atomic.Int64
	messagesPosted     atomic.Int64
	messagesDeleted    atomic.Int64
	gatewayConnections atomic.Int64
	gatewayMessages    atomic.Int64
	commandsExecuted   atomic.Int64
	activeGateways     atomic.Int64
	routeErrors        atomic.Int64
	messageSequence    atomic.Int64
	pollsEmitted       atomic.Int64
	pollReleaseUnixNS  atomic.Int64
	firstPollUnixNS    atomic.Int64
	lastPollUnixNS     atomic.Int64

	mu            sync.Mutex
	pollRelease   chan struct{}
	connections   map[*websocket.Conn]struct{}
	sessions      map[string]*gatewaySession
	pendingDelete map[string]string
	executed      map[string]struct{}
}

func newDiscordClone(parent context.Context, identities []syntheticIdentity) (*discordClone, error) {
	if parent == nil || len(identities) == 0 {
		return nil, errors.New("Discord clone configuration is invalid")
	}
	ctx, cancel := context.WithCancel(parent)
	clone := &discordClone{
		ctx: ctx, cancel: cancel,
		identities: make(map[string]syntheticIdentity, len(identities)),
		channels:   make(map[string]string, len(identities)), callbacks: make(map[string]callbackPlan),
		connections: make(map[*websocket.Conn]struct{}), sessions: make(map[string]*gatewaySession),
		pendingDelete: make(map[string]string), executed: make(map[string]struct{}),
		pollRelease: make(chan struct{}),
	}
	for _, identity := range identities {
		if identity.Token == "" || identity.BotID == "" || identity.ChannelID == "" {
			cancel()
			return nil, errors.New("Discord clone identity is invalid")
		}
		if _, duplicate := clone.identities[identity.Token]; duplicate {
			cancel()
			return nil, errors.New("Discord clone contains a duplicate token")
		}
		if _, duplicate := clone.channels[identity.ChannelID]; duplicate {
			cancel()
			return nil, errors.New("Discord clone contains a duplicate channel")
		}
		for _, callback := range identity.Callbacks {
			if callback.Protocol == nil || callback.PollDocument == "" || callback.ListenerID == "" || callback.GenerationID == "" {
				cancel()
				return nil, errors.New("Discord clone callback plan is invalid")
			}
			if _, duplicate := clone.callbacks[callback.CallbackID]; duplicate {
				cancel()
				return nil, errors.New("Discord clone contains a duplicate callback")
			}
			clone.callbacks[callback.CallbackID] = callback
		}
		sort.Slice(identity.Callbacks, func(left, right int) bool {
			return identity.Callbacks[left].PollDueAfter < identity.Callbacks[right].PollDueAfter
		})
		clone.identities[identity.Token] = identity
		clone.channels[identity.ChannelID] = identity.Token
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		cancel()
		return nil, errors.New("Discord clone loopback listener failed")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(clone.serveHTTP))
	server.Listener = listener
	server.Start()
	clone.server = server
	return clone, nil
}

func (clone *discordClone) APIOrigin() string {
	return clone.server.URL + "/api"
}

func (clone *discordClone) GatewayOrigin() string {
	return "ws" + strings.TrimPrefix(clone.server.URL, "http")
}

func (clone *discordClone) CDNOrigin() string {
	return clone.server.URL
}

func (clone *discordClone) Counters() cloneCounters {
	releaseAt := clone.pollReleaseUnixNS.Load()
	firstOffset := pollOffsetMilliseconds(releaseAt, clone.firstPollUnixNS.Load())
	lastOffset := pollOffsetMilliseconds(releaseAt, clone.lastPollUnixNS.Load())
	return cloneCounters{
		CredentialRequests: clone.credentialRequests.Load(),
		HistoryRequests:    clone.historyRequests.Load(), MessagesPosted: clone.messagesPosted.Load(),
		MessagesDeleted: clone.messagesDeleted.Load(), GatewayConnections: clone.gatewayConnections.Load(),
		GatewayMessages: clone.gatewayMessages.Load(), PollsEmitted: clone.pollsEmitted.Load(),
		FirstPollOffsetMS: firstOffset, LastPollOffsetMS: lastOffset,
		CommandsExecuted: clone.commandsExecuted.Load(),
		ActiveGateways:   clone.activeGateways.Load(), RouteErrors: clone.routeErrors.Load(),
	}
}

func (clone *discordClone) ReleaseCallbacks() error {
	if clone == nil || clone.pollRelease == nil {
		return errors.New("callback release barrier is unavailable")
	}
	now := time.Now().UnixNano()
	if !clone.pollReleaseUnixNS.CompareAndSwap(0, now) {
		return errors.New("callback release barrier is already open")
	}
	close(clone.pollRelease)
	return nil
}

func (clone *discordClone) Close() {
	if clone == nil {
		return
	}
	clone.cancel()
	clone.mu.Lock()
	connections := make([]*websocket.Conn, 0, len(clone.connections))
	for connection := range clone.connections {
		connections = append(connections, connection)
	}
	clone.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close(websocket.StatusNormalClosure, "load test complete")
	}
	clone.server.Close()
}

func (clone *discordClone) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/" {
		clone.serveGateway(response, request)
		return
	}
	identity, ok := clone.authorize(request)
	if !ok {
		clone.failRoute(response, http.StatusUnauthorized, "unauthorized")
		return
	}
	response.Header().Set("Content-Type", "application/json")
	prefix := "/api/v10/channels/"
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/api/v10/users/@me":
		clone.credentialRequests.Add(1)
		_ = json.NewEncoder(response).Encode(map[string]string{"id": identity.BotID, "username": "load-bot"})
	case strings.HasPrefix(request.URL.Path, prefix):
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, prefix), "/")
		if len(parts) < 2 || parts[0] != identity.ChannelID || parts[1] != "messages" {
			clone.failRoute(response, http.StatusNotFound, "unknown channel route")
			return
		}
		switch {
		case request.Method == http.MethodGet && len(parts) == 2:
			clone.historyRequests.Add(1)
			_, _ = response.Write([]byte("[]"))
		case request.Method == http.MethodPost && len(parts) == 2:
			clone.receiveTask(response, request, identity)
		case request.Method == http.MethodDelete && len(parts) == 3:
			clone.deleteMessage(response, identity.ChannelID, parts[2])
		default:
			clone.failRoute(response, http.StatusNotFound, "unknown message route")
		}
	default:
		clone.failRoute(response, http.StatusNotFound, "unknown route")
	}
}

func (clone *discordClone) receiveTask(response http.ResponseWriter, request *http.Request, identity syntheticIdentity) {
	var payload struct {
		Content string `json:"content"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 2_097_152))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || payload.Content == "" {
		clone.failRoute(response, http.StatusBadRequest, "invalid outbound document")
		return
	}
	protocol, err := envelope.NewProtocol(identity.Wire, nil)
	if err != nil {
		clone.failRoute(response, http.StatusBadRequest, "invalid outbound protocol")
		return
	}
	decoded, err := protocol.Decode(payload.Content, envelope.ServerToAgent)
	if err != nil {
		clone.failRoute(response, http.StatusBadRequest, "invalid outbound envelope")
		return
	}
	clientID, task, err := decodeCallbackPacket(decoded.Body)
	plan, found := clone.callbacks[clientID]
	if err != nil || !found || decoded.ClientID != clientID || task.Kind != "task" ||
		task.TaskID != plan.TaskID || task.Argument != plan.Argument {
		clone.failRoute(response, http.StatusBadRequest, "invalid callback task")
		return
	}
	result, err := executeSyntheticTask(task)
	if err != nil {
		clone.failRoute(response, http.StatusBadRequest, "callback command failed")
		return
	}
	clone.mu.Lock()
	if _, duplicate := clone.executed[clientID]; duplicate {
		clone.mu.Unlock()
		clone.failRoute(response, http.StatusConflict, "callback command was duplicated")
		return
	}
	clone.executed[clientID] = struct{}{}
	session := clone.sessions[identity.Token]
	clone.mu.Unlock()
	if session == nil {
		clone.failRoute(response, http.StatusServiceUnavailable, "callback Gateway is unavailable")
		return
	}
	body, err := encodeCallbackPacket(clientID, result)
	if err != nil {
		clone.failRoute(response, http.StatusInternalServerError, "callback result encoding failed")
		return
	}
	document, err := plan.Protocol.Encode(envelope.Message{
		Body: body, SenderID: clientID, ToServer: true, Format: envelope.RawV1,
	}, envelope.AgentToServer)
	if err != nil || session.sendAgentMessage(clone, document, false) != nil {
		clone.failRoute(response, http.StatusServiceUnavailable, "callback result delivery failed")
		return
	}
	clone.commandsExecuted.Add(1)
	clone.messagesPosted.Add(1)
	messageID := clone.nextMessageID()
	_ = json.NewEncoder(response).Encode(map[string]any{
		"id": messageID, "channel_id": identity.ChannelID,
		"content": payload.Content, "attachments": []any{},
	})
}

func (clone *discordClone) deleteMessage(response http.ResponseWriter, channelID, messageID string) {
	clone.mu.Lock()
	registeredChannel, ok := clone.pendingDelete[messageID]
	if ok && registeredChannel == channelID {
		delete(clone.pendingDelete, messageID)
	}
	clone.mu.Unlock()
	if !ok || registeredChannel != channelID {
		clone.failRoute(response, http.StatusNotFound, "unknown ingress message")
		return
	}
	clone.messagesDeleted.Add(1)
	response.WriteHeader(http.StatusNoContent)
}

func (clone *discordClone) failRoute(response http.ResponseWriter, status int, message string) {
	clone.routeErrors.Add(1)
	http.Error(response, message, status)
}

func (clone *discordClone) authorize(request *http.Request) (syntheticIdentity, bool) {
	value := request.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bot ") {
		return syntheticIdentity{}, false
	}
	identity, ok := clone.identities[strings.TrimPrefix(value, "Bot ")]
	return identity, ok
}

func (clone *discordClone) serveGateway(response http.ResponseWriter, request *http.Request) {
	connection, err := websocket.Accept(response, request, nil)
	if err != nil {
		clone.routeErrors.Add(1)
		return
	}
	clone.mu.Lock()
	clone.connections[connection] = struct{}{}
	clone.mu.Unlock()
	active := false
	var token string
	defer func() {
		clone.mu.Lock()
		delete(clone.connections, connection)
		if token != "" {
			if current := clone.sessions[token]; current != nil && current.connection == connection {
				delete(clone.sessions, token)
			}
		}
		clone.mu.Unlock()
		if active {
			clone.activeGateways.Add(-1)
		}
		_ = connection.Close(websocket.StatusNormalClosure, "done")
	}()

	if err := wsjson.Write(clone.ctx, connection, map[string]any{
		"op": 10, "d": map[string]any{"heartbeat_interval": 300000},
	}); err != nil {
		return
	}
	var identify struct {
		Op int `json:"op"`
		D  struct {
			Token string `json:"token"`
		} `json:"d"`
	}
	if err := wsjson.Read(clone.ctx, connection, &identify); err != nil || identify.Op != 2 {
		clone.routeErrors.Add(1)
		return
	}
	identity, ok := clone.identities[identify.D.Token]
	if !ok {
		clone.routeErrors.Add(1)
		return
	}
	token = identify.D.Token
	session := &gatewaySession{connection: connection, identity: identity}
	clone.mu.Lock()
	if clone.sessions[token] != nil {
		clone.mu.Unlock()
		clone.routeErrors.Add(1)
		return
	}
	clone.sessions[token] = session
	clone.mu.Unlock()
	clone.gatewayConnections.Add(1)
	clone.activeGateways.Add(1)
	active = true
	select {
	case <-clone.ctx.Done():
		return
	case <-clone.pollRelease:
	}
	releaseAt := time.Unix(0, clone.pollReleaseUnixNS.Load())
	for _, callback := range identity.Callbacks {
		if err := waitUntil(clone.ctx, releaseAt.Add(callback.PollDueAfter)); err != nil {
			return
		}
		if err := session.sendAgentMessage(clone, callback.PollDocument, true); err != nil {
			return
		}
	}

	for {
		var frame struct {
			Op int `json:"op"`
		}
		if err := wsjson.Read(clone.ctx, connection, &frame); err != nil {
			return
		}
		if frame.Op == 1 {
			session.writeMu.Lock()
			err := wsjson.Write(clone.ctx, connection, map[string]any{"op": 11, "d": nil})
			session.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (session *gatewaySession) sendAgentMessage(clone *discordClone, document string, poll bool) error {
	messageID := clone.nextMessageID()
	clone.mu.Lock()
	clone.pendingDelete[messageID] = session.identity.ChannelID
	clone.mu.Unlock()
	session.writeMu.Lock()
	session.sequence++
	err := wsjson.Write(clone.ctx, session.connection, map[string]any{
		"op": 0, "t": "MESSAGE_CREATE", "s": session.sequence,
		"d": map[string]any{
			"id": messageID, "channel_id": session.identity.ChannelID,
			"content": document, "attachments": []any{},
		},
	})
	session.writeMu.Unlock()
	if err != nil {
		clone.mu.Lock()
		delete(clone.pendingDelete, messageID)
		clone.mu.Unlock()
		return err
	}
	clone.gatewayMessages.Add(1)
	if poll {
		now := time.Now().UnixNano()
		clone.pollsEmitted.Add(1)
		storeMinimum(&clone.firstPollUnixNS, now)
		storeMaximum(&clone.lastPollUnixNS, now)
	}
	return nil
}

func (clone *discordClone) nextMessageID() string {
	return fmt.Sprintf("7%017d", clone.messageSequence.Add(1))
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	duration := time.Until(deadline)
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func pollOffsetMilliseconds(releaseAt, observedAt int64) int64 {
	if releaseAt == 0 || observedAt < releaseAt {
		return 0
	}
	return time.Duration(observedAt - releaseAt).Milliseconds()
}

func storeMinimum(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if (current != 0 && current <= value) || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func storeMaximum(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if current >= value || target.CompareAndSwap(current, value) {
			return
		}
	}
}
