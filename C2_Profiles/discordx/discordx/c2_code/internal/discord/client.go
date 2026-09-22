// Package discord contains the provider-bound Discord REST and Gateway
// adapter. Each Client owns immutable endpoints and one fail-closed direct or
// proxy transport; it never consults process proxy environment variables.
package discord

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	maximumResponseBytes   = 4 << 20
	maximumAttachmentBytes = 2_097_152
)

var ErrCrossOriginRedirect = errors.New("cross-origin provider redirect rejected")

var (
	ErrGatewayReconnect      = errors.New("Discord Gateway requested reconnect")
	ErrGatewayInvalidSession = errors.New("Discord Gateway session is invalid")
)

type RateLimitError struct {
	RetryAfter time.Duration
	Global     bool
}

func (rateLimit *RateLimitError) Error() string {
	return fmt.Sprintf("Discord rate limit requires a %s wait", rateLimit.RetryAfter)
}

type HTTPStatusError struct {
	StatusCode int
}

func (status *HTTPStatusError) Error() string {
	return fmt.Sprintf("Discord provider returned HTTP status %d", status.StatusCode)
}

type BotIdentity struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

type Attachment struct {
	ID       string `json:"id,omitempty"`
	Filename string `json:"filename,omitempty"`
	URL      string `json:"url"`
	Size     int64  `json:"size,omitempty"`
}

type Message struct {
	ID          string       `json:"id"`
	ChannelID   string       `json:"channel_id"`
	Content     string       `json:"content"`
	Attachments []Attachment `json:"attachments"`
}

type OutboundDocument struct {
	Content        string
	AttachmentName string
	Attachment     []byte
}

type Client struct {
	provider   config.Provider
	egress     config.EgressProxy
	token      string
	httpClient *http.Client
	requestMu  sync.Mutex
}

func NewClient(provider config.Provider, egress config.EgressProxy, token string, testMode bool) (*Client, error) {
	normalizedProvider, err := provider.Normalize(testMode)
	if err != nil {
		return nil, err
	}
	normalizedEgress, err := egress.Normalize()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("Discord credential is required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.MaxConnsPerHost = 8
	transport.MaxIdleConnsPerHost = 8
	if normalizedEgress.Mode != "direct" {
		proxyURL, err := url.Parse(normalizedEgress.URL)
		if err != nil {
			return nil, errors.New("server proxy URL is invalid")
		}
		if normalizedEgress.Username != "" || normalizedEgress.Password != "" {
			proxyURL.User = url.UserPassword(normalizedEgress.Username, normalizedEgress.Password)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) == 0 || sameOrigin(request.URL, via[0].URL) {
				return nil
			}
			return ErrCrossOriginRedirect
		},
	}
	return &Client{
		provider: normalizedProvider, egress: normalizedEgress,
		token: token, httpClient: httpClient,
	}, nil
}

func (client *Client) ValidateCredential(ctx context.Context) (BotIdentity, error) {
	var identity BotIdentity
	if err := client.doAPI(ctx, http.MethodGet, "/users/@me", nil, "", &identity); err != nil {
		return BotIdentity{}, err
	}
	if !config.ValidSnowflake(identity.ID) || identity.Username == "" {
		return BotIdentity{}, errors.New("Discord provider returned an invalid bot identity")
	}
	return identity, nil
}

func (client *Client) MessagesAfter(ctx context.Context, channelID, after string, limit int) ([]Message, error) {
	if !config.ValidSnowflake(channelID) || (after != "" && !config.ValidSnowflake(after)) || limit < 1 || limit > 100 {
		return nil, errors.New("Discord message cursor request is invalid")
	}
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if after != "" {
		query.Set("after", after)
	}
	var messages []Message
	endpoint := "/channels/" + channelID + "/messages?" + query.Encode()
	if err := client.doAPI(ctx, http.MethodGet, endpoint, nil, "", &messages); err != nil {
		return nil, err
	}
	for _, message := range messages {
		if !config.ValidSnowflake(message.ID) || message.ChannelID != channelID {
			return nil, errors.New("Discord provider returned an invalid message route")
		}
	}
	sort.Slice(messages, func(i, j int) bool {
		if len(messages[i].ID) == len(messages[j].ID) {
			return messages[i].ID < messages[j].ID
		}
		return len(messages[i].ID) < len(messages[j].ID)
	})
	return messages, nil
}

func (client *Client) Send(ctx context.Context, channelID string, document OutboundDocument) (Message, error) {
	if !config.ValidSnowflake(channelID) || !utf8.ValidString(document.Content) || len([]byte(document.Content)) > maximumAttachmentBytes {
		return Message{}, errors.New("Discord outbound document is invalid")
	}
	var body io.Reader
	contentType := "application/json"
	if len(document.Attachment) == 0 {
		encoded, err := json.Marshal(struct {
			Content string `json:"content"`
		}{Content: document.Content})
		if err != nil {
			return Message{}, ErrInvalidDocument
		}
		body = bytes.NewReader(encoded)
	} else {
		if len(document.Attachment) > maximumAttachmentBytes || !validAttachmentName(document.AttachmentName) {
			return Message{}, ErrInvalidDocument
		}
		var encoded bytes.Buffer
		writer := multipart.NewWriter(&encoded)
		payload, _ := json.Marshal(struct {
			Content     string `json:"content"`
			Attachments []struct {
				ID       string `json:"id"`
				Filename string `json:"filename"`
			} `json:"attachments"`
		}{Content: document.Content, Attachments: []struct {
			ID       string `json:"id"`
			Filename string `json:"filename"`
		}{{ID: "0", Filename: document.AttachmentName}}})
		field, err := writer.CreateFormField("payload_json")
		if err != nil {
			return Message{}, ErrInvalidDocument
		}
		_, _ = field.Write(payload)
		file, err := writer.CreateFormFile("files[0]", document.AttachmentName)
		if err != nil {
			return Message{}, ErrInvalidDocument
		}
		_, _ = file.Write(document.Attachment)
		if err := writer.Close(); err != nil {
			return Message{}, ErrInvalidDocument
		}
		body = &encoded
		contentType = writer.FormDataContentType()
	}
	var sent Message
	if err := client.doAPI(ctx, http.MethodPost, "/channels/"+channelID+"/messages", body, contentType, &sent); err != nil {
		return Message{}, err
	}
	if !config.ValidSnowflake(sent.ID) || sent.ChannelID != channelID {
		return Message{}, errors.New("Discord provider returned an invalid sent message")
	}
	return sent, nil
}

var ErrInvalidDocument = errors.New("Discord outbound document is invalid")

func (client *Client) Delete(ctx context.Context, channelID, messageID string) error {
	if !config.ValidSnowflake(channelID) || !config.ValidSnowflake(messageID) {
		return errors.New("Discord delete route is invalid")
	}
	return client.doAPI(ctx, http.MethodDelete, "/channels/"+channelID+"/messages/"+messageID, nil, "", nil)
}

func (client *Client) BulkDelete(ctx context.Context, channelID string, messageIDs []string) error {
	if !config.ValidSnowflake(channelID) || len(messageIDs) < 2 || len(messageIDs) > 100 {
		return errors.New("Discord bulk-delete request is invalid")
	}
	for _, messageID := range messageIDs {
		if !config.ValidSnowflake(messageID) {
			return errors.New("Discord bulk-delete request is invalid")
		}
	}
	body, _ := json.Marshal(struct {
		Messages []string `json:"messages"`
	}{Messages: messageIDs})
	return client.doAPI(ctx, http.MethodPost, "/channels/"+channelID+"/messages/bulk-delete", bytes.NewReader(body), "application/json", nil)
}

func (client *Client) React(ctx context.Context, channelID, messageID, emoji string) error {
	if !config.ValidSnowflake(channelID) || !config.ValidSnowflake(messageID) || emoji == "" || len([]byte(emoji)) > 128 {
		return errors.New("Discord reaction route is invalid")
	}
	endpoint := "/channels/" + channelID + "/messages/" + messageID + "/reactions/" + url.PathEscape(emoji) + "/@me"
	return client.doAPI(ctx, http.MethodPut, endpoint, nil, "", nil)
}

func (client *Client) DownloadAttachment(ctx context.Context, attachment Attachment) ([]byte, error) {
	if attachment.Size > maximumAttachmentBytes {
		return nil, errors.New("Discord attachment exceeds the byte limit")
	}
	attachmentURL, err := url.Parse(attachment.URL)
	cdnURL, cdnErr := url.Parse(client.provider.CDNBaseURL)
	if err != nil || cdnErr != nil || attachmentURL.User != nil || attachmentURL.Fragment != "" ||
		!sameOrigin(attachmentURL, cdnURL) {
		return nil, errors.New("Discord attachment origin is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, attachmentURL.String(), nil)
	if err != nil {
		return nil, errors.New("Discord attachment request is invalid")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, redactTransportError(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &HTTPStatusError{StatusCode: response.StatusCode}
	}
	if response.ContentLength > maximumAttachmentBytes {
		return nil, errors.New("Discord attachment exceeds the byte limit")
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, maximumAttachmentBytes+1))
	if err != nil || len(value) > maximumAttachmentBytes {
		return nil, errors.New("Discord attachment read failed or exceeded the byte limit")
	}
	return value, nil
}

func (client *Client) doAPI(ctx context.Context, method, endpoint string, body io.Reader, contentType string, output any) error {
	fullURL := strings.TrimSuffix(client.provider.APIBaseURL, "/") + "/v" + strconv.Itoa(client.provider.APIVersion) + endpoint
	var requestBody []byte
	if body != nil {
		var err error
		requestBody, err = io.ReadAll(io.LimitReader(body, maximumAttachmentBytes+maximumResponseBytes+1))
		if err != nil || len(requestBody) > maximumAttachmentBytes+maximumResponseBytes {
			return errors.New("Discord API request body exceeded the byte limit")
		}
	}
	client.requestMu.Lock()
	defer client.requestMu.Unlock()
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(requestBody))
		if err != nil {
			return errors.New("Discord API request is invalid")
		}
		request.Header.Set("Authorization", "Bot "+client.token)
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		response, err := client.httpClient.Do(request)
		if err != nil {
			if errors.Is(err, ErrCrossOriginRedirect) {
				return ErrCrossOriginRedirect
			}
			return redactTransportError(err)
		}
		value, readErr := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
		_ = response.Body.Close()
		if readErr != nil || len(value) > maximumResponseBytes {
			return errors.New("Discord provider response exceeded the byte limit")
		}
		if response.StatusCode == http.StatusTooManyRequests {
			rateLimit := parseRateLimit(response, value)
			if attempt == 4 || rateLimit.RetryAfter <= 0 || rateLimit.RetryAfter > 24*time.Hour {
				return rateLimit
			}
			timer := time.NewTimer(rateLimit.RetryAfter)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return &HTTPStatusError{StatusCode: response.StatusCode}
		}
		if output != nil {
			if err := json.Unmarshal(value, output); err != nil {
				return errors.New("Discord provider returned malformed JSON")
			}
		}
		return nil
	}
	return errors.New("Discord provider request retry bound reached")
}

func parseRateLimit(response *http.Response, body []byte) *RateLimitError {
	retrySeconds, _ := strconv.ParseFloat(response.Header.Get("Retry-After"), 64)
	payload := struct {
		RetryAfter float64 `json:"retry_after"`
		Global     bool    `json:"global"`
	}{}
	_ = json.Unmarshal(body, &payload)
	if retrySeconds <= 0 {
		retrySeconds = payload.RetryAfter
	}
	if retrySeconds < 0 {
		retrySeconds = 0
	}
	global := payload.Global || strings.EqualFold(response.Header.Get("X-RateLimit-Global"), "true")
	return &RateLimitError{RetryAfter: time.Duration(retrySeconds * float64(time.Second)), Global: global}
}

func redactTransportError(err error) error {
	if errors.Is(err, ErrCrossOriginRedirect) {
		return ErrCrossOriginRedirect
	}
	return errors.New("Discord provider transport failed")
}

func validAttachmentName(value string) bool {
	return value != "" && len(value) <= 512 && path.Base(value) == value && value != "." && value != ".." && utf8.ValidString(value)
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

type GatewayConnection struct {
	connection *websocket.Conn
}

func (client *Client) ConnectGateway(ctx context.Context) (*GatewayConnection, error) {
	gatewayURL := strings.TrimSuffix(client.provider.GatewayBaseURL, "/") +
		"/?v=" + strconv.Itoa(client.provider.APIVersion) + "&encoding=json"
	connection, _, err := websocket.Dial(ctx, gatewayURL, &websocket.DialOptions{
		HTTPClient:      client.httpClient,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		if errors.Is(err, ErrCrossOriginRedirect) {
			return nil, ErrCrossOriginRedirect
		}
		return nil, errors.New("Discord Gateway connection failed")
	}
	return &GatewayConnection{connection: connection}, nil
}

func (connection *GatewayConnection) ReadJSON(ctx context.Context, value any) error {
	if connection == nil || connection.connection == nil {
		return errors.New("Discord Gateway is not connected")
	}
	return wsjson.Read(ctx, connection.connection, value)
}

func (connection *GatewayConnection) WriteJSON(ctx context.Context, value any) error {
	if connection == nil || connection.connection == nil {
		return errors.New("Discord Gateway is not connected")
	}
	return wsjson.Write(ctx, connection.connection, value)
}

func (connection *GatewayConnection) Close() error {
	if connection == nil || connection.connection == nil {
		return nil
	}
	return connection.connection.Close(websocket.StatusNormalClosure, "shutdown")
}

type gatewayFrame struct {
	Op       int             `json:"op"`
	Data     json.RawMessage `json:"d"`
	Sequence *int64          `json:"s"`
	Type     string          `json:"t"`
}

type gatewayHello struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

// ListenGateway runs one identified Gateway session. Reconnect policy and
// cursor catch-up belong to the bot worker, so a reconnect or invalid-session
// opcode is returned as a stable class instead of being hidden here.
func (client *Client) ListenGateway(ctx context.Context, handle func(Message) error) error {
	if handle == nil {
		return errors.New("Discord Gateway message handler is required")
	}
	connection, err := client.ConnectGateway(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()

	var helloFrame gatewayFrame
	if err := connection.ReadJSON(ctx, &helloFrame); err != nil || helloFrame.Op != 10 {
		return errors.New("Discord Gateway hello was invalid")
	}
	var hello gatewayHello
	if err := json.Unmarshal(helloFrame.Data, &hello); err != nil || hello.HeartbeatInterval < 1 || hello.HeartbeatInterval > 300_000 {
		return errors.New("Discord Gateway heartbeat interval was invalid")
	}
	identify := map[string]any{
		"op": 2,
		"d": map[string]any{
			"token":      client.token,
			"intents":    37376,
			"properties": map[string]string{"os": "linux", "browser": "discordx", "device": "discordx"},
		},
	}
	if err := connection.WriteJSON(ctx, identify); err != nil {
		return errors.New("Discord Gateway identify failed")
	}

	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sequence atomic.Int64
	sequence.Store(-1)
	heartbeatErrors := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Duration(hello.HeartbeatInterval) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				var value any
				if current := sequence.Load(); current >= 0 {
					value = current
				}
				if err := connection.WriteJSON(heartbeatCtx, map[string]any{"op": 1, "d": value}); err != nil {
					select {
					case heartbeatErrors <- errors.New("Discord Gateway heartbeat failed"):
					default:
					}
					return
				}
			}
		}
	}()

	for {
		select {
		case err := <-heartbeatErrors:
			return err
		default:
		}
		var frame gatewayFrame
		if err := connection.ReadJSON(ctx, &frame); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("Discord Gateway read failed")
		}
		if frame.Sequence != nil {
			sequence.Store(*frame.Sequence)
		}
		switch frame.Op {
		case 0:
			if frame.Type != "MESSAGE_CREATE" {
				continue
			}
			var message Message
			if err := json.Unmarshal(frame.Data, &message); err != nil || !config.ValidSnowflake(message.ID) || !config.ValidSnowflake(message.ChannelID) {
				return errors.New("Discord Gateway message event was invalid")
			}
			if err := handle(message); err != nil {
				return err
			}
		case 1:
			var value any
			if current := sequence.Load(); current >= 0 {
				value = current
			}
			if err := connection.WriteJSON(ctx, map[string]any{"op": 1, "d": value}); err != nil {
				return errors.New("Discord Gateway heartbeat failed")
			}
		case 7:
			return ErrGatewayReconnect
		case 9:
			return ErrGatewayInvalidSession
		}
	}
}
