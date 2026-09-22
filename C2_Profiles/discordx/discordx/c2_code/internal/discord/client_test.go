package discord_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const testToken = "discord-token-canary"

func mockProvider(t *testing.T, serverURL string) config.Provider {
	t.Helper()
	gateway := "ws" + strings.TrimPrefix(serverURL, "http")
	provider, err := (config.Provider{
		Kind: "mock", APIBaseURL: serverURL + "/api", GatewayBaseURL: gateway,
		CDNBaseURL: serverURL, APIVersion: 10, TestOnlyAllowInsecureTransport: true,
	}).Normalize(true)
	if err != nil {
		t.Fatalf("Provider.Normalize() error = %v", err)
	}
	return provider
}

func directEgress(t *testing.T) config.EgressProxy {
	t.Helper()
	egress, err := (config.EgressProxy{Mode: "direct"}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	return egress
}

func TestRESTClientUsesBoundOriginCursorAndReturnedRateLimit(t *testing.T) {
	var calls atomic.Int32
	var limitedCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bot "+testToken {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		switch {
		case request.URL.Path == "/api/v10/users/@me":
			_, _ = io.WriteString(response, `{"id":"900000000000000001","username":"bot-a"}`)
		case request.URL.Path == "/api/v10/channels/100000000000000001/messages" && request.Method == http.MethodGet:
			if request.URL.Query().Get("after") != "200000000000000001" || request.URL.Query().Get("limit") != "50" {
				t.Errorf("messages query = %s", request.URL.RawQuery)
			}
			_, _ = io.WriteString(response, `[{"id":"200000000000000002","channel_id":"100000000000000001","content":"payload","attachments":[]}]`)
		case request.URL.Path == "/api/v10/channels/100000000000000002/messages":
			if limitedCalls.Add(1) == 1 {
				response.Header().Set("Retry-After", "0.01")
				response.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(response, `{"retry_after":0.01,"global":false}`)
				return
			}
			_, _ = io.WriteString(response, `[]`)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client, err := discord.NewClient(mockProvider(t, server.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	identity, err := client.ValidateCredential(context.Background())
	if err != nil || identity.ID != "900000000000000001" || identity.Username != "bot-a" {
		t.Fatalf("ValidateCredential() = %#v, %v", identity, err)
	}
	messages, err := client.MessagesAfter(context.Background(), "100000000000000001", "200000000000000001", 50)
	if err != nil || len(messages) != 1 || messages[0].ID != "200000000000000002" {
		t.Fatalf("MessagesAfter() = %#v, %v", messages, err)
	}
	messages, err = client.MessagesAfter(context.Background(), "100000000000000002", "200000000000000001", 50)
	if err != nil || len(messages) != 0 || limitedCalls.Load() != 2 {
		t.Fatalf("rate-limited MessagesAfter() = %#v, %v; calls=%d", messages, err, limitedCalls.Load())
	}
	if strings.Contains(fmt.Sprint(err), testToken) {
		t.Fatalf("error leaked token: %v", err)
	}
	if calls.Load() != 4 {
		t.Fatalf("request count = %d", calls.Load())
	}
}

func TestAuthenticatedRedirectIsRejectedBeforeCredentialCrossesOrigin(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		redirected.Add(1)
		if request.Header.Get("Authorization") != "" {
			t.Errorf("cross-origin request carried Authorization")
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, target.URL+"/stolen", http.StatusFound)
	}))
	defer origin.Close()

	client, err := discord.NewClient(mockProvider(t, origin.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ValidateCredential(context.Background())
	if !errors.Is(err, discord.ErrCrossOriginRedirect) {
		t.Fatalf("ValidateCredential() error = %v", err)
	}
	if redirected.Load() != 0 {
		t.Fatalf("redirect target received %d requests", redirected.Load())
	}
}

func TestAttachmentDownloadUsesCDNOriginWithoutAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/cdn/message.txt" {
			http.NotFound(response, request)
			return
		}
		if request.Header.Get("Authorization") != "" {
			t.Errorf("CDN request carried Authorization")
		}
		_, _ = io.WriteString(response, "attachment-body")
	}))
	defer server.Close()
	client, err := discord.NewClient(mockProvider(t, server.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	body, err := client.DownloadAttachment(context.Background(), discord.Attachment{
		URL: server.URL + "/cdn/message.txt", Size: 15,
	})
	if err != nil || string(body) != "attachment-body" {
		t.Fatalf("DownloadAttachment() = %q, %v", body, err)
	}
	if _, err := client.DownloadAttachment(context.Background(), discord.Attachment{URL: "http://example.invalid/file"}); err == nil {
		t.Fatal("cross-origin attachment URL accepted")
	}
}

func TestHTTPProxyIsUsedAndFailureNeverFallsBackToDirect(t *testing.T) {
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		originCalls.Add(1)
		_, _ = io.WriteString(response, `{"id":"900000000000000001","username":"bot-a"}`)
	}))
	defer origin.Close()
	var proxyCalls atomic.Int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		proxyCalls.Add(1)
		outbound := request.Clone(request.Context())
		outbound.RequestURI = ""
		outbound.Header.Del("Proxy-Authorization")
		result, err := http.DefaultTransport.RoundTrip(outbound)
		if err != nil {
			http.Error(response, "proxy failure", http.StatusBadGateway)
			return
		}
		defer result.Body.Close()
		for key, values := range result.Header {
			for _, value := range values {
				response.Header().Add(key, value)
			}
		}
		response.WriteHeader(result.StatusCode)
		_, _ = io.Copy(response, result.Body)
	}))
	defer proxyServer.Close()
	proxy, err := (config.EgressProxy{Mode: "http", URL: proxyServer.URL}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	client, err := discord.NewClient(mockProvider(t, origin.URL), proxy, testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ValidateCredential(context.Background()); err != nil {
		t.Fatalf("proxied ValidateCredential() error = %v", err)
	}
	if proxyCalls.Load() != 1 || originCalls.Load() != 1 {
		t.Fatalf("proxy/origin calls = %d/%d", proxyCalls.Load(), originCalls.Load())
	}

	failedProxy, err := (config.EgressProxy{Mode: "http", URL: "http://127.0.0.1:1"}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	failed, err := discord.NewClient(mockProvider(t, origin.URL), failedProxy, testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failed.ValidateCredential(context.Background()); err == nil {
		t.Fatal("failed proxy unexpectedly reached provider")
	}
	if originCalls.Load() != 1 {
		t.Fatalf("proxy failure fell back to direct; origin calls = %d", originCalls.Load())
	}
}

func TestGatewayConnectionUsesConfiguredOriginAndJSONFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			t.Errorf("Accept() error = %v", err)
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		var identify map[string]any
		if err := wsjson.Read(context.Background(), connection, &identify); err != nil {
			t.Errorf("gateway Read() error = %v", err)
			return
		}
		if identify["op"] != float64(2) {
			t.Errorf("identify frame = %#v", identify)
		}
		_ = wsjson.Write(context.Background(), connection, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	}))
	defer server.Close()
	client, err := discord.NewClient(mockProvider(t, server.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := client.ConnectGateway(context.Background())
	if err != nil {
		t.Fatalf("ConnectGateway() error = %v", err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(context.Background(), map[string]any{"op": 2}); err != nil {
		t.Fatal(err)
	}
	var hello map[string]any
	if err := connection.ReadJSON(context.Background(), &hello); err != nil || hello["op"] != float64(10) {
		t.Fatalf("ReadJSON() = %#v, %v", hello, err)
	}
}

func TestListenGatewayIdentifiesAndEmitsOnlyMessageCreate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			t.Errorf("Accept() error = %v", err)
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		_ = wsjson.Write(context.Background(), connection, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 1000}})
		var identify struct {
			Op int `json:"op"`
			D  struct {
				Token   string `json:"token"`
				Intents int    `json:"intents"`
			} `json:"d"`
		}
		if err := wsjson.Read(context.Background(), connection, &identify); err != nil {
			t.Errorf("identify read error = %v", err)
			return
		}
		if identify.Op != 2 || identify.D.Token != testToken || identify.D.Intents != 37376 {
			t.Errorf("identify = %#v", identify)
		}
		_ = wsjson.Write(context.Background(), connection, map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{}})
		_ = wsjson.Write(context.Background(), connection, map[string]any{
			"op": 0, "t": "MESSAGE_CREATE", "s": 2,
			"d": map[string]any{"id": "200000000000000001", "channel_id": "100000000000000001", "content": "document", "attachments": []any{}},
		})
	}))
	defer server.Close()
	client, err := discord.NewClient(mockProvider(t, server.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	seen := make(chan discord.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- client.ListenGateway(ctx, func(message discord.Message) error {
			seen <- message
			cancel()
			return nil
		})
	}()
	select {
	case message := <-seen:
		if message.ID != "200000000000000001" || message.Content != "document" {
			t.Fatalf("Gateway message = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Gateway message")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ListenGateway() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenGateway() did not stop with its context")
	}
}

func TestSendDeleteReactAndBulkDeleteUseBoundedLocalRoutes(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		seen = append(seen, request.Method+" "+request.URL.EscapedPath())
		if request.Header.Get("Authorization") != "Bot "+testToken {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/messages") {
			_, _ = io.WriteString(response, `{"id":"200000000000000010","channel_id":"100000000000000001","content":"reply","attachments":[]}`)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := discord.NewClient(mockProvider(t, server.URL), directEgress(t), testToken, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.Send(ctx, "100000000000000001", discord.OutboundDocument{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Delete(ctx, "100000000000000001", "200000000000000010"); err != nil {
		t.Fatal(err)
	}
	if err := client.React(ctx, "100000000000000001", "200000000000000010", "👍"); err != nil {
		t.Fatal(err)
	}
	if err := client.BulkDelete(ctx, "100000000000000001", []string{"200000000000000010", "200000000000000011"}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 4 || !strings.Contains(strings.Join(seen, "\n"), url.PathEscape("👍")) {
		t.Fatalf("routes = %#v", seen)
	}
}
