package mythic_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
)

type fakeStream struct {
	sent chan *mythic.FromAgent
	recv chan *mythic.FromMythic
}

func (stream *fakeStream) Send(message *mythic.FromAgent) error {
	copyMessage := *message
	copyMessage.Message = append([]byte(nil), message.Message...)
	copyMessage.Base64Message = append([]byte(nil), message.Base64Message...)
	stream.sent <- &copyMessage
	return nil
}

func (stream *fakeStream) Recv() (*mythic.FromMythic, error) {
	message, ok := <-stream.recv
	if !ok {
		return nil, io.EOF
	}
	return message, nil
}

type fakeConnector struct {
	stream *fakeStream
	opens  atomic.Int32
}

func (connector *fakeConnector) Open(context.Context) (mythic.Stream, error) {
	connector.opens.Add(1)
	return connector.stream, nil
}

func runningBridge(t *testing.T, outboundDepth int) (*mythic.Bridge, *fakeStream, context.CancelFunc, <-chan error) {
	t.Helper()
	stream := &fakeStream{sent: make(chan *mythic.FromAgent, 8), recv: make(chan *mythic.FromMythic, 8)}
	connector := &fakeConnector{stream: stream}
	bridge, err := mythic.NewBridge(connector, 8, outboundDepth)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for connector.opens.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if connector.opens.Load() != 1 {
		cancel()
		t.Fatal("bridge did not open exactly one stream")
	}
	for index, expectedRoute := range []string{"", mythic.RegistryRequestRoute, mythic.ActivityRequestRoute} {
		select {
		case control := <-stream.sent:
			if control.C2ProfileName != "discordx" || control.TrackingID != expectedRoute {
				t.Fatalf("startup control %d = %#v", index, control)
			}
		case <-time.After(time.Second):
			cancel()
			t.Fatal("bridge did not send startup registry controls")
		}
	}
	return bridge, stream, cancel, done
}

func TestIngressWaitsForMatchingReceiptAndUsesOneStream(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 4)
	defer func() { cancel(); <-done }()
	result := make(chan struct {
		receipt mythic.Receipt
		err     error
	}, 1)
	go func() {
		receipt, err := bridge.SendIngress(context.Background(), mythic.Ingress{
			ID: "ingress-1", TrackingID: "dx2:route", Message: []byte("body"), Lane: mythic.Standard,
		})
		result <- struct {
			receipt mythic.Receipt
			err     error
		}{receipt, err}
	}()
	sent := <-stream.sent
	if sent.C2ProfileName != "discordx" || sent.IngressID != "ingress-1" || sent.TrackingID != "dx2:route" || string(sent.Message) != "body" {
		t.Fatalf("sent ingress = %#v", sent)
	}
	select {
	case <-result:
		t.Fatal("SendIngress returned before receipt")
	default:
	}
	stream.recv <- &mythic.FromMythic{IngressID: "ingress-1", IsIngressReceipt: true, Success: true}
	got := <-result
	if got.err != nil || !got.receipt.Accepted || got.receipt.IngressID != "ingress-1" {
		t.Fatalf("SendIngress() = %#v, %v", got.receipt, got.err)
	}
}

func TestOutboundAndOutboundReceiptPreserveLaneAndIDs(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 4)
	defer func() { cancel(); <-done }()
	stream.recv <- &mythic.FromMythic{
		Success: true, Message: []byte("task"), TrackingID: "dx2:route",
		DeliveryLane: mythic.Socks, OutboundID: "outbound-1",
	}
	select {
	case outbound := <-bridge.Outbound():
		if outbound.ID != "outbound-1" || outbound.TrackingID != "dx2:route" || outbound.Lane != mythic.Socks || string(outbound.Message) != "task" {
			t.Fatalf("outbound = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outbound")
	}
	receiptDone := make(chan error, 1)
	go func() { receiptDone <- bridge.SendOutboundReceipt(context.Background(), "outbound-1", true) }()
	sent := <-stream.sent
	if !sent.IsOutboundReceipt || !sent.OutboundSuccess || sent.OutboundID != "outbound-1" {
		t.Fatalf("outbound receipt = %#v", sent)
	}
	if err := <-receiptDone; err != nil {
		t.Fatal(err)
	}
}

func TestHealthSnapshotUsesBoundedControlFrame(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 4)
	defer func() { cancel(); <-done }()
	healthDone := make(chan error, 1)
	go func() { healthDone <- bridge.SendHealth(context.Background(), []byte(`{"revision":1}`)) }()
	sent := <-stream.sent
	if sent.C2ProfileName != "discordx" || sent.TrackingID != mythic.HealthSnapshotRoute ||
		string(sent.Message) != `{"revision":1}` || len(sent.Base64Message) != 0 || sent.IngressID != "" {
		t.Fatalf("health control frame = %#v", sent)
	}
	if err := <-healthDone; err != nil {
		t.Fatal(err)
	}
	if err := bridge.SendHealth(context.Background(), nil); err == nil {
		t.Fatal("empty health snapshot was accepted")
	}
}

func TestBridgeRejectsSecondRunAndFailsClosedWhenOutboundQueueFills(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 1)
	if err := bridge.Run(context.Background()); err == nil {
		t.Fatal("second Run() succeeded")
	}
	stream.recv <- &mythic.FromMythic{Success: true, OutboundID: "one", TrackingID: "dx2:one", Message: []byte("one")}
	stream.recv <- &mythic.FromMythic{Success: true, OutboundID: "two", TrackingID: "dx2:two", Message: []byte("two")}
	select {
	case err := <-done:
		if !errors.Is(err, mythic.ErrOutboundFull) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bridge did not fail closed on outbound saturation")
	}
	cancel()
	if _, err := bridge.SendIngress(context.Background(), mythic.Ingress{ID: "later", TrackingID: "route", Message: []byte("x")}); !errors.Is(err, mythic.ErrNotRunning) {
		t.Fatalf("SendIngress after failure = %v", err)
	}
}

func TestRegistrySnapshotUsesDedicatedBoundedControlChannel(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 4)
	defer func() { cancel(); <-done }()
	stream.recv <- &mythic.FromMythic{Success: true, TrackingID: mythic.RegistrySnapshotRoute, Message: []byte(`{"revision":7}`)}
	select {
	case snapshot := <-bridge.RegistrySnapshots():
		if string(snapshot) != `{"revision":7}` {
			t.Fatalf("snapshot = %s", snapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry snapshot")
	}
	select {
	case outbound := <-bridge.Outbound():
		t.Fatalf("registry leaked into outbound delivery: %#v", outbound)
	default:
	}
}

func TestActivitySnapshotUsesDedicatedBoundedControlChannel(t *testing.T) {
	bridge, stream, cancel, done := runningBridge(t, 4)
	defer func() { cancel(); <-done }()
	stream.recv <- &mythic.FromMythic{Success: true, TrackingID: mythic.ActivitySnapshotRoute, Message: []byte(`{"sequence":7,"callbacks":[]}`)}
	select {
	case snapshot := <-bridge.ActivitySnapshots():
		if string(snapshot) != `{"sequence":7,"callbacks":[]}` {
			t.Fatalf("snapshot = %s", snapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for activity snapshot")
	}
	select {
	case outbound := <-bridge.Outbound():
		t.Fatalf("activity leaked into outbound delivery: %#v", outbound)
	default:
	}
}
