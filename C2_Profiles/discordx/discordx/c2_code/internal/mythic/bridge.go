// Package mythic owns the single Discordx one-to-many Push C2 stream. The
// transport adapter is intentionally small so receipt, queue, and routing
// behavior can be tested without a live gRPC server.
package mythic

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	RegistryRequestRoute  = "dx-control:registry-request-v1"
	RegistrySnapshotRoute = "dx-control:registry-snapshot-v1"
	ActivityRequestRoute  = "dx-control:activity-request-v1"
	ActivitySnapshotRoute = "dx-control:activity-snapshot-v1"
	HealthSnapshotRoute   = "dx-control:health-snapshot-v1"
	maximumHealthBytes    = 1 << 20
)

type Lane int32

const (
	Standard Lane = 0
	Socks    Lane = 1
)

type FromAgent struct {
	C2ProfileName     string
	Message           []byte
	Base64Message     []byte
	TrackingID        string
	IngressID         string
	IngressLane       Lane
	OutboundID        string
	IsOutboundReceipt bool
	OutboundSuccess   bool
}

type FromMythic struct {
	Success          bool
	Error            string
	Message          []byte
	TrackingID       string
	DeliveryLane     Lane
	IngressID        string
	IsIngressReceipt bool
	OutboundID       string
}

type Stream interface {
	Send(*FromAgent) error
	Recv() (*FromMythic, error)
}

type Connector interface {
	Open(context.Context) (Stream, error)
}

var (
	ErrNotRunning      = errors.New("Mythic bridge is not running")
	ErrIngressRejected = errors.New("Mythic rejected ingress")
	ErrReceiptMismatch = errors.New("Mythic ingress receipt is invalid")
	ErrOutboundFull    = errors.New("Mythic outbound queue is full")
)

type Ingress struct {
	ID            string
	TrackingID    string
	Message       []byte
	Base64Message []byte
	Lane          Lane
}

type Receipt struct {
	IngressID string
	Accepted  bool
}

type Outbound struct {
	ID         string
	TrackingID string
	Message    []byte
	Lane       Lane
	Success    bool
	ErrorClass string
}

type sendRequest struct {
	message *FromAgent
	done    chan error
}

type receiptResult struct {
	receipt Receipt
	err     error
}

type Bridge struct {
	connector Connector
	sendQueue chan sendRequest
	outbound  chan Outbound
	registry  chan []byte
	activity  chan []byte

	mu       sync.Mutex
	running  bool
	receipts map[string]chan receiptResult
}

func NewBridge(connector Connector, sendDepth, outboundDepth int) (*Bridge, error) {
	if connector == nil || sendDepth < 1 || sendDepth > 4096 || outboundDepth < 1 || outboundDepth > 4096 {
		return nil, errors.New("Mythic bridge bounds are invalid")
	}
	return &Bridge{
		connector: connector, sendQueue: make(chan sendRequest, sendDepth),
		outbound: make(chan Outbound, outboundDepth), registry: make(chan []byte, 4), activity: make(chan []byte, 4),
		receipts: make(map[string]chan receiptResult),
	}, nil
}

func (bridge *Bridge) Outbound() <-chan Outbound { return bridge.outbound }

func (bridge *Bridge) RegistrySnapshots() <-chan []byte { return bridge.registry }

func (bridge *Bridge) ActivitySnapshots() <-chan []byte { return bridge.activity }

func (bridge *Bridge) IsRunning() bool {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.running
}

func (bridge *Bridge) Run(ctx context.Context) error {
	bridge.mu.Lock()
	if bridge.running {
		bridge.mu.Unlock()
		return errors.New("Mythic bridge already has an active stream")
	}
	bridge.running = true
	bridge.mu.Unlock()
	defer func() {
		bridge.mu.Lock()
		bridge.running = false
		for id, waiter := range bridge.receipts {
			delete(bridge.receipts, id)
			select {
			case waiter <- receiptResult{err: ErrNotRunning}:
			default:
			}
		}
		bridge.mu.Unlock()
	}()

	stream, err := bridge.connector.Open(ctx)
	if err != nil {
		return err
	}
	// The server uses the first frame to register the one-to-many profile name.
	// The following exact, content-free control request obtains the complete
	// cold-start snapshot over the already authenticated internal gRPC channel.
	if err := stream.Send(&FromAgent{C2ProfileName: "discordx"}); err != nil {
		return err
	}
	if err := stream.Send(&FromAgent{C2ProfileName: "discordx", TrackingID: RegistryRequestRoute}); err != nil {
		return err
	}
	if err := stream.Send(&FromAgent{C2ProfileName: "discordx", TrackingID: ActivityRequestRoute}); err != nil {
		return err
	}
	receiveResults := make(chan error, 1)
	go func() { receiveResults <- bridge.receive(ctx, stream) }()
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()
	activityRefresh := time.NewTicker(30 * time.Second)
	defer activityRefresh.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-receiveResults:
			if errors.Is(err, io.EOF) {
				return ErrNotRunning
			}
			return err
		case request := <-bridge.sendQueue:
			err := stream.Send(request.message)
			select {
			case request.done <- err:
			default:
			}
			if err != nil {
				return err
			}
		case <-heartbeat.C:
			if err := stream.Send(&FromAgent{C2ProfileName: "discordx"}); err != nil {
				return err
			}
		case <-activityRefresh.C:
			if err := stream.Send(&FromAgent{C2ProfileName: "discordx", TrackingID: RegistryRequestRoute}); err != nil {
				return err
			}
			if err := stream.Send(&FromAgent{C2ProfileName: "discordx", TrackingID: ActivityRequestRoute}); err != nil {
				return err
			}
		}
	}
}

func (bridge *Bridge) SendIngress(ctx context.Context, ingress Ingress) (Receipt, error) {
	if ingress.ID == "" || ingress.TrackingID == "" || (len(ingress.Message) == 0) == (len(ingress.Base64Message) == 0) ||
		(ingress.Lane != Standard && ingress.Lane != Socks) {
		return Receipt{}, errors.New("Mythic ingress is invalid")
	}
	waiter := make(chan receiptResult, 1)
	bridge.mu.Lock()
	if !bridge.running {
		bridge.mu.Unlock()
		return Receipt{}, ErrNotRunning
	}
	if _, duplicate := bridge.receipts[ingress.ID]; duplicate {
		bridge.mu.Unlock()
		return Receipt{}, errors.New("Mythic ingress ID is already pending")
	}
	bridge.receipts[ingress.ID] = waiter
	bridge.mu.Unlock()
	defer func() {
		bridge.mu.Lock()
		delete(bridge.receipts, ingress.ID)
		bridge.mu.Unlock()
	}()

	done := make(chan error, 1)
	request := sendRequest{message: &FromAgent{
		C2ProfileName: "discordx", Message: append([]byte(nil), ingress.Message...),
		Base64Message: append([]byte(nil), ingress.Base64Message...), TrackingID: ingress.TrackingID,
		IngressID: ingress.ID, IngressLane: ingress.Lane,
	}, done: done}
	select {
	case bridge.sendQueue <- request:
	case <-ctx.Done():
		return Receipt{}, ctx.Err()
	}
	select {
	case err := <-done:
		if err != nil {
			return Receipt{}, err
		}
	case <-ctx.Done():
		return Receipt{}, ctx.Err()
	}
	select {
	case result := <-waiter:
		return result.receipt, result.err
	case <-ctx.Done():
		return Receipt{}, ctx.Err()
	}
}

func (bridge *Bridge) SendOutboundReceipt(ctx context.Context, outboundID string, success bool) error {
	if outboundID == "" {
		return errors.New("Mythic outbound receipt ID is required")
	}
	done := make(chan error, 1)
	request := sendRequest{message: &FromAgent{
		C2ProfileName: "discordx", OutboundID: outboundID,
		IsOutboundReceipt: true, OutboundSuccess: success,
	}, done: done}
	bridge.mu.Lock()
	running := bridge.running
	bridge.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	select {
	case bridge.sendQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bridge *Bridge) SendHealth(ctx context.Context, payload []byte) error {
	if len(payload) == 0 || len(payload) > maximumHealthBytes {
		return errors.New("Discordx health snapshot byte length is invalid")
	}
	done := make(chan error, 1)
	request := sendRequest{message: &FromAgent{
		C2ProfileName: "discordx", TrackingID: HealthSnapshotRoute,
		Message: append([]byte(nil), payload...),
	}, done: done}
	bridge.mu.Lock()
	running := bridge.running
	bridge.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	select {
	case bridge.sendQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bridge *Bridge) receive(ctx context.Context, stream Stream) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		if message.IsIngressReceipt {
			bridge.mu.Lock()
			waiter := bridge.receipts[message.IngressID]
			bridge.mu.Unlock()
			if waiter != nil {
				result := receiptResult{receipt: Receipt{IngressID: message.IngressID, Accepted: message.Success}}
				if !message.Success {
					result.err = ErrIngressRejected
				}
				select {
				case waiter <- result:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		}
		if message.Success && message.TrackingID == RegistrySnapshotRoute && len(message.Message) > 0 &&
			message.OutboundID == "" && message.IngressID == "" {
			payload := append([]byte(nil), message.Message...)
			select {
			case bridge.registry <- payload:
			default:
				return ErrOutboundFull
			}
			continue
		}
		if message.Success && message.TrackingID == ActivitySnapshotRoute && len(message.Message) > 0 &&
			message.OutboundID == "" && message.IngressID == "" {
			payload := append([]byte(nil), message.Message...)
			select {
			case bridge.activity <- payload:
			default:
				return ErrOutboundFull
			}
			continue
		}
		outbound := Outbound{
			ID: message.OutboundID, TrackingID: message.TrackingID,
			Message: append([]byte(nil), message.Message...), Lane: message.DeliveryLane,
			Success: message.Success,
		}
		if !message.Success {
			outbound.ErrorClass = "mythic_outbound_failed"
		}
		select {
		case bridge.outbound <- outbound:
		default:
			return ErrOutboundFull
		}
	}
}
