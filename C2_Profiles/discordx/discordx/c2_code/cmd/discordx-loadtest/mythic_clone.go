package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

type mythicCounters struct {
	CommandsQueued             int64 `json:"commands_queued"`
	CallbackPolls              int64 `json:"callback_polls"`
	CommandsIssued             int64 `json:"commands_issued"`
	CommandResults             int64 `json:"command_results"`
	IngressReceiptsIssued      int64 `json:"ingress_receipts_issued"`
	SuccessfulOutboundReceipts int64 `json:"successful_outbound_receipts"`
	FailedOutboundReceipts     int64 `json:"failed_outbound_receipts"`
	ProtocolErrors             int64 `json:"protocol_errors"`
}

type callbackLifecycle struct {
	plan              callbackPlan
	taskBody          []byte
	polled            bool
	resultReceived    bool
	outboundReceipted bool
}

type syntheticMythic struct {
	callbacks map[string]*callbackLifecycle
	outbounds map[string]string
	recv      chan *mythic.FromMythic

	mu        sync.Mutex
	open      bool
	streamCtx context.Context

	commandsQueued             atomic.Int64
	callbackPolls              atomic.Int64
	commandsIssued             atomic.Int64
	commandResults             atomic.Int64
	ingressReceiptsIssued      atomic.Int64
	successfulOutboundReceipts atomic.Int64
	failedOutboundReceipts     atomic.Int64
	protocolErrors             atomic.Int64
}

func newSyntheticMythic(plans []callbackPlan) (*syntheticMythic, error) {
	if len(plans) == 0 {
		return nil, errors.New("synthetic Mythic requires callbacks")
	}
	server := &syntheticMythic{
		callbacks: make(map[string]*callbackLifecycle, len(plans)),
		outbounds: make(map[string]string, len(plans)),
		recv:      make(chan *mythic.FromMythic, len(plans)*3+16),
	}
	for _, plan := range plans {
		if _, duplicate := server.callbacks[plan.CallbackID]; duplicate {
			return nil, errors.New("synthetic Mythic callback ID is duplicated")
		}
		task := callbackPacket{
			Kind: "task", CallbackID: plan.CallbackID, TaskID: plan.TaskID,
			Command: syntheticCommandEcho, Argument: plan.Argument,
		}
		body, err := encodeCallbackPacket(plan.CallbackID, task)
		if err != nil {
			return nil, errors.New("synthetic Mythic task queue encoding failed")
		}
		copyPlan := plan
		server.callbacks[plan.CallbackID] = &callbackLifecycle{plan: copyPlan, taskBody: body}
	}
	server.commandsQueued.Store(int64(len(plans)))
	return server, nil
}

func (server *syntheticMythic) Open(ctx context.Context) (mythic.Stream, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.open || ctx == nil {
		return nil, errors.New("synthetic Mythic stream is already open")
	}
	server.open = true
	server.streamCtx = ctx
	return server, nil
}

func (server *syntheticMythic) Send(message *mythic.FromAgent) error {
	if message == nil || message.C2ProfileName != "discordx" {
		server.protocolErrors.Add(1)
		return errors.New("synthetic Mythic received an invalid profile frame")
	}
	if message.IsOutboundReceipt {
		return server.acceptOutboundReceipt(message)
	}
	if message.IngressID != "" {
		return server.acceptIngress(message)
	}
	// Registration, registry/activity requests, heartbeats, and health frames
	// are valid control traffic but do not change the callback lifecycle.
	return nil
}

func (server *syntheticMythic) Recv() (*mythic.FromMythic, error) {
	server.mu.Lock()
	ctx := server.streamCtx
	server.mu.Unlock()
	if ctx == nil {
		return nil, errors.New("synthetic Mythic stream is not open")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case message, ok := <-server.recv:
		if !ok {
			return nil, io.EOF
		}
		return message, nil
	}
}

func (server *syntheticMythic) acceptIngress(message *mythic.FromAgent) error {
	if message.TrackingID == "" || len(message.Message) == 0 || len(message.Base64Message) != 0 || message.IngressLane != mythic.Standard {
		return server.protocolError("synthetic Mythic ingress fields are invalid")
	}
	routeValue, err := route.ParseDX2(message.TrackingID)
	if err != nil || routeValue.Kind != route.Fixed {
		return server.protocolError("synthetic Mythic tracking route is invalid")
	}
	clientID, packet, err := decodeCallbackPacket(message.Message)
	if err != nil || clientID != routeValue.ClientID {
		return server.protocolError("synthetic Mythic callback body is invalid")
	}

	server.mu.Lock()
	lifecycle := server.callbacks[clientID]
	if lifecycle == nil || lifecycle.plan.ListenerID != routeValue.ListenerID || lifecycle.plan.GenerationID != routeValue.GenerationID {
		server.mu.Unlock()
		return server.protocolError("synthetic Mythic callback route is unknown")
	}
	switch packet.Kind {
	case "poll":
		if lifecycle.polled || lifecycle.resultReceived {
			server.mu.Unlock()
			return server.protocolError("synthetic Mythic received a duplicate poll")
		}
		lifecycle.polled = true
		outboundID := "outbound-" + lifecycle.plan.TaskID
		server.outbounds[outboundID] = clientID
		body := lifecycle.taskBody
		server.callbackPolls.Add(1)
		server.commandsIssued.Add(1)
		server.mu.Unlock()
		if err := server.enqueue(&mythic.FromMythic{IngressID: message.IngressID, IsIngressReceipt: true, Success: true}); err != nil {
			return err
		}
		server.ingressReceiptsIssued.Add(1)
		return server.enqueue(&mythic.FromMythic{
			Success: true, Message: body, TrackingID: message.TrackingID,
			DeliveryLane: mythic.Standard, OutboundID: outboundID,
		})
	case "result":
		if !lifecycle.polled || lifecycle.resultReceived || packet.TaskID != lifecycle.plan.TaskID || packet.Output != lifecycle.plan.Argument {
			server.mu.Unlock()
			return server.protocolError("synthetic Mythic command result is invalid")
		}
		lifecycle.resultReceived = true
		server.commandResults.Add(1)
		server.mu.Unlock()
		if err := server.enqueue(&mythic.FromMythic{IngressID: message.IngressID, IsIngressReceipt: true, Success: true}); err != nil {
			return err
		}
		server.ingressReceiptsIssued.Add(1)
		return nil
	default:
		server.mu.Unlock()
		return server.protocolError("synthetic Mythic received an unexpected callback packet")
	}
}

func (server *syntheticMythic) acceptOutboundReceipt(message *mythic.FromAgent) error {
	server.mu.Lock()
	clientID, ok := server.outbounds[message.OutboundID]
	lifecycle := server.callbacks[clientID]
	if !ok || lifecycle == nil || lifecycle.outboundReceipted {
		server.mu.Unlock()
		return server.protocolError("synthetic Mythic outbound receipt is invalid")
	}
	lifecycle.outboundReceipted = true
	server.mu.Unlock()
	if message.OutboundSuccess {
		server.successfulOutboundReceipts.Add(1)
	} else {
		server.failedOutboundReceipts.Add(1)
	}
	return nil
}

func (server *syntheticMythic) enqueue(message *mythic.FromMythic) error {
	server.mu.Lock()
	ctx := server.streamCtx
	server.mu.Unlock()
	if ctx == nil {
		return errors.New("synthetic Mythic stream is not open")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case server.recv <- message:
		return nil
	}
}

func (server *syntheticMythic) protocolError(message string) error {
	server.protocolErrors.Add(1)
	return errors.New(message)
}

func (server *syntheticMythic) Counters() mythicCounters {
	return mythicCounters{
		CommandsQueued:             server.commandsQueued.Load(),
		CallbackPolls:              server.callbackPolls.Load(),
		CommandsIssued:             server.commandsIssued.Load(),
		CommandResults:             server.commandResults.Load(),
		IngressReceiptsIssued:      server.ingressReceiptsIssued.Load(),
		SuccessfulOutboundReceipts: server.successfulOutboundReceipts.Load(),
		FailedOutboundReceipts:     server.failedOutboundReceipts.Load(),
		ProtocolErrors:             server.protocolErrors.Load(),
	}
}

type loadBridgeAdapter struct{ bridge *mythic.Bridge }

func (adapter loadBridgeAdapter) SendIngress(ctx context.Context, ingress worker.Ingress) (worker.Receipt, error) {
	lane := mythic.Standard
	if ingress.Lane == worker.Socks {
		lane = mythic.Socks
	}
	receipt, err := adapter.bridge.SendIngress(ctx, mythic.Ingress{
		ID: ingress.ID, TrackingID: ingress.TrackingID, Message: ingress.Message,
		Base64Message: ingress.Base64Message, Lane: lane,
	})
	return worker.Receipt{IngressID: receipt.IngressID, Accepted: receipt.Accepted}, err
}
