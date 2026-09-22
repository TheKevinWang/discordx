package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/planner"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/registry"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/scheduler"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/worker"
)

const maximumSnapshotBytes = 8 << 20

type bridgeAdapter struct{ bridge *mythic.Bridge }

func (adapter bridgeAdapter) SendIngress(ctx context.Context, ingress worker.Ingress) (worker.Receipt, error) {
	lane := mythic.Standard
	if ingress.Lane == worker.Socks {
		lane = mythic.Socks
	}
	receipt, err := adapter.bridge.SendIngress(ctx, mythic.Ingress{
		ID: ingress.ID, TrackingID: ingress.TrackingID,
		Message: ingress.Message, Base64Message: ingress.Base64Message, Lane: lane,
	})
	return worker.Receipt{IngressID: receipt.IngressID, Accepted: receipt.Accepted}, err
}

type botGroup struct {
	client        *discord.Client
	scheduler     *scheduler.Scheduler
	runtime       *worker.BotRuntime
	botUserID     string
	configuration []groupConfiguration
	generations   []generationKey
	cancel        context.CancelFunc
}

type groupConfiguration struct {
	ListenerID string
	Ingress    config.Ingress
	Egress     config.EgressProxy
	Generation registry.Generation
}

type generationKey struct {
	listener   string
	generation string
}

type runtimeController struct {
	ctx     context.Context
	bridge  *mythic.Bridge
	store   *state.Store
	manager *registry.Manager

	mu               sync.RWMutex
	groups           map[string]*botGroup
	generationGroups map[generationKey]*botGroup
}

type runtimeHealthSnapshot struct {
	Revision   uint64                  `json:"revision"`
	ObservedAt time.Time               `json:"observed_at"`
	Listeners  []runtimeListenerHealth `json:"listeners"`
}

type runtimeListenerHealth struct {
	ListenerID              string    `json:"listener_id"`
	OperationID             int       `json:"operation_id"`
	State                   string    `json:"state"`
	BotUserID               string    `json:"bot_user_id,omitempty"`
	StandardQueueDepth      int       `json:"standard_queue_depth"`
	SocksQueueDepth         int       `json:"socks_queue_depth"`
	QueueSaturationCount    uint64    `json:"queue_saturation_count"`
	AccumulatedQueueWaitMS  int64     `json:"accumulated_queue_wait_ms"`
	NextPollAt              time.Time `json:"next_poll_at,omitempty"`
	EffectiveIntervalSecond int64     `json:"effective_interval_seconds,omitempty"`
	SchedulingReason        string    `json:"scheduling_reason,omitempty"`
	ErrorClass              string    `json:"error_class,omitempty"`
}

func main() {
	if err := run(); err != nil {
		log.Printf("discordx server stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	snapshot, hasSnapshot, err := loadSnapshot()
	if err != nil {
		return err
	}

	statePath := os.Getenv("DISCORDX_STATE_PATH")
	if statePath == "" {
		statePath = "/var/lib/discordx/state.db"
	}
	store, err := state.Open(filepath.Clean(statePath), 5000)
	if err != nil {
		return fmt.Errorf("open recovery state: %w", err)
	}
	defer store.Close()

	mythicAddress := os.Getenv("MYTHIC_GRPC_ADDRESS")
	if mythicAddress == "" {
		mythicAddress = "127.0.0.1:17444"
	}
	connector, err := mythic.NewGRPCConnector(mythicAddress)
	if err != nil {
		return err
	}
	bridge, err := mythic.NewBridge(connector, 256, 1000)
	if err != nil {
		return err
	}
	go maintainBridge(ctx, bridge)
	if err := waitForBridge(ctx, bridge, 15*time.Second); err != nil {
		return err
	}
	select {
	case payload := <-bridge.RegistrySnapshots():
		coreSnapshot, decodeErr := decodeSnapshot(payload)
		if decodeErr != nil {
			return decodeErr
		}
		// A one-listener in-memory migration remains the bootstrap source until
		// Mythic has durable Discordx listener rows. Once any rows exist, the
		// database snapshot is authoritative.
		if !hasSnapshot || len(coreSnapshot.Listeners) > 0 {
			snapshot = coreSnapshot
			hasSnapshot = true
		}
	case <-time.After(15 * time.Second):
		if !hasSnapshot {
			return errors.New("Mythic did not provide the initial Discordx registry snapshot")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	if !hasSnapshot {
		return errors.New("Discordx has no initial registry snapshot")
	}
	controller := &runtimeController{
		ctx: ctx, bridge: bridge, store: store, manager: registry.NewManager(),
		groups: make(map[string]*botGroup), generationGroups: make(map[generationKey]*botGroup),
	}
	if err := controller.Apply(snapshot); err != nil {
		return err
	}
	go controller.watchRegistry()
	go controller.publishHealth()
	go outboundLoop(ctx, bridge, controller)
	log.Printf("discordx server active: revision=%d listeners=%d workers=%d", snapshot.Revision, len(snapshot.Listeners), controller.workerCount())
	<-ctx.Done()
	controller.Drain()
	return nil
}

func loadSnapshot() (registry.Snapshot, bool, error) {
	value := os.Getenv("DISCORDX_REGISTRY_JSON")
	if value == "" {
		return registry.Snapshot{}, false, nil
	}
	snapshot, err := decodeSnapshot([]byte(value))
	if err != nil {
		return registry.Snapshot{}, false, err
	}
	return snapshot, true, nil
}

func decodeSnapshot(value []byte) (registry.Snapshot, error) {
	if len(value) == 0 || len(value) > maximumSnapshotBytes {
		return registry.Snapshot{}, errors.New("Discordx registry snapshot has an invalid byte length")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var snapshot registry.Snapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return registry.Snapshot{}, errors.New("Discordx registry snapshot is invalid JSON")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return registry.Snapshot{}, errors.New("Discordx registry snapshot contains trailing data")
	}
	return snapshot, nil
}

func decodeActivitySnapshot(value []byte) (planner.ActivitySnapshot, error) {
	if len(value) == 0 || len(value) > maximumSnapshotBytes {
		return planner.ActivitySnapshot{}, errors.New("Discordx activity snapshot has an invalid byte length")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var snapshot planner.ActivitySnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return planner.ActivitySnapshot{}, errors.New("Discordx activity snapshot is invalid JSON")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || snapshot.Sequence == 0 {
		return planner.ActivitySnapshot{}, errors.New("Discordx activity snapshot contains invalid trailing data or sequence")
	}
	return snapshot, nil
}

func maintainBridge(ctx context.Context, bridge *mythic.Bridge) {
	for ctx.Err() == nil {
		_ = bridge.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func waitForBridge(ctx context.Context, bridge *mythic.Bridge, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("Mythic Push C2 stream did not become ready")
		case <-ticker.C:
			if bridge.IsRunning() {
				return nil
			}
		}
	}
}

func buildBotGroups(ctx context.Context, manager *registry.Manager, store *state.Store, bridge *mythic.Bridge) (map[string]*botGroup, error) {
	type pendingGroup struct {
		client         *discord.Client
		provider       config.Provider
		egress         config.EgressProxy
		token          string
		botID          string
		bindings       []worker.ListenerBinding
		generations    []generationKey
		configurations []groupConfiguration
	}
	pending := make(map[string]*pendingGroup)
	for _, listener := range manager.Listeners() {
		if !listener.Enabled {
			continue
		}
		for _, generation := range listener.Generations {
			if generation.State != registry.Active && generation.State != registry.Draining {
				continue
			}
			client, err := discord.NewClient(generation.Provider, listener.Egress, generation.DiscordToken, generation.Provider.TestOnlyAllowInsecureTransport)
			if err != nil {
				return nil, errors.New("construct Discord provider client failed")
			}
			validationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			identity, err := client.ValidateCredential(validationCtx)
			cancel()
			if err != nil {
				log.Printf("Discord worker degraded: listener=%s generation=%s class=credential_validation_failed", listener.ID, generation.ID)
				continue
			}
			if generation.BotUserID != "" && generation.BotUserID != identity.ID {
				log.Printf("Discord worker degraded: listener=%s generation=%s class=credential_identity_changed", listener.ID, generation.ID)
				continue
			}
			groupKey := generation.Provider.ID + "\x00" + identity.ID
			current := pending[groupKey]
			if current == nil {
				current = &pendingGroup{
					client: client, provider: generation.Provider, egress: listener.Egress,
					token: generation.DiscordToken, botID: identity.ID,
				}
				pending[groupKey] = current
			} else if current.egress != listener.Egress {
				return nil, errors.New("listeners authenticated as one bot have conflicting egress")
			}
			if generation.State == registry.Active {
				pollEvery := time.Duration(listener.Ingress.PollIntervalSeconds) * time.Second
				if listener.Ingress.PollStrategy == "adaptive" {
					pollEvery = time.Duration(listener.Ingress.PollBaseIntervalSeconds) * time.Second
				}
				current.bindings = append(current.bindings, worker.ListenerBinding{
					ListenerID: listener.ID, GenerationID: generation.ID,
					TaskChannel: generation.TaskChannelID, SocksChannel: generation.SocksChannelID,
					IngressMode: listener.Ingress.Mode, PollEvery: pollEvery, Ingress: listener.Ingress,
				})
			}
			routeKey := generationKey{listener: listener.ID, generation: generation.ID}
			current.generations = append(current.generations, routeKey)
			current.configurations = append(current.configurations, groupConfiguration{
				ListenerID: listener.ID, Ingress: listener.Ingress, Egress: listener.Egress, Generation: generation,
			})
		}
	}

	groups := make(map[string]*botGroup, len(pending))
	for key, desired := range pending {
		queue, err := scheduler.New(scheduler.Config{
			MaxListeners: len(desired.bindings), StandardDepth: 128, SocksDepth: 128,
			CleanupDepth: 1024, RecoveryDepth: 5000,
		})
		if err != nil {
			return nil, err
		}
		processor, err := worker.NewProcessor(manager, store, desired.client, bridgeAdapter{bridge})
		if err != nil {
			return nil, err
		}
		runtime, err := worker.NewBotRuntime(worker.BotSpec{
			Provider: desired.provider, Token: desired.token, BotUserID: desired.botID,
			Listeners: desired.bindings,
		}, store, desired.client, processor)
		if err != nil {
			return nil, err
		}
		groups[key] = &botGroup{
			client: desired.client, scheduler: queue, runtime: runtime,
			botUserID:     desired.botID,
			configuration: append([]groupConfiguration(nil), desired.configurations...),
			generations:   append([]generationKey(nil), desired.generations...),
		}
	}
	return groups, nil
}

func (controller *runtimeController) Apply(snapshot registry.Snapshot) error {
	if snapshot.Revision <= controller.manager.Revision() {
		return nil
	}
	candidateManager := registry.NewManager()
	if _, err := candidateManager.ApplySnapshot(controller.ctx, snapshot); err != nil {
		return fmt.Errorf("registry snapshot rejected: %w", err)
	}
	candidates, err := buildBotGroups(controller.ctx, candidateManager, controller.store, controller.bridge)
	if err != nil {
		return err
	}

	controller.mu.RLock()
	current := make(map[string]*botGroup, len(controller.groups))
	for key, group := range controller.groups {
		current[key] = group
	}
	controller.mu.RUnlock()

	for key, candidate := range candidates {
		if existing, ok := current[key]; ok && equalGroupConfigurations(existing.configuration, candidate.configuration) {
			candidates[key] = existing
		}
	}
	started := make([]*botGroup, 0)
	for key, group := range candidates {
		if existing, reused := current[key]; reused && existing == group {
			continue
		}
		groupCtx, cancel := context.WithCancel(controller.ctx)
		group.cancel = cancel
		if err := group.runtime.Start(groupCtx); err != nil {
			cancel()
			for _, active := range started {
				controller.drainGroup(active)
			}
			return errors.New("start Discord bot worker failed")
		}
		go sendLoop(groupCtx, controller.bridge, group)
		started = append(started, group)
	}
	controller.mu.Lock()
	if _, err := controller.manager.ApplySnapshot(controller.ctx, snapshot); err != nil {
		controller.mu.Unlock()
		for _, active := range started {
			controller.drainGroup(active)
		}
		return fmt.Errorf("publish registry snapshot: %w", err)
	}
	generationGroups := make(map[generationKey]*botGroup)
	for _, group := range candidates {
		for _, generation := range group.generations {
			generationGroups[generation] = group
		}
	}
	controller.groups = candidates
	controller.generationGroups = generationGroups
	controller.mu.Unlock()
	for key, existing := range current {
		if candidates[key] != existing {
			controller.drainGroup(existing)
		}
	}
	return nil
}

func equalGroupConfigurations(left, right []groupConfiguration) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (controller *runtimeController) watchRegistry() {
	for {
		select {
		case <-controller.ctx.Done():
			return
		case payload := <-controller.bridge.RegistrySnapshots():
			snapshot, err := decodeSnapshot(payload)
			if err != nil {
				log.Printf("Discordx registry update rejected: invalid snapshot")
				continue
			}
			if err := controller.Apply(snapshot); err != nil {
				log.Printf("Discordx registry update rejected: %v", err)
				continue
			}
			log.Printf("Discordx registry applied: revision=%d listeners=%d workers=%d", snapshot.Revision, len(snapshot.Listeners), controller.workerCount())
		case payload := <-controller.bridge.ActivitySnapshots():
			snapshot, err := decodeActivitySnapshot(payload)
			if err != nil {
				log.Printf("Discordx activity update rejected: invalid snapshot")
				continue
			}
			if err := controller.applyActivitySnapshot(snapshot); err != nil {
				log.Printf("Discordx activity update rejected: invalid callback state")
			}
		}
	}
}

func (controller *runtimeController) applyActivitySnapshot(snapshot planner.ActivitySnapshot) error {
	controller.mu.RLock()
	partitioned := partitionActivitySnapshots(snapshot, controller.groups)
	controller.mu.RUnlock()
	for group, local := range partitioned {
		if err := group.runtime.ApplyActivitySnapshot(local); err != nil {
			return err
		}
	}
	return nil
}

func partitionActivitySnapshots(snapshot planner.ActivitySnapshot, groups map[string]*botGroup) map[*botGroup]planner.ActivitySnapshot {
	partitioned := make(map[*botGroup]planner.ActivitySnapshot, len(groups))
	owners := make(map[string]*botGroup)
	for _, group := range groups {
		partitioned[group] = planner.ActivitySnapshot{Sequence: snapshot.Sequence}
		for _, current := range group.configuration {
			owners[current.ListenerID] = group
		}
	}
	for _, callback := range snapshot.Callbacks {
		group := owners[callback.ListenerID]
		if group == nil {
			continue
		}
		local := partitioned[group]
		local.Callbacks = append(local.Callbacks, callback)
		partitioned[group] = local
	}
	return partitioned
}

func (controller *runtimeController) delivery(outbound mythic.Outbound) (worker.Delivery, *botGroup, error) {
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	encoder, _ := worker.NewOutboundEncoder(controller.manager)
	delivery, err := encoder.Encode(outbound)
	if err != nil {
		return worker.Delivery{}, nil, err
	}
	group := controller.generationGroups[generationKey{listener: delivery.ListenerID, generation: delivery.GenerationID}]
	return delivery, group, nil
}

func (controller *runtimeController) workerCount() int {
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return len(controller.groups)
}

func (controller *runtimeController) publishHealth() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		payload, err := controller.healthSnapshot(time.Now().UTC())
		if err == nil {
			sendCtx, cancel := context.WithTimeout(controller.ctx, 5*time.Second)
			_ = controller.bridge.SendHealth(sendCtx, payload)
			cancel()
		}
		select {
		case <-controller.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (controller *runtimeController) healthSnapshot(now time.Time) ([]byte, error) {
	configured := controller.manager.Status()
	controller.mu.RLock()
	groups := make(map[string]*botGroup)
	for _, group := range controller.groups {
		for _, current := range group.configuration {
			groups[current.ListenerID] = group
		}
	}
	controller.mu.RUnlock()
	snapshot := runtimeHealthSnapshot{
		Revision: configured.Revision, ObservedAt: now,
		Listeners: make([]runtimeListenerHealth, 0, len(configured.Listeners)),
	}
	for _, listener := range configured.Listeners {
		health := runtimeListenerHealth{
			ListenerID: listener.ListenerID, OperationID: listener.OperationID,
			State: "degraded", ErrorClass: "worker_unavailable",
		}
		group := groups[listener.ListenerID]
		if group != nil {
			health.BotUserID = group.botUserID
			queue := group.scheduler.Status(listener.ListenerID)
			health.StandardQueueDepth = queue.StandardDepth
			health.SocksQueueDepth = queue.SocksDepth
			health.QueueSaturationCount = queue.SaturationCount
			health.AccumulatedQueueWaitMS = queue.AccumulatedWait.Milliseconds()
			status, statusErr := group.runtime.Status(listener.ListenerID, now)
			if statusErr == nil {
				health.State = status.State
				health.NextPollAt = status.NextPollAt
				health.EffectiveIntervalSecond = status.EffectiveIntervalSeconds
				health.SchedulingReason = status.SchedulingReason
				health.ErrorClass = ""
			}
		}
		snapshot.Listeners = append(snapshot.Listeners, health)
	}
	return json.Marshal(snapshot)
}

func (controller *runtimeController) drainGroup(group *botGroup) {
	if group == nil {
		return
	}
	if group.cancel != nil {
		group.cancel()
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = group.runtime.Drain(drainCtx)
}

func (controller *runtimeController) Drain() {
	controller.mu.RLock()
	groups := make([]*botGroup, 0, len(controller.groups))
	for _, group := range controller.groups {
		groups = append(groups, group)
	}
	controller.mu.RUnlock()
	for _, group := range groups {
		controller.drainGroup(group)
	}
}

func outboundLoop(ctx context.Context, bridge *mythic.Bridge, controller *runtimeController) {
	for {
		select {
		case <-ctx.Done():
			return
		case outbound := <-bridge.Outbound():
			delivery, group, err := controller.delivery(outbound)
			if err != nil {
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
				continue
			}
			if group == nil {
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
				continue
			}
			lane := scheduler.Standard
			if delivery.Lane == worker.Socks {
				lane = scheduler.Socks
			}
			enqueueCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err = group.scheduler.Enqueue(enqueueCtx, scheduler.Item{ListenerID: delivery.ListenerID, Lane: lane, Value: delivery})
			cancel()
			if err != nil {
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
			} else {
				group.runtime.ObserveOutbound(delivery.ListenerID)
			}
		}
	}
}

func sendLoop(ctx context.Context, bridge *mythic.Bridge, group *botGroup) {
	for {
		item, err := group.scheduler.Next(ctx)
		if err != nil {
			return
		}
		delivery, ok := item.Value.(worker.Delivery)
		if !ok {
			continue
		}
		sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err = group.client.Send(sendCtx, delivery.ChannelID, delivery.Document)
		cancel()
		_ = bridge.SendOutboundReceipt(ctx, delivery.OutboundID, err == nil)
	}
}
