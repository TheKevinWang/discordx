package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type scaleEntry struct {
	Identity     syntheticIdentity
	ListenerID   string
	GenerationID string
}

type resourceSample struct {
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes    uint64 `json:"heap_sys_bytes"`
	RSSBytes        uint64 `json:"rss_bytes,omitempty"`
	Goroutines      int    `json:"goroutines"`
	FileDescriptors int    `json:"file_descriptors,omitempty"`
}

type durationReport struct {
	RegistryApplyMS        int64 `json:"registry_apply_ms"`
	CredentialValidationMS int64 `json:"credential_validation_ms"`
	WorkerReadyMS          int64 `json:"worker_ready_ms"`
	ActivityApplyMS        int64 `json:"activity_apply_ms"`
	CallbackRoundTripMS    int64 `json:"callback_round_trip_ms"`
	SettleMS               int64 `json:"settle_ms"`
	CleanupMS              int64 `json:"cleanup_ms"`
	TotalMS                int64 `json:"total_ms"`
}

type pollScheduleReport struct {
	BaseIntervalMS         int64 `json:"base_interval_ms"`
	JitterPercent          int   `json:"jitter_percent"`
	MinEffectiveMS         int64 `json:"min_effective_interval_ms"`
	MaxEffectiveMS         int64 `json:"max_effective_interval_ms"`
	PlannedFirstPollMS     int64 `json:"planned_first_poll_ms"`
	PlannedLastPollMS      int64 `json:"planned_last_poll_ms"`
	PlannedDispatchSpanMS  int64 `json:"planned_dispatch_span_ms"`
	ObservedFirstPollMS    int64 `json:"observed_first_poll_ms"`
	ObservedLastPollMS     int64 `json:"observed_last_poll_ms"`
	ObservedDispatchSpanMS int64 `json:"observed_dispatch_span_ms"`
}

type loadReport struct {
	Passed                     bool               `json:"passed"`
	ErrorClass                 string             `json:"error_class,omitempty"`
	Bots                       int                `json:"bots"`
	Listeners                  int                `json:"listeners"`
	Callbacks                  int                `json:"callbacks"`
	Command                    string             `json:"command"`
	StateBackend               string             `json:"state_backend"`
	CallbacksApplied           int                `json:"callbacks_applied"`
	CommandsQueued             int64              `json:"commands_queued"`
	CallbackPolls              int64              `json:"callback_polls"`
	CommandsIssued             int64              `json:"commands_issued"`
	CommandsExecuted           int64              `json:"commands_executed"`
	CommandResults             int64              `json:"command_results"`
	SuccessfulOutboundReceipts int64              `json:"successful_outbound_receipts"`
	IngressMessagesProcessed   int64              `json:"ingress_messages_processed"`
	ActiveGatewaysAfterCleanup int64              `json:"active_gateways_after_cleanup"`
	Clone                      cloneCounters      `json:"clone"`
	Mythic                     mythicCounters     `json:"mythic"`
	PollSchedule               pollScheduleReport `json:"poll_schedule"`
	Durations                  durationReport     `json:"durations"`
	Baseline                   resourceSample     `json:"baseline"`
	Loaded                     resourceSample     `json:"loaded"`
	Final                      resourceSample     `json:"final"`
}

type countingProcessor struct {
	inner    *worker.Processor
	counters *processorCounters
}

type processorCounters struct {
	processed atomic.Int64
	errors    atomic.Int64
}

func (processor *countingProcessor) Process(ctx context.Context, providerID string, message discord.Message) (worker.Result, error) {
	result, err := processor.inner.Process(ctx, providerID, message)
	if result.Accepted && !result.Duplicate {
		processor.counters.processed.Add(1)
	}
	if err != nil && !errors.Is(err, worker.ErrCleanupPending) {
		processor.counters.errors.Add(1)
	}
	return result, err
}

type loadBot struct {
	entry   scaleEntry
	client  *discord.Client
	queue   *scheduler.Scheduler
	runtime *worker.BotRuntime
}

type loadStateStore interface {
	worker.StateStore
	Cursor(context.Context, state.ChannelKey) (string, error)
}

type loadFailure struct {
	mu  sync.Mutex
	err error
}

func (failure *loadFailure) Record(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	failure.mu.Lock()
	if failure.err == nil {
		failure.err = err
	}
	failure.mu.Unlock()
}

func (failure *loadFailure) Err() error {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	return failure.err
}

func runLoadTest(ctx context.Context, load loadConfig) (loadReport, error) {
	started := time.Now()
	report := loadReport{
		Bots: load.Bots, Listeners: load.Bots, Callbacks: load.Callbacks,
		Command: load.Command, StateBackend: load.StateBackend, Baseline: sampleResources(),
	}
	entries := makeScaleEntries(load.Bots)
	plans, err := makeCallbackPlans(entries, load.Callbacks, load.Command, load.PollInterval, load.JitterPercent)
	if err != nil {
		return report, err
	}
	report.PollSchedule = summarizePollSchedule(plans, load)
	identities := make([]syntheticIdentity, len(entries))
	for index, entry := range entries {
		identities[index] = entry.Identity
	}
	clone, err := newDiscordClone(ctx, identities)
	if err != nil {
		return report, err
	}
	cloneClosed := false
	defer func() {
		if !cloneClosed {
			clone.Close()
		}
	}()

	provider, err := (config.Provider{
		Kind: "mock", APIBaseURL: clone.APIOrigin(), GatewayBaseURL: clone.GatewayOrigin(),
		CDNBaseURL: clone.CDNOrigin(), APIVersion: 10, TestOnlyAllowInsecureTransport: true,
	}).Normalize(true)
	if err != nil {
		return report, err
	}

	registryStarted := time.Now()
	manager := registry.NewManager()
	result, err := manager.ApplySnapshot(ctx, makeScaleSnapshot(entries, provider))
	report.Durations.RegistryApplyMS = time.Since(registryStarted).Milliseconds()
	if err != nil {
		return report, fmt.Errorf("registry apply failed: %w", err)
	}
	if !result.Applied || result.StartedWorkers != load.Bots {
		return report, errors.New("registry did not publish every synthetic worker")
	}

	var store loadStateStore
	closeStore := func() error { return nil }
	cleanupStore := func() {}
	if load.StateBackend == "sqlite" {
		stateDirectory, directoryErr := os.MkdirTemp("", "discordx-callback-load-")
		if directoryErr != nil {
			return report, errors.New("create temporary recovery directory failed")
		}
		cleanupStore = func() { _ = os.RemoveAll(stateDirectory) }
		journalLimit := 2*maximumCallbacksPerListener + 8
		durableStore, storeErr := state.Open(filepath.Join(stateDirectory, "state.db"), journalLimit)
		if storeErr != nil {
			cleanupStore()
			return report, errors.New("open temporary recovery store failed")
		}
		store = durableStore
		closeStore = durableStore.Close
	} else {
		store = newMemoryCallbackStore()
	}
	defer cleanupStore()
	storeClosed := false
	defer func() {
		if !storeClosed {
			_ = closeStore()
		}
	}()

	mythicClone, err := newSyntheticMythic(plans)
	if err != nil {
		return report, err
	}
	bridge, err := mythic.NewBridge(mythicClone, 4096, 4096)
	if err != nil {
		return report, err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	failures := &loadFailure{}
	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		bridgeErr := bridge.Run(runCtx)
		if runCtx.Err() == nil {
			failures.Record(fmt.Errorf("Mythic bridge stopped: %w", bridgeErr))
		}
	}()
	if err := waitForHealthy(ctx, "Mythic bridge startup", failures, func() (bool, error) {
		return bridge.IsRunning(), nil
	}); err != nil {
		cancelRun()
		background.Wait()
		return report, err
	}

	clients := make([]*discord.Client, 0, load.Bots)
	credentialStarted := time.Now()
	for _, entry := range entries {
		client, clientErr := discord.NewClient(provider, config.EgressProxy{Mode: "direct"}, entry.Identity.Token, true)
		if clientErr != nil {
			cancelRun()
			background.Wait()
			return report, errors.New("construct synthetic Discord client failed")
		}
		identity, validationErr := client.ValidateCredential(ctx)
		if validationErr != nil || identity.ID != entry.Identity.BotID {
			cancelRun()
			background.Wait()
			return report, errors.New("synthetic credential validation failed")
		}
		clients = append(clients, client)
	}
	report.Durations.CredentialValidationMS = time.Since(credentialStarted).Milliseconds()

	processor := &processorCounters{}
	bots := make([]*loadBot, 0, load.Bots)
	botByGeneration := make(map[string]*loadBot, load.Bots)
	for index, entry := range entries {
		inner, processorErr := worker.NewProcessor(manager, store, clients[index], loadBridgeAdapter{bridge: bridge})
		if processorErr != nil {
			cancelRun()
			background.Wait()
			return report, errors.New("construct production message processor failed")
		}
		counted := &countingProcessor{inner: inner, counters: processor}
		queue, queueErr := scheduler.New(scheduler.Config{
			MaxListeners: 1, StandardDepth: 128, SocksDepth: 128,
			CleanupDepth: 1024, RecoveryDepth: 5000,
		})
		if queueErr != nil {
			cancelRun()
			background.Wait()
			return report, errors.New("construct production scheduler failed")
		}
		runtimeValue, runtimeErr := worker.NewBotRuntime(worker.BotSpec{
			Provider: provider, Token: entry.Identity.Token, BotUserID: entry.Identity.BotID,
			Listeners: []worker.ListenerBinding{{
				ListenerID: entry.ListenerID, GenerationID: entry.GenerationID,
				TaskChannel: entry.Identity.ChannelID, IngressMode: "gateway", Ingress: scaleIngress(),
			}},
		}, store, clients[index], counted)
		if runtimeErr != nil {
			cancelRun()
			background.Wait()
			return report, errors.New("construct synthetic bot runtime failed")
		}
		bot := &loadBot{entry: entry, client: clients[index], queue: queue, runtime: runtimeValue}
		bots = append(bots, bot)
		botByGeneration[entry.ListenerID+"\x00"+entry.GenerationID] = bot
	}

	encoder, err := worker.NewOutboundEncoder(manager)
	if err != nil {
		cancelRun()
		background.Wait()
		return report, err
	}
	background.Add(1)
	go func() {
		defer background.Done()
		runOutboundDispatcher(runCtx, bridge, encoder, botByGeneration, failures)
	}()
	for _, bot := range bots {
		background.Add(1)
		go func(current *loadBot) {
			defer background.Done()
			runLoadSendLoop(runCtx, bridge, current, failures)
		}(bot)
	}

	workerStarted := time.Now()
	runtimes := make([]*worker.BotRuntime, 0, len(bots))
	cleanupComplete := false
	defer func() {
		if !cleanupComplete {
			cancelRun()
			_ = drainRuntimes(runtimes, 30*time.Second)
			background.Wait()
		}
	}()
	for _, bot := range bots {
		if startErr := bot.runtime.Start(runCtx); startErr != nil {
			return report, errors.New("start synthetic bot runtime failed")
		}
		runtimes = append(runtimes, bot.runtime)
	}
	if err := waitForHealthy(ctx, "bot workers", failures, func() (bool, error) {
		counters := clone.Counters()
		if counters.GatewayConnections > int64(load.Bots) || counters.RouteErrors != 0 {
			return false, errors.New("Discord clone observed an unexpected Gateway or route count")
		}
		return counters.ActiveGateways == int64(load.Bots), nil
	}); err != nil {
		captureLifecycleReport(&report, clone, mythicClone, processor)
		return report, err
	}
	report.Durations.WorkerReadyMS = time.Since(workerStarted).Milliseconds()

	activityStarted := time.Now()
	hints := partitionActivityHints(entries, plans, load, time.Now().UTC())
	applied := 0
	for index, bot := range bots {
		local := planner.ActivitySnapshot{Sequence: 1, Callbacks: hints[index]}
		if applyErr := bot.runtime.ApplyActivitySnapshot(local); applyErr != nil {
			return report, errors.New("apply synthetic callback activity failed")
		}
		applied += len(local.Callbacks)
		status, statusErr := bot.runtime.Status(entries[index].ListenerID, time.Now().UTC())
		if statusErr != nil || status.State != "active" {
			return report, errors.New("synthetic bot runtime health is not active")
		}
	}
	report.CallbacksApplied = applied
	report.Durations.ActivityApplyMS = time.Since(activityStarted).Milliseconds()
	if applied != load.Callbacks {
		return report, errors.New("synthetic callback activity count mismatch")
	}

	expectedCallbacks := int64(load.Callbacks)
	expectedIngress := 2 * expectedCallbacks
	if mythicClone.Counters().CommandsQueued != expectedCallbacks {
		captureLifecycleReport(&report, clone, mythicClone, processor)
		return report, errors.New("mass command queue did not contain every callback")
	}
	roundTripStarted := time.Now()
	if err := clone.ReleaseCallbacks(); err != nil {
		return report, err
	}
	if err := waitForHealthy(ctx, "callback command round trips", failures, func() (bool, error) {
		mythicStatus := mythicClone.Counters()
		cloneStatus := clone.Counters()
		if mythicStatus.CommandsQueued != expectedCallbacks || mythicStatus.CallbackPolls > expectedCallbacks ||
			mythicStatus.CommandsIssued > expectedCallbacks ||
			mythicStatus.CommandResults > expectedCallbacks || mythicStatus.IngressReceiptsIssued > expectedIngress ||
			mythicStatus.SuccessfulOutboundReceipts > expectedCallbacks || mythicStatus.FailedOutboundReceipts != 0 ||
			mythicStatus.ProtocolErrors != 0 || cloneStatus.CommandsExecuted > expectedCallbacks ||
			cloneStatus.MessagesPosted > expectedCallbacks || cloneStatus.MessagesDeleted > expectedIngress ||
			cloneStatus.GatewayMessages > expectedIngress || cloneStatus.PollsEmitted > expectedCallbacks ||
			cloneStatus.RouteErrors != 0 ||
			processor.errors.Load() != 0 || processor.processed.Load() > expectedIngress {
			return false, errors.New("callback lifecycle counter exceeded its exact bound")
		}
		complete := mythicStatus.CallbackPolls == expectedCallbacks && cloneStatus.PollsEmitted == expectedCallbacks &&
			mythicStatus.CommandsIssued == expectedCallbacks && mythicStatus.CommandResults == expectedCallbacks &&
			mythicStatus.IngressReceiptsIssued == expectedIngress &&
			mythicStatus.SuccessfulOutboundReceipts == expectedCallbacks &&
			cloneStatus.CommandsExecuted == expectedCallbacks && cloneStatus.MessagesPosted == expectedCallbacks &&
			cloneStatus.MessagesDeleted == expectedIngress && cloneStatus.GatewayMessages == expectedIngress &&
			processor.processed.Load() == expectedIngress
		return complete, nil
	}); err != nil {
		captureLifecycleReport(&report, clone, mythicClone, processor)
		return report, err
	}
	report.Durations.CallbackRoundTripMS = time.Since(roundTripStarted).Milliseconds()

	settleStarted := time.Now()
	if err := waitDuration(ctx, load.Settle); err != nil {
		return report, err
	}
	report.Durations.SettleMS = time.Since(settleStarted).Milliseconds()
	report.Loaded = sampleResources()

	cleanupStarted := time.Now()
	cancelRun()
	if err := drainRuntimes(runtimes, 30*time.Second); err != nil {
		return report, err
	}
	background.Wait()
	if err := waitFor(ctx, "Gateway cleanup", func() bool {
		return clone.Counters().ActiveGateways == 0
	}); err != nil {
		return report, err
	}
	cleanupComplete = true
	if err := closeStore(); err != nil {
		return report, errors.New("close temporary recovery store failed")
	}
	storeClosed = true
	report.Durations.CleanupMS = time.Since(cleanupStarted).Milliseconds()
	captureLifecycleReport(&report, clone, mythicClone, processor)
	clone.Close()
	cloneClosed = true
	_ = waitDuration(context.Background(), 50*time.Millisecond)
	report.Final = sampleResources()
	report.Durations.TotalMS = time.Since(started).Milliseconds()
	if err := validateFinalReport(report); err != nil {
		return report, err
	}
	report.Passed = true
	return report, nil
}

func captureLifecycleReport(report *loadReport, clone *discordClone, mythicClone *syntheticMythic, processor *processorCounters) {
	report.Clone = clone.Counters()
	report.Mythic = mythicClone.Counters()
	report.CommandsQueued = report.Mythic.CommandsQueued
	report.CallbackPolls = report.Mythic.CallbackPolls
	report.CommandsIssued = report.Mythic.CommandsIssued
	report.CommandsExecuted = report.Clone.CommandsExecuted
	report.CommandResults = report.Mythic.CommandResults
	report.SuccessfulOutboundReceipts = report.Mythic.SuccessfulOutboundReceipts
	report.IngressMessagesProcessed = processor.processed.Load()
	report.ActiveGatewaysAfterCleanup = report.Clone.ActiveGateways
	report.PollSchedule.ObservedFirstPollMS = report.Clone.FirstPollOffsetMS
	report.PollSchedule.ObservedLastPollMS = report.Clone.LastPollOffsetMS
	if report.Clone.LastPollOffsetMS >= report.Clone.FirstPollOffsetMS {
		report.PollSchedule.ObservedDispatchSpanMS = report.Clone.LastPollOffsetMS - report.Clone.FirstPollOffsetMS
	}
}

func validateFinalReport(report loadReport) error {
	expectedCallbacks := int64(report.Callbacks)
	expectedIngress := 2 * expectedCallbacks
	if report.Clone.CredentialRequests != int64(report.Bots) || report.Clone.HistoryRequests != int64(report.Bots) ||
		report.Clone.GatewayConnections != int64(report.Bots) || report.CommandsQueued != expectedCallbacks ||
		report.CallbackPolls != expectedCallbacks || report.Clone.PollsEmitted != expectedCallbacks ||
		report.CommandsIssued != expectedCallbacks || report.CommandsExecuted != expectedCallbacks ||
		report.CommandResults != expectedCallbacks || report.SuccessfulOutboundReceipts != expectedCallbacks ||
		report.IngressMessagesProcessed != expectedIngress || report.Clone.MessagesDeleted != expectedIngress ||
		report.Clone.MessagesPosted != expectedCallbacks || report.Clone.GatewayMessages != expectedIngress ||
		report.Mythic.IngressReceiptsIssued != expectedIngress || report.ActiveGatewaysAfterCleanup != 0 ||
		report.Clone.RouteErrors != 0 || report.Mythic.ProtocolErrors != 0 || report.Mythic.FailedOutboundReceipts != 0 {
		return errors.New("load-test completion counters did not match the requested callback lifecycle")
	}
	return nil
}

func runOutboundDispatcher(ctx context.Context, bridge *mythic.Bridge, encoder *worker.OutboundEncoder, bots map[string]*loadBot, failures *loadFailure) {
	for {
		select {
		case <-ctx.Done():
			return
		case outbound := <-bridge.Outbound():
			delivery, err := encoder.Encode(outbound)
			if err != nil {
				failures.Record(errors.New("encode synthetic outbound failed"))
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
				return
			}
			bot := bots[delivery.ListenerID+"\x00"+delivery.GenerationID]
			if bot == nil {
				failures.Record(errors.New("synthetic outbound route has no bot worker"))
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
				return
			}
			lane := scheduler.Standard
			if delivery.Lane == worker.Socks {
				lane = scheduler.Socks
			}
			if err := bot.queue.Enqueue(ctx, scheduler.Item{ListenerID: delivery.ListenerID, Lane: lane, Value: delivery}); err != nil {
				failures.Record(errors.New("enqueue synthetic outbound failed"))
				_ = bridge.SendOutboundReceipt(ctx, outbound.ID, false)
				return
			}
			bot.runtime.ObserveOutbound(delivery.ListenerID)
		}
	}
}

func runLoadSendLoop(ctx context.Context, bridge *mythic.Bridge, bot *loadBot, failures *loadFailure) {
	for {
		item, err := bot.queue.Next(ctx)
		if err != nil {
			return
		}
		delivery, ok := item.Value.(worker.Delivery)
		if !ok {
			failures.Record(errors.New("synthetic scheduler returned an invalid delivery"))
			return
		}
		_, sendErr := bot.client.Send(ctx, delivery.ChannelID, delivery.Document)
		receiptErr := bridge.SendOutboundReceipt(ctx, delivery.OutboundID, sendErr == nil)
		if sendErr != nil {
			failures.Record(errors.New("send synthetic callback task failed"))
			return
		}
		if receiptErr != nil {
			failures.Record(errors.New("send synthetic outbound receipt failed"))
			return
		}
	}
}

func makeScaleEntries(count int) []scaleEntry {
	entries := make([]scaleEntry, count)
	for index := range entries {
		value := index + 1
		entries[index] = scaleEntry{
			Identity: syntheticIdentity{
				Token: fmt.Sprintf("synthetic-load-token-%06d", value),
				BotID: fmt.Sprintf("9%017d", value), ChannelID: fmt.Sprintf("8%017d", value),
				Wire: scaleWire(),
			},
			ListenerID: syntheticUUID(value), GenerationID: syntheticUUID(value + maximumLoadBots),
		}
	}
	return entries
}

func syntheticUUID(value int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", value, value)
}

func scaleWire() config.Wire {
	return config.Wire{
		Protocol: "fixed", EnvelopeFormat: "binary-v1", Presentation: "base64",
		Protection: "none", KeyMode: "single",
	}
}

func makeScaleSnapshot(entries []scaleEntry, provider config.Provider) registry.Snapshot {
	listeners := make([]registry.Listener, 0, len(entries))
	for index, entry := range entries {
		listeners = append(listeners, registry.Listener{
			ID: entry.ListenerID, Name: fmt.Sprintf("load-listener-%d", index+1),
			OperationID: 1, Enabled: true, ActiveGenerationID: entry.GenerationID,
			Ingress: scaleIngress(), Egress: config.EgressProxy{Mode: "direct"},
			Generations: []registry.Generation{{
				ID: entry.GenerationID, State: registry.Active,
				DiscordToken: entry.Identity.Token, BotUserID: entry.Identity.BotID,
				TaskChannelID: entry.Identity.ChannelID, Provider: provider, Wire: scaleWire(),
			}},
		})
	}
	return registry.Snapshot{
		ProfileName: "discordx", Revision: 1, MaxListeners: len(entries),
		MaxBotWorkers: len(entries), TestMode: true, Listeners: listeners,
	}
}

func scaleIngress() config.Ingress {
	return config.Ingress{
		Mode: "gateway", PollStrategy: "adaptive", PollIntervalSeconds: 15,
		PollMinIntervalSeconds: 2, PollBaseIntervalSeconds: 15,
		PollMaxIntervalSeconds: 300, PollBuildWarmSeconds: 900,
		ReconciliationIntervalSeconds: 0,
	}
}

func partitionActivityHints(entries []scaleEntry, plans []callbackPlan, load loadConfig, now time.Time) [][]planner.CallbackHint {
	result := make([][]planner.CallbackHint, len(entries))
	for index, plan := range plans {
		entryIndex := index % len(entries)
		result[entryIndex] = append(result[entryIndex], planner.CallbackHint{
			ListenerID: entries[entryIndex].ListenerID, CallbackID: plan.CallbackID, Active: true,
			LastCheckin: now, IntervalSeconds: int(load.PollInterval / time.Second), JitterPercent: load.JitterPercent,
			MessageChecks: 10, TimeBetweenChecksSeconds: 10,
		})
	}
	return result
}

func summarizePollSchedule(plans []callbackPlan, load loadConfig) pollScheduleReport {
	report := pollScheduleReport{
		BaseIntervalMS: load.PollInterval.Milliseconds(), JitterPercent: load.JitterPercent,
	}
	if len(plans) == 0 {
		return report
	}
	minimumEffective := plans[0].EffectiveInterval
	maximumEffective := plans[0].EffectiveInterval
	minimumDue := plans[0].PollDueAfter
	maximumDue := plans[0].PollDueAfter
	for _, plan := range plans[1:] {
		if plan.EffectiveInterval < minimumEffective {
			minimumEffective = plan.EffectiveInterval
		}
		if plan.EffectiveInterval > maximumEffective {
			maximumEffective = plan.EffectiveInterval
		}
		if plan.PollDueAfter < minimumDue {
			minimumDue = plan.PollDueAfter
		}
		if plan.PollDueAfter > maximumDue {
			maximumDue = plan.PollDueAfter
		}
	}
	report.MinEffectiveMS = minimumEffective.Milliseconds()
	report.MaxEffectiveMS = maximumEffective.Milliseconds()
	report.PlannedFirstPollMS = minimumDue.Milliseconds()
	report.PlannedLastPollMS = maximumDue.Milliseconds()
	report.PlannedDispatchSpanMS = (maximumDue - minimumDue).Milliseconds()
	return report
}

func waitForHealthy(ctx context.Context, description string, failures *loadFailure, ready func() (bool, error)) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := failures.Err(); err != nil {
			return err
		}
		complete, err := ready()
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s: %w", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitFor(ctx context.Context, description string, ready func() bool) error {
	return waitForHealthy(ctx, description, &loadFailure{}, func() (bool, error) { return ready(), nil })
}

func waitDuration(ctx context.Context, duration time.Duration) error {
	if duration == 0 {
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

func drainRuntimes(runtimes []*worker.BotRuntime, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var wait sync.WaitGroup
	errorsFound := make(chan error, len(runtimes))
	for _, runtimeValue := range runtimes {
		wait.Add(1)
		go func(current *worker.BotRuntime) {
			defer wait.Done()
			if err := current.Drain(ctx); err != nil {
				errorsFound <- err
			}
		}(runtimeValue)
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			return errors.New("synthetic bot runtime cleanup failed")
		}
	}
	return nil
}

func sampleResources() resourceSample {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	sample := resourceSample{
		HeapAllocBytes: memory.HeapAlloc, HeapSysBytes: memory.HeapSys,
		Goroutines: runtime.NumGoroutine(),
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		sample.FileDescriptors = len(entries)
	}
	if value, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(value))
		if len(fields) >= 2 {
			if residentPages, parseErr := strconv.ParseUint(fields[1], 10, 64); parseErr == nil {
				sample.RSSBytes = residentPages * uint64(os.Getpagesize())
			}
		}
	}
	return sample
}
