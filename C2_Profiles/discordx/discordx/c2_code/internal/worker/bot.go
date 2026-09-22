package worker

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/discord"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/planner"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/state"
)

type BotProvider interface {
	HistoryProvider
	ListenGateway(context.Context, func(discord.Message) error) error
}

type BotRuntime struct {
	spec      BotSpec
	store     CursorStore
	provider  BotProvider
	processor MessageProcessor
	planner   *planner.Planner
	listeners map[string]struct{}

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

type ListenerRuntimeStatus struct {
	State                    string
	NextPollAt               time.Time
	EffectiveIntervalSeconds int64
	SchedulingReason         string
}

func NewBotRuntime(spec BotSpec, store CursorStore, provider BotProvider, processor MessageProcessor) (*BotRuntime, error) {
	if store == nil || provider == nil || processor == nil {
		return nil, errors.New("bot runtime dependencies are invalid")
	}
	pollPlanner := planner.New(10000, 100000)
	listeners := make(map[string]struct{}, len(spec.Listeners))
	for _, binding := range spec.Listeners {
		listeners[binding.ListenerID] = struct{}{}
		if binding.Ingress.Mode != "" {
			if err := pollPlanner.Configure(binding.ListenerID, binding.TaskChannel, binding.Ingress); err != nil {
				return nil, err
			}
		}
	}
	return &BotRuntime{
		spec: spec, store: store, provider: provider, processor: processor,
		planner: pollPlanner, listeners: listeners,
	}, nil
}

func (runtime *BotRuntime) Start(parent context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.started {
		return errors.New("bot runtime is already started")
	}
	if parent == nil {
		return errors.New("bot runtime context is required")
	}
	ctx, cancel := context.WithCancel(parent)
	runtime.cancel = cancel
	runtime.done = make(chan struct{})
	runtime.started = true
	go runtime.run(ctx)
	return nil
}

func (runtime *BotRuntime) Drain(ctx context.Context) error {
	runtime.mu.Lock()
	if !runtime.started {
		runtime.mu.Unlock()
		return nil
	}
	cancel, done := runtime.cancel, runtime.done
	runtime.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (runtime *BotRuntime) run(ctx context.Context) {
	defer close(runtime.done)
	for _, binding := range runtime.spec.Listeners {
		stats, err := runtime.catchUp(ctx, binding)
		runtime.observe(binding, stats, time.Now())
		if err != nil && ctx.Err() != nil {
			return
		}
	}
	var wait sync.WaitGroup
	if runtime.hasGateway() {
		wait.Add(1)
		go func() {
			defer wait.Done()
			runtime.gatewayLoop(ctx)
		}()
	}
	if runtime.hasPolling() {
		wait.Add(1)
		go func() {
			defer wait.Done()
			runtime.pollingLoop(ctx)
		}()
	}
	if runtime.hasReconciliation() {
		wait.Add(1)
		go func() {
			defer wait.Done()
			runtime.reconciliationLoop(ctx)
		}()
	}
	wait.Wait()
}

func (runtime *BotRuntime) catchUp(ctx context.Context, binding ListenerBinding) (RecoveryStats, error) {
	recoverer, err := NewRecoverer(runtime.store, runtime.provider, runtime.processor, RecoveryConfig{
		PageSize: 100, MaxMessages: 5000, TimeBudget: 2 * time.Minute,
	})
	if err != nil {
		return RecoveryStats{}, err
	}
	combined := RecoveryStats{}
	for _, channelID := range []string{binding.TaskChannel, binding.SocksChannel} {
		if channelID == "" {
			continue
		}
		stats, err := recoverer.CatchUp(ctx, state.ChannelKey{
			ProviderID: runtime.spec.Provider.ID, ListenerID: binding.ListenerID, ChannelID: channelID,
		})
		if err != nil && !errors.Is(err, ErrRecoveryBound) && !errors.Is(err, ErrRecoveryTime) {
			return combined, err
		}
		combined.Pages += stats.Pages
		combined.Observed += stats.Observed
		combined.Accepted += stats.Accepted
		combined.Duplicates += stats.Duplicates
		combined.Rejected += stats.Rejected
	}
	return combined, nil
}

func (runtime *BotRuntime) hasGateway() bool {
	for _, binding := range runtime.spec.Listeners {
		if bindingMode(binding) == "gateway" {
			return true
		}
	}
	return false
}

func (runtime *BotRuntime) hasPolling() bool {
	for _, binding := range runtime.spec.Listeners {
		if bindingMode(binding) == "polling" {
			return true
		}
	}
	return false
}

func (runtime *BotRuntime) gatewayLoop(ctx context.Context) {
	for ctx.Err() == nil {
		err := runtime.provider.ListenGateway(ctx, func(message discord.Message) error {
			binding, ok := runtime.bindingFor(message.ChannelID)
			if !ok || bindingMode(binding) != "gateway" {
				return nil
			}
			result, processErr := runtime.processor.Process(ctx, runtime.spec.Provider.ID, message)
			if result.Accepted {
				runtime.planner.Observe(planner.Observation{
					ListenerID: binding.ListenerID, At: time.Now(), FoundMessages: true,
				})
			}
			if errors.Is(processErr, ErrInvalidDocument) || errors.Is(processErr, ErrCleanupPending) {
				return nil
			}
			return processErr
		})
		if ctx.Err() != nil {
			return
		}
		for _, binding := range runtime.spec.Listeners {
			if bindingMode(binding) == "gateway" {
				stats, _ := runtime.catchUp(ctx, binding)
				runtime.observe(binding, stats, time.Now())
			}
		}
		if err == nil {
			continue
		}
		timer := time.NewTimer(time.Second)
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

type pollDeadline struct {
	binding ListenerBinding
	at      time.Time
}

func (runtime *BotRuntime) pollingLoop(ctx context.Context) {
	now := time.Now()
	deadlines := make([]pollDeadline, 0, len(runtime.spec.Listeners))
	for _, binding := range runtime.spec.Listeners {
		if bindingMode(binding) != "polling" {
			continue
		}
		deadline := runtime.nextPoll(binding, now)
		interval := deadline.Sub(now)
		deadline = deadline.Add(stagger(binding.TaskChannel, minDuration(interval/10, 5*time.Second)))
		deadlines = append(deadlines, pollDeadline{binding: binding, at: deadline})
	}
	for len(deadlines) > 0 {
		sort.Slice(deadlines, func(i, j int) bool { return deadlines[i].at.Before(deadlines[j].at) })
		wait := time.Until(deadlines[0].at)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		current := &deadlines[0]
		stats, _ := runtime.catchUp(ctx, current.binding)
		now = time.Now()
		runtime.observe(current.binding, stats, now)
		current.at = runtime.nextPoll(current.binding, now)
	}
}

func (runtime *BotRuntime) hasReconciliation() bool {
	for _, binding := range runtime.spec.Listeners {
		if bindingMode(binding) == "gateway" && binding.Ingress.ReconciliationIntervalSeconds > 0 {
			return true
		}
	}
	return false
}

func (runtime *BotRuntime) reconciliationLoop(ctx context.Context) {
	now := time.Now()
	deadlines := make([]pollDeadline, 0, len(runtime.spec.Listeners))
	for _, binding := range runtime.spec.Listeners {
		if bindingMode(binding) != "gateway" || binding.Ingress.ReconciliationIntervalSeconds <= 0 {
			continue
		}
		interval := time.Duration(binding.Ingress.ReconciliationIntervalSeconds) * time.Second
		deadlines = append(deadlines, pollDeadline{binding: binding, at: now.Add(interval).Add(stagger(binding.TaskChannel, minDuration(interval/10, 30*time.Second)))})
	}
	for len(deadlines) > 0 {
		sort.Slice(deadlines, func(i, j int) bool { return deadlines[i].at.Before(deadlines[j].at) })
		wait := time.Until(deadlines[0].at)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		current := &deadlines[0]
		stats, _ := runtime.catchUp(ctx, current.binding)
		runtime.observe(current.binding, stats, time.Now())
		current.at = time.Now().Add(time.Duration(current.binding.Ingress.ReconciliationIntervalSeconds) * time.Second)
	}
}

func (runtime *BotRuntime) nextPoll(binding ListenerBinding, now time.Time) time.Time {
	if binding.Ingress.Mode != "" {
		decision, err := runtime.planner.NextPoll(binding.ListenerID, binding.TaskChannel, now)
		if err == nil {
			return decision.Deadline
		}
	}
	interval := binding.PollEvery
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return now.Add(interval)
}

func (runtime *BotRuntime) observe(binding ListenerBinding, stats RecoveryStats, at time.Time) {
	if binding.Ingress.Mode == "" {
		return
	}
	runtime.planner.Observe(planner.Observation{
		ListenerID: binding.ListenerID, At: at,
		FoundMessages: stats.Observed > 0, EmptyPoll: stats.Observed == 0,
		PageFull: stats.Observed >= 5000,
	})
}

func (runtime *BotRuntime) ApplyActivitySnapshot(snapshot planner.ActivitySnapshot) error {
	filtered := planner.ActivitySnapshot{Sequence: snapshot.Sequence, Callbacks: make([]planner.CallbackHint, 0, len(snapshot.Callbacks))}
	for _, callback := range snapshot.Callbacks {
		if _, ok := runtime.listeners[callback.ListenerID]; ok {
			filtered.Callbacks = append(filtered.Callbacks, callback)
		}
	}
	return runtime.planner.ApplyActivitySnapshot(filtered)
}

func (runtime *BotRuntime) ObserveOutbound(listenerID string) {
	if _, ok := runtime.listeners[listenerID]; !ok {
		return
	}
	runtime.planner.Observe(planner.Observation{
		ListenerID: listenerID, At: time.Now(), OutboundResponse: true,
	})
}

func (runtime *BotRuntime) Status(listenerID string, now time.Time) (ListenerRuntimeStatus, error) {
	if now.IsZero() {
		return ListenerRuntimeStatus{}, errors.New("runtime status time is required")
	}
	runtime.mu.Lock()
	started := runtime.started
	runtime.mu.Unlock()
	if !started {
		return ListenerRuntimeStatus{State: "degraded", SchedulingReason: "worker_unavailable"}, nil
	}
	for _, binding := range runtime.spec.Listeners {
		if binding.ListenerID != listenerID {
			continue
		}
		if bindingMode(binding) == "gateway" {
			return ListenerRuntimeStatus{State: "active", SchedulingReason: "gateway"}, nil
		}
		decision, err := runtime.planner.NextPoll(binding.ListenerID, binding.TaskChannel, now)
		if err != nil {
			return ListenerRuntimeStatus{State: "degraded", SchedulingReason: "fixed_fallback"}, nil
		}
		seconds := int64(decision.Deadline.Sub(now).Round(time.Second) / time.Second)
		if seconds < 0 {
			seconds = 0
		}
		return ListenerRuntimeStatus{
			State: "active", NextPollAt: decision.Deadline,
			EffectiveIntervalSeconds: seconds, SchedulingReason: string(decision.Reason),
		}, nil
	}
	return ListenerRuntimeStatus{}, errors.New("runtime listener is unknown")
}

func bindingMode(binding ListenerBinding) string {
	if binding.Ingress.Mode != "" {
		return binding.Ingress.Mode
	}
	return binding.IngressMode
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func stagger(channelID string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(channelID))
	return time.Duration(hash.Sum64() % uint64(interval))
}

func (runtime *BotRuntime) bindingFor(channelID string) (ListenerBinding, bool) {
	for _, binding := range runtime.spec.Listeners {
		if binding.TaskChannel == channelID || binding.SocksChannel == channelID {
			return binding, true
		}
	}
	return ListenerBinding{}, false
}
