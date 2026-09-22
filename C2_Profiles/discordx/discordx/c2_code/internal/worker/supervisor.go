package worker

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

type ListenerBinding struct {
	ListenerID   string
	GenerationID string
	TaskChannel  string
	SocksChannel string
	IngressMode  string
	PollEvery    time.Duration
	Ingress      config.Ingress
}

type BotSpec struct {
	Provider  config.Provider
	Egress    config.EgressProxy
	Token     string
	BotUserID string
	Listeners []ListenerBinding
}

type Runtime interface {
	Start(context.Context) error
	Drain(context.Context) error
}

type RuntimeFactory interface {
	Build(BotSpec) (Runtime, error)
}

type BotStatus struct {
	WorkerID       string   `json:"worker_id"`
	ProviderID     string   `json:"provider_id"`
	BotUserID      string   `json:"bot_user_id,omitempty"`
	Egress         string   `json:"server_egress"`
	ListenerIDs    []string `json:"listener_ids"`
	GatewayEnabled bool     `json:"gateway_enabled"`
	State          string   `json:"state"`
	ErrorClass     string   `json:"error_class,omitempty"`
}

type managedRuntime struct {
	spec    BotSpec
	runtime Runtime
	status  BotStatus
}

type Supervisor struct {
	mu           sync.RWMutex
	reconcile    sync.Mutex
	factory      RuntimeFactory
	drainTimeout time.Duration
	workers      map[string]managedRuntime
	failed       map[string]BotStatus
}

func NewSupervisor(factory RuntimeFactory, drainTimeout time.Duration) (*Supervisor, error) {
	if factory == nil || drainTimeout <= 0 || drainTimeout > 5*time.Minute {
		return nil, errors.New("bot supervisor configuration is invalid")
	}
	return &Supervisor{
		factory: factory, drainTimeout: drainTimeout,
		workers: make(map[string]managedRuntime), failed: make(map[string]BotStatus),
	}, nil
}

func (supervisor *Supervisor) Reconcile(ctx context.Context, desired []BotSpec) error {
	supervisor.reconcile.Lock()
	defer supervisor.reconcile.Unlock()

	normalized := make(map[string]BotSpec, len(desired))
	for _, candidate := range desired {
		spec, key, err := normalizeBotSpec(candidate)
		if err != nil {
			return err
		}
		if _, duplicate := normalized[key]; duplicate {
			return errors.New("desired bot workers contain a duplicate identity")
		}
		normalized[key] = spec
	}

	supervisor.mu.RLock()
	current := make(map[string]managedRuntime, len(supervisor.workers))
	for key, worker := range supervisor.workers {
		current[key] = worker
	}
	supervisor.mu.RUnlock()

	next := make(map[string]managedRuntime, len(normalized))
	failed := make(map[string]BotStatus)
	for key, spec := range normalized {
		if existing, ok := current[key]; ok && equalBotSpecs(existing.spec, spec) {
			next[key] = existing
			continue
		}
		status := statusFor(key, spec, "starting", "")
		runtime, err := supervisor.factory.Build(spec)
		if err == nil {
			err = runtime.Start(ctx)
		}
		if err != nil {
			status.State = "degraded"
			status.ErrorClass = "worker_start_failed"
			failed[key] = status
			continue
		}
		status.State = "active"
		next[key] = managedRuntime{spec: spec, runtime: runtime, status: status}
	}

	supervisor.mu.Lock()
	supervisor.workers = next
	supervisor.failed = failed
	supervisor.mu.Unlock()

	for key, existing := range current {
		replacement, retained := next[key]
		if retained && replacement.runtime == existing.runtime {
			continue
		}
		drainCtx, cancel := context.WithTimeout(context.Background(), supervisor.drainTimeout)
		_ = existing.runtime.Drain(drainCtx)
		cancel()
	}
	return nil
}

func (supervisor *Supervisor) Status() []BotStatus {
	supervisor.mu.RLock()
	defer supervisor.mu.RUnlock()
	statuses := make([]BotStatus, 0, len(supervisor.workers)+len(supervisor.failed))
	for _, worker := range supervisor.workers {
		status := worker.status
		status.ListenerIDs = append([]string(nil), status.ListenerIDs...)
		statuses = append(statuses, status)
	}
	for _, status := range supervisor.failed {
		status.ListenerIDs = append([]string(nil), status.ListenerIDs...)
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].WorkerID < statuses[j].WorkerID })
	return statuses
}

func normalizeBotSpec(spec BotSpec) (BotSpec, string, error) {
	spec.Listeners = append([]ListenerBinding(nil), spec.Listeners...)
	provider, err := spec.Provider.Normalize(spec.Provider.TestOnlyAllowInsecureTransport)
	if err != nil {
		return BotSpec{}, "", err
	}
	egress, err := spec.Egress.Normalize()
	if err != nil {
		return BotSpec{}, "", err
	}
	if strings.TrimSpace(spec.Token) == "" || !config.ValidSnowflake(spec.BotUserID) || len(spec.Listeners) == 0 {
		return BotSpec{}, "", errors.New("bot worker identity or listeners are invalid")
	}
	seenListeners := make(map[string]struct{}, len(spec.Listeners))
	seenChannels := make(map[string]struct{}, len(spec.Listeners)*2)
	for index := range spec.Listeners {
		listener := spec.Listeners[index]
		if listener.PollEvery == 0 {
			listener.PollEvery = 15 * time.Second
			spec.Listeners[index].PollEvery = listener.PollEvery
		}
		if !route.IsCanonicalUUID(listener.ListenerID) || !route.IsCanonicalUUID(listener.GenerationID) ||
			!config.ValidSnowflake(listener.TaskChannel) || listener.SocksChannel != "" && !config.ValidSnowflake(listener.SocksChannel) ||
			(listener.IngressMode != "gateway" && listener.IngressMode != "polling") ||
			listener.PollEvery < 2*time.Second || listener.PollEvery > time.Hour {
			return BotSpec{}, "", errors.New("bot worker listener binding is invalid")
		}
		if _, duplicate := seenListeners[listener.ListenerID]; duplicate {
			return BotSpec{}, "", errors.New("bot worker contains a duplicate listener")
		}
		seenListeners[listener.ListenerID] = struct{}{}
		for _, channel := range []string{listener.TaskChannel, listener.SocksChannel} {
			if channel == "" {
				continue
			}
			if _, duplicate := seenChannels[channel]; duplicate {
				return BotSpec{}, "", errors.New("bot worker contains a duplicate channel")
			}
			seenChannels[channel] = struct{}{}
		}
	}
	sort.Slice(spec.Listeners, func(i, j int) bool { return spec.Listeners[i].ListenerID < spec.Listeners[j].ListenerID })
	spec.Provider = provider
	spec.Egress = egress
	key := provider.ID[:16] + ":" + spec.BotUserID
	return spec, key, nil
}

func equalBotSpecs(left, right BotSpec) bool {
	if left.Provider != right.Provider || left.Egress != right.Egress || left.Token != right.Token ||
		left.BotUserID != right.BotUserID || len(left.Listeners) != len(right.Listeners) {
		return false
	}
	for index := range left.Listeners {
		if left.Listeners[index] != right.Listeners[index] {
			return false
		}
	}
	return true
}

func statusFor(key string, spec BotSpec, stateName, errorClass string) BotStatus {
	listeners := make([]string, 0, len(spec.Listeners))
	gateway := false
	for _, listener := range spec.Listeners {
		listeners = append(listeners, listener.ListenerID)
		gateway = gateway || listener.IngressMode == "gateway"
	}
	return BotStatus{
		WorkerID: key, ProviderID: spec.Provider.ID, BotUserID: spec.BotUserID,
		Egress: spec.Egress.SafeSummary(), ListenerIDs: listeners,
		GatewayEnabled: gateway, State: stateName, ErrorClass: errorClass,
	}
}
