// Package registry validates and atomically publishes complete Discordx
// listener snapshots. It performs no network I/O, which keeps a rejected
// snapshot from partially changing live state.
package registry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

const (
	// Omitted snapshot limits preserve the production control-plane default.
	// An explicit higher limit is reserved for bounded capacity validation and
	// remains constrained by the absolute hard ceilings.
	defaultMaxListeners  = 1000
	defaultMaxBotWorkers = 1000
	hardMaxListeners     = 4096
	hardMaxBotWorkers    = 4096
	defaultGenerations   = 4
)

type GenerationState string

const (
	Active   GenerationState = "active"
	Draining GenerationState = "draining"
	Disabled GenerationState = "disabled"
	Failed   GenerationState = "failed"
)

type Generation struct {
	ID             string          `json:"id"`
	State          GenerationState `json:"state"`
	DiscordToken   string          `json:"discord_token"`
	BotUserID      string          `json:"bot_user_id,omitempty"`
	TaskChannelID  string          `json:"task_channel_id"`
	SocksChannelID string          `json:"socks_channel_id,omitempty"`
	Provider       config.Provider `json:"provider"`
	Wire           config.Wire     `json:"wire"`
	Fingerprint    string          `json:"fingerprint,omitempty"`
}

type Listener struct {
	ID                 string             `json:"id"`
	Name               string             `json:"name"`
	OperationID        int                `json:"operation_id"`
	Enabled            bool               `json:"enabled"`
	ActiveGenerationID string             `json:"active_generation_id"`
	Ingress            config.Ingress     `json:"ingress"`
	Egress             config.EgressProxy `json:"egress"`
	Generations        []Generation       `json:"generations"`
}

type GenerationRef struct {
	ListenerID   string `json:"listener_id"`
	GenerationID string `json:"generation_id"`
}

type MigrationAliases struct {
	DTE1       map[string]GenerationRef `json:"dte1,omitempty"`
	BareLegacy *GenerationRef           `json:"bare_legacy,omitempty"`
}

type Snapshot struct {
	ProfileName      string           `json:"profile_name"`
	Revision         uint64           `json:"revision"`
	MaxListeners     int              `json:"max_listeners"`
	MaxBotWorkers    int              `json:"max_bot_workers"`
	MaxGenerations   int              `json:"max_generations,omitempty"`
	TestMode         bool             `json:"test_mode,omitempty"`
	Listeners        []Listener       `json:"listeners"`
	MigrationAliases MigrationAliases `json:"migration_aliases,omitempty"`
}

type ApplyResult struct {
	Applied        bool   `json:"applied"`
	Revision       uint64 `json:"revision"`
	ReusedWorkers  int    `json:"reused_workers"`
	StartedWorkers int    `json:"started_workers"`
	StoppedWorkers int    `json:"stopped_workers"`
}

type ResolvedRoute struct {
	ListenerID     string `json:"listener_id"`
	GenerationID   string `json:"generation_id"`
	Lane           string `json:"lane"`
	TaskChannelID  string `json:"task_channel_id"`
	SocksChannelID string `json:"socks_channel_id,omitempty"`
	ClientID       string `json:"client_id,omitempty"`
	LegacySender   string `json:"legacy_sender,omitempty"`
	WireProtocol   string `json:"wire_protocol"`
}

type ListenerStatus struct {
	ListenerID       string `json:"listener_id"`
	Name             string `json:"name"`
	OperationID      int    `json:"operation_id"`
	Enabled          bool   `json:"enabled"`
	ActiveGeneration string `json:"active_generation"`
	RetainedCount    int    `json:"retained_generation_count"`
	ProviderID       string `json:"provider_id"`
	ProviderKind     string `json:"provider_kind"`
	APIHost          string `json:"api_host"`
	IngressMode      string `json:"ingress_mode"`
	PollStrategy     string `json:"poll_strategy"`
	Egress           string `json:"server_egress"`
	Wire             string `json:"wire"`
	TaskChannelID    string `json:"task_channel_id"`
	SocksChannelID   string `json:"socks_channel_id,omitempty"`
}

type Status struct {
	Revision  uint64           `json:"revision"`
	Listeners []ListenerStatus `json:"listeners"`
}

type generationRecord struct {
	listener   Listener
	generation Generation
}

type workerRecord struct {
	egress config.EgressProxy
}

type publishedState struct {
	revision    uint64
	listeners   map[string]Listener
	generations map[GenerationRef]generationRecord
	channels    map[string]ResolvedRoute
	workers     map[string]workerRecord
	aliases     MigrationAliases
	status      Status
}

type Manager struct {
	mu    sync.RWMutex
	state *publishedState
}

func NewManager() *Manager {
	return &Manager{state: &publishedState{
		generations: make(map[GenerationRef]generationRecord),
		listeners:   make(map[string]Listener),
		channels:    make(map[string]ResolvedRoute),
		workers:     make(map[string]workerRecord),
		aliases:     MigrationAliases{DTE1: make(map[string]GenerationRef)},
		status:      Status{Listeners: []ListenerStatus{}},
	}}
}

func (manager *Manager) Revision() uint64 {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.state.revision
}

func (manager *Manager) ApplySnapshot(ctx context.Context, snapshot Snapshot) (ApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return ApplyResult{}, err
	}
	manager.mu.RLock()
	current := manager.state
	if snapshot.Revision <= current.revision {
		result := ApplyResult{Revision: current.revision}
		manager.mu.RUnlock()
		return result, nil
	}
	manager.mu.RUnlock()

	next, err := buildState(snapshot)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ApplyResult{}, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if snapshot.Revision <= manager.state.revision {
		return ApplyResult{Revision: manager.state.revision}, nil
	}
	result := ApplyResult{Applied: true, Revision: snapshot.Revision}
	for key := range next.workers {
		if currentWorker, ok := manager.state.workers[key]; ok && currentWorker == next.workers[key] {
			result.ReusedWorkers++
		} else {
			result.StartedWorkers++
			if ok {
				result.StoppedWorkers++
			}
		}
	}
	for key := range manager.state.workers {
		if _, ok := next.workers[key]; !ok {
			result.StoppedWorkers++
		}
	}
	manager.state = next
	return result, nil
}

func buildState(snapshot Snapshot) (*publishedState, error) {
	if snapshot.ProfileName != "discordx" {
		return nil, errors.New("registry snapshot is for the wrong C2 profile")
	}
	if snapshot.Revision == 0 {
		return nil, errors.New("registry revision must be positive")
	}
	if snapshot.MaxListeners == 0 {
		snapshot.MaxListeners = defaultMaxListeners
	}
	if snapshot.MaxBotWorkers == 0 {
		snapshot.MaxBotWorkers = defaultMaxBotWorkers
	}
	if snapshot.MaxGenerations == 0 {
		snapshot.MaxGenerations = defaultGenerations
	}
	if snapshot.MaxListeners < 1 || snapshot.MaxListeners > hardMaxListeners || len(snapshot.Listeners) > snapshot.MaxListeners {
		return nil, errors.New("registry listener limit is invalid or exceeded")
	}
	if snapshot.MaxBotWorkers < 1 || snapshot.MaxBotWorkers > hardMaxBotWorkers {
		return nil, errors.New("registry bot-worker limit is invalid")
	}
	if snapshot.MaxGenerations < 1 || snapshot.MaxGenerations > 32 {
		return nil, errors.New("registry generation-retention limit is invalid")
	}

	next := &publishedState{
		revision:    snapshot.Revision,
		generations: make(map[GenerationRef]generationRecord),
		listeners:   make(map[string]Listener),
		channels:    make(map[string]ResolvedRoute),
		workers:     make(map[string]workerRecord),
		aliases:     MigrationAliases{DTE1: make(map[string]GenerationRef)},
		status:      Status{Revision: snapshot.Revision, Listeners: make([]ListenerStatus, 0, len(snapshot.Listeners))},
	}
	listenerIDs := make(map[string]struct{}, len(snapshot.Listeners))
	listenerNames := make(map[string]struct{}, len(snapshot.Listeners))
	for index := range snapshot.Listeners {
		listener, err := normalizeListener(snapshot.Listeners[index], snapshot, next)
		if err != nil {
			return nil, fmt.Errorf("listener %d is invalid: %w", index+1, err)
		}
		if _, duplicate := listenerIDs[listener.ID]; duplicate {
			return nil, errors.New("registry contains a duplicate listener ID")
		}
		if _, duplicate := listenerNames[listener.Name]; duplicate {
			return nil, errors.New("registry contains a duplicate listener name")
		}
		listenerIDs[listener.ID] = struct{}{}
		listenerNames[listener.Name] = struct{}{}
		next.listeners[listener.ID] = listener
	}
	if len(next.workers) > snapshot.MaxBotWorkers {
		return nil, errors.New("registry bot-worker limit is exceeded")
	}
	if err := validateAliases(snapshot.MigrationAliases, next); err != nil {
		return nil, err
	}
	sort.Slice(next.status.Listeners, func(i, j int) bool {
		return next.status.Listeners[i].ListenerID < next.status.Listeners[j].ListenerID
	})
	return next, nil
}

func normalizeListener(listener Listener, snapshot Snapshot, next *publishedState) (Listener, error) {
	if err := route.RequireCanonicalUUID(listener.ID, "listener ID"); err != nil {
		return Listener{}, err
	}
	listener.Name = strings.TrimSpace(listener.Name)
	if listener.Name == "" || listener.OperationID <= 0 {
		return Listener{}, errors.New("listener name and operation are required")
	}
	if len(listener.Generations) == 0 || len(listener.Generations) > snapshot.MaxGenerations {
		return Listener{}, errors.New("listener generation count is invalid")
	}
	ingress, err := listener.Ingress.Normalize()
	if err != nil {
		return Listener{}, err
	}
	egress, err := listener.Egress.Normalize()
	if err != nil {
		return Listener{}, err
	}
	listener.Ingress = ingress
	listener.Egress = egress

	foundActive := false
	seenGeneration := make(map[string]struct{}, len(listener.Generations))
	for index := range listener.Generations {
		generation, err := normalizeGeneration(listener, listener.Generations[index], snapshot.TestMode)
		if err != nil {
			return Listener{}, fmt.Errorf("generation %d is invalid: %w", index+1, err)
		}
		if _, duplicate := seenGeneration[generation.ID]; duplicate {
			return Listener{}, errors.New("listener contains a duplicate generation ID")
		}
		seenGeneration[generation.ID] = struct{}{}
		listener.Generations[index] = generation
		ref := GenerationRef{ListenerID: listener.ID, GenerationID: generation.ID}
		if _, duplicate := next.generations[ref]; duplicate {
			return Listener{}, errors.New("registry contains a duplicate generation route")
		}
		next.generations[ref] = generationRecord{listener: listener, generation: generation}
		if generation.ID == listener.ActiveGenerationID {
			foundActive = generation.State == Active
		}
		if listener.Enabled && generation.State == Active {
			resolved := ResolvedRoute{
				ListenerID: listener.ID, GenerationID: generation.ID,
				TaskChannelID: generation.TaskChannelID, SocksChannelID: generation.SocksChannelID,
				WireProtocol: generation.Wire.Protocol,
			}
			if err := addChannel(next, generation.Provider.ID, generation.TaskChannelID, "standard", resolved); err != nil {
				return Listener{}, err
			}
			if generation.SocksChannelID != "" {
				if err := addChannel(next, generation.Provider.ID, generation.SocksChannelID, "socks", resolved); err != nil {
					return Listener{}, err
				}
			}
		}
		if listener.Enabled && (generation.State == Active || generation.State == Draining) {
			key := workerKey(generation)
			if existing, ok := next.workers[key]; ok {
				if existing.egress != listener.Egress {
					return Listener{}, errors.New("listeners sharing one bot require one server egress policy")
				}
			} else {
				next.workers[key] = workerRecord{egress: listener.Egress}
			}
		}
	}
	if listener.Enabled && (!route.IsCanonicalUUID(listener.ActiveGenerationID) || !foundActive) {
		return Listener{}, errors.New("enabled listener requires one active generation")
	}
	active := findGeneration(listener.Generations, listener.ActiveGenerationID)
	if active != nil {
		next.status.Listeners = append(next.status.Listeners, listenerStatus(listener, *active))
	}
	return listener, nil
}

func normalizeGeneration(listener Listener, generation Generation, testMode bool) (Generation, error) {
	if err := route.RequireCanonicalUUID(generation.ID, "generation ID"); err != nil {
		return Generation{}, err
	}
	switch generation.State {
	case Active, Draining, Disabled, Failed:
	default:
		return Generation{}, errors.New("generation state is unsupported")
	}
	if strings.TrimSpace(generation.DiscordToken) == "" {
		return Generation{}, errors.New("Discord credential is required")
	}
	if generation.BotUserID != "" && !config.ValidSnowflake(generation.BotUserID) {
		return Generation{}, errors.New("authenticated bot user ID is invalid")
	}
	if !config.ValidSnowflake(generation.TaskChannelID) {
		return Generation{}, errors.New("task channel ID is invalid")
	}
	if generation.SocksChannelID != "" {
		if !config.ValidSnowflake(generation.SocksChannelID) || generation.SocksChannelID == generation.TaskChannelID {
			return Generation{}, errors.New("SOCKS channel ID is invalid or reuses the task channel")
		}
	}
	provider, err := generation.Provider.Normalize(testMode)
	if err != nil {
		return Generation{}, err
	}
	wire, err := generation.Wire.Normalize()
	if err != nil {
		return Generation{}, err
	}
	if generation.SocksChannelID != "" && (wire.Protocol != "fixed" || wire.EnvelopeFormat != "binary-v1") {
		return Generation{}, errors.New("SOCKS channel requires fixed binary-v1 transport")
	}
	generation.Provider = provider
	generation.Wire = wire
	generation.Fingerprint = wire.DiagnosticFingerprint()
	return generation, nil
}

func addChannel(next *publishedState, providerID, channelID, lane string, resolved ResolvedRoute) error {
	key := providerID + "\x00" + channelID
	if _, exists := next.channels[key]; exists {
		return errors.New("provider channel is assigned to more than one listener lane")
	}
	resolved.Lane = lane
	next.channels[key] = resolved
	return nil
}

func workerKey(generation Generation) string {
	identity := "token\x00" + generation.DiscordToken
	if generation.BotUserID != "" {
		identity = "bot\x00" + generation.BotUserID
	}
	return generation.Provider.ID + "\x00" + identity
}

func findGeneration(generations []Generation, id string) *Generation {
	for index := range generations {
		if generations[index].ID == id {
			return &generations[index]
		}
	}
	return nil
}

func listenerStatus(listener Listener, generation Generation) ListenerStatus {
	apiHost := ""
	if parts := strings.SplitN(strings.TrimPrefix(strings.TrimPrefix(generation.Provider.APIBaseURL, "https://"), "http://"), "/", 2); len(parts) > 0 {
		apiHost = parts[0]
	}
	return ListenerStatus{
		ListenerID: listener.ID, Name: listener.Name, OperationID: listener.OperationID,
		Enabled: listener.Enabled, ActiveGeneration: listener.ActiveGenerationID,
		RetainedCount: len(listener.Generations), ProviderID: generation.Provider.ID,
		ProviderKind: generation.Provider.Kind, APIHost: apiHost,
		IngressMode: listener.Ingress.Mode, PollStrategy: listener.Ingress.PollStrategy,
		Egress: listener.Egress.SafeSummary(), Wire: generation.Wire.SafeSummary(),
		TaskChannelID: generation.TaskChannelID, SocksChannelID: generation.SocksChannelID,
	}
}

func validateAliases(aliases MigrationAliases, next *publishedState) error {
	for fingerprint, ref := range aliases.DTE1 {
		if _, err := route.ParseDTE1("dte1:" + fingerprint + ":00000000-0000-0000-0000-000000000000"); err != nil {
			return errors.New("dte1 migration alias fingerprint is invalid")
		}
		record, ok := next.generations[ref]
		if !ok || record.generation.Wire.Protocol != "fixed" ||
			!record.listener.Enabled || (record.generation.State != Active && record.generation.State != Draining) {
			return errors.New("dte1 migration alias target is missing, disabled, or not fixed")
		}
		if _, duplicate := next.aliases.DTE1[fingerprint]; duplicate {
			return errors.New("dte1 migration alias is ambiguous")
		}
		next.aliases.DTE1[fingerprint] = ref
	}
	if aliases.BareLegacy != nil {
		record, ok := next.generations[*aliases.BareLegacy]
		if !ok || record.generation.Wire.Protocol != "legacy" || !record.listener.Enabled ||
			(record.generation.State != Active && record.generation.State != Draining) {
			return errors.New("bare legacy migration alias target is missing, disabled, or not legacy")
		}
		copyRef := *aliases.BareLegacy
		next.aliases.BareLegacy = &copyRef
	}
	return nil
}

func (manager *Manager) ResolveTrackingRoute(value string) (ResolvedRoute, error) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if strings.HasPrefix(value, "dx2:") {
		parsed, err := route.ParseDX2(value)
		if err != nil {
			return ResolvedRoute{}, err
		}
		ref := GenerationRef{ListenerID: parsed.ListenerID, GenerationID: parsed.GenerationID}
		resolved, err := manager.resolveRef(ref)
		if err != nil {
			return ResolvedRoute{}, err
		}
		if (parsed.Kind == route.Fixed) != (resolved.WireProtocol == "fixed") {
			return ResolvedRoute{}, errors.New("tracking route kind does not match listener generation")
		}
		resolved.ClientID = parsed.ClientID
		resolved.LegacySender = parsed.LegacySender
		return resolved, nil
	}
	if strings.HasPrefix(value, "dte1:") {
		parsed, err := route.ParseDTE1(value)
		if err != nil {
			return ResolvedRoute{}, err
		}
		ref, ok := manager.state.aliases.DTE1[parsed.Fingerprint]
		if !ok {
			return ResolvedRoute{}, errors.New("unknown dte1 migration alias")
		}
		resolved, err := manager.resolveRef(ref)
		if err != nil || resolved.WireProtocol != "fixed" {
			return ResolvedRoute{}, errors.New("invalid dte1 migration alias target")
		}
		resolved.ClientID = parsed.ClientID
		return resolved, nil
	}
	if manager.state.aliases.BareLegacy == nil {
		return ResolvedRoute{}, errors.New("bare legacy route has no migration alias")
	}
	if value == "" || len([]byte(value)) > 256 || !utf8.ValidString(value) {
		return ResolvedRoute{}, errors.New("bare legacy route is invalid")
	}
	resolved, err := manager.resolveRef(*manager.state.aliases.BareLegacy)
	if err != nil || resolved.WireProtocol != "legacy" {
		return ResolvedRoute{}, errors.New("invalid bare legacy migration alias target")
	}
	resolved.LegacySender = value
	return resolved, nil
}

func (manager *Manager) resolveRef(ref GenerationRef) (ResolvedRoute, error) {
	record, ok := manager.state.generations[ref]
	if !ok || !record.listener.Enabled || (record.generation.State != Active && record.generation.State != Draining) {
		return ResolvedRoute{}, errors.New("tracking route generation is unknown or disabled")
	}
	return ResolvedRoute{
		ListenerID: ref.ListenerID, GenerationID: ref.GenerationID,
		TaskChannelID: record.generation.TaskChannelID, SocksChannelID: record.generation.SocksChannelID,
		WireProtocol: record.generation.Wire.Protocol,
	}, nil
}

func (manager *Manager) ResolveChannel(providerID, channelID string) (ResolvedRoute, bool) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	resolved, ok := manager.state.channels[providerID+"\x00"+channelID]
	return resolved, ok
}

func (manager *Manager) Status() Status {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	listeners := append([]ListenerStatus(nil), manager.state.status.Listeners...)
	return Status{Revision: manager.state.revision, Listeners: listeners}
}

// Listeners returns a deep copy of the normalized runtime configuration. It is
// an in-process supervisor API and must never be serialized to health, logs,
// metrics, or exports because generations contain credentials and wire keys.
func (manager *Manager) Listeners() []Listener {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	listeners := make([]Listener, 0, len(manager.state.listeners))
	for _, current := range manager.state.listeners {
		listener := current
		listener.Generations = append([]Generation(nil), current.Generations...)
		listeners = append(listeners, listener)
	}
	sort.Slice(listeners, func(i, j int) bool { return listeners[i].ID < listeners[j].ID })
	return listeners
}

// Generation returns a copy of one normalized listener and generation. It is
// intended for supervisors and hot-path processors that must construct exactly
// one decoder after channel routing; callers cannot mutate the published
// registry through the returned values.
func (manager *Manager) Generation(listenerID, generationID string) (Listener, Generation, bool) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	record, ok := manager.state.generations[GenerationRef{ListenerID: listenerID, GenerationID: generationID}]
	if !ok {
		return Listener{}, Generation{}, false
	}
	listener := record.listener
	listener.Generations = append([]Generation(nil), listener.Generations...)
	return listener, record.generation, true
}
