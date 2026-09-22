// Package planner computes one bounded polling deadline per listener channel.
// It stores callback hints in maps and a heap and never creates callback
// goroutines or timers.
package planner

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/config"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/route"
)

var (
	ErrSequenceGap = errors.New("activity hint sequence gap")
	ErrHintLimit   = errors.New("activity hint limit exceeded")
	ErrInvalidHint = errors.New("activity hint is invalid")
)

type Reason string

const (
	Backlog          Reason = "backlog"
	RecentActivity   Reason = "recent_activity"
	AgentDue         Reason = "agent_due"
	OutboundResponse Reason = "outbound_response"
	SocksActive      Reason = "socks_active"
	BuildWarm        Reason = "build_warm"
	ColdProbe        Reason = "cold_probe"
	FixedFallback    Reason = "fixed_fallback"
)

type Decision struct {
	Deadline time.Time `json:"deadline"`
	Reason   Reason    `json:"reason"`
}

type CallbackHint struct {
	ListenerID               string    `json:"listener_id"`
	CallbackID               string    `json:"callback_id"`
	Active                   bool      `json:"active"`
	LastCheckin              time.Time `json:"last_checkin"`
	IntervalSeconds          int       `json:"interval_seconds"`
	JitterPercent            int       `json:"jitter_percent"`
	MessageChecks            int       `json:"message_checks"`
	TimeBetweenChecksSeconds int       `json:"time_between_checks_seconds"`
}

type ActivitySnapshot struct {
	Sequence  uint64         `json:"sequence"`
	Callbacks []CallbackHint `json:"callbacks"`
}

type ActivityHint struct {
	Sequence uint64       `json:"sequence"`
	Callback CallbackHint `json:"callback"`
}

type Observation struct {
	ListenerID       string
	At               time.Time
	PageFull         bool
	MorePages        bool
	FoundMessages    bool
	EmptyPoll        bool
	OutboundResponse bool
	SocksActive      bool
	ResponseActive   bool
	PayloadBuilt     bool
}

type callbackState struct {
	hint    CallbackHint
	lower   time.Time
	upper   time.Time
	expires time.Time
	cadence time.Duration
	entry   *deadlineEntry
}

type deadlineEntry struct {
	callbackID string
	lower      time.Time
	index      int
}

type deadlineHeap []*deadlineEntry

func (value deadlineHeap) Len() int { return len(value) }
func (value deadlineHeap) Less(i, j int) bool {
	if value[i].lower.Equal(value[j].lower) {
		return value[i].callbackID < value[j].callbackID
	}
	return value[i].lower.Before(value[j].lower)
}
func (value deadlineHeap) Swap(i, j int) {
	value[i], value[j] = value[j], value[i]
	value[i].index = i
	value[j].index = j
}
func (value *deadlineHeap) Push(item any) {
	entry := item.(*deadlineEntry)
	entry.index = len(*value)
	*value = append(*value, entry)
}
func (value *deadlineHeap) Pop() any {
	old := *value
	item := old[len(old)-1]
	old[len(old)-1] = nil
	item.index = -1
	*value = old[:len(old)-1]
	return item
}

type listenerState struct {
	channelID      string
	ingress        config.Ingress
	activityValid  bool
	callbacks      map[string]callbackState
	deadlines      deadlineHeap
	backlog        bool
	recentUntil    time.Time
	buildWarmUntil time.Time
	outboundUntil  time.Time
	socksActive    bool
	responseActive bool
	emptyPolls     int
}

type Planner struct {
	mu             sync.Mutex
	listeners      map[string]*listenerState
	perListenerMax int
	globalMax      int
	globalCount    int
	sequence       uint64
	sequenceValid  bool
}

func New(perListenerMax, globalMax int) *Planner {
	if perListenerMax < 1 {
		perListenerMax = 1
	}
	if globalMax < perListenerMax {
		globalMax = perListenerMax
	}
	return &Planner{
		listeners:      make(map[string]*listenerState),
		perListenerMax: perListenerMax,
		globalMax:      globalMax,
	}
}

func (planner *Planner) Configure(listenerID, channelID string, ingress config.Ingress) error {
	if !route.IsCanonicalUUID(listenerID) || !config.ValidSnowflake(channelID) {
		return errors.New("planner listener or channel is invalid")
	}
	normalized, err := ingress.Normalize()
	if err != nil {
		return err
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if current, ok := planner.listeners[listenerID]; ok {
		current.channelID = channelID
		current.ingress = normalized
		return nil
	}
	planner.listeners[listenerID] = &listenerState{
		channelID: channelID, ingress: normalized,
		callbacks: make(map[string]callbackState), deadlines: make(deadlineHeap, 0),
	}
	return nil
}

func (planner *Planner) ApplyActivitySnapshot(snapshot ActivitySnapshot) error {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	for _, listener := range planner.listeners {
		listener.activityValid = false
		listener.callbacks = make(map[string]callbackState)
		listener.deadlines = listener.deadlines[:0]
	}
	planner.globalCount = 0
	planner.sequenceValid = false
	if snapshot.Sequence == 0 {
		return ErrInvalidHint
	}
	perListener := make(map[string]int)
	for _, hint := range snapshot.Callbacks {
		if !hint.Active {
			continue
		}
		listener, ok := planner.listeners[hint.ListenerID]
		if !ok || validateHint(hint) != nil {
			return ErrInvalidHint
		}
		perListener[hint.ListenerID]++
		planner.globalCount++
		if perListener[hint.ListenerID] > planner.perListenerMax || planner.globalCount > planner.globalMax {
			return ErrHintLimit
		}
		planner.putHint(listener, hint)
	}
	for _, listener := range planner.listeners {
		listener.activityValid = true
	}
	planner.sequence = snapshot.Sequence
	planner.sequenceValid = true
	return nil
}

func (planner *Planner) ApplyActivityHint(update ActivityHint) error {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if !planner.sequenceValid || update.Sequence != planner.sequence+1 {
		planner.invalidateActivity()
		return ErrSequenceGap
	}
	listener, ok := planner.listeners[update.Callback.ListenerID]
	if !ok || update.Callback.CallbackID == "" {
		planner.invalidateActivity()
		return ErrInvalidHint
	}
	if !update.Callback.Active {
		if current, exists := listener.callbacks[update.Callback.CallbackID]; exists {
			heap.Remove(&listener.deadlines, current.entry.index)
			delete(listener.callbacks, update.Callback.CallbackID)
			planner.globalCount--
		}
	} else {
		if err := validateHint(update.Callback); err != nil {
			listener.activityValid = false
			return err
		}
		_, exists := listener.callbacks[update.Callback.CallbackID]
		if !exists && (len(listener.callbacks) >= planner.perListenerMax || planner.globalCount >= planner.globalMax) {
			listener.activityValid = false
			return ErrHintLimit
		}
		if !exists {
			planner.globalCount++
		}
		planner.putHint(listener, update.Callback)
	}
	planner.sequence = update.Sequence
	return nil
}

func (planner *Planner) putHint(listener *listenerState, hint CallbackHint) {
	interval := time.Duration(hint.IntervalSeconds) * time.Second
	lower := hint.LastCheckin.Add(interval)
	upper := lower.Add(interval * time.Duration(hint.JitterPercent) / 100)
	responseWindow := time.Duration(hint.MessageChecks-1) * time.Duration(hint.TimeBetweenChecksSeconds) * time.Second
	cadence := responseWindow / 2
	minimum := time.Duration(listener.ingress.PollMinIntervalSeconds) * time.Second
	base := time.Duration(listener.ingress.PollBaseIntervalSeconds) * time.Second
	if cadence < minimum {
		cadence = minimum
	}
	if cadence > base {
		cadence = base
	}
	var entry *deadlineEntry
	if existing, ok := listener.callbacks[hint.CallbackID]; ok {
		entry = existing.entry
		entry.lower = lower
		heap.Fix(&listener.deadlines, entry.index)
	} else {
		entry = &deadlineEntry{callbackID: hint.CallbackID, lower: lower, index: -1}
		heap.Push(&listener.deadlines, entry)
	}
	current := callbackState{
		hint: hint, lower: lower, upper: upper,
		expires: upper.Add(responseWindow), cadence: cadence, entry: entry,
	}
	listener.callbacks[hint.CallbackID] = current
}

func validateHint(hint CallbackHint) error {
	if !route.IsCanonicalUUID(hint.ListenerID) || hint.CallbackID == "" || hint.LastCheckin.IsZero() ||
		hint.IntervalSeconds <= 0 || hint.IntervalSeconds > 86_400 || hint.JitterPercent < 0 || hint.JitterPercent > 100 ||
		hint.MessageChecks < 2 || hint.MessageChecks > 10_000 || hint.TimeBetweenChecksSeconds <= 0 || hint.TimeBetweenChecksSeconds > 3600 {
		return ErrInvalidHint
	}
	return nil
}

func (planner *Planner) invalidateActivity() {
	planner.sequenceValid = false
	for _, listener := range planner.listeners {
		listener.activityValid = false
	}
}

func (planner *Planner) Observe(observation Observation) {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	listener, ok := planner.listeners[observation.ListenerID]
	if !ok || observation.At.IsZero() {
		return
	}
	listener.socksActive = observation.SocksActive
	listener.responseActive = observation.ResponseActive
	if observation.MorePages || observation.PageFull {
		listener.backlog = true
	} else if observation.EmptyPoll || observation.FoundMessages {
		listener.backlog = false
	}
	if observation.FoundMessages {
		listener.emptyPolls = 0
		listener.recentUntil = observation.At.Add(time.Duration(listener.ingress.PollBaseIntervalSeconds) * time.Second)
	}
	if observation.EmptyPoll {
		listener.emptyPolls++
	}
	if observation.PayloadBuilt {
		listener.buildWarmUntil = observation.At.Add(time.Duration(listener.ingress.PollBuildWarmSeconds) * time.Second)
	}
	if observation.OutboundResponse {
		listener.outboundUntil = observation.At.Add(time.Duration(listener.ingress.PollBaseIntervalSeconds) * time.Second)
	}
}

func (planner *Planner) NextPoll(listenerID, channelID string, now time.Time) (Decision, error) {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	listener, ok := planner.listeners[listenerID]
	if !ok || listener.channelID != channelID || now.IsZero() {
		return Decision{}, errors.New("poll planner route is unknown")
	}
	minimum := time.Duration(listener.ingress.PollMinIntervalSeconds) * time.Second
	base := time.Duration(listener.ingress.PollBaseIntervalSeconds) * time.Second
	maximum := time.Duration(listener.ingress.PollMaxIntervalSeconds) * time.Second
	fixed := time.Duration(listener.ingress.PollIntervalSeconds) * time.Second
	if listener.backlog {
		return Decision{Deadline: now, Reason: Backlog}, nil
	}
	if listener.socksActive {
		return Decision{Deadline: now.Add(minimum), Reason: SocksActive}, nil
	}
	if listener.responseActive {
		return Decision{Deadline: now.Add(minimum), Reason: OutboundResponse}, nil
	}
	if now.Before(listener.recentUntil) {
		return Decision{Deadline: now.Add(minimum), Reason: RecentActivity}, nil
	}
	if listener.ingress.PollStrategy == "fixed" || !planner.sequenceValid || !listener.activityValid {
		return Decision{Deadline: now.Add(fixed), Reason: FixedFallback}, nil
	}
	if now.Before(listener.outboundUntil) {
		return Decision{Deadline: now.Add(base), Reason: OutboundResponse}, nil
	}
	if now.Before(listener.buildWarmUntil) {
		return Decision{Deadline: now.Add(base), Reason: BuildWarm}, nil
	}

	var earliest time.Time
	for callbackID, callback := range listener.callbacks {
		if now.After(callback.expires) {
			heap.Remove(&listener.deadlines, callback.entry.index)
			delete(listener.callbacks, callbackID)
			planner.globalCount--
			continue
		}
		deadline := callback.lower
		if !now.Before(callback.lower) {
			deadline = now.Add(callback.cadence)
		}
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	if !earliest.IsZero() {
		cold := now.Add(maximum)
		if cold.Before(earliest) {
			return Decision{Deadline: cold, Reason: ColdProbe}, nil
		}
		return Decision{Deadline: earliest, Reason: AgentDue}, nil
	}
	interval := base
	for count := 0; count < listener.emptyPolls && interval < maximum; count++ {
		if interval > maximum/2 {
			interval = maximum
			break
		}
		interval *= 2
	}
	if interval > maximum {
		interval = maximum
	}
	return Decision{Deadline: now.Add(interval), Reason: ColdProbe}, nil
}

func (planner *Planner) HintCount(listenerID string) int {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if listener, ok := planner.listeners[listenerID]; ok {
		return len(listener.callbacks)
	}
	return 0
}

func (planner *Planner) DeadlineCount(listenerID string) int {
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if listener, ok := planner.listeners[listenerID]; ok {
		return len(listener.deadlines)
	}
	return 0
}

func ValidateResponseWindow(ingress config.Ingress, messageChecks, timeBetweenChecksSeconds int) error {
	normalized, err := ingress.Normalize()
	if err != nil {
		return err
	}
	if normalized.Mode != "polling" {
		return nil
	}
	if messageChecks < 2 || timeBetweenChecksSeconds <= 0 {
		return errors.New("Discord response-check settings are invalid")
	}
	window := (messageChecks - 1) * timeBetweenChecksSeconds
	if window < 2*normalized.PollMinIntervalSeconds {
		return fmt.Errorf("Discord response window must cover at least two minimum poll intervals")
	}
	return nil
}
