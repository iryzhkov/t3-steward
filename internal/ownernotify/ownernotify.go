// Package ownernotify delivers campaign notifications to the owner's own
// channel: a Discord webhook, or a local command that receives the event as
// JSON on its standard input.
//
// It is additive to the thread wake that "campaign submit" and "task run"
// register. A thread wake reaches the agent that submitted the work; an owner
// notification reaches the person who owns the fleet, on a channel they read
// when no agent is looking, and it covers every run rather than only the runs
// somebody registered a wait on.
//
// Delivery is durable and at least once. The coordinator records every event
// it will deliver as an outbox row in its own SQLite store before anything is
// sent, keyed by sink, event and the identity of what happened, so a restart
// neither loses an event nor records it twice. A send that fails is retried
// with exponential backoff up to a bound and then abandoned with a log line;
// the loop runs beside the coordinator's boundaries and never inside one, so
// a slow or absent webhook cannot delay scheduling.
//
// The package imports nothing else from this module. The configuration
// validates event names against it and the store records its types, and
// neither of those may be imported here without a cycle.
package ownernotify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Event names one kind of thing the owner can be told about. The names are
// the configuration vocabulary, so they are part of the operator contract.
type Event string

const (
	// EventRunSucceeded, EventRunFailed, EventRunCancelled and EventRunSkipped
	// are a workflow run reaching that terminal outcome. A run reaches exactly
	// one of them, once.
	EventRunSucceeded Event = "run-succeeded"
	EventRunFailed    Event = "run-failed"
	EventRunCancelled Event = "run-cancelled"
	EventRunSkipped   Event = "run-skipped"
	// EventNeedsInput is a task asking the operator a question: an attention
	// wait (approval or direction) that nobody has answered yet.
	EventNeedsInput Event = "needs-input"
	// EventSupervisionEscalated is a supervised run that is waiting for an
	// operator: an escalated review incident or gate, or an overseer activation
	// that spent its budget or needs its dispatch reconciled, each of which
	// stays put until an operator reassesses or resolves it.
	EventSupervisionEscalated Event = "supervision-escalated"
	// EventGateReview is a supervision gate whose evidence is ready for review.
	// An overseer normally takes these itself, which is why it is not a default.
	EventGateReview Event = "gate-review"
)

// Events lists every event in the order help and validation describe them.
func Events() []Event {
	return []Event{
		EventRunSucceeded, EventRunFailed, EventRunCancelled, EventRunSkipped,
		EventNeedsInput, EventSupervisionEscalated, EventGateReview,
	}
}

// DefaultEvents is what a sink delivers when its configuration names no
// events: every terminal outcome and every condition that is waiting for a
// person. Gate review is left out because an overseer, not the owner, is the
// one expected to act on it, and a default that pages the owner for work an
// agent is already doing is the noise that gets a channel muted.
func DefaultEvents() []Event {
	return []Event{
		EventRunSucceeded, EventRunFailed, EventRunCancelled, EventRunSkipped,
		EventNeedsInput, EventSupervisionEscalated,
	}
}

// ParseEvents validates configured event names. An empty list is the default
// set; an unknown or repeated name is an error that names the accepted ones,
// because a typo that silently delivered nothing would look exactly like a
// quiet fleet.
func ParseEvents(names []string) ([]Event, error) {
	if len(names) == 0 {
		return DefaultEvents(), nil
	}
	known := make(map[Event]bool, len(Events()))
	accepted := make([]string, 0, len(Events()))
	for _, event := range Events() {
		known[event] = true
		accepted = append(accepted, string(event))
	}
	seen := make(map[Event]bool, len(names))
	events := make([]Event, 0, len(names))
	for _, name := range names {
		event := Event(strings.TrimSpace(name))
		if !known[event] {
			return nil, fmt.Errorf("unknown event %q (accepted: %s)", name, strings.Join(accepted, ", "))
		}
		if seen[event] {
			return nil, fmt.Errorf("event %q is listed twice", name)
		}
		seen[event] = true
		events = append(events, event)
	}
	return events, nil
}

// RunOutcomeEvent maps a terminal run progress to its event.
func RunOutcomeEvent(progress string) (Event, bool) {
	switch progress {
	case "succeeded":
		return EventRunSucceeded, true
	case "failed":
		return EventRunFailed, true
	case "cancelled":
		return EventRunCancelled, true
	case "skipped":
		return EventRunSkipped, true
	default:
		return "", false
	}
}

// Notification is one event bound for one sink. Everything but the delivery
// bookkeeping is frozen when the event is recorded, so a retry after a restart
// sends what the first attempt would have sent.
type Notification struct {
	// ID is the idempotency key: sink, event and the identity of what
	// happened. A receiver that deduplicates can use it as is.
	ID    string `json:"id"`
	Sink  string `json:"sink"`
	Event Event  `json:"event"`
	RunID string `json:"runId"`
	// Campaign is the workflow name, when the coordinator knows it.
	Campaign string `json:"campaign,omitempty"`
	// Outcome is the terminal run progress of a run event.
	Outcome        string   `json:"outcome,omitempty"`
	FailedTasks    []string `json:"failedTasks,omitempty"`
	CancelledTasks []string `json:"cancelledTasks,omitempty"`
	// Task, WaitID and Prompt describe a needs-input question.
	Task   string `json:"task,omitempty"`
	WaitID string `json:"waitId,omitempty"`
	Prompt string `json:"prompt,omitempty"`
	// IncidentID, ActivationID and Gate identify the supervision record that
	// is waiting, and Reason says why.
	IncidentID   string    `json:"incidentId,omitempty"`
	ActivationID string    `json:"activationId,omitempty"`
	GateID       string    `json:"gateId,omitempty"`
	Gate         string    `json:"gate,omitempty"`
	Reason       string    `json:"reason,omitempty"`
	OccurredAt   time.Time `json:"occurredAt"`

	// Attempts is how many sends have already failed. It is bookkeeping and
	// not part of the event.
	Attempts int `json:"-"`
}

// Commands are the next commands the owner would type for this event. They
// are part of the payload so that a Discord line and a command sink say the
// same thing.
func (n Notification) Commands() map[string]string {
	commands := map[string]string{}
	switch n.Event {
	case EventRunSucceeded, EventRunFailed, EventRunCancelled, EventRunSkipped:
		commands["result"] = "t3-steward task result " + n.RunID
		commands["show"] = "t3-steward campaign show " + n.RunID
	case EventNeedsInput:
		commands["inspect"] = "t3-steward wait inspect " + n.WaitID
		commands["show"] = "t3-steward campaign show " + n.RunID
	case EventSupervisionEscalated, EventGateReview:
		commands["supervision"] = "t3-steward campaign supervision show " + n.RunID
	}
	return commands
}

// State is the delivery state of an outbox row.
type State string

const (
	// StatePending is waiting to be sent, now or at its next attempt time.
	StatePending State = "pending"
	// StateDelivered is a send the sink acknowledged. It is terminal.
	StateDelivered State = "delivered"
	// StateAbandoned is a send given up on, after the attempt bound or a
	// refusal that retrying cannot change. It is terminal.
	StateAbandoned State = "abandoned"
	// StateBaseline is an event that already existed when the sink started
	// delivering that event kind. It is recorded so that it is never sent: a
	// newly configured webhook must not replay the coordinator's history.
	StateBaseline State = "baseline"
)

// Detection is what one detection pass recorded for one sink.
type Detection struct {
	// Enqueued is the number of new events recorded for delivery.
	Enqueued int
	// Baselined is the number of existing events recorded as never to be
	// sent, because their event kind was being enabled for the first time.
	Baselined int
}

// AttemptResult is the durable outcome of one send.
type AttemptResult struct {
	State         State
	Attempts      int
	NextAttemptAt time.Time
	// Error is the sanitized reason the send failed. It is empty on success,
	// and it never contains a secret: sinks sanitize before returning.
	Error string
	At    time.Time
}

// Selection is what one sink is told about.
type Selection struct {
	// Events are the event kinds delivered.
	Events []Event
	// ScheduledSuccess also delivers run-succeeded and run-skipped for runs a
	// schedule created. It is off by default: a schedule that runs every hour
	// would otherwise post every hour, and a recurring job's owner needs to
	// hear when it breaks, not that it worked again. Failures, cancellations
	// and anything waiting for a person are delivered for scheduled runs
	// either way.
	ScheduledSuccess bool
}

// ScheduledSuccessEvent reports whether an event is one that a scheduled run
// delivers only when the sink opted into scheduled successes.
func ScheduledSuccessEvent(event Event) bool {
	return event == EventRunSucceeded || event == EventRunSkipped
}

// Store is the coordinator outbox.
type Store interface {
	// DetectOwnerNotifications records every selected event that has no row
	// for this sink yet. The first pass for a (sink, event) pair records what
	// already exists as baseline instead of pending; scheduled successes are
	// baselined separately, so opting into them later replays nothing.
	DetectOwnerNotifications(ctx context.Context, sink string, selection Selection, now time.Time) (Detection, error)
	// DueOwnerNotifications returns pending rows whose next attempt is due,
	// oldest first.
	DueOwnerNotifications(ctx context.Context, sink string, now time.Time, limit int) ([]Notification, error)
	// RecordOwnerNotificationAttempt stores the outcome of one send. It
	// changes only a row that is still pending.
	RecordOwnerNotificationAttempt(ctx context.Context, id string, result AttemptResult) error
}

// Sink is one owner channel.
type Sink interface {
	// Name is the sink's stable identity. It is part of every outbox key, so
	// renaming a sink starts it over from a fresh baseline.
	Name() string
	// Selection is what this sink delivers.
	Selection() Selection
	// Deliver sends one notification. A *DeliveryError says whether and when
	// to retry; any other error is retried with the ordinary backoff.
	Deliver(ctx context.Context, notification Notification) error
}

// DeliveryError classifies a failed send.
type DeliveryError struct {
	// Reason is safe to log and to store. It never contains a credential.
	Reason string
	// Permanent marks a refusal that retrying cannot change, such as a
	// webhook that no longer exists.
	Permanent bool
	// RetryAfter is the delay the receiver asked for, when it asked for one.
	RetryAfter time.Duration
}

func (e *DeliveryError) Error() string { return e.Reason }

// Default delivery bounds. Eight attempts with a doubling delay from 30 s,
// capped at 30 min, keep trying for a little over an hour and a half, which
// outlasts an ordinary webhook outage without retrying a dead one all day.
const (
	DefaultInterval    = 15 * time.Second
	DefaultMaxAttempts = 8
	DefaultBaseDelay   = 30 * time.Second
	DefaultMaxDelay    = 30 * time.Minute
	DefaultBatchSize   = 10
	// DefaultSendTimeout bounds one send, so one stuck receiver holds the
	// notifier for at most this long per row.
	DefaultSendTimeout = 10 * time.Second
	// maxRetryAfter caps a receiver's requested delay. A rate limit that asks
	// for longer than this is treated as this, so a malformed header cannot
	// park the outbox for days.
	maxRetryAfter = time.Hour
)

// Notifier runs detection and delivery for a set of sinks.
type Notifier struct {
	Store  Store
	Sinks  []Sink
	Logger *slog.Logger
	// Interval is how often a pass runs. Zero means DefaultInterval.
	Interval time.Duration
	// MaxAttempts bounds the sends of one row. Zero means DefaultMaxAttempts.
	MaxAttempts int
	// BaseDelay and MaxDelay shape the exponential backoff.
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// BatchSize bounds the rows one sink sends in one pass.
	BatchSize int
	// SendTimeout bounds one send. Zero means DefaultSendTimeout.
	SendTimeout time.Duration
	Now         func() time.Time
}

// Run executes a pass immediately and then on every interval until ctx ends.
func (n *Notifier) Run(ctx context.Context) {
	interval := n.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	n.Tick(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.Tick(ctx)
		}
	}
}

// Tick runs one detection and delivery pass for every sink. A failure in one
// sink is logged and does not stop the others.
func (n *Notifier) Tick(ctx context.Context) {
	for _, sink := range n.Sinks {
		if ctx.Err() != nil {
			return
		}
		n.tickSink(ctx, sink)
	}
}

func (n *Notifier) tickSink(ctx context.Context, sink Sink) {
	logger := n.logger().With("sink", sink.Name())
	detection, err := n.Store.DetectOwnerNotifications(ctx, sink.Name(), sink.Selection(), n.now())
	if err != nil {
		logger.Error("owner notification detection failed", "error", err)
		// Rows already recorded can still be delivered.
	} else {
		if detection.Baselined != 0 {
			logger.Info("owner notifications baselined existing events; they will not be sent",
				"events", detection.Baselined)
		}
		if detection.Enqueued != 0 {
			logger.Debug("owner notifications recorded", "events", detection.Enqueued)
		}
	}
	batch := n.BatchSize
	if batch <= 0 {
		batch = DefaultBatchSize
	}
	due, err := n.Store.DueOwnerNotifications(ctx, sink.Name(), n.now(), batch)
	if err != nil {
		logger.Error("owner notification outbox read failed", "error", err)
		return
	}
	for _, notification := range due {
		if ctx.Err() != nil {
			return
		}
		if rateLimited := n.deliver(ctx, logger, sink, notification); rateLimited {
			// The receiver asked this sender to slow down. Every later row
			// goes to the same receiver, so it waits for the next pass.
			return
		}
	}
}

// deliver sends one row and records the outcome. It reports whether the
// receiver rate-limited the send.
func (n *Notifier) deliver(ctx context.Context, logger *slog.Logger, sink Sink, notification Notification) bool {
	timeout := n.SendTimeout
	if timeout <= 0 {
		timeout = DefaultSendTimeout
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	sendErr := sink.Deliver(sendCtx, notification)
	cancel()
	now := n.now()
	attempts := notification.Attempts + 1
	fields := []any{"event", notification.Event, "run", notification.RunID, "id", notification.ID, "attempts", attempts}
	if sendErr == nil {
		if err := n.Store.RecordOwnerNotificationAttempt(ctx, notification.ID, AttemptResult{
			State: StateDelivered, Attempts: attempts, At: now,
		}); err != nil {
			// The send happened and its record did not. The row stays pending
			// and is sent again, which is what at-least-once means.
			logger.Error("owner notification delivered but not recorded; it will be sent again",
				append(fields, "error", err)...)
			return false
		}
		logger.Info("owner notification delivered", fields...)
		return false
	}
	if ctx.Err() != nil {
		// Shutting down is not the receiver's fault and spends no attempt.
		return false
	}
	var classified *DeliveryError
	if !errors.As(sendErr, &classified) {
		classified = &DeliveryError{Reason: sendErr.Error()}
	}
	result := AttemptResult{State: StatePending, Attempts: attempts, Error: classified.Reason, At: now}
	maxAttempts := n.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	switch {
	case classified.Permanent || attempts >= maxAttempts:
		result.State = StateAbandoned
	case classified.RetryAfter > 0:
		result.NextAttemptAt = now.Add(min(classified.RetryAfter, maxRetryAfter))
	default:
		result.NextAttemptAt = now.Add(n.backoff(attempts))
	}
	if err := n.Store.RecordOwnerNotificationAttempt(ctx, notification.ID, result); err != nil {
		logger.Error("owner notification attempt could not be recorded", append(fields, "error", err)...)
		return classified.RetryAfter > 0
	}
	if result.State == StateAbandoned {
		logger.Warn("owner notification abandoned",
			append(fields, "permanent", classified.Permanent, "error", classified.Reason)...)
		return false
	}
	logger.Warn("owner notification send failed; will retry",
		append(fields, "next_attempt_at", result.NextAttemptAt, "error", classified.Reason)...)
	return classified.RetryAfter > 0
}

// backoff is the delay after the given number of failed attempts: BaseDelay,
// doubled for every attempt after the first, capped at MaxDelay.
func (n *Notifier) backoff(attempts int) time.Duration {
	base, ceiling := n.BaseDelay, n.MaxDelay
	if base <= 0 {
		base = DefaultBaseDelay
	}
	if ceiling <= 0 {
		ceiling = DefaultMaxDelay
	}
	delay := base
	for i := 1; i < attempts && delay < ceiling; i++ {
		delay *= 2
	}
	return min(delay, ceiling)
}

func (n *Notifier) now() time.Time {
	if n.Now != nil {
		return n.Now().UTC()
	}
	return time.Now().UTC()
}

func (n *Notifier) logger() *slog.Logger {
	if n.Logger != nil {
		return n.Logger
	}
	return slog.Default()
}
