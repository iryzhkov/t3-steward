package domain

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// QuotaWaitCondition is the structured condition of a quota wait: one pool
// and exactly one of a usage threshold, the normal phase, or the window
// reset. The coordinator settles it from the merged bucket observations,
// which is the same reading its admission is derived from.
type QuotaWaitCondition struct {
	Pool string `json:"pool"`
	// Below is met when every bucket of the pool is used less than this
	// percent.
	Below *float64 `json:"below,omitempty"`
	// Phase is met when every bucket of the pool is in this phase; only
	// normal is accepted.
	Phase Phase `json:"phase,omitempty"`
	// Reset is met when the window that was current at registration has
	// reset: ResetAt, recorded at registration, has passed or a later reset
	// time is observed.
	Reset   bool       `json:"reset,omitempty"`
	ResetAt *time.Time `json:"resetAt,omitempty"`
}

// Validate checks that exactly one condition is named.
func (c QuotaWaitCondition) Validate() error {
	if strings.TrimSpace(c.Pool) == "" {
		return errors.New("--quota needs the pool id")
	}
	named := 0
	if c.Below != nil {
		named++
		if *c.Below <= 0 || *c.Below > 100 {
			return fmt.Errorf("--below %v is not a percent above 0 and at most 100", *c.Below)
		}
	}
	if c.Phase != "" {
		named++
		if c.Phase != PhaseNormal {
			return fmt.Errorf("--phase %q: a quota wait waits for the pool to be normal again; the other phases are what it waits out", c.Phase)
		}
	}
	if c.Reset {
		named++
	}
	if named != 1 {
		return errors.New("--quota takes exactly one of --below N, --phase normal or --reset")
	}
	return nil
}

// String is the condition text: "quota <pool> below 50", "quota <pool>
// phase normal" or "quota <pool> reset".
func (c QuotaWaitCondition) String() string {
	switch {
	case c.Below != nil:
		return fmt.Sprintf("quota %s below %s", c.Pool, formatPercent(*c.Below))
	case c.Phase != "":
		return fmt.Sprintf("quota %s phase %s", c.Pool, c.Phase)
	default:
		return fmt.Sprintf("quota %s reset", c.Pool)
	}
}

// QuotaPoolObservation is a pool read as its most used bucket: Percent,
// ResetsAt and ObservedAt all come from that one bucket, named by Window, so
// the triple is a reading some observation actually made (F4: the highest
// percent of one window, the earliest reset of another and the newest time of
// a third described nothing). Phase is the highest phase of any bucket, and
// Windows lists every bucket's own reading.
type QuotaPoolObservation struct {
	Pool       string               `json:"pool"`
	Phase      Phase                `json:"phase"`
	Percent    float64              `json:"percent"`
	ResetsAt   *time.Time           `json:"resetsAt,omitempty"`
	Buckets    int                  `json:"buckets"`
	ObservedAt time.Time            `json:"observedAt"`
	Window     string               `json:"window,omitempty"`
	Windows    []QuotaWindowReading `json:"windows,omitempty"`
}

// ObserveQuotaPool folds the bucket states that belong to a pool into one
// observation; PoolBucketMatcher says which belong. The most used bucket is
// the one with the highest percent, then the highest phase, then the earliest
// reset, then the lowest key, so the choice does not depend on input order.
func ObserveQuotaPool(pool QuotaPool, states []BucketState) QuotaPoolObservation {
	observation := QuotaPoolObservation{Pool: pool.ID, Phase: PhaseNormal}
	belongs := PoolBucketMatcher(pool)
	var worst *BucketState
	for index := range states {
		state := &states[index]
		if !belongs(*state) {
			continue
		}
		observation.Buckets++
		observation.Windows = append(observation.Windows, quotaWindowReading(*state))
		if state.Phase.Rank() > observation.Phase.Rank() {
			observation.Phase = state.Phase
		}
		if worst == nil || moreUsedBucket(*state, *worst) {
			worst = state
		}
	}
	if worst != nil {
		reading := quotaWindowReading(*worst)
		observation.Percent, observation.ResetsAt, observation.ObservedAt = reading.UsedPercent, reading.ResetsAt, reading.ObservedAt
		observation.Window = worst.Key.String()
	}
	sort.Slice(observation.Windows, func(i, j int) bool {
		return observation.Windows[i].Key.String() < observation.Windows[j].Key.String()
	})
	return observation
}

// moreUsedBucket orders buckets for ObserveQuotaPool.
func moreUsedBucket(candidate, current BucketState) bool {
	if candidate.UsedPercent != current.UsedPercent {
		return candidate.UsedPercent > current.UsedPercent
	}
	if candidate.Phase.Rank() != current.Phase.Rank() {
		return candidate.Phase.Rank() > current.Phase.Rank()
	}
	if (candidate.ResetsAt == nil) != (current.ResetsAt == nil) {
		return candidate.ResetsAt != nil
	}
	if candidate.ResetsAt != nil && !candidate.ResetsAt.Equal(*current.ResetsAt) {
		return candidate.ResetsAt.Before(*current.ResetsAt)
	}
	return candidate.Key.String() < current.Key.String()
}

// earliestReset is the earliest reset among the observation's buckets: a
// --reset wait recorded the earliest one at registration, and its window has
// reset once every bucket reports a later one.
func (o QuotaPoolObservation) earliestReset() *time.Time {
	var earliest *time.Time
	for _, window := range o.Windows {
		if window.ResetsAt != nil && (earliest == nil || window.ResetsAt.Before(*earliest)) {
			earliest = window.ResetsAt
		}
	}
	return earliest
}

// EarliestQuotaReset is the earliest reset any bucket of the pool reports, the
// window a --reset wait waits out; nil when none reports one.
func EarliestQuotaReset(pool QuotaPool, states []BucketState) *time.Time {
	belongs := PoolBucketMatcher(pool)
	var earliest *time.Time
	for _, state := range states {
		if !belongs(state) || state.ResetsAt == nil {
			continue
		}
		if earliest == nil || state.ResetsAt.Before(*earliest) {
			at := *state.ResetsAt
			earliest = &at
		}
	}
	return earliest
}

// EvaluateQuotaWait is the quota wait evaluator: it finds the condition's
// pool, reads it from the observations that govern it and evaluates the
// condition. A pool whose governing buckets are unknown stays pending with
// that reason instead of being read from every window.
//
// A --below or --phase wait is met only by a complete, fresh window set (F3:
// a stale window that was present, or a declared weekly window that was
// missing, met it). maxAge is the oldest reading that counts as fresh; zero
// means DefaultQuotaStaleAfter. A --reset wait waits for the clock to pass
// the reset recorded at registration, a fact no reading can make stale, so it
// is evaluated as before.
func EvaluateQuotaWait(condition QuotaWaitCondition, pools []QuotaPool, states []BucketState, now time.Time, maxAge time.Duration) (QuotaPoolObservation, TaskWaitOutcome, string, error) {
	pool, err := FindQuotaPool(pools, condition.Pool)
	if err != nil {
		return QuotaPoolObservation{}, "", "", err
	}
	if pool.Collector != nil && !condition.Reset {
		selected := map[BucketKey]bool{}
		for _, reading := range ReadQuotaWindows(pool, states, now, maxAge).Windows {
			if !reading.Missing {
				selected[reading.Key] = true
			}
		}
		var governing []BucketState
		for _, state := range NewestQuotaStates(states) {
			if selected[state.Key] {
				governing = append(governing, state)
			}
		}
		observation := ObserveQuotaPool(pool, governing)
		if problem, bad := ReadQuotaWindows(pool, states, now, maxAge).Problem(); bad {
			return observation, "", fmt.Sprintf("pending: pool %s has %s", condition.Pool, problem), nil
		}
		outcome, reason := condition.Evaluate(observation, now)
		return observation, outcome, reason, nil
	}
	observation := ObserveQuotaPool(pool, states)
	if pool.BucketSelection == BucketSelectionUnknown {
		return observation, "", fmt.Sprintf("pending: the buckets that govern pool %s are unknown (the coordinator could not read its quota observations)", condition.Pool), nil
	}
	if observation.Buckets > 0 && !condition.Reset {
		set := ReadQuotaWindows(pool, states, now, maxAge)
		if problem, found := set.Problem(); found && problem.Code != QuotaWindowExhausted {
			return observation, "", fmt.Sprintf("pending: pool %s has %s; a quota wait is met only when every window of the pool has a fresh reading", condition.Pool, problem), nil
		}
	}
	outcome, reason := condition.Evaluate(observation, now)
	return observation, outcome, reason, nil
}

// Evaluate reports whether the condition holds against an observation: met,
// or empty while it does not. A quota condition has no failed outcome; only
// the deadline, a cancellation or a pool that disappears end it otherwise.
func (c QuotaWaitCondition) Evaluate(observation QuotaPoolObservation, now time.Time) (TaskWaitOutcome, string) {
	if observation.Buckets == 0 {
		return "", fmt.Sprintf("pending: no observation of pool %s yet", c.Pool)
	}
	switch {
	case c.Below != nil:
		if observation.Percent < *c.Below {
			return TaskWaitMet, fmt.Sprintf("pool %s is at %s%%, below %s%%", c.Pool, formatPercent(observation.Percent), formatPercent(*c.Below))
		}
		return "", fmt.Sprintf("pending: pool %s is at %s%%, not below %s%%", c.Pool, formatPercent(observation.Percent), formatPercent(*c.Below))
	case c.Phase != "":
		if observation.Phase == c.Phase {
			return TaskWaitMet, fmt.Sprintf("pool %s is %s", c.Pool, observation.Phase)
		}
		return "", fmt.Sprintf("pending: pool %s is %s", c.Pool, observation.Phase)
	default:
		if c.ResetAt == nil {
			return "", "pending: no reset time was recorded at registration"
		}
		if earliest := observation.earliestReset(); !now.Before(*c.ResetAt) || (earliest != nil && earliest.After(*c.ResetAt)) {
			return TaskWaitMet, fmt.Sprintf("pool %s reset at %s", c.Pool, c.ResetAt.UTC().Format(time.RFC3339))
		}
		return "", fmt.Sprintf("pending: pool %s resets at %s", c.Pool, c.ResetAt.UTC().Format(time.RFC3339))
	}
}

// QuotaTrailerFields are the wake trailer pairs of a quota observation.
func QuotaTrailerFields(observation QuotaPoolObservation) map[string]string {
	fields := map[string]string{
		"pool":    observation.Pool,
		"phase":   string(observation.Phase),
		"percent": formatPercent(observation.Percent),
	}
	if observation.ResetsAt != nil {
		fields["resetsAt"] = observation.ResetsAt.UTC().Format(time.RFC3339)
	}
	if observation.Window != "" {
		fields["window"] = observation.Window
	}
	return fields
}

func formatPercent(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// FindQuotaPool returns the configured pool by id, or the ids of the pools
// that exist, for a refusal that names them.
func FindQuotaPool(pools []QuotaPool, id string) (QuotaPool, error) {
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		if pool.ID == id {
			return pool, nil
		}
		names = append(names, pool.ID)
	}
	if len(names) == 0 {
		return QuotaPool{}, fmt.Errorf("quota pool %q is unknown: the coordinator has reconciled no quota pools yet", id)
	}
	return QuotaPool{}, fmt.Errorf("quota pool %q is unknown; the pools are %s", id, strings.Join(names, ", "))
}

// MergeQuotaObservations preserves equally timed local and worker competitors.
// Worker identity is supplied by the authenticated snapshot boundary, not by a
// claimed producer field. Newer reports replace older local keys as before.
func MergeQuotaObservations(local []BucketState, workers []WorkerSnapshot) []BucketState {
	newestLocal := map[BucketKey]time.Time{}
	for _, state := range local {
		if at, ok := newestLocal[state.Key]; !ok || state.ObservedAt.After(at) {
			newestLocal[state.Key] = state.ObservedAt
		}
	}
	workerStates := map[BucketKey][]BucketState{}
	for _, worker := range workers {
		for _, observed := range worker.QuotaObservations {
			if observed.ObservedAt.IsZero() {
				continue
			}
			if localAt, ok := newestLocal[observed.Key]; ok && observed.ObservedAt.Before(localAt) {
				continue
			}
			current := workerStates[observed.Key]
			if len(current) > 0 && observed.ObservedAt.Before(current[0].ObservedAt) {
				continue
			}
			if len(current) > 0 && observed.ObservedAt.After(current[0].ObservedAt) {
				current = nil
			}
			p := CloneQuotaProvenance(observed.Provenance)
			var originUntil *time.Time
			if p != nil {
				p.OriginWorkerID = worker.WorkerID
				at := worker.ObservedAt.Add(time.Minute)
				if !worker.ValidUntil.IsZero() && worker.ValidUntil.Before(at) {
					at = worker.ValidUntil
				}
				if !worker.Connected || worker.ObservedAt.IsZero() {
					at = time.Time{}
				}
				originUntil = &at
			}
			state := BucketState{Key: observed.Key, Phase: observed.Phase, Epoch: observed.Epoch,
				LimitName: observed.LimitName, ModelSelector: observed.ModelSelector, UsedPercent: observed.UsedPercent,
				ResetsAt: observed.ResetsAt, ObservedAt: observed.ObservedAt, Healthy: observed.Healthy,
				UpdatedAt: observed.ObservedAt, DrainsAt: observed.DrainsAt, DrainDeadline: observed.DrainDeadline, Provenance: p, OriginValidUntil: originUntil}
			if observed.RatePerMinute > 0 && observed.RatePerMinute <= 100 {
				state.RatePerMinute = observed.RatePerMinute
			}
			if observed.DrainPercent > 0 && observed.DrainPercent <= 100 {
				state.AppliedThresholds = &ThresholdSet{DrainPercent: observed.DrainPercent}
			}
			if state.Epoch == "" {
				state.Epoch = EpochFor(observed.ResetsAt)
			}
			workerStates[observed.Key] = append(current, state)
		}
	}
	if len(workerStates) == 0 {
		return local
	}
	merged := make([]BucketState, 0, len(local)+len(workerStates))
	for _, state := range local {
		replacement := workerStates[state.Key]
		if len(replacement) > 0 && replacement[0].ObservedAt.After(newestLocal[state.Key]) {
			continue
		}
		merged = append(merged, state)
	}
	keys := make([]BucketKey, 0, len(workerStates))
	for key := range workerStates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, key := range keys {
		merged = append(merged, workerStates[key]...)
	}
	return merged
}
