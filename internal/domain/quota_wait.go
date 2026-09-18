package domain

import (
	"errors"
	"fmt"
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

// QuotaPoolObservation is a pool read as the worst of its buckets: the
// highest phase, the highest usage and the earliest reset.
type QuotaPoolObservation struct {
	Pool       string     `json:"pool"`
	Phase      Phase      `json:"phase"`
	Percent    float64    `json:"percent"`
	ResetsAt   *time.Time `json:"resetsAt,omitempty"`
	Buckets    int        `json:"buckets"`
	ObservedAt time.Time  `json:"observedAt"`
}

// ObserveQuotaPool folds the bucket states that belong to a pool into one
// observation. A pool that has been reconciled names its buckets; one that
// has not is matched by its provider instances.
func ObserveQuotaPool(pool QuotaPool, states []BucketState) QuotaPoolObservation {
	observation := QuotaPoolObservation{Pool: pool.ID, Phase: PhaseNormal}
	named := make(map[BucketKey]bool, len(pool.Buckets))
	for _, key := range pool.Buckets {
		named[key] = true
	}
	instances := make(map[string]bool, len(pool.ProviderInstanceIDs))
	for _, instance := range pool.ProviderInstanceIDs {
		instances[instance] = true
	}
	for _, state := range states {
		belongs := named[state.Key]
		if len(named) == 0 {
			belongs = instances[state.Key.ProviderInstanceID] && (pool.AccountID == "" || pool.AccountID == state.Key.AccountID)
		}
		if !belongs {
			continue
		}
		observation.Buckets++
		if state.Phase.Rank() > observation.Phase.Rank() {
			observation.Phase = state.Phase
		}
		if state.UsedPercent > observation.Percent {
			observation.Percent = state.UsedPercent
		}
		if state.ResetsAt != nil && (observation.ResetsAt == nil || state.ResetsAt.Before(*observation.ResetsAt)) {
			at := *state.ResetsAt
			observation.ResetsAt = &at
		}
		if state.ObservedAt.After(observation.ObservedAt) {
			observation.ObservedAt = state.ObservedAt
		}
	}
	return observation
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
		if !now.Before(*c.ResetAt) || (observation.ResetsAt != nil && observation.ResetsAt.After(*c.ResetAt)) {
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

// MergeQuotaObservations folds the bucket observations workers reported into
// the coordinator's own bucket states. For each bucket key the freshest
// observation wins: a worker reading newer than every local reading of the
// same key replaces them, an older or equally old one is dropped, and a key
// no local reading has is added. Duplicate local readings of one key are
// kept as they were, for the conflict handling downstream.
func MergeQuotaObservations(local []BucketState, workers []WorkerSnapshot) []BucketState {
	newestLocal := make(map[BucketKey]time.Time, len(local))
	for _, state := range local {
		if state.ObservedAt.After(newestLocal[state.Key]) {
			newestLocal[state.Key] = state.ObservedAt
		}
	}
	fresher := make(map[BucketKey]BucketState)
	for _, worker := range workers {
		for _, observed := range worker.QuotaObservations {
			if observed.ObservedAt.IsZero() {
				continue
			}
			if localAt, ok := newestLocal[observed.Key]; ok && !observed.ObservedAt.After(localAt) {
				continue
			}
			if current, ok := fresher[observed.Key]; ok && !observed.ObservedAt.After(current.ObservedAt) {
				continue
			}
			state := BucketState{
				Key: observed.Key, Phase: observed.Phase, Epoch: observed.Epoch,
				LimitName: observed.LimitName, ModelSelector: observed.ModelSelector,
				UsedPercent: observed.UsedPercent, ResetsAt: observed.ResetsAt,
				ObservedAt: observed.ObservedAt, Healthy: observed.Healthy, UpdatedAt: observed.ObservedAt,
			}
			if state.Epoch == "" {
				state.Epoch = EpochFor(observed.ResetsAt)
			}
			fresher[observed.Key] = state
		}
	}
	if len(fresher) == 0 {
		return local
	}
	merged := make([]BucketState, 0, len(local)+len(fresher))
	for _, state := range local {
		if _, replaced := fresher[state.Key]; replaced {
			continue
		}
		merged = append(merged, state)
	}
	for _, state := range fresher {
		merged = append(merged, state)
	}
	return merged
}
