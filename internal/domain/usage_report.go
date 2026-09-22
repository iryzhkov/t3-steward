package domain

import (
	"sort"
	"strings"
	"time"
)

type UsageCoverageState string

const (
	UsageCoverageComplete    UsageCoverageState = "complete"
	UsageCoveragePartial     UsageCoverageState = "partial"
	UsageCoverageStale       UsageCoverageState = "stale"
	UsageCoverageUnsupported UsageCoverageState = "unsupported"
)

const UsageFreshnessWindow = 15 * time.Minute

type UsageTotals struct {
	UncachedInputTokens  int64    `json:"uncachedInputTokens"`
	CacheWriteTokens     int64    `json:"cacheWriteTokens"`
	CacheReadTokens      int64    `json:"cacheReadTokens"`
	OutputTokens         int64    `json:"outputTokens"`
	ProviderCostUSD      float64  `json:"providerCostUsd,omitempty"`
	ProviderCostReported bool     `json:"providerCostReported"`
	EstimatedCostUSD     *float64 `json:"estimatedCostUsd,omitempty"`
	NormalizedSamples    int64    `json:"normalizedSamples"`
	Calls                int64    `json:"calls"`
	Turns                int64    `json:"turns"`
}

type UsageAggregate struct {
	Key       string        `json:"key"`
	TaskID    string        `json:"taskId,omitempty"`
	AttemptID string        `json:"attemptId,omitempty"`
	Role      ExecutionRole `json:"role,omitempty"`
	Model     string        `json:"model,omitempty"`
	Outcome   ProgressState `json:"outcome,omitempty"`
	Totals    UsageTotals   `json:"totals"`
}

type UsageNormalizationContext struct {
	Now             time.Time
	RunProgress     ProgressState
	RunCompletedAt  *time.Time
	TaskProgress    map[string]ProgressState
	AttemptProgress map[string]ProgressState
	HardTruncated   bool
}

type usageCandidate struct {
	sample UsageSample
	key    string
}

func NormalizeUsageReport(report UsageReport, context UsageNormalizationContext) UsageReport {
	samples := append([]UsageSample(nil), report.Samples...)
	sort.SliceStable(samples, func(i, j int) bool {
		if !samples[i].ObservedAt.Equal(samples[j].ObservedAt) {
			return samples[i].ObservedAt.Before(samples[j].ObservedAt)
		}
		if samples[i].WorkerID != samples[j].WorkerID {
			return samples[i].WorkerID < samples[j].WorkerID
		}
		return samples[i].SourceEventID < samples[j].SourceEventID
	})
	report.Samples = samples
	report.Coverage.RawSampleCount = int64(len(samples))
	report.Coverage.AttributedCount = int64(len(samples))
	report.Coverage.State = UsageCoverageComplete

	reasons := map[string]bool{}
	if report.Coverage.UnscopedUnattributedCount > 0 {
		report.Coverage.UnattributedCount = report.Coverage.UnscopedUnattributedCount
		reasons["unscoped samples lack an authoritative dispatch binding"] = true
	}
	if context.HardTruncated {
		report.Coverage.Truncated = true
		reasons["run usage exceeded the aggregation safety bound"] = true
	}

	lastTurnAt := map[string]time.Time{}
	candidates := make([]usageCandidate, 0, len(samples))
	seenEvents := map[string]bool{}
	for _, sample := range samples {
		if report.Coverage.ObservedFrom == nil || sample.ObservedAt.Before(*report.Coverage.ObservedFrom) {
			at := sample.ObservedAt
			report.Coverage.ObservedFrom = &at
		}
		if report.Coverage.ObservedThrough == nil || sample.ObservedAt.After(*report.Coverage.ObservedThrough) {
			at := sample.ObservedAt
			report.Coverage.ObservedThrough = &at
		}
		eventKey := sample.WorkerID + "\x00" + sample.SourceEventID
		if sample.SourceEventID == "" || sample.ObservedAt.IsZero() ||
			sample.InputTokens < 0 || sample.CacheWriteTokens < 0 ||
			sample.CacheReadTokens < 0 || sample.OutputTokens < 0 || sample.CostUSD < 0 {
			report.Coverage.MalformedCount++
			reasons["malformed provider usage evidence was excluded"] = true
			continue
		}
		if seenEvents[eventKey] {
			report.Coverage.DuplicateCount++
			reasons["replayed provider events were deduplicated"] = true
			continue
		}
		seenEvents[eventKey] = true
		if sample.Kind != UsageKindCall && sample.Kind != UsageKindTurn {
			report.Coverage.UnsupportedCount++
			reasons["unsupported provider usage representation was excluded"] = true
			continue
		}
		key := usageSessionKey(sample)
		if sample.Kind == UsageKindTurn && sample.ObservedAt.After(lastTurnAt[key]) {
			lastTurnAt[key] = sample.ObservedAt
		}
		candidates = append(candidates, usageCandidate{sample: sample, key: key})
	}

	selected := make([]UsageSample, 0, len(candidates))
	lastCumulative := map[string]int64{}
	for _, candidate := range candidates {
		sample := candidate.sample
		if sample.Kind == UsageKindCall {
			if turnAt := lastTurnAt[candidate.key]; !turnAt.IsZero() {
				if !sample.ObservedAt.After(turnAt) {
					report.Coverage.ExcludedOverlapCount++
					continue
				}
				report.Coverage.UnmatchedCallCount++
				reasons["call rows after the latest whole-turn summary were retained as unmatched evidence"] = true
			}
		}
		if sample.Kind == UsageKindCall && sample.CumulativeTokens > 0 {
			if previous := lastCumulative[candidate.key]; previous > 0 {
				switch {
				case sample.CumulativeTokens == previous:
					report.Coverage.DuplicateCount++
					reasons["repeated cumulative updates were deduplicated"] = true
					continue
				case sample.CumulativeTokens < previous:
					report.Coverage.ResetCount++
					reasons["provider cumulative counter rotation/reset was observed"] = true
				}
			}
			lastCumulative[candidate.key] = sample.CumulativeTokens
		}
		if sample.Model == "" {
			report.Coverage.UnknownModelCount++
			reasons["provider model identity is missing or unknown"] = true
		}
		if context.RunCompletedAt != nil && sample.ObservedAt.After(context.RunCompletedAt.Add(time.Minute)) {
			report.Coverage.LateCount++
			reasons["provider evidence arrived after workflow completion"] = true
		}
		selected = append(selected, sample)
		addUsageTotals(&report.Totals, sample)
	}

	report.Coverage.NormalizedSampleCount = int64(len(selected))
	if report.Coverage.ExcludedOverlapCount > 0 {
		reasons["whole-turn totals superseded overlapping call rows"] = true
	}

	report.ByTask = aggregateUsage(selected, func(s UsageSample) (string, UsageAggregate) {
		key := s.Attribution.TaskID
		if key == "" {
			key = "workflow-overhead"
		}
		return key, UsageAggregate{Key: key, TaskID: s.Attribution.TaskID, Outcome: context.TaskProgress[s.Attribution.TaskID]}
	})
	report.ByAttempt = aggregateUsage(selected, func(s UsageSample) (string, UsageAggregate) {
		key := s.Attribution.AttemptID
		if key == "" {
			key = "workflow-overhead"
		}
		return key, UsageAggregate{Key: key, AttemptID: s.Attribution.AttemptID, Outcome: context.AttemptProgress[s.Attribution.AttemptID]}
	})
	report.ByRole = aggregateUsage(selected, func(s UsageSample) (string, UsageAggregate) {
		key := string(s.Attribution.Role)
		if key == "" {
			key = "unknown"
		}
		return key, UsageAggregate{Key: key, Role: s.Attribution.Role}
	})
	report.ByModel = aggregateUsage(selected, func(s UsageSample) (string, UsageAggregate) {
		key := s.Model
		if key == "" {
			key = "unknown"
		}
		return key, UsageAggregate{Key: key, Model: s.Model}
	})
	report.RunProgress = context.RunProgress

	if len(selected) == 0 {
		reasons["no usable provider usage evidence was observed"] = true
	}
	if context.RunProgress != "" && !context.RunProgress.Terminal() && report.Coverage.ObservedThrough != nil &&
		context.Now.Sub(*report.Coverage.ObservedThrough) > UsageFreshnessWindow {
		report.Coverage.State = UsageCoverageStale
		reasons["latest provider usage evidence is stale for an active workflow"] = true
	} else if len(selected) == 0 && report.Coverage.UnsupportedCount > 0 && report.Coverage.MalformedCount == 0 {
		report.Coverage.State = UsageCoverageUnsupported
	} else if len(selected) == 0 || report.Coverage.UnknownModelCount > 0 || report.Coverage.MalformedCount > 0 ||
		report.Coverage.UnsupportedCount > 0 || report.Coverage.LateCount > 0 || report.Coverage.UnmatchedCallCount > 0 || report.Coverage.Truncated ||
		report.Coverage.UnattributedCount > 0 {
		report.Coverage.State = UsageCoveragePartial
	}
	report.Coverage.Reasons = sortedUsageReasons(reasons)
	if len(report.Coverage.Reasons) > 0 {
		report.Coverage.Reason = strings.Join(report.Coverage.Reasons, "; ")
	}
	return report
}

func usageSessionKey(sample UsageSample) string {
	a := sample.Attribution
	return sample.WorkerID + "\x00" + sample.ProviderInstanceID + "\x00" + sample.ThreadID + "\x00" +
		a.AssignmentID + "\x00" + a.AttemptID
}

func addUsageTotals(total *UsageTotals, sample UsageSample) {
	total.UncachedInputTokens += sample.InputTokens
	total.CacheWriteTokens += sample.CacheWriteTokens
	total.CacheReadTokens += sample.CacheReadTokens
	total.OutputTokens += sample.OutputTokens
	total.NormalizedSamples++
	if sample.Kind == UsageKindCall {
		total.Calls++
	} else if sample.Kind == UsageKindTurn {
		total.Turns++
	}
	if sample.CostReported {
		total.ProviderCostUSD += sample.CostUSD
		total.ProviderCostReported = true
	}
}

func aggregateUsage(samples []UsageSample, identify func(UsageSample) (string, UsageAggregate)) []UsageAggregate {
	byKey := map[string]UsageAggregate{}
	for _, sample := range samples {
		key, aggregate := identify(sample)
		current, ok := byKey[key]
		if !ok {
			current = aggregate
		}
		addUsageTotals(&current.Totals, sample)
		byKey[key] = current
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]UsageAggregate, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}

func sortedUsageReasons(reasons map[string]bool) []string {
	out := make([]string, 0, len(reasons))
	for reason := range reasons {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}
