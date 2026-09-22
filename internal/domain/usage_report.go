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

type UsageCostCoverage string

const (
	UsageCostComplete    UsageCostCoverage = "complete"
	UsageCostPartial     UsageCostCoverage = "partial"
	UsageCostUnavailable UsageCostCoverage = "unavailable"
)

type UsageTotals struct {
	UncachedInputTokens  int64             `json:"uncachedInputTokens"`
	CacheWriteTokens     int64             `json:"cacheWriteTokens"`
	CacheReadTokens      int64             `json:"cacheReadTokens"`
	OutputTokens         int64             `json:"outputTokens"`
	ProviderCostUSD      float64           `json:"providerCostUsd,omitempty"`
	ProviderCostReported bool              `json:"providerCostReported"`
	ProviderCostCoverage UsageCostCoverage `json:"providerCostCoverage"`
	EstimatedCostUSD     *float64          `json:"estimatedCostUsd,omitempty"`
	NormalizedSamples    int64             `json:"normalizedSamples"`
	Calls                int64             `json:"calls"`
	Turns                int64             `json:"turns"`
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
	Now                  time.Time
	RunProgress          ProgressState
	RunCompletedAt       *time.Time
	TaskProgress         map[string]ProgressState
	AttemptProgress      map[string]ProgressState
	AcceptedOutcomeCount int64
	HardTruncated        bool
}

type usageCandidate struct {
	sample UsageSample
	key    string
}

func NormalizeUsageReport(report UsageReport, context UsageNormalizationContext) UsageReport {
	samples := append([]UsageSample(nil), report.Samples...)
	sort.SliceStable(samples, func(i, j int) bool { return usageSampleLess(samples[i], samples[j]) })
	report.Samples = samples
	report.Coverage.RawSampleCount = int64(len(samples))
	report.Coverage.AttributedCount = int64(len(samples))
	report.Coverage.State = UsageCoverageComplete
	report.Totals.ProviderCostCoverage = UsageCostUnavailable
	report.MeasuredCostPerAcceptedOutcomeUSD = nil

	reasons := map[string]bool{}
	if report.Coverage.UnscopedUnattributedCount > 0 {
		report.Coverage.UnattributedCount = report.Coverage.UnscopedUnattributedCount
		reasons["unscoped samples lack an authoritative dispatch binding"] = true
	}
	if context.HardTruncated {
		report.Coverage.Truncated = true
		reasons["run usage exceeded the aggregation safety bound"] = true
	}
	if report.Coverage.DiagnosticDroppedCount > 0 {
		reasons["sanitized usage diagnostics exceeded the per-worker retention bound"] = true
	}

	turnSessions := map[string]bool{}
	turnBoundaries := map[string]bool{}
	candidates := make([]usageCandidate, 0, len(samples))
	seenEvents := map[string]bool{}
	for _, sample := range samples {
		updateUsageObservedRange(&report.Coverage, sample.ObservedAt)
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
		if sample.Kind == UsageKindDiagnostic {
			report.Coverage.DiagnosticCount++
			switch sample.DiagnosticCode {
			case "unsupported":
				report.Coverage.UnsupportedCount++
				reasons["unsupported provider usage evidence was recorded without content"] = true
			case "missing-fields":
				report.Coverage.MissingFieldCount++
				reasons["provider usage numeric fields were absent"] = true
			case "overflow":
				if sample.CumulativeTokens > report.Coverage.DiagnosticDroppedCount {
					report.Coverage.DiagnosticDroppedCount = sample.CumulativeTokens
				}
				reasons["provider usage diagnostics exceeded the bounded retention cap"] = true
			default:
				report.Coverage.MalformedCount++
				reasons["malformed provider usage evidence was recorded without content"] = true
			}
			continue
		}
		if sample.Kind != UsageKindCall && sample.Kind != UsageKindTurn {
			report.Coverage.UnsupportedCount++
			reasons["unsupported provider usage representation was excluded"] = true
			continue
		}
		if sample.FieldPresence != UsageFieldsAll {
			reasons["provider usage numeric fields were absent; present fields remain measured"] = true
		}
		key := usageSessionKey(sample)
		if sample.Kind == UsageKindTurn {
			turnSessions[key] = true
			if sample.BoundaryID != "" {
				turnBoundaries[key+"\x00"+sample.BoundaryID] = true
			}
		}
		candidates = append(candidates, usageCandidate{sample: sample, key: key})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.key == right.key && left.sample.Incarnation != "" &&
			left.sample.Incarnation == right.sample.Incarnation &&
			left.sample.Sequence > 0 && right.sample.Sequence > 0 &&
			left.sample.Sequence != right.sample.Sequence {
			return left.sample.Sequence < right.sample.Sequence
		}
		return usageSampleLess(left.sample, right.sample)
	})

	selected := make([]UsageSample, 0, len(candidates))
	lastCumulative := map[string]int64{}
	lastSequence := map[string]int64{}
	seenIncarnation := map[string]map[string]bool{}
	for _, candidate := range candidates {
		sample := candidate.sample
		if sample.Kind == UsageKindCall && turnSessions[candidate.key] {
			if sample.BoundaryID != "" && turnBoundaries[candidate.key+"\x00"+sample.BoundaryID] {
				report.Coverage.ExcludedOverlapCount++
				reasons["whole-turn totals superseded calls with the same causal boundary"] = true
			} else {
				report.Coverage.AmbiguousOverlapCount++
				report.Coverage.UnmatchedCallCount++
				reasons["calls without a matching causal turn boundary were conservatively excluded"] = true
			}
			continue
		}
		if sample.Kind == UsageKindCall && sample.CumulativeTokens > 0 {
			causal := sample.Incarnation != "" && sample.Sequence > 0
			cumulativeKey := candidate.key
			if causal {
				cumulativeKey += "\x00" + sample.Incarnation
				incarnations := seenIncarnation[candidate.key]
				if incarnations == nil {
					incarnations = map[string]bool{}
					seenIncarnation[candidate.key] = incarnations
				}
				if !incarnations[sample.Incarnation] {
					if len(incarnations) > 0 {
						report.Coverage.ResetCount++
						reasons["provider cumulative counter changed causal incarnation"] = true
					}
					incarnations[sample.Incarnation] = true
				}
				if prior := lastSequence[cumulativeKey]; prior > 0 && sample.Sequence <= prior {
					report.Coverage.DuplicateCount++
					reasons["replayed causal cumulative updates were deduplicated"] = true
					continue
				}
				lastSequence[cumulativeKey] = sample.Sequence
			}
			if previous := lastCumulative[cumulativeKey]; previous > 0 {
				switch {
				case sample.CumulativeTokens == previous:
					report.Coverage.DuplicateCount++
					reasons["repeated cumulative updates were deduplicated"] = true
					continue
				case sample.CumulativeTokens < previous:
					report.Coverage.CumulativeAmbiguityCount++
					if causal {
						reasons["cumulative counter decreased within one causal incarnation"] = true
					} else {
						reasons["cumulative counter decreased without incarnation/sequence evidence"] = true
					}
					continue
				}
			}
			lastCumulative[cumulativeKey] = sample.CumulativeTokens
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
	report.Coverage.ExpectedSessionCount = int64(len(report.ExpectedSessions))
	report.Coverage.MissingLogSessionCount = 0
	usableSessions := make(map[string]bool, len(selected))
	for _, sample := range selected {
		usableSessions[usageSessionKey(sample)] = true
	}
	for _, session := range report.ExpectedSessions {
		if !usableSessions[usageExpectedSessionKey(session)] {
			report.Coverage.MissingLogSessionCount++
		}
	}
	if report.Coverage.MissingLogSessionCount > 0 {
		reasons["expected execution session has no usable provider token evidence"] = true
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
	report.AcceptedOutcomeCount = context.AcceptedOutcomeCount

	if len(selected) == 0 {
		reasons["no usable provider usage evidence was observed"] = true
	}
	if context.RunProgress != "" && !context.RunProgress.Terminal() && report.Coverage.ObservedThrough != nil &&
		context.Now.Sub(*report.Coverage.ObservedThrough) > UsageFreshnessWindow {
		report.Coverage.State = UsageCoverageStale
		reasons["latest provider usage evidence is stale for an active workflow"] = true
	} else if len(selected) == 0 && report.Coverage.UnsupportedCount > 0 &&
		report.Coverage.MalformedCount == 0 && report.Coverage.MissingFieldCount == 0 {
		report.Coverage.State = UsageCoverageUnsupported
	} else if len(selected) == 0 || report.Coverage.UnknownModelCount > 0 || report.Coverage.MalformedCount > 0 ||
		report.Coverage.UnsupportedCount > 0 || report.Coverage.DiagnosticCount > 0 || report.Coverage.DiagnosticDroppedCount > 0 ||
		report.Coverage.MissingFieldCount > 0 || report.Coverage.AmbiguousOverlapCount > 0 ||
		report.Coverage.CumulativeAmbiguityCount > 0 || report.Coverage.LateCount > 0 ||
		report.Coverage.UnmatchedCallCount > 0 || report.Coverage.MissingLogSessionCount > 0 ||
		report.Coverage.Truncated || report.Coverage.UnattributedCount > 0 {
		report.Coverage.State = UsageCoveragePartial
	}
	report.Coverage.Reasons = sortedUsageReasons(reasons)
	if len(report.Coverage.Reasons) > 0 {
		report.Coverage.Reason = strings.Join(report.Coverage.Reasons, "; ")
	}
	if report.RunProgress == ProgressSucceeded && report.AcceptedOutcomeCount > 0 &&
		report.Coverage.State == UsageCoverageComplete && !report.Coverage.Truncated &&
		report.Coverage.MissingLogSessionCount == 0 && report.Coverage.UnattributedCount == 0 &&
		report.Totals.ProviderCostCoverage == UsageCostComplete {
		costPerOutcome := report.Totals.ProviderCostUSD / float64(report.AcceptedOutcomeCount)
		report.MeasuredCostPerAcceptedOutcomeUSD = &costPerOutcome
	}
	return report
}

func updateUsageObservedRange(coverage *UsageCoverage, observedAt time.Time) {
	if observedAt.IsZero() {
		return
	}
	if coverage.ObservedFrom == nil || observedAt.Before(*coverage.ObservedFrom) {
		at := observedAt
		coverage.ObservedFrom = &at
	}
	if coverage.ObservedThrough == nil || observedAt.After(*coverage.ObservedThrough) {
		at := observedAt
		coverage.ObservedThrough = &at
	}
}

func usageSampleLess(left, right UsageSample) bool {
	if !left.ObservedAt.Equal(right.ObservedAt) {
		return left.ObservedAt.Before(right.ObservedAt)
	}
	if left.WorkerID != right.WorkerID {
		return left.WorkerID < right.WorkerID
	}
	return left.SourceEventID < right.SourceEventID
}

func usageSessionKey(sample UsageSample) string {
	a := sample.Attribution
	return sample.WorkerID + "\x00" + sample.ProviderInstanceID + "\x00" + sample.ThreadID + "\x00" +
		a.AssignmentID + "\x00" + a.AttemptID
}

func usageExpectedSessionKey(session UsageExecutionSession) string {
	a := session.Attribution
	return session.WorkerID + "\x00" + session.ProviderInstanceID + "\x00" + session.ThreadID + "\x00" +
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
	switch {
	case sample.CostReported && total.NormalizedSamples == 1:
		total.ProviderCostCoverage = UsageCostComplete
	case !sample.CostReported && total.NormalizedSamples == 1:
		total.ProviderCostCoverage = UsageCostUnavailable
	case sample.CostReported && total.ProviderCostCoverage == UsageCostUnavailable:
		total.ProviderCostCoverage = UsageCostPartial
	case !sample.CostReported && total.ProviderCostCoverage == UsageCostComplete:
		total.ProviderCostCoverage = UsageCostPartial
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
