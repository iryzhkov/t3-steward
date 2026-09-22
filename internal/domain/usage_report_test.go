package domain

import (
	"testing"
	"time"
)

func TestNormalizeUsageReportAvoidsOverlapAndCumulativeInflation(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	attribution := UsageAttribution{
		Status: UsageAttributed, WorkerID: "worker", WorkflowRunID: "run",
		TaskID: "task", AttemptID: "attempt", AssignmentID: "assignment", Role: ExecutionRoleExecutor,
	}
	sample := func(id, provider, thread, model, kind string, cumulative, input, write, read, output int64) UsageSample {
		return UsageSample{
			WorkerID: "worker", ProviderInstanceID: provider, ThreadID: thread, Model: model,
			ObservedAt: now.Add(time.Duration(len(id)) * time.Second), SourceEventID: id, Kind: kind,
			CumulativeTokens: cumulative, InputTokens: input, CacheWriteTokens: write,
			CacheReadTokens: read, OutputTokens: output, FieldPresence: UsageFieldsAll, Attribution: attribution,
		}
	}
	claudeCall := sample("claude-call", "claude", "claude-thread", "claude-sonnet", UsageKindCall, 0, 100, 10, 20, 30)
	claudeCall.BoundaryID = "turn-1"
	claudeTurnA := sample("claude-turn#a", "claude", "claude-thread", "claude-sonnet", UsageKindTurn, 0, 20, 5, 6, 7)
	claudeTurnA.BoundaryID = "turn-1"
	claudeTurnA.CostUSD, claudeTurnA.CostReported = 1, true
	claudeTurnB := sample("claude-turn#b", "claude", "claude-thread", "claude-haiku", UsageKindTurn, 0, 8, 1, 2, 3)
	claudeTurnB.BoundaryID = "turn-1"
	claudeTurnB.CostUSD, claudeTurnB.CostReported = .5, true
	codexFirst := sample("codex-1", "codex", "codex-thread", "gpt-6-sol", UsageKindCall, 50, 9, 0, 4, 2)
	codexFirst.Incarnation, codexFirst.Sequence = "session-1", 1
	codexReplay := sample("codex-2", "codex", "codex-thread", "gpt-6-sol", UsageKindCall, 50, 9, 0, 4, 2)
	codexReplay.Incarnation, codexReplay.Sequence = "session-1", 2
	codexReset := sample("codex-3", "codex", "codex-thread", "gpt-6-sol", UsageKindCall, 20, 3, 0, 1, 1)
	codexReset.Incarnation, codexReset.Sequence = "session-2", 1

	report := NormalizeUsageReport(UsageReport{WorkflowRunID: "run", Samples: []UsageSample{
		codexReplay, claudeCall, claudeTurnB, codexReset, claudeTurnA, codexFirst,
	}}, UsageNormalizationContext{
		Now: now.Add(time.Minute), RunProgress: ProgressSucceeded,
		TaskProgress:    map[string]ProgressState{"task": ProgressSucceeded},
		AttemptProgress: map[string]ProgressState{"attempt": ProgressSucceeded},
	})

	if got := report.Totals; got.UncachedInputTokens != 40 || got.CacheWriteTokens != 6 ||
		got.CacheReadTokens != 13 || got.OutputTokens != 13 || got.ProviderCostUSD != 1.5 ||
		!got.ProviderCostReported || got.ProviderCostCoverage != UsageCostPartial ||
		got.NormalizedSamples != 4 || got.Calls != 2 || got.Turns != 2 {
		t.Fatalf("normalized totals = %#v", got)
	}
	if report.Coverage.State != UsageCoverageComplete || report.Coverage.ExcludedOverlapCount != 1 ||
		report.Coverage.DuplicateCount != 1 || report.Coverage.ResetCount != 1 ||
		report.Coverage.RawSampleCount != 6 || report.Coverage.NormalizedSampleCount != 4 {
		t.Fatalf("coverage = %#v", report.Coverage)
	}
	if len(report.ByTask) != 1 || report.ByTask[0].Outcome != ProgressSucceeded ||
		len(report.ByRole) != 1 || report.ByRole[0].Role != ExecutionRoleExecutor ||
		len(report.ByModel) != 3 {
		t.Fatalf("breakdowns: task=%#v role=%#v model=%#v", report.ByTask, report.ByRole, report.ByModel)
	}
}

func TestNormalizeUsageReportUsesCausalBoundaryNotTimestampForClaudeOverlap(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	attribution := UsageAttribution{Status: UsageAttributed, WorkerID: "worker", WorkflowRunID: "run",
		TaskID: "task", AttemptID: "attempt", AssignmentID: "assignment", Role: ExecutionRoleExecutor}
	covered := UsageSample{WorkerID: "worker", ProviderInstanceID: "claude", ThreadID: "thread",
		Model: "claude-sonnet", ObservedAt: now.Add(2 * time.Minute), SourceEventID: "call-covered", Kind: UsageKindCall,
		BoundaryID: "turn-1", FieldPresence: UsageFieldsAll, InputTokens: 100, OutputTokens: 10, Attribution: attribution}
	turn := UsageSample{WorkerID: "worker", ProviderInstanceID: "claude", ThreadID: "thread",
		Model: "claude-sonnet", ObservedAt: now, SourceEventID: "turn#sonnet", Kind: UsageKindTurn,
		BoundaryID: "turn-1", FieldPresence: UsageFieldsAll, InputTokens: 100, OutputTokens: 10, Attribution: attribution}
	middle := UsageSample{WorkerID: "worker", ProviderInstanceID: "claude", ThreadID: "thread",
		Model: "claude-sonnet", ObservedAt: now.Add(time.Minute), SourceEventID: "call-middle-gap", Kind: UsageKindCall,
		FieldPresence: UsageFieldsAll, InputTokens: 7, OutputTokens: 3, Attribution: attribution}
	skewed := UsageSample{WorkerID: "worker", ProviderInstanceID: "claude", ThreadID: "thread",
		Model: "claude-sonnet", ObservedAt: now.Add(-time.Minute), SourceEventID: "call-skewed", Kind: UsageKindCall,
		BoundaryID: "other-turn", FieldPresence: UsageFieldsAll, InputTokens: 9, OutputTokens: 4, Attribution: attribution}

	report := NormalizeUsageReport(UsageReport{Samples: []UsageSample{covered, middle, turn, skewed}},
		UsageNormalizationContext{Now: now.Add(3 * time.Minute), RunProgress: ProgressActive})
	if report.Totals.UncachedInputTokens != 100 || report.Totals.OutputTokens != 10 ||
		report.Totals.NormalizedSamples != 1 || report.Coverage.ExcludedOverlapCount != 1 ||
		report.Coverage.AmbiguousOverlapCount != 2 || report.Coverage.UnmatchedCallCount != 2 ||
		report.Coverage.State != UsageCoveragePartial {
		t.Fatalf("causal overlap report = %#v", report)
	}
}

func TestNormalizeUsageReportExcludesUnidentifiedCumulativeDecrease(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	makeSample := func(id string, total, input int64, offset time.Duration) UsageSample {
		return UsageSample{WorkerID: "worker", ProviderInstanceID: "codex", ThreadID: "thread", Model: "gpt",
			ObservedAt: now.Add(offset), SourceEventID: id, Kind: UsageKindCall, CumulativeTokens: total,
			InputTokens: input, FieldPresence: UsageFieldsAll}
	}
	report := NormalizeUsageReport(UsageReport{Samples: []UsageSample{
		makeSample("new-high", 100, 10, 0), makeSample("late-old", 20, 2, time.Second),
		makeSample("post-ambiguity", 90, 9, 2*time.Second),
	}}, UsageNormalizationContext{Now: now.Add(time.Minute), RunProgress: ProgressSucceeded})
	if report.Totals.UncachedInputTokens != 10 || report.Coverage.CumulativeAmbiguityCount != 2 ||
		report.Coverage.State != UsageCoveragePartial {
		t.Fatalf("cumulative ambiguity report = %#v", report)
	}
}

func TestNormalizeUsageReportClassifiesLateUnknownAndMalformedEvidence(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	completed := now.Add(-2 * time.Minute)
	attribution := UsageAttribution{Status: UsageAttributed, WorkerID: "worker", WorkflowRunID: "run"}
	report := NormalizeUsageReport(UsageReport{WorkflowRunID: "run", Samples: []UsageSample{
		{WorkerID: "worker", ProviderInstanceID: "codex", ThreadID: "thread", ObservedAt: now,
			SourceEventID: "late-unknown", Kind: UsageKindCall, InputTokens: 5, Attribution: attribution},
		{WorkerID: "worker", ProviderInstanceID: "codex", ThreadID: "thread", ObservedAt: now,
			SourceEventID: "malformed", Kind: UsageKindCall, InputTokens: -1, Attribution: attribution},
	}}, UsageNormalizationContext{Now: now, RunProgress: ProgressSucceeded, RunCompletedAt: &completed})
	if report.Coverage.State != UsageCoveragePartial || report.Coverage.LateCount != 1 ||
		report.Coverage.UnknownModelCount != 1 || report.Coverage.MalformedCount != 1 ||
		report.Coverage.NormalizedSampleCount != 1 || report.Totals.UncachedInputTokens != 5 {
		t.Fatalf("coverage classification = %#v", report)
	}
}

func TestNormalizeUsageReportCoverageNeverTurnsMissingIntoZero(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	unsupported := UsageSample{
		WorkerID: "worker", ProviderInstanceID: "future-provider", ThreadID: "thread",
		Model: "", ObservedAt: now.Add(-time.Hour), SourceEventID: "unsupported", Kind: "cumulative-snapshot",
		Attribution: UsageAttribution{Status: UsageAttributed, WorkflowRunID: "run"},
	}
	report := NormalizeUsageReport(UsageReport{WorkflowRunID: "run", Samples: []UsageSample{unsupported}},
		UsageNormalizationContext{Now: now, RunProgress: ProgressActive})
	if report.Coverage.State != UsageCoverageStale && report.Coverage.State != UsageCoverageUnsupported {
		t.Fatalf("coverage state = %q", report.Coverage.State)
	}
	if report.Coverage.UnsupportedCount != 1 || report.Coverage.NormalizedSampleCount != 0 ||
		len(report.Coverage.Reasons) == 0 {
		t.Fatalf("coverage = %#v", report.Coverage)
	}
	if report.Totals.NormalizedSamples != 0 || report.Totals.ProviderCostReported {
		t.Fatalf("missing evidence became measured totals: %#v", report.Totals)
	}
}
