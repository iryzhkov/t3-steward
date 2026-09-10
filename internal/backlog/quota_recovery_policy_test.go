package backlog

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanQuotaRecoveryOrdersInteractiveRequiredAndDeadlineException(t *testing.T) {
	now := throttleDeliveryTime.Add(2 * time.Hour)
	soon := now.Add(10 * time.Minute)
	later := now.Add(4 * time.Hour)
	tests := []struct {
		name        string
		interactive []string
		deadline    *time.Time
		wantResume  string
		wantNew     string
	}{
		{name: "paused required before ordinary new required", deadline: &later, wantResume: "paused"},
		{name: "immediate deadline exception", deadline: &soon, wantNew: "new-required"},
		{name: "interactive resume before immediate deadline", interactive: []string{"paused"}, deadline: &soon, wantResume: "paused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := quotaRecoveryPolicyFixture(t, "paused")
			input.Now = now
			input.InteractiveResumeAttemptIDs = test.interactive
			input.NewRequired = []QuotaRecoveryNewRequired{{
				AttemptID: "new-required", QuotaPoolID: "shared", Deadline: test.deadline,
			}}
			got, err := PlanQuotaRecovery(input)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantResume == "" {
				if len(got.ResumeCommands) != 0 {
					t.Fatalf("resume commands = %#v, want none", got.ResumeCommands)
				}
			} else if len(got.ResumeCommands) != 1 || got.ResumeCommands[0].AttemptID != test.wantResume {
				t.Fatalf("resume commands = %#v, want %q", got.ResumeCommands, test.wantResume)
			}
			if test.wantNew == "" {
				if len(got.NewRequiredAttemptIDs) != 0 {
					t.Fatalf("new required = %#v, want none", got.NewRequiredAttemptIDs)
				}
			} else if !reflect.DeepEqual(got.NewRequiredAttemptIDs, []string{test.wantNew}) {
				t.Fatalf("new required = %#v, want %q", got.NewRequiredAttemptIDs, test.wantNew)
			}
		})
	}
}

func TestPlanQuotaRecoverySurplusValidityAndAdmission(t *testing.T) {
	now := throttleDeliveryTime.Add(2 * time.Hour)
	tests := []struct {
		name       string
		admission  domain.AdmissionState
		eligible   bool
		expired    bool
		wantResume bool
		wantReason string
	}{
		{name: "eligible open surplus resumes", admission: domain.AdmissionOpen, eligible: true, wantResume: true},
		{name: "recovering pool closes surplus occurrence", admission: domain.AdmissionRecovering, eligible: true, wantReason: "no longer eligible"},
		{name: "surplus budget disappeared", admission: domain.AdmissionOpen, wantReason: "no longer eligible"},
		{name: "expired occurrence", admission: domain.AdmissionOpen, eligible: true, expired: true, wantReason: "expired"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := quotaRecoveryPolicyFixture(t, "surplus")
			input.Now = now
			input.Admissions[0].Admission = test.admission
			input.SurplusEligiblePools["shared"] = test.eligible
			if test.expired {
				input.PlanningState.ResumeReservations[0].ExpiresAt = planningTimeValue(now)
			}
			got, err := PlanQuotaRecovery(input)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantResume {
				if len(got.ResumeCommands) != 1 || got.ResumeCommands[0].AttemptID != "surplus" ||
					len(got.SkippedSurplus) != 0 {
					t.Fatalf("eligible surplus plan = %#v", got)
				}
				return
			}
			if len(got.ResumeCommands) != 0 || len(got.SkippedSurplus) != 1 {
				t.Fatalf("ineligible surplus plan = %#v", got)
			}
			skip := got.SkippedSurplus[0]
			if skip.AttemptID != "surplus" || skip.ExpectedRevision != 4 ||
				skip.Progress != domain.ProgressSkipped || skip.Control != domain.ControlStopped ||
				!strings.Contains(skip.Reason, test.wantReason) || !skip.CompletedAt.Equal(now) {
				t.Fatalf("skip = %#v, want retained-artifact terminal transition", skip)
			}
		})
	}
}

func TestPlanQuotaRecoveryStableAcrossRestartAndContention(t *testing.T) {
	input := quotaRecoveryPolicyFixture(t, "paused", "forced")
	first, err := PlanQuotaRecovery(input)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(input.PlanningState.QuotaPools)
	slices.Reverse(input.PlanningState.ResumeReservations)
	slices.Reverse(input.ThrottleRecords)
	slices.Reverse(input.Admissions)
	second, err := PlanQuotaRecovery(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("recovery changed after restart reorder: first %#v second %#v", first, second)
	}
	if len(first.ResumeCommands) != 1 || first.ResumeCommands[0].AttemptID != "forced" {
		t.Fatalf("contention winner = %#v, want stable attempt-ID order", first.ResumeCommands)
	}
}

func quotaRecoveryPolicyFixture(t *testing.T, attemptIDs ...string) QuotaRecoveryInput {
	t.Helper()
	source := quotaRecoveryFixture()
	wanted := make(map[string]struct{}, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		wanted[attemptID] = struct{}{}
	}
	source.QuotaPools[0].MaxConcurrent = 1
	source.QuotaPools[0].ActiveAssignments = 0
	source.QuotaWindows = source.QuotaWindows[:1]
	source.Tasks = slices.DeleteFunc(source.Tasks, func(task domain.Task) bool {
		return !hasAttemptTask(source.Attempts, wanted, task.ID)
	})
	source.Attempts = slices.DeleteFunc(source.Attempts, func(attempt domain.Attempt) bool {
		_, keep := wanted[attempt.ID]
		return !keep
	})
	for index := range source.Attempts {
		source.Attempts[index].Revision = 4
		source.Attempts[index].CheckpointArtifactID = "checkpoint-" + source.Attempts[index].ID
	}
	source.Assignments = slices.DeleteFunc(source.Assignments, func(assignment domain.Assignment) bool {
		_, keep := wanted[assignment.AttemptID]
		return !keep
	})
	source.ThrottleRecords = slices.DeleteFunc(source.ThrottleRecords, func(record domain.ThrottleAttemptRecord) bool {
		_, keep := wanted[record.AttemptID]
		return !keep
	})
	source.RouteEstimates = slices.DeleteFunc(source.RouteEstimates, func(estimate RouteEstimate) bool {
		_, keep := wanted[estimate.AttemptID]
		return !keep
	})
	state, err := DeriveQuotaPlanningState(source)
	if err != nil {
		t.Fatal(err)
	}
	return QuotaRecoveryInput{
		Now: throttleDeliveryTime.Add(time.Hour), DeadlineRiskWindow: time.Hour,
		PlanningState: state, ThrottleRecords: source.ThrottleRecords,
		Admissions: []domain.QuotaAdmissionRecord{{
			QuotaPoolID: "shared", Revision: 2, Admission: domain.AdmissionOpen,
		}},
		SurplusEligiblePools: map[string]bool{"shared": true},
	}
}

func hasAttemptTask(attempts []domain.Attempt, wanted map[string]struct{}, taskID string) bool {
	for _, attempt := range attempts {
		if attempt.TaskID == taskID {
			_, keep := wanted[attempt.ID]
			return keep
		}
	}
	return false
}
