package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func titlePackage() ExecutionPackage {
	return ExecutionPackage{
		Identity: ExecutionIdentity{WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "task-1"},
		Display:  &SessionDisplay{WorkflowName: "M16 review authority", TaskName: "implement"},
	}
}

func runSuffix(runID string) string {
	sum := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(sum[:3])
}

func TestSessionTitleFormat(t *testing.T) {
	got := SessionTitle(titlePackage(), SessionRunning, &CampaignProgress{Completed: 2, Total: 5})
	want := "[Steward] M16 review authority: implement · executor · running · campaign 2/5 · run " + runSuffix("run-1")
	if got != want {
		t.Fatalf("title = %q\nwant    %q", got, want)
	}
	if again := SessionTitle(titlePackage(), SessionRunning, &CampaignProgress{Completed: 2, Total: 5}); again != got {
		t.Fatalf("title is not deterministic: %q then %q", got, again)
	}
}

func TestSessionTitleEveryLifecycleState(t *testing.T) {
	for _, state := range []SessionState{SessionStarting, SessionRunning, SessionWaiting, SessionCollecting, SessionCompleted, SessionFailed, SessionCancelled} {
		title := SessionTitle(titlePackage(), state, nil)
		if !strings.Contains(title, " · executor · "+string(state)+" · run ") {
			t.Fatalf("state %q is not shown in %q", state, title)
		}
		if !state.Valid() {
			t.Fatalf("state %q is not valid", state)
		}
	}
	if SessionState("accepted").Valid() || SessionState("").Valid() {
		t.Fatal("an undefined lifecycle state is valid")
	}
}

func TestSessionTitleProgressOnlyWithAnAuthoritativeTotal(t *testing.T) {
	for _, progress := range []*CampaignProgress{nil, {Completed: 0, Total: 0}} {
		if title := SessionTitle(titlePackage(), SessionWaiting, progress); strings.Contains(title, "campaign") {
			t.Fatalf("progress shown without a total: %q", title)
		}
	}
	if title := SessionTitle(titlePackage(), SessionCompleted, &CampaignProgress{Completed: 5, Total: 5}); !strings.Contains(title, " · completed · campaign 5/5 · ") {
		t.Fatalf("campaign progress missing or unlabelled: %q", title)
	}
}

func TestSessionTitleRoleComesOnlyFromRecordedMetadata(t *testing.T) {
	// A task named "review" is still an executor: the role is never read from names.
	named := titlePackage()
	named.Display.TaskName = "review"
	if title := SessionTitle(named, SessionRunning, nil); !strings.Contains(title, ": review · executor · ") {
		t.Fatalf("role inferred from the task name: %q", title)
	}
	judge := titlePackage()
	judge.Display.ReviewJudge = true
	if title := SessionTitle(judge, SessionRunning, nil); !strings.Contains(title, " · review · running") {
		t.Fatalf("review judge not labelled review: %q", title)
	}
	supervision := titlePackage()
	supervision.Display = &SessionDisplay{WorkflowName: "M16 review authority"}
	supervision.Supervision = &SupervisionActivation{ActivationID: "activation-1"}
	if title := SessionTitle(supervision, SessionWaiting, nil); !strings.HasPrefix(title, "[Steward] M16 review authority · supervision · waiting · run ") {
		t.Fatalf("supervision title = %q", title)
	}
}

func TestSessionTitleFallsBackToIdentity(t *testing.T) {
	pkg := titlePackage()
	pkg.Display = &SessionDisplay{}
	want := "[Steward] workflow-1: task-1 · executor · starting · run " + runSuffix("run-1")
	if got := SessionTitle(pkg, SessionStarting, nil); got != want {
		t.Fatalf("fallback title = %q, want %q", got, want)
	}
	pkg.Display = nil
	if got := SessionTitle(pkg, SessionStarting, nil); got != want {
		t.Fatalf("title without display metadata = %q, want %q", got, want)
	}
}

func TestSessionTitleIsSanitisedAndBounded(t *testing.T) {
	pkg := titlePackage()
	// Display names are validated sanitised at the protocol boundary, but the
	// identity fallback is not; both paths must stay bounded and printable.
	pkg.Display = &SessionDisplay{}
	pkg.Identity.WorkflowID = "\x1b[31m" + string(rune(0x202e)) + strings.Repeat("日本語ワークフロー", 40)
	pkg.Identity.TaskID = "line\nbreak\ttab\x00" + strings.Repeat("𝔘", 200)
	title := SessionTitle(pkg, SessionCancelled, &CampaignProgress{Completed: 999, Total: 1000})
	if !utf8.ValidString(title) || len(title) > SessionTitleMaxBytes {
		t.Fatalf("title is not bounded UTF-8 (%d bytes): %q", len(title), title)
	}
	for _, r := range title {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			t.Fatalf("title keeps control or format rune %U: %q", r, title)
		}
	}
	if !strings.Contains(title, " · executor · cancelled · campaign 999/1000 · run "+runSuffix("run-1")) {
		t.Fatalf("bounding cut the lifecycle suffix: %q", title)
	}
}

func sessionStatement() SnapshotRequest {
	return SnapshotRequest{SessionStatesReported: true, SessionStates: []AssignmentSessionState{{
		AssignmentID: "assignment-1", AssignmentEpoch: 2, AttemptID: "attempt-1", AttemptRevision: 4,
		State: SessionRunning, Progress: &CampaignProgress{Completed: 1, Total: 3},
	}}}
}

func TestSessionStatementValidation(t *testing.T) {
	if err := ValidateSnapshotRequest(sessionStatement()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*SnapshotRequest){
		"unflagged":         func(r *SnapshotRequest) { r.SessionStatesReported = false },
		"unknown state":     func(r *SnapshotRequest) { r.SessionStates[0].State = "accepted" },
		"missing identity":  func(r *SnapshotRequest) { r.SessionStates[0].AttemptID = "" },
		"zero epoch":        func(r *SnapshotRequest) { r.SessionStates[0].AssignmentEpoch = 0 },
		"completed > total": func(r *SnapshotRequest) { r.SessionStates[0].Progress = &CampaignProgress{Completed: 4, Total: 3} },
		"negative":          func(r *SnapshotRequest) { r.SessionStates[0].Progress = &CampaignProgress{Completed: -1, Total: 3} },
		"zero total":        func(r *SnapshotRequest) { r.SessionStates[0].Progress = &CampaignProgress{} },
		"huge total": func(r *SnapshotRequest) {
			r.SessionStates[0].Progress = &CampaignProgress{Total: MaxCampaignProgressTotal + 1}
		},
		"repeated": func(r *SnapshotRequest) { r.SessionStates = append(r.SessionStates, r.SessionStates[0]) },
	}
	for name, mutate := range cases {
		request := sessionStatement()
		mutate(&request)
		if err := ValidateSnapshotRequest(request); err == nil {
			t.Fatalf("%s: statement accepted", name)
		}
	}
	oversized := SnapshotRequest{SessionStatesReported: true}
	for index := 0; index <= MaxSessionStates; index++ {
		oversized.SessionStates = append(oversized.SessionStates, AssignmentSessionState{
			AssignmentID:    "assignment-" + strings.Repeat("x", index%7) + string(rune('a'+index%26)) + string(rune('a'+index/26%26)) + string(rune('a'+index/676)),
			AssignmentEpoch: 1, AttemptID: "attempt", State: SessionRunning,
		})
	}
	if err := ValidateSnapshotRequest(oversized); err == nil {
		t.Fatal("an unbounded session statement was accepted")
	}
}
