package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// providerSession is a fake T3 session: the test ends its turns with or
// without a provider error, and a resume the worker sends starts a new turn
// in the same session, as T3 does.
type providerSession struct {
	*turnEndDriver
	failure      backlog.ProviderTurnError
	provider     bool
	classifyErr  error
	resumeErr    error
	resumeTokens []string
	resumeTexts  []string
}

func (d *providerSession) ProviderTurnError(context.Context, workerproto.ExecutionPackage) (backlog.ProviderTurnError, bool, error) {
	return d.failure, d.provider, d.classifyErr
}

func (d *providerSession) ResumeAfterProviderError(_ context.Context, _ workerproto.ExecutionPackage, token, text string) error {
	if d.resumeErr != nil {
		return d.resumeErr
	}
	d.resumeTokens = append(d.resumeTokens, token)
	d.resumeTexts = append(d.resumeTexts, text)
	// The message starts the resumed turn in the same session.
	d.state = backlog.DispatchThreadActive
	return nil
}

// failTurn ends the current turn on a provider error of kind.
func (d *providerSession) failTurn(turn string, kind domain.ProviderErrorKind) {
	d.endTurn(turn)
	d.failure, d.provider = backlog.ProviderTurnError{Kind: kind, Detail: "provider said " + string(kind)}, true
}

// finishTurn ends the current turn the way the agent ends it.
func (d *providerSession) finishTurn(turn string) {
	d.endTurn(turn)
	d.failure, d.provider = backlog.ProviderTurnError{}, false
}

type providerHarness struct {
	root    string
	now     time.Time
	driver  *providerSession
	runtime *Runtime
	backoff []time.Duration
	quota   QuotaGuard
}

func (h *providerHarness) open(t *testing.T) *Runtime {
	t.Helper()
	journal, err := OpenJournal(h.root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(func() time.Time { return h.now })
	config.LiveTaskWait = func(context.Context, workerproto.ExecutionPackage) (bool, error) { return false, nil }
	config.ProviderResumeBackoff = h.backoff
	config.Quota = h.quota
	runtime, err := New(config, journal, h.driver)
	if err != nil {
		t.Fatal(err)
	}
	h.runtime = runtime
	return runtime
}

// runningProviderSession is a claimed attempt whose provider turn is running.
func runningProviderSession(t *testing.T, backoff ...time.Duration) *providerHarness {
	t.Helper()
	root := t.TempDir()
	h := &providerHarness{root: root, now: runtimeTestNow, backoff: backoff, driver: &providerSession{turnEndDriver: &turnEndDriver{
		fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
		state:      backlog.DispatchThreadActive, turn: "turn-1",
	}}}
	runtime := h.open(t)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", h.driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *providerHarness) resume(t *testing.T) ProviderResumeRecord {
	t.Helper()
	record := mustRecord(t, h.runtime, "assignment-1")
	if record.ProviderResume == nil {
		t.Fatal("no provider resume state recorded")
	}
	return *record.ProviderResume
}

var providerErrorKinds = []domain.ProviderErrorKind{
	domain.ProviderErrorCapacity, domain.ProviderErrorOverload, domain.ProviderErrorRateLimit,
	domain.ProviderErrorServer, domain.ProviderErrorSessionNotReady,
}

// A turn a provider error ended is not collected. The error is recorded on
// the attempt, the same session is resumed once its backoff has passed, the
// resumed turn runs, and when it ends as the task's own ending the attempt is
// collected as usual. One resume per ended turn, for every provider kind.
func TestProviderErrorResumesTheSameSessionAndRecovers(t *testing.T) {
	for _, kind := range providerErrorKinds {
		t.Run(string(kind), func(t *testing.T) {
			h := runningProviderSession(t)
			h.driver.failTurn("turn-1", kind)
			reconcileOnce(t, h.runtime)
			if got := phaseOf(t, h.runtime, "assignment-1"); got != PhaseStopped {
				t.Fatalf("phase = %q, want stopped and held", got)
			}
			if h.driver.collectCalls != 0 || h.driver.collectFailureCalls != 0 || len(h.driver.resumeTokens) != 0 {
				t.Fatalf("collect=%d failure=%d resumes=%d before the backoff", h.driver.collectCalls, h.driver.collectFailureCalls, len(h.driver.resumeTokens))
			}
			state := h.resume(t)
			if state.State != domain.ProviderResumeScheduled || state.Kind != kind || state.Resumes != 1 || state.Budget != 3 || state.Errors != 1 ||
				state.ResumeAt == nil || !state.ResumeAt.Equal(runtimeTestNow.Add(time.Minute)) {
				t.Fatalf("resume state = %+v", state)
			}

			h.now = h.now.Add(time.Minute)
			reconcileOnce(t, h.runtime)
			if len(h.driver.resumeTokens) != 1 {
				t.Fatalf("resumes = %d, want 1", len(h.driver.resumeTokens))
			}
			if token := h.driver.resumeTokens[0]; !strings.Contains(token, "dispatch-1") || !strings.Contains(token, "turn-1") {
				t.Fatalf("resume token %q is not bound to the dispatch and the ended turn", token)
			}
			if text := h.driver.resumeTexts[0]; !strings.Contains(text, "continuation.md") || !strings.Contains(text, string(kind)) {
				t.Fatalf("resume text:\n%s", text)
			}
			// The resumed turn is running in the same session.
			reconcileOnce(t, h.runtime)
			if got := phaseOf(t, h.runtime, "assignment-1"); got != PhaseRunning {
				t.Fatalf("phase = %q, want running after the resume", got)
			}
			if len(h.driver.resumeTokens) != 1 {
				t.Fatalf("the same ended turn was resumed %d times", len(h.driver.resumeTokens))
			}

			h.driver.finishTurn("turn-2")
			reconcileOnce(t, h.runtime)
			if got := phaseOf(t, h.runtime, "assignment-1"); got != PhaseCompleted {
				t.Fatalf("phase = %q, want completed", got)
			}
			if h.driver.collectCalls != 1 || h.driver.collectFailureCalls != 0 {
				t.Fatalf("collect=%d failure=%d", h.driver.collectCalls, h.driver.collectFailureCalls)
			}
			if state := h.resume(t); state.State != domain.ProviderResumeRecovered || state.Resumes != 1 || state.Errors != 1 {
				t.Fatalf("resume state after recovery = %+v", state)
			}
		})
	}
}

// A provider error that outlasts every resume fails the attempt with failure
// class infrastructure, after the uncommitted work is snapshotted, and never
// earlier: each resume in the schedule is spent first.
func TestProviderErrorExhaustingTheBudgetFailsAsInfrastructure(t *testing.T) {
	for _, kind := range providerErrorKinds {
		t.Run(string(kind), func(t *testing.T) {
			h := runningProviderSession(t, time.Minute, 2*time.Minute)
			for index, turn := range []string{"turn-1", "turn-2"} {
				h.driver.failTurn(turn, kind)
				reconcileOnce(t, h.runtime)
				h.now = h.now.Add(time.Duration(index+1) * time.Minute)
				reconcileOnce(t, h.runtime)
				if len(h.driver.resumeTokens) != index+1 {
					t.Fatalf("after %s: resumes = %d", turn, len(h.driver.resumeTokens))
				}
				reconcileOnce(t, h.runtime)
				if h.driver.collectFailureCalls != 0 {
					t.Fatalf("failed before the budget was spent, after %s", turn)
				}
			}
			h.driver.failTurn("turn-3", kind)
			reconcileOnce(t, h.runtime)
			if len(h.driver.resumeTokens) != 2 {
				t.Fatalf("resumes = %d, want the 2 of the schedule", len(h.driver.resumeTokens))
			}
			if h.driver.collectFailureCalls != 1 || h.driver.collectCalls != 0 {
				t.Fatalf("collect=%d failure=%d", h.driver.collectCalls, h.driver.collectFailureCalls)
			}
			record := mustRecord(t, h.runtime, "assignment-1")
			want := "infrastructure: provider error (" + string(kind) + ") after 2 of 2 in-session resumes: provider said " + string(kind)
			if !strings.HasPrefix(record.Failure, want) {
				t.Fatalf("failure = %q, want prefix %q", record.Failure, want)
			}
			if !strings.Contains(record.Failure, "wip.bundle retained") || h.driver.snapshots != 1 {
				t.Fatalf("work in progress was not snapshotted: snapshots=%d failure=%q", h.driver.snapshots, record.Failure)
			}
			if record.ProviderResume == nil || record.ProviderResume.State != domain.ProviderResumeExhausted || record.ProviderResume.Errors != 3 {
				t.Fatalf("resume state = %+v", record.ProviderResume)
			}
			if record.WorkspacePath == "" {
				t.Fatal("the failed attempt lost its workspace")
			}
		})
	}
}

// Turns that end for any reason but a provider error are the task's own
// ending and are collected exactly as before: no resume and no state.
func TestNonProviderTurnEndIsNeverResumed(t *testing.T) {
	h := runningProviderSession(t)
	h.driver.finishTurn("turn-1")
	reconcileOnce(t, h.runtime)
	if got := phaseOf(t, h.runtime, "assignment-1"); got != PhaseCompleted {
		t.Fatalf("phase = %q, want completed", got)
	}
	if len(h.driver.resumeTokens) != 0 || h.driver.collectCalls != 1 {
		t.Fatalf("resumes=%d collect=%d", len(h.driver.resumeTokens), h.driver.collectCalls)
	}
	if record := mustRecord(t, h.runtime, "assignment-1"); record.ProviderResume != nil {
		t.Fatalf("a clean turn end left resume state: %+v", record.ProviderResume)
	}
}

// A thread that cannot be read leaves the decision for a later pass: it is
// neither collected nor resumed.
func TestProviderErrorCheckFailureDefersCollection(t *testing.T) {
	h := runningProviderSession(t)
	h.driver.finishTurn("turn-1")
	h.driver.classifyErr = errors.New("T3 export failed")
	reconcileOnce(t, h.runtime)
	if h.driver.collectCalls != 0 || len(h.driver.resumeTokens) != 0 {
		t.Fatalf("collect=%d resumes=%d", h.driver.collectCalls, len(h.driver.resumeTokens))
	}
	h.driver.classifyErr = nil
	reconcileOnce(t, h.runtime)
	if h.driver.collectCalls != 1 {
		t.Fatalf("collect=%d after the thread could be read", h.driver.collectCalls)
	}
}

// A resume that is due while the coordinator says the route's pool is closed
// waits with a typed reason, which the snapshot reports, and is sent once the
// pool reopens.
func TestProviderResumeWaitsWhileThePoolIsClosed(t *testing.T) {
	h := runningProviderSession(t)
	closed := workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{
		MaxResumes: 3, MaxDelaySeconds: 3600,
		ClosedPools: []workerproto.ClosedQuotaPool{{PoolID: "codex-main", Admission: domain.AdmissionClosed, Reason: "five_hour at 96%"}},
	}}
	if err := h.runtime.ApplyParkedAssignments(closed); err != nil {
		t.Fatal(err)
	}
	h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
	reconcileOnce(t, h.runtime)
	h.now = h.now.Add(time.Minute)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 0 {
		t.Fatal("resumed on a closed pool")
	}
	state := h.resume(t)
	want := "quota-closed: pool codex-main admission is closed: five_hour at 96%"
	if state.State != domain.ProviderResumeQuotaWait || state.WaitReason != want {
		t.Fatalf("resume state = %+v", state)
	}
	snapshot, err := h.runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reported := snapshot.Assignments[0].Journal.ProviderError
	if reported == nil || reported.State != domain.ProviderResumeQuotaWait || reported.WaitReason != want || !reported.Active() {
		t.Fatalf("reported provider error = %+v", reported)
	}
	if !strings.Contains(reported.Summary(), want) {
		t.Fatalf("summary = %q", reported.Summary())
	}

	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 3, MaxDelaySeconds: 3600}}); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 1 {
		t.Fatalf("resumes = %d after the pool reopened", len(h.driver.resumeTokens))
	}
	if state := h.resume(t); state.State != domain.ProviderResumeSent || state.WaitReason != "" {
		t.Fatalf("resume state = %+v", state)
	}
}

// The host watchdog's bucket gates the resumed turn as well: a bucket in
// its drain or stop phase holds the resume with the bucket as the reason.
func TestProviderResumeWaitsForTheHostQuotaBucket(t *testing.T) {
	h := runningProviderSession(t)
	guard := &fakeQuotaGuard{}
	h.runtime.config.Quota = guard
	h.driver.failTurn("turn-1", domain.ProviderErrorRateLimit)
	reconcileOnce(t, h.runtime)
	guard.pause, guard.pauseNeeded = stoppedPause(), true
	h.now = h.now.Add(time.Minute)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 0 {
		t.Fatal("resumed while the bucket is stopped")
	}
	if state := h.resume(t); state.State != domain.ProviderResumeQuotaWait || !strings.HasPrefix(state.WaitReason, ProviderResumeQuotaClosed+": codex") {
		t.Fatalf("resume state = %+v", state)
	}
	guard.pauseNeeded = false
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 1 {
		t.Fatalf("resumes = %d after the bucket recovered", len(h.driver.resumeTokens))
	}
}

// The coordinator's maximum cuts the worker's schedule: fewer resumes and no
// delay longer than its maximum. A maximum of zero fails the first provider
// error at once, still as infrastructure.
func TestCoordinatorMaximumCapsTheResumeSchedule(t *testing.T) {
	h := runningProviderSession(t, 10*time.Minute, 10*time.Minute)
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 1, MaxDelaySeconds: 30}}); err != nil {
		t.Fatal(err)
	}
	h.driver.failTurn("turn-1", domain.ProviderErrorOverload)
	reconcileOnce(t, h.runtime)
	if state := h.resume(t); state.Budget != 1 || state.ResumeAt == nil || !state.ResumeAt.Equal(runtimeTestNow.Add(30*time.Second)) {
		t.Fatalf("resume state = %+v", state)
	}
	h.now = h.now.Add(30 * time.Second)
	reconcileOnce(t, h.runtime)
	reconcileOnce(t, h.runtime)
	h.driver.failTurn("turn-2", domain.ProviderErrorOverload)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 1 || h.driver.collectFailureCalls != 1 {
		t.Fatalf("resumes=%d failures=%d", len(h.driver.resumeTokens), h.driver.collectFailureCalls)
	}

	h = runningProviderSession(t)
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 0}}); err != nil {
		t.Fatal(err)
	}
	h.driver.failTurn("turn-1", domain.ProviderErrorServer)
	reconcileOnce(t, h.runtime)
	record := mustRecord(t, h.runtime, "assignment-1")
	if len(h.driver.resumeTokens) != 0 || !strings.HasPrefix(record.Failure, "infrastructure: provider error (server-error) after 0 of 0") {
		t.Fatalf("resumes=%d failure=%q", len(h.driver.resumeTokens), record.Failure)
	}
}

// A resume T3 did not take is sent again under the same identity, and a
// worker restart after it was sent sends nothing more.
func TestProviderResumeIsRetriedUnderOneIdentityAcrossRestarts(t *testing.T) {
	h := runningProviderSession(t)
	h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
	reconcileOnce(t, h.runtime)
	h.now = h.now.Add(time.Minute)
	h.driver.resumeErr = errors.New("T3 unavailable")
	reconcileOnce(t, h.runtime)
	if state := h.resume(t); state.State != domain.ProviderResumeScheduled {
		t.Fatalf("an undelivered resume is recorded as %q", state.State)
	}
	h.driver.resumeErr = nil
	h.open(t)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 1 {
		t.Fatalf("resumes = %d", len(h.driver.resumeTokens))
	}
	// The session has not started the resumed turn yet.
	h.driver.state = backlog.DispatchThreadStopped
	h.open(t)
	reconcileOnce(t, h.runtime)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 1 || h.driver.collectCalls != 0 {
		t.Fatalf("after a restart: resumes=%d collect=%d", len(h.driver.resumeTokens), h.driver.collectCalls)
	}
}

// The provider error travels to a coordinator that sent a resume policy, and
// only to one: an older coordinator never meets the field. The build
// advertises the capability either way.
func TestProviderErrorReportIsGatedByTheCoordinatorPolicy(t *testing.T) {
	h := runningProviderSession(t)
	h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
	reconcileOnce(t, h.runtime)
	snapshot, err := h.runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Assignments[0].Journal.ProviderError != nil {
		t.Fatal("provider error sent to a coordinator that did not ask")
	}
	if !slicesContains(snapshot.Inventory.Capabilities, workerproto.CapabilityProviderResume) {
		t.Fatalf("capability not advertised: %v", snapshot.Inventory.Capabilities)
	}
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 3, MaxDelaySeconds: 60}}); err != nil {
		t.Fatal(err)
	}
	if snapshot, err = h.runtime.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	reported := snapshot.Assignments[0].Journal.ProviderError
	if reported == nil || reported.Kind != domain.ProviderErrorCapacity || reported.State != domain.ProviderResumeScheduled || reported.TurnID != "turn-1" {
		t.Fatalf("reported = %+v", reported)
	}
}

// The schedule falls back to the default, and neither a worker schedule nor
// a coordinator policy can exceed the fixed ceilings.
func TestProviderResumeScheduleDefaultsAndCeilings(t *testing.T) {
	h := runningProviderSession(t)
	if got := h.runtime.providerResumeSchedule(); len(got) != 3 || got[0] != time.Minute || got[2] != 15*time.Minute {
		t.Fatalf("default schedule = %v", got)
	}
	long := make([]time.Duration, 20)
	for index := range long {
		long[index] = 24 * time.Hour
	}
	h.runtime.config.ProviderResumeBackoff = long
	got := h.runtime.providerResumeSchedule()
	if len(got) != domain.MaxProviderResumes || got[0] != domain.MaxProviderResumeDelay {
		t.Fatalf("schedule over the ceilings = %v", got)
	}
}
