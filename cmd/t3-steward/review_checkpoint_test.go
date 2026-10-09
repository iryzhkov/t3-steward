package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// fakeCheckpointRefs stands in for the worker exact-ref resolution of M16-1a.
// Each ref resolves to the object it maps to, or is not found. It records
// every query so a test can prove which worker was asked and that a fenced
// call never reached the remote at all.
type fakeCheckpointRefs struct {
	mu      sync.Mutex
	heads   map[string]string
	err     error
	status  workerproto.RefResolutionStatus
	queries []repositoryRefQuery
	// during runs inside the probe, after the query is recorded and before the
	// answer is returned, so a test can move durable state while the
	// coordinator waits on the remote.
	during func()
}

func (f *fakeCheckpointRefs) ResolveRef(_ context.Context, query repositoryRefQuery) (repositoryRefAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, query)
	if f.during != nil {
		f.during()
	}
	if f.err != nil {
		return repositoryRefAnswer{}, f.err
	}
	answer := repositoryRefAnswer{WorkerID: query.WorkerID, Ref: query.Ref, ObservedAt: time.Now().UTC()}
	if f.status != "" {
		answer.Status = f.status
		return answer, nil
	}
	object, ok := f.heads[query.Ref]
	if !ok {
		answer.Status = workerproto.RefResolutionNotFound
		return answer, nil
	}
	answer.Status, answer.ObjectID = workerproto.RefResolutionResolved, object
	return answer, nil
}

func (f *fakeCheckpointRefs) push(ref, object string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads[ref] = object
}

func (f *fakeCheckpointRefs) asked() []repositoryRefQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]repositoryRefQuery(nil), f.queries...)
}

type reviewCheckpointHarness struct {
	admin   *backlogadmin.Service
	db      *sqlite.Store
	cfg     config.BacklogV2
	op      *coordinatorReviewCheckpoint
	refs    *fakeCheckpointRefs
	epoch   int64
	request domain.ReviewCheckpointRequest
	parent  domain.Attempt
	assign  domain.Assignment
}

var checkpointPrincipal = backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}

// reviewCheckpointFixture submits a real campaign through the coordinator's
// submission path, with or without a manifest review declaration, and claims
// its first attempt on worker homelab the way dispatch would.
func reviewCheckpointFixture(t *testing.T, declared bool) *reviewCheckpointHarness {
	t.Helper()
	ctx := context.Background()
	admin, db := probeReadinessService(t, nil, "homelab")
	submissions := probeSubmissions(t, admin, db)
	cfg := declaredCoordinatorSettings(submissions.StorageRoot)
	cfg.Storage.Artifacts = reviewInputTempDir(t)
	catalog, err := workerruntime.NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	submissions.Permanent = coordinatorPermanentValidator{admin: admin, reviews: catalog}
	bundle := probeCampaignFixture(t)
	raw := strings.Replace(probeCampaignManifest, "project: dev-fleet", "project: dev-fleet\n  ref: "+strings.Repeat("c", 40), 1)
	if declared {
		raw += "    review_requirements:\n      version: 1\n      risk: routine\n      criteria_file: inputs/plan.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: t3-primary/opus, required: true}\n        - {id: b, role: independent, route: other/model, required: true}\n"
	}
	if err = os.WriteFile(filepath.Join(bundle, "workflow.yaml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = submissions.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "checkpoint", BundleDir: bundle}); err != nil {
		t.Fatal(err)
	}
	records, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, run, parent := records.Tasks[0], records.WorkflowRuns[0], records.Attempts[0]
	parent.Progress, parent.Control = domain.ProgressActive, domain.ControlRunning
	parent.ThreadID, parent.AssignmentID, parent.Revision = "checkpoint-thread", "checkpoint-assignment", 1
	assignment := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "checkpoint-token", AttemptID: parent.ID, WorkerID: "homelab", WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: records.Workflows[0].Project, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
	if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	epoch, err := db.CoordinatorEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	refs := &fakeCheckpointRefs{heads: map[string]string{}}
	op, err := newCoordinatorReviewCheckpoint(cfg, db, epoch, refs)
	if err != nil {
		t.Fatal(err)
	}
	admin.SetReviewCheckpoint(op)
	return &reviewCheckpointHarness{
		admin: admin, db: db, cfg: cfg, op: op, refs: refs, epoch: epoch, parent: parent, assign: assignment,
		request: domain.ReviewCheckpointRequest{
			WorkflowRunID: run.ID, TaskID: task.ID, AttemptID: parent.ID, IssuedRevision: 1,
			AssignmentID: assignment.ID, ThreadID: parent.ThreadID, CheckpointID: "cp-1",
		},
	}
}

func (h *reviewCheckpointHarness) branch() string {
	return domain.ReviewCheckpointBranch(h.request.WorkflowRunID, h.request.TaskID, h.request.CheckpointID)
}

func (h *reviewCheckpointHarness) call(t *testing.T, request domain.ReviewCheckpointRequest) (domain.ReviewCheckpointRound, error) {
	t.Helper()
	response, err := h.admin.NodeWait(context.Background(), checkpointPrincipal, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointAction, Checkpoint: &request})
	if err != nil {
		return domain.ReviewCheckpointRound{}, err
	}
	if response.Checkpoint == nil {
		t.Fatal("review checkpoint answer is missing")
	}
	return response.Checkpoint.Outcome()
}

func requireCheckpointRefusal(t *testing.T, err error, code domain.ReviewCheckpointCode, retryable bool) {
	t.Helper()
	var refusal *domain.ReviewCheckpointRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("want structured refusal %s, got %v", code, err)
	}
	if refusal.Code != code || refusal.Retryable != retryable || refusal.Reason == "" {
		t.Fatalf("want refusal %s retryable=%v, got %+v", code, retryable, refusal)
	}
}

// noReviewRows proves a refusal wrote nothing: no frozen authority, and so no
// checkpoint, round or child can exist for the task.
func (h *reviewCheckpointHarness) noReviewRows(t *testing.T) {
	t.Helper()
	if _, found, err := h.db.GetFrozenReviewAuthority(context.Background(), h.request.WorkflowRunID, h.request.TaskID); err != nil || found {
		t.Fatalf("refused checkpoint froze review authority: found=%v err=%v", found, err)
	}
	if runs := h.runCount(t); runs != 1 {
		t.Fatalf("refused checkpoint created %d runs", runs)
	}
}

func (h *reviewCheckpointHarness) runCount(t *testing.T) int {
	t.Helper()
	records, err := h.db.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(records.WorkflowRuns)
}

func TestReviewCheckpointOpensRoundThroughAssignedWorker(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	head := strings.Repeat("d", 40)
	h.refs.push(h.branch(), head)
	round, err := h.call(t, h.request)
	if err != nil {
		t.Fatal(err)
	}
	asked := h.refs.asked()
	if len(asked) != 1 || asked[0].WorkerID != "homelab" || asked[0].Ref != h.branch() || asked[0].Repository != probeRepository {
		t.Fatalf("head was not resolved through the assigned worker: %+v", asked)
	}
	if round.HeadCommit != head || round.BaseCommit != strings.Repeat("c", 40) || round.Number != 1 || round.CheckpointID != "cp-1" ||
		round.Branch != h.branch() || round.ChildRunID != round.RoundID || round.ChildWorkflowID == "" || len(round.Members) != 2 || round.Replayed {
		t.Fatalf("unexpected round %+v", round)
	}
	stored, err := h.db.GetReviewRound(ctx, round.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if !round.Deadline.Equal(stored.CreatedAt.Add(reviewCheckpointDeadline)) || !stored.Deadline.Equal(round.Deadline) {
		t.Fatalf("deadline %v is not derived from round creation %v", round.Deadline, stored.CreatedAt)
	}
	// The materialized child is a review run with one reviewer task per member.
	graph, err := h.admin.Query(ctx, backlogadmin.Query{Version: backlogadmin.Version, Kind: backlogadmin.QueryGraph, Principal: checkpointPrincipal, WorkflowRunID: round.ChildRunID})
	if err != nil {
		t.Fatal(err)
	}
	var nodes []string
	for _, node := range graph.Graph.Nodes {
		if node.Sink == nil {
			nodes = append(nodes, node.TaskID)
		}
	}
	for _, member := range round.Members {
		if !slices.Contains(nodes, member.TaskID) {
			t.Fatalf("member %s task %s is not in the child graph %v", member.ID, member.TaskID, nodes)
		}
	}
	runs := h.runCount(t)
	again, err := h.call(t, h.request)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Replayed || again.RoundID != round.RoundID || again.ChildRunID != round.ChildRunID || !again.Deadline.Equal(round.Deadline) || h.runCount(t) != runs {
		t.Fatalf("replay did not return the same round: %+v", again)
	}
}

func TestReviewCheckpointEndToEndOverLocalTransport(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	h.refs.push(h.branch(), strings.Repeat("d", 40))
	root, err := os.MkdirTemp("", "t3-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "admin.sock")
	listener, err := backlogadmin.ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	server := &backlogadmin.LocalServer{
		Listener: listener, Service: coordinatorLocalService{admin: h.admin}, AllowedUID: uint32(os.Getuid()),
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: testtiming.Bound(30 * time.Second), MaxConcurrent: 4,
	}
	go func() { done <- server.Serve(serveCtx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}()
	client := backlogadmin.LocalClient{Path: path, MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: testtiming.Bound(30 * time.Second)}
	request := h.request
	response, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointAction, Checkpoint: &request})
	if err != nil {
		t.Fatal(err)
	}
	if response.Checkpoint == nil {
		t.Fatal("transport dropped the checkpoint answer")
	}
	round, err := response.Checkpoint.Outcome()
	if err != nil {
		t.Fatal(err)
	}
	records, err := h.db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var child *domain.WorkflowRun
	for i := range records.WorkflowRuns {
		if records.WorkflowRuns[i].ID == round.ChildRunID {
			child = &records.WorkflowRuns[i]
		}
	}
	if child == nil || child.WorkflowID != round.ChildWorkflowID {
		t.Fatalf("materialized child run %s is not in the coordinator records", round.ChildRunID)
	}
	tasks := domain.TasksForRun(*child, records.Tasks)
	if len(tasks) != len(round.Members) {
		t.Fatalf("child run has %d tasks, want %d reviewers", len(tasks), len(round.Members))
	}
	for _, task := range tasks {
		if len(task.Outputs) != 2 || task.Outputs[0].Name != "review.md" || task.Outputs[1].Name != "verdict.json" {
			t.Fatalf("child task %s is not a review task: %+v", task.ID, task.Outputs)
		}
	}
	// A refusal crosses the carrier with its code and retry flag intact.
	other := h.request
	other.HeadCommit = strings.Repeat("f", 40)
	response, err = client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointAction, Checkpoint: &other})
	if err != nil {
		t.Fatal(err)
	}
	_, err = response.Checkpoint.Outcome()
	requireCheckpointRefusal(t, err, domain.ReviewCheckpointHeadMismatch, false)
}

func TestReviewCheckpointRefusesChangedHeadUnderSameID(t *testing.T) {
	h := reviewCheckpointFixture(t, true)
	h.refs.push(h.branch(), strings.Repeat("d", 40))
	if _, err := h.call(t, h.request); err != nil {
		t.Fatal(err)
	}
	runs := h.runCount(t)
	h.refs.push(h.branch(), strings.Repeat("e", 40))
	_, err := h.call(t, h.request)
	requireCheckpointRefusal(t, err, domain.ReviewCheckpointHeadConflict, false)
	if h.runCount(t) != runs {
		t.Fatal("a changed head created a second child")
	}
	// A new checkpoint id opens the next round for the moved head.
	next := h.request
	next.CheckpointID = "cp-2"
	h.refs.push(domain.ReviewCheckpointBranch(next.WorkflowRunID, next.TaskID, "cp-2"), strings.Repeat("e", 40))
	round, err := h.call(t, next)
	if err != nil {
		t.Fatal(err)
	}
	if round.Number != 2 || h.runCount(t) != runs+1 {
		t.Fatalf("second checkpoint did not open round 2: %+v", round)
	}
}

func TestReviewCheckpointRefusesForgedOrUnresolvedHead(t *testing.T) {
	for name, setup := range map[string]func(*reviewCheckpointHarness, *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool){
		"forged head": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			r.HeadCommit = strings.Repeat("f", 40)
			return domain.ReviewCheckpointHeadMismatch, false
		},
		"worker-local commit": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			r.HeadCommit = strings.Repeat("d", 40)
			return domain.ReviewCheckpointBranchNotFound, true
		},
		"unreachable remote": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			h.refs.status = workerproto.RefResolutionUnreachable
			return domain.ReviewCheckpointRemote, true
		},
		"old worker": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			h.refs.err = fmt.Errorf("%w %q", errRepositoryRefUnsupportedByWorker, "homelab")
			return domain.ReviewCheckpointWorkerSupport, false
		},
		"malformed head": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			r.HeadCommit = "HEAD"
			return domain.ReviewCheckpointInvalidRequest, false
		},
		"unsafe checkpoint id": func(h *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) (domain.ReviewCheckpointCode, bool) {
			r.CheckpointID = "../main"
			return domain.ReviewCheckpointInvalidRequest, false
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := reviewCheckpointFixture(t, true)
			request := h.request
			code, retryable := setup(h, &request)
			_, err := h.call(t, request)
			requireCheckpointRefusal(t, err, code, retryable)
			h.noReviewRows(t)
		})
	}
}

func TestReviewCheckpointFencesBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(*testing.T, *reviewCheckpointHarness, *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode{
		"stale coordinator epoch": func(t *testing.T, h *reviewCheckpointHarness, _ *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			if _, err := h.db.AdvanceCoordinatorEpoch(ctx, h.epoch); err != nil {
				t.Fatal(err)
			}
			return domain.ReviewCheckpointStaleCoordinator
		},
		"finished attempt": func(t *testing.T, h *reviewCheckpointHarness, _ *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			parent := h.parent
			parent.Progress, parent.Control, parent.Revision = domain.ProgressSucceeded, domain.ControlStopped, 2
			if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
				t.Fatal(err)
			}
			return domain.ReviewCheckpointAttemptStale
		},
		"superseded attempt": func(t *testing.T, h *reviewCheckpointHarness, _ *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			newer := domain.Attempt{ID: h.parent.ID + "-retry", WorkflowRunID: h.parent.WorkflowRunID, TaskID: h.parent.TaskID, Number: h.parent.Number + 1, Revision: 1, Progress: domain.ProgressQueued, UpdatedAt: time.Now().UTC()}
			if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{newer}}); err != nil {
				t.Fatal(err)
			}
			return domain.ReviewCheckpointAttemptStale
		},
		"released assignment": func(t *testing.T, h *reviewCheckpointHarness, _ *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			assignment := h.assign
			assignment.State = domain.AssignmentReleased
			if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			return domain.ReviewCheckpointAttemptStale
		},
		"foreign thread": func(_ *testing.T, _ *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			r.ThreadID = "another-thread"
			return domain.ReviewCheckpointAttemptStale
		},
		"foreign assignment": func(_ *testing.T, _ *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			r.AssignmentID = "another-assignment"
			return domain.ReviewCheckpointAttemptStale
		},
		"future revision": func(_ *testing.T, _ *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			r.IssuedRevision = 99
			return domain.ReviewCheckpointAttemptStale
		},
		"other task's attempt": func(_ *testing.T, _ *reviewCheckpointHarness, r *domain.ReviewCheckpointRequest) domain.ReviewCheckpointCode {
			r.TaskID = "another-task"
			return domain.ReviewCheckpointAttemptStale
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := reviewCheckpointFixture(t, true)
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			request := h.request
			code := setup(t, h, &request)
			_, err := h.call(t, request)
			requireCheckpointRefusal(t, err, code, code == domain.ReviewCheckpointStaleCoordinator)
			h.noReviewRows(t)
			if len(h.refs.asked()) != 0 {
				t.Fatal("a fenced call reached the remote")
			}
		})
	}
	t.Run("unauthorized principal", func(t *testing.T) {
		h := reviewCheckpointFixture(t, true)
		request := h.request
		_, err := h.admin.NodeWait(ctx, backlogadmin.Principal{ID: "untrusted", Roles: []string{"worker"}}, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointAction, Checkpoint: &request})
		if err == nil {
			t.Fatal("unauthorized principal opened a review checkpoint")
		}
		h.noReviewRows(t)
	})
}

func TestReviewCheckpointRequiresDeclaredReview(t *testing.T) {
	h := reviewCheckpointFixture(t, false)
	h.refs.push(h.branch(), strings.Repeat("d", 40))
	_, err := h.call(t, h.request)
	requireCheckpointRefusal(t, err, domain.ReviewCheckpointNotDeclared, false)
	h.noReviewRows(t)
	if len(h.refs.asked()) != 0 {
		t.Fatal("an undeclared review reached the remote")
	}
}

func TestReviewCheckpointCompletesAfterCrashBetweenStagingAndMaterialization(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []string{"allocated", "staged"} {
		t.Run(phase, func(t *testing.T) {
			h := reviewCheckpointFixture(t, true)
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			h.op.fault = func(at string) error {
				if at == phase {
					return errors.New("injected crash at " + at)
				}
				return nil
			}
			_, err := h.call(t, h.request)
			requireCheckpointRefusal(t, err, domain.ReviewCheckpointInternal, true)
			if h.runCount(t) != 1 {
				t.Fatal("a crash before materialization left a child run")
			}
			// A restarted coordinator builds a fresh operation over the same state,
			// later in time; the replay must derive the same deadline.
			restarted, err := newCoordinatorReviewCheckpoint(h.cfg, h.db, h.epoch, h.refs)
			if err != nil {
				t.Fatal(err)
			}
			restarted.now = func() time.Time { return time.Now().Add(time.Minute) }
			h.admin.SetReviewCheckpoint(restarted)
			round, err := h.call(t, h.request)
			if err != nil {
				t.Fatal(err)
			}
			if round.Replayed || h.runCount(t) != 2 {
				t.Fatalf("completion after crash: %+v runs=%d", round, h.runCount(t))
			}
			stored, err := h.db.GetReviewRound(ctx, round.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			if !round.Deadline.Equal(stored.CreatedAt.Add(reviewCheckpointDeadline)) {
				t.Fatal("replayed deadline was not derived from round creation")
			}
			again, err := h.call(t, h.request)
			if err != nil || !again.Replayed || again.ChildRunID != round.ChildRunID || h.runCount(t) != 2 {
				t.Fatalf("second replay: %+v %v", again, err)
			}
		})
	}
}
