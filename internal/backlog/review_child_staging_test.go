package backlog

import (
	"context"
	"database/sql"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func newStageAdmissionFixture(t *testing.T) admissionFixture {
	f := newAdmissionFixture(t)
	rewriteBundleManifest(t, f.source, strings.Replace(declaredManifestYAML(), "inputs: [inputs/criteria.md]", "inputs: [inputs/*.md]", 1))
	root := f.service.Artifacts.SubmissionRoot
	ingested, err := (BundleIngester{Store: f.store, StorageRoot: root, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	f.records = ingested.Records
	a := &f.records.Attempts[0]
	a.Revision = 1
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	a.ThreadID = "declared-thread"
	a.AssignmentID = "declared-assignment"
	task := f.records.Tasks[0]
	run := f.records.WorkflowRuns[0]
	f.records.Assignments = []domain.Assignment{{ID: a.AssignmentID, DispatchToken: "declared-token", AttemptID: a.ID, Project: "t3-steward", ThreadID: a.ThreadID, Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "org/sol"}, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision}}
	if err = f.store.SaveCoordinatorRecords(context.Background(), f.records); err != nil {
		t.Fatal(err)
	}
	f.request = AdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: a.ID, Policy: declaredPolicy(task.ReviewRequirements)}
	return f
}

func newStageOwner(t *testing.T) (admissionFixture, *DeclaredReviewStaging, DeclaredChildStageRequest) {
	t.Helper()
	f := newStageAdmissionFixture(t)
	f.service.Store = f.store.Store
	f.service.Artifacts.Root = t.TempDir()
	owner, err := NewDeclaredReviewStaging(f.service, f.store.Store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return f, owner, DeclaredChildStageRequest{Parent: declaredRequest(f), CheckpointID: "staged", HeadCommit: strings.Repeat("d", 40), Deadline: time.Now().UTC().Add(time.Hour)}
}
func stagingSQL(t *testing.T, f admissionFixture) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", f.store.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func assertStageNoGraph(t *testing.T, f admissionFixture, before sqlite.CoordinatorRecords) {
	t.Helper()
	after, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("staging changed graph/artifact/attempt/wait records")
	}
}
func stageFile(t *testing.T, o *DeclaredReviewStaging, r DeclaredChildStageResult) string {
	t.Helper()
	return filepath.Join(o.admission.Artifacts.SubmissionRoot, r.Receipt.Artifacts[0].StoragePath)
}
func TestReviewChildStagingReopenAndOriginalIssue(t *testing.T) {
	ctx := context.Background()
	f, o, req := newStageOwner(t)
	before, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := o.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assertStageNoGraph(t, f, before)
	db := stagingSQL(t, f)
	beforeTables := independentDeclaredTables(t, db)
	parent := f.records.Attempts[0]
	parent.Revision++
	if err = f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
		t.Fatal(err)
	}
	f.catalog.catalog.Classifications = nil // valid replay must keep original issue
	reopened, err := sqlite.OpenMigrated(f.store.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	admission := f.service
	admission.Store = reopened
	admission.Artifacts.Catalog = reopened
	replay, err := NewDeclaredReviewStaging(admission, reopened, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	next, err := replay.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, next) {
		t.Fatal("reopened original issue/opaque files changed")
	}
	// Repeat is read-only even in all logical SQL tables and native audit.
	repeatBefore := independentDeclaredTables(t, db)
	if _, err = replay.StageDeclared(ctx, req); err != nil {
		t.Fatal(err)
	}
	if repeatBefore != independentDeclaredTables(t, db) {
		t.Fatal("replay rewrote SQL")
	}
	if beforeTables == "" {
		t.Fatal("missing logical evidence")
	}
	graph, err := next.Preparation.Build(next.Admission.Authority, next.Checkpoint, next.Receipt.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range graph.Artifacts {
		if a.CreatedAt != first.Receipt.CreatedAt {
			t.Fatal("unstable CreatedAt")
		}
	}
	for _, change := range []string{"head", "deadline", "expired"} {
		changed := req
		switch change {
		case "head":
			changed.HeadCommit = strings.Repeat("e", 40)
		case "deadline":
			changed.Deadline = changed.Deadline.Add(time.Minute)
		case "expired":
			replay.now = func() time.Time { return req.Deadline }
		}
		if _, err = replay.StageDeclared(ctx, changed); err == nil {
			t.Fatal("changed/expired request admitted", change)
		}
		if repeatBefore != independentDeclaredTables(t, db) {
			t.Fatal("conflict changed SQL")
		}
	}
}
func TestReviewChildStagingFaultRecovery(t *testing.T) {
	for _, boundary := range []string{"allocated", "request-retained", "file-retained", "files-verified", "receipt-retained"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			f, o, req := newStageOwner(t)
			before, err := f.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			fired := false
			o.fault = func(phase string) error {
				if !fired && (phase == boundary || boundary == "file-retained" && strings.HasPrefix(phase, boundary+":")) {
					fired = true
					return errors.New("injected real boundary")
				}
				return nil
			}
			if _, err = o.StageDeclared(ctx, req); err == nil || !fired {
				t.Fatal("fault did not execute actual boundary", err)
			}
			assertStageNoGraph(t, f, before)
			db := stagingSQL(t, f)
			var checkpoints, rounds int
			if err = db.QueryRow("SELECT count(*) FROM coordinator_review_checkpoints").Scan(&checkpoints); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow("SELECT count(*) FROM coordinator_review_rounds").Scan(&rounds); err != nil {
				t.Fatal(err)
			}
			if checkpoints != 1 || rounds != 1 {
				t.Fatal("lost allocation-only evidence", checkpoints, rounds)
			}
			reopened, err := sqlite.OpenMigrated(f.store.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			admission := f.service
			admission.Store = reopened
			admission.Artifacts.Catalog = reopened
			fresh, err := NewDeclaredReviewStaging(admission, reopened, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fresh.StageDeclared(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			again, err := fresh.StageDeclared(ctx, req)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatal("partial restart changed exact winner", err)
			}
			assertStageNoGraph(t, f, before)
		})
	}
}
func TestReviewChildStagingConcurrent(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "identical", true: "conflicting"}[conflict], func(t *testing.T) {
			f, o, req := newStageOwner(t)
			before, err := f.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			other := req
			if conflict {
				other.Deadline = other.Deadline.Add(time.Minute)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			results := make([]DeclaredChildStageResult, 2)
			errs := make([]error, 2)
			for i, r := range []DeclaredChildStageRequest{req, other} {
				wg.Add(1)
				go func(i int, r DeclaredChildStageRequest) {
					defer wg.Done()
					<-start
					results[i], errs[i] = o.StageDeclared(context.Background(), r)
				}(i, r)
			}
			close(start)
			wg.Wait()
			if !conflict {
				if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(results[0], results[1]) {
					t.Fatal("identical calls did not share winner", errs)
				}
			} else {
				winner := 0
				if errs[0] != nil {
					winner = 1
				}
				if errs[winner] != nil || errs[1-winner] == nil {
					t.Fatal("conflicting calls did not produce one winner", errs)
				}
				r := req
				if winner == 1 {
					r = other
				}
				again, err := o.StageDeclared(context.Background(), r)
				if err != nil || !reflect.DeepEqual(results[winner], again) {
					t.Fatal("loser disturbed winner", err)
				}
			}
			assertStageNoGraph(t, f, before)
		})
	}
}
func TestReviewChildStagingCorruptionAndFences(t *testing.T) {
	for _, kind := range []string{"bytes", "prompt", "receipt", "request", "missing", "symlink", "directory-symlink", "unknown", "mode", "pending"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			result, err := o.StageDeclared(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			target := stageFile(t, o, result)
			dir := filepath.Dir(target)
			switch kind {
			case "receipt":
				target = filepath.Join(dir, "receipt.json")
			case "request":
				target = filepath.Join(dir, "request.json")
			case "prompt":
				for _, a := range result.Receipt.Artifacts {
					if strings.HasPrefix(a.Name, "prompts/") {
						target = filepath.Join(o.admission.Artifacts.SubmissionRoot, a.StoragePath)
						break
					}
				}
			case "unknown":
				target = filepath.Join(dir, "unknown")
			case "pending":
				target = filepath.Join(dir, ".pending-"+filepath.Base(target))
			}
			switch kind {
			case "missing":
				err = os.Remove(target)
			case "symlink":
				if err = os.Remove(target); err == nil {
					err = os.Symlink(result.Receipt.Artifacts[1].ID+".blob", target)
				}
			case "directory-symlink":
				if err = os.Rename(dir, dir+"-old"); err == nil {
					err = os.Symlink(dir+"-old", dir)
				}
			case "mode":
				err = os.Chmod(target, 0600)
			default:
				if _, e := os.Stat(target); e == nil {
					if err = os.Chmod(target, 0600); err != nil {
						t.Fatal(err)
					}
				}
				err = os.WriteFile(target, []byte("wrong immutable bytes"), 0400)
			}
			if err != nil {
				t.Fatal(err)
			}
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			if _, err = o.StageDeclared(context.Background(), req); err == nil {
				t.Fatal("corrupt stage adopted", kind)
			}
			if independentDeclaredTables(t, db) != before {
				t.Fatal("corruption caused SQL effects")
			}
			if kind != "missing" && kind != "directory-symlink" {
				if _, err = os.Lstat(target); err != nil {
					t.Fatal("refusal cleaned shared evidence")
				}
			}
		})
	}
}
func TestReviewChildStagingLateCurrentCustody(t *testing.T) {
	for _, boundary := range []string{"allocated", "files-verified"} {
		for _, kind := range []string{"membership", "project", "activation"} {
			t.Run(boundary+"/"+kind, func(t *testing.T) {
				f, o, req := newStageOwner(t)
				db := stagingSQL(t, f)
				var before string
				fired := false
				o.fault = func(phase string) error {
					if phase != boundary || fired {
						return nil
					}
					fired = true
					var err error
					switch kind {
					case "membership":
						_, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json('[\"other\"]')) WHERE id=?", f.records.Workflows[0].ID)
					case "project":
						_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.project','foreign') WHERE id=?", f.records.Assignments[0].ID)
					case "activation":
						_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','late') WHERE id=?", f.records.Assignments[0].ID)
					}
					if err != nil {
						t.Fatal(err)
					}
					before = independentDeclaredTables(t, db)
					return nil
				}
				if _, err := o.StageDeclared(context.Background(), req); err == nil || !fired {
					t.Fatal("late custody admitted", err)
				}
				if before != independentDeclaredTables(t, db) {
					t.Fatal("failed handoff changed SQL/native audit")
				}
			})
		}
	}
}
func TestReviewChildStagingAllocationCurrentWriterInitialReplay(t *testing.T) {
	ctx := context.Background()
	for _, replay := range []bool{false, true} {
		for _, kind := range []string{"healthy-advance", "membership", "unrelated-duplicate", "project", "activation"} {
			t.Run(map[bool]string{false: "initial", true: "replay"}[replay]+"/"+kind, func(t *testing.T) {
				f := newDeclaredAdmissionFixture(t)
				snap, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
				if err != nil {
					t.Fatal(err)
				}
				cp := review.Checkpoint{ID: "allocation", HeadCommit: strings.Repeat("d", 40), InputDigest: snap.Provenance.InputManifest.Digest}
				var original review.CheckpointAuthority
				if replay {
					original, err = f.store.AllocateReviewCheckpoint(ctx, snap.Authority, cp)
					if err != nil {
						t.Fatal(err)
					}
				}
				db := stagingSQL(t, f)
				switch kind {
				case "healthy-advance":
					_, err = db.Exec("UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?", f.request.AttemptID)
					f.catalog.catalog.Classifications = nil
				case "membership":
					_, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json('[]')) WHERE id=?", f.records.Workflows[0].ID)
				case "unrelated-duplicate":
					_, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json_array(?,'other','other')) WHERE id=?", f.request.TaskID, f.records.Workflows[0].ID)
				case "project":
					_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.project','foreign') WHERE id=?", snap.Authority.Parent.AssignmentID)
				case "activation":
					_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','late') WHERE id=?", snap.Authority.Parent.AssignmentID)
				}
				if err != nil {
					t.Fatal(err)
				}
				before := independentDeclaredTables(t, db)
				got, err := f.store.AllocateReviewCheckpoint(ctx, snap.Authority, cp)
				if kind == "healthy-advance" {
					if err != nil || replay && got != original {
						t.Fatal("healthy original issue", err)
					}
				} else {
					if err == nil {
						t.Fatal("owning allocation admitted invalid late custody")
					}
					if before != independentDeclaredTables(t, db) {
						t.Fatal("allocation refusal changed full SQL/native audit")
					}
				}
				reopened, e := sqlite.OpenMigrated(f.store.dbPath)
				if e != nil {
					t.Fatal(e)
				}
				defer reopened.Close()
				next, e := reopened.AllocateReviewCheckpoint(ctx, snap.Authority, cp)
				if kind == "healthy-advance" {
					if e != nil || next != got {
						t.Fatal("healthy reopened allocation", e)
					}
				} else {
					if e == nil || before != independentDeclaredTables(t, db) {
						t.Fatal("reopened invalid allocation effects", e)
					}
				}
			})
		}
	}
}
func TestReviewChildStagingBoundsAndConfiguration(t *testing.T) {
	_, o, req := newStageOwner(t)
	for _, r := range []DeclaredChildStageRequest{
		{Parent: req.Parent, CheckpointID: "../escape", HeadCommit: req.HeadCommit, Deadline: req.Deadline},
		{Parent: req.Parent, CheckpointID: req.CheckpointID, HeadCommit: "main", Deadline: req.Deadline},
		{Parent: req.Parent, CheckpointID: req.CheckpointID, HeadCommit: req.HeadCommit},
	} {
		if _, err := o.StageDeclared(context.Background(), r); err == nil {
			t.Fatal("invalid bounded request")
		}
	}
	// Exercise real retention helper on immutable files and bounded manifest.
	root := t.TempDir()
	dir, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	raw := []byte("expected")
	if err = childStageRetain(dir, root, "file", raw); err != nil {
		t.Fatal(err)
	}
	if err = childStageRetain(dir, root, "file", []byte("wrong")); err == nil {
		t.Fatal("overwrite allowed")
	}
	entries := make([]pinnedinput.Entry, pinnedinput.MaxFiles+1)
	for i := range entries {
		entries[i] = pinnedinput.Entry{Name: "valid", SHA256: strings.Repeat("a", 64)}
	}
	if _, err = pinnedinput.NewManifest(entries); err == nil {
		t.Fatal("file limit")
	}
	entries = []pinnedinput.Entry{{Name: "valid", Size: pinnedinput.MaxFileBytes + 1, SHA256: strings.Repeat("a", 64)}}
	if _, err = pinnedinput.NewManifest(entries); err == nil {
		t.Fatal("byte limit")
	}
	outside := t.TempDir()
	link := filepath.Join(t.TempDir(), "root")
	if err = os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	admission := o.admission
	admission.Artifacts.SubmissionRoot = link
	if _, err = NewDeclaredReviewStaging(admission, o.store, time.Now); err == nil {
		t.Fatal("symlink configuration accepted")
	}
	admission = o.admission
	admission.Artifacts.Root = ""
	if _, err = NewDeclaredReviewStaging(admission, o.store, time.Now); err == nil {
		t.Fatal("incomplete configured root accepted")
	}
}
