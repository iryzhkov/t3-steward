package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func reviewAuthorityFixture(t *testing.T) (*Store, review.FrozenAuthority) {
	t.Helper()
	s, a, now := taskWaitFixture(t)
	_, err := s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.route.providerInstanceId','codex','$.route.model','sol') WHERE id=?", a.AssignmentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w", Project: "repo", Environment: domain.ExecutionEnvironment{Type: "repository", Scope: "repo"}, CreatedAt: now}}}); err != nil {
		t.Fatal(err)
	}
	requirements, err := review.NewRequirements(review.RequirementsSpec{Risk: "routine", CriteriaDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), RequiredReviewers: 2, MinProviderFamilies: 2, Members: []review.MemberRequirement{
		{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true},
		{ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := review.NewFrozenAuthority(review.ParentBinding{RunID: a.WorkflowRunID, TaskID: a.TaskID, AttemptID: a.ID, ThreadID: a.ThreadID, AssignmentID: a.AssignmentID, AssignmentEpoch: 1, IssuedRevision: a.Revision, Repository: "repo", BaseCommit: strings.Repeat("c", 40), ExecutorRoute: "codex/sol"}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}

func TestReviewAuthorityInitialFreezeAndAtomicRollback(t *testing.T) {
	ctx := context.Background()
	t.Run("issued revision", func(t *testing.T) {
		s, f := reviewAuthorityFixture(t)
		f.Parent.IssuedRevision++
		if _, err := s.FreezeReviewAuthority(ctx, f); err == nil {
			t.Fatal("future revision accepted")
		}
		var count int
		_ = s.db.QueryRow("SELECT count(*) FROM coordinator_review_authorities").Scan(&count)
		if count != 0 {
			t.Fatal("failed freeze mutated authority")
		}
	})
	t.Run("unbound collision", func(t *testing.T) {
		s, f := reviewAuthorityFixture(t)
		cp := reviewCheckpoint("cp")
		if _, err := s.AllocateReviewCheckpoint(ctx, f, cp); err == nil {
			t.Fatal("allocation without persisted freeze")
		}
		if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
			t.Fatal(err)
		}
		a := review.CheckpointAuthority{AuthorityKey: f.Key(), Checkpoint: cp}
		_, err := s.CreateReviewRound(ctx, review.Round{ID: a.Key(), InputManifestDigest: cp.InputDigest, Reviewers: []review.Reviewer{{ID: "legacy", Role: "review", Route: "codex/sol", Required: true}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.AllocateReviewCheckpoint(ctx, f, cp); err == nil {
			t.Fatal("claimed historical round")
		}
		if _, err := s.CheckReviewAuthorityEvidence(ctx, f, cp, cp.HeadCommit); err == nil {
			t.Fatal("unbound round qualifies")
		}
		var count int
		_ = s.db.QueryRow("SELECT count(*) FROM coordinator_review_checkpoints").Scan(&count)
		if count != 0 {
			t.Fatal("failed allocation consumed round")
		}
		if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("next")); err != nil {
			t.Fatal(err)
		}
	})
}
func TestReviewAuthorityEvidenceSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	s, f := reviewAuthorityFixture(t)
	cp := reviewCheckpoint("cp")
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	a, err := s.AllocateReviewCheckpoint(ctx, f, cp)
	if err != nil {
		t.Fatal(err)
	}
	round, err := s.GetReviewRound(ctx, a.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range round.Reviewers {
		round, err = s.RecordReviewResult(ctx, a.RoundID, m.ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + cp.InputDigest + `","reviewerRoute":"` + m.Route + `"}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if verdict, err := reopened.CheckReviewAuthorityEvidence(ctx, f, cp, cp.HeadCommit); err != nil || verdict != "accept" {
		t.Fatalf("reopened evidence %s %v", verdict, err)
	}
}

func reviewCheckpoint(id string) review.Checkpoint {
	return review.Checkpoint{ID: id, HeadCommit: strings.Repeat("d", 40), InputDigest: strings.Repeat("e", 64)}
}
func TestReviewAuthorityFreezeReplayAndConflicts(t *testing.T) {
	ctx := context.Background()
	s, f := reviewAuthorityFixture(t)
	got, err := s.FreezeReviewAuthority(ctx, f)
	if err != nil || !reflect.DeepEqual(got, f) {
		t.Fatalf("freeze: %v", err)
	}
	f.Requirements.Members[0].Route = "forged/route"
	if _, err := s.FreezeReviewAuthority(ctx, f); err == nil {
		t.Fatal("modified freeze accepted")
	}
	f = got
	cp := reviewCheckpoint("cp")
	first, err := s.AllocateReviewCheckpoint(ctx, f, cp)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.AllocateReviewCheckpoint(ctx, f, cp)
	if err != nil || replay != first {
		t.Fatalf("replay %v %v", replay, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*review.FrozenAuthority, *review.Checkpoint)
	}{
		{"head", func(_ *review.FrozenAuthority, c *review.Checkpoint) { c.HeadCommit = strings.Repeat("f", 40) }},
		{"input", func(_ *review.FrozenAuthority, c *review.Checkpoint) { c.InputDigest = strings.Repeat("f", 64) }},
		{"criteria", func(f *review.FrozenAuthority, _ *review.Checkpoint) {
			f.Requirements.CriteriaDigest = strings.Repeat("f", 64)
		}},
		{"policy", func(f *review.FrozenAuthority, _ *review.Checkpoint) {
			f.Requirements.PolicyDigest = strings.Repeat("f", 64)
		}},
		{"repo", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Parent.Repository = "other" }},
		{"attempt", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Parent.AttemptID = "other" }},
		{"thread", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Parent.ThreadID = "other" }},
		{"epoch", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Parent.AssignmentEpoch++ }},
		{"base", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Parent.BaseCommit = strings.Repeat("f", 40) }},
		{"route", func(f *review.FrozenAuthority, _ *review.Checkpoint) {
			f.Requirements.Members[0].Route = "forged/route"
		}},
		{"limit", func(f *review.FrozenAuthority, _ *review.Checkpoint) { f.Requirements.RoundLimit = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, _ := f.Canonical()
			c := cp
			tc.change(&clone, &c)
			// Even a fully rehashed internally valid downgrade cannot replace stored authority.
			r, e := review.NewRequirements(clone.Requirements)
			if e == nil {
				clone.RequirementsDigest = r.Digest()
			}
			if _, e := s.AllocateReviewCheckpoint(ctx, clone, c); e == nil {
				t.Fatal("changed checkpoint/expected policy accepted")
			}
			if _, e := s.FreezeReviewAuthority(ctx, clone); tc.name != "head" && tc.name != "input" && e == nil {
				t.Fatal("replaced frozen authority")
			}
		})
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM coordinator_review_checkpoints").Scan(&count); err != nil || count != 1 {
		t.Fatalf("mutation on conflict: %d %v", count, err)
	}
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	replay, err = s.AllocateReviewCheckpoint(ctx, f, cp)
	if err != nil || replay != first {
		t.Fatalf("reopened replay: %v", err)
	}
	if _, err := s.db.Exec("UPDATE coordinator_review_authorities SET record='{}'"); err == nil {
		t.Fatal("freeze mutable")
	}
	if _, err := s.db.Exec("DELETE FROM coordinator_review_checkpoints"); err == nil {
		t.Fatal("allocation deletable")
	}
}
func TestReviewAuthorityRiskDowngradeRefused(t *testing.T) {
	s, f := reviewAuthorityFixture(t)
	ctx := context.Background()
	f.Requirements.Risk = "risky"
	f.Requirements.RoundLimit = 3
	f.Requirements.Members[0].Tier = "critical"
	r, err := review.NewRequirements(f.Requirements)
	if err != nil {
		t.Fatal(err)
	}
	f.RequirementsDigest = r.Digest()
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	weaker, _ := f.Canonical()
	weaker.Requirements.Risk = "routine"
	weaker.Requirements.RoundLimit = 2
	weaker.Requirements.Members[0].Tier = "executor"
	r, err = review.NewRequirements(weaker.Requirements)
	if err != nil {
		t.Fatal(err)
	}
	weaker.RequirementsDigest = r.Digest()
	if _, err := s.FreezeReviewAuthority(ctx, weaker); !errors.Is(err, ErrReviewAuthorityConflict) {
		t.Fatalf("risk downgrade: %v", err)
	}
	if _, err := s.AllocateReviewCheckpoint(ctx, weaker, reviewCheckpoint("cp")); !errors.Is(err, ErrReviewAuthorityConflict) {
		t.Fatalf("weaker allocation: %v", err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("four")); !errors.Is(err, ErrReviewAuthorityLimit) {
		t.Fatalf("risky limit: %v", err)
	}
}

func TestReviewAuthorityCurrentIdentityRefused(t *testing.T) {
	for _, kind := range []string{"thread", "terminal", "retry", "assignment", "revision", "run", "task", "repository", "executor"} {
		t.Run(kind, func(t *testing.T) {
			s, f := reviewAuthorityFixture(t)
			ctx := context.Background()
			if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "thread":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.threadId','foreign')")
			case "terminal":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded')")
			case "retry":
				a := loadAttempt(t, s, f.Parent.AttemptID)
				a.ID = "retry"
				a.Number++
				if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
					t.Fatal(err)
				}
			case "assignment":
				_, _ = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.epoch',2)")
			case "revision":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET revision=0,record=json_set(record,'$.revision',0)")
			case "run":
				_, _ = s.db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,'$.progress','cancelled')")
			case "task":
				_, _ = s.db.Exec("UPDATE coordinator_tasks SET record=json_set(record,'$.workflowId','foreign')")
			case "repository":
				_, _ = s.db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.project','foreign')")
			case "executor":
				_, _ = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.route.model','other')")
			}
			if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("cp")); err == nil {
				t.Fatal("stale/foreign live identity accepted")
			}
		})
	}
}
func TestReviewAuthorityConcurrentAllocationBounded(t *testing.T) {
	s, f := reviewAuthorityFixture(t)
	ctx := context.Background()
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	// Independent connections exercise SQLite's transaction boundary, not only db's pool.
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := map[int]string{}
	unexpected := []error{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := s
			if i%2 == 1 {
				db = other
			}
			id := "one"
			if i%3 == 1 {
				id = "two"
			}
			if i%3 == 2 {
				id = "three"
			}
			a, e := db.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint(id))
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				if old, ok := success[a.Number]; ok && old != a.RoundID {
					unexpected = append(unexpected, errors.New("duplicate number"))
				}
				success[a.Number] = a.RoundID
			} else if !errors.Is(e, ErrReviewAuthorityLimit) {
				unexpected = append(unexpected, e)
			}
		}(i)
	}
	wg.Wait()
	if len(unexpected) > 0 || len(success) != 2 {
		t.Fatalf("success=%v errors=%v", success, unexpected)
	}
	var count int
	_ = s.db.QueryRow("SELECT count(*) FROM coordinator_review_checkpoints").Scan(&count)
	if count != 2 {
		t.Fatalf("count=%d", count)
	}
}
func TestReviewAuthorityDurableEvidenceOnly(t *testing.T) {
	for _, kind := range []string{"valid", "pending", "missing", "forged", "malformed", "failed", "timed-out", "cancelled", "head", "criteria", "member"} {
		t.Run(kind, func(t *testing.T) {
			s, f := reviewAuthorityFixture(t)
			ctx := context.Background()
			if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
				t.Fatal(err)
			}
			cp := reviewCheckpoint("cp")
			a, err := s.AllocateReviewCheckpoint(ctx, f, cp)
			if err != nil {
				t.Fatal(err)
			}
			round, err := s.GetReviewRound(ctx, a.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range round.Reviewers {
				if kind == "pending" {
					break
				}
				result := review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + cp.InputDigest + `","reviewerRoute":"` + member.Route + `"}`)}
				switch kind {
				case "malformed":
					result.VerdictJSON = []byte("{}")
				case "missing":
					result.ReviewMD = ""
				case "failed", "timed-out":
					result.State = kind
					result.Failure = "no result"
				}
				round, err = s.RecordReviewResult(ctx, a.RoundID, member.ID, round.Revision, result)
				if err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "forged":
				for i := range round.Reviewers {
					round.Reviewers[i].VerdictJSON = nil
					round.Reviewers[i].Verdict = &review.Verdict{Verdict: "accept"}
				}
			case "cancelled":
				round.Reviewers[0].State = "cancelled"
			case "member":
				round.Reviewers[0].Route = "forged/route"
			}
			if kind == "forged" || kind == "cancelled" || kind == "member" {
				raw, _ := json.Marshal(round)
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET record=? WHERE id=?", raw, a.RoundID)
				if err != nil {
					t.Fatal(err)
				}
			}
			head := cp.HeadCommit
			if kind == "head" {
				head = strings.Repeat("f", 40)
			}
			if kind == "criteria" {
				f.Requirements.CriteriaDigest = strings.Repeat("f", 64)
				r, _ := review.NewRequirements(f.Requirements)
				f.RequirementsDigest = r.Digest()
			}
			verdict, err := s.CheckReviewAuthorityEvidence(ctx, f, cp, head)
			if kind == "valid" {
				if err != nil || verdict != "accept" {
					t.Fatalf("valid %q %v", verdict, err)
				}
			} else if err == nil {
				t.Fatalf("accepted %s: %s", kind, verdict)
			}
		})
	}
}
