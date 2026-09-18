package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var bucketTestKey = domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}

// bucketTestState writes a configuration with its own state database holding
// one bucket in the given phase, and returns the state and config paths.
func bucketTestState(t *testing.T, phase domain.Phase, used float64) (string, string) {
	t.Helper()
	root := shortTempDir(t)
	statePath := filepath.Join(root, "state.db")
	configPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(configPath, []byte("state_path: "+statePath+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(3 * time.Hour)
	stopped := now.Add(-time.Hour)
	st := domain.BucketState{
		Key: bucketTestKey, Phase: phase, Epoch: domain.EpochFor(&reset), LimitName: "Claude five hour",
		UsedPercent: used, ResetsAt: &reset, ObservedAt: stopped, UpdatedAt: stopped, ETAStrikes: 2,
	}
	if phase == domain.PhaseStopped {
		st.StoppedAt = &stopped
	}
	if err := store.SaveBucket(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	return statePath, configPath
}

func loadTestBucket(t *testing.T, statePath string) (domain.BucketState, []domain.ActionRecord) {
	t.Helper()
	store, err := sqlite.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	st, err := store.LoadBucket(context.Background(), bucketTestKey)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := store.RecentActions(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	return st, actions
}

// F-1: a stopped bucket had no operator rearm; the audit's homelab operator
// relaxed the policy and still waited for the reset. bucket rearm sets the
// phase to normal with RecoveredAt now, clears the stop bookkeeping and
// records one rearm action naming the actor, the reason and both phases.
func TestBucketRearmRearmsAStoppedBucket(t *testing.T) {
	statePath, configPath := bucketTestState(t, domain.PhaseStopped, 60)
	before := time.Now()
	output := captureStdout(t, func() {
		if err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--reason", "thresholds restored after the audit", "--config", configPath}); err != nil {
			t.Fatalf("bucket rearm: %v", err)
		}
	})
	st, actions := loadTestBucket(t, statePath)
	if st.Phase != domain.PhaseNormal {
		t.Fatalf("phase after rearm = %s, want normal", st.Phase)
	}
	if st.RecoveredAt == nil || st.RecoveredAt.Before(before.Add(-time.Second)) {
		t.Fatalf("RecoveredAt = %v, want now", st.RecoveredAt)
	}
	if st.StoppedAt != nil || st.DrainDeadline != nil || st.ETAStrikes != 0 {
		t.Fatalf("stop bookkeeping not cleared: %+v", st)
	}
	if st.UsedPercent != 60 || st.ResetsAt == nil {
		t.Fatalf("rearm invented a reading: %+v", st)
	}
	if len(actions) != 1 || actions[0].Kind != domain.ActionRearm || actions[0].Bucket != bucketTestKey.String() {
		t.Fatalf("actions = %+v, want one rearm", actions)
	}
	for _, want := range []string{"thresholds restored after the audit", "stopped", "normal", "@"} {
		if !strings.Contains(actions[0].Detail, want) {
			t.Fatalf("rearm detail %q does not name %q", actions[0].Detail, want)
		}
	}
	for _, want := range []string{"stopped", "normal", "60%"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output %q does not show %q", output, want)
		}
	}
}

// At or above stop_percent the rearm is refused unless --force: the next
// reading would stop the bucket again at once.
func TestBucketRearmRefusesAtOrAboveStopPercentWithoutForce(t *testing.T) {
	statePath, configPath := bucketTestState(t, domain.PhaseStopped, 96)
	err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--reason", "trying", "--config", configPath})
	if err == nil {
		t.Fatal("rearm at 96% was accepted without --force")
	}
	if !strings.Contains(err.Error(), "--force") || !strings.Contains(err.Error(), "95%") {
		t.Fatalf("refusal %q does not name --force and the stop threshold", err)
	}
	st, actions := loadTestBucket(t, statePath)
	if st.Phase != domain.PhaseStopped || len(actions) != 0 {
		t.Fatalf("refused rearm changed state: phase=%s actions=%+v", st.Phase, actions)
	}
	captureStdout(t, func() {
		if err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--reason", "provider extended the quota", "--force", "--config", configPath}); err != nil {
			t.Fatalf("bucket rearm --force: %v", err)
		}
	})
	st, actions = loadTestBucket(t, statePath)
	if st.Phase != domain.PhaseNormal || len(actions) != 1 || !strings.Contains(actions[0].Detail, "forced") {
		t.Fatalf("forced rearm: phase=%s actions=%+v", st.Phase, actions)
	}
}

func TestBucketRearmNeedsAReason(t *testing.T) {
	_, configPath := bucketTestState(t, domain.PhaseStopped, 60)
	err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--config", configPath})
	if err == nil || !strings.Contains(err.Error(), "--reason") {
		t.Fatalf("rearm without a reason: %v", err)
	}
}

func TestBucketRearmUnknownKeyListsKnownKeys(t *testing.T) {
	_, configPath := bucketTestState(t, domain.PhaseStopped, 60)
	err := run([]string{"bucket", "rearm", "codex/codex/primary", "--reason", "x", "--config", configPath})
	if err == nil {
		t.Fatal("unknown bucket was rearmed")
	}
	if !strings.Contains(err.Error(), "codex/codex/primary") || !strings.Contains(err.Error(), bucketTestKey.String()) {
		t.Fatalf("error %q does not name the unknown key and the known keys", err)
	}
}

func TestBucketRearmJSONPrintsOneDocument(t *testing.T) {
	_, configPath := bucketTestState(t, domain.PhaseStopped, 60)
	output := captureStdout(t, func() {
		if err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--reason", "json", "--json", "--config", configPath}); err != nil {
			t.Fatalf("bucket rearm --json: %v", err)
		}
	})
	assertExactlyOneJSONDocument(t, "bucket rearm", []byte(output))
	var document struct {
		Before domain.BucketState  `json:"before"`
		After  domain.BucketState  `json:"after"`
		Action domain.ActionRecord `json:"action"`
	}
	if err := json.Unmarshal([]byte(output), &document); err != nil {
		t.Fatal(err)
	}
	if document.Before.Phase != domain.PhaseStopped || document.After.Phase != domain.PhaseNormal || document.Action.Kind != domain.ActionRearm {
		t.Fatalf("document = %s", output)
	}
}

func TestBucketListShowsEveryBucket(t *testing.T) {
	_, configPath := bucketTestState(t, domain.PhaseStopped, 60)
	output := captureStdout(t, func() {
		if err := run([]string{"bucket", "list", "--config", configPath}); err != nil {
			t.Fatalf("bucket list: %v", err)
		}
	})
	for _, want := range []string{bucketTestKey.String(), "stopped", "60%"} {
		if !strings.Contains(output, want) {
			t.Fatalf("bucket list output %q lacks %q", output, want)
		}
	}
	// After a rearm the list names it with its reason.
	captureStdout(t, func() {
		if err := run([]string{"bucket", "rearm", bucketTestKey.String(), "--reason", "listed reason", "--config", configPath}); err != nil {
			t.Fatalf("bucket rearm: %v", err)
		}
	})
	output = captureStdout(t, func() {
		if err := run([]string{"bucket", "list", "--config", configPath}); err != nil {
			t.Fatalf("bucket list: %v", err)
		}
	})
	if !strings.Contains(output, "last rearm") || !strings.Contains(output, "listed reason") {
		t.Fatalf("bucket list after a rearm does not name it: %q", output)
	}
	// The JSON form is one document with the same rows, on a fresh stopped
	// bucket.
	_, configPath = bucketTestState(t, domain.PhaseStopped, 60)
	output = captureStdout(t, func() {
		if err := run([]string{"bucket", "list", "--json", "--config", configPath}); err != nil {
			t.Fatalf("bucket list --json: %v", err)
		}
	})
	assertExactlyOneJSONDocument(t, "bucket list", []byte(output))
	var document struct {
		Buckets []struct {
			Key       string     `json:"key"`
			Phase     string     `json:"phase"`
			StoppedAt *time.Time `json:"stoppedAt"`
		} `json:"buckets"`
	}
	if err := json.Unmarshal([]byte(output), &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Buckets) != 1 || document.Buckets[0].Key != bucketTestKey.String() || document.Buckets[0].Phase != "stopped" || document.Buckets[0].StoppedAt == nil {
		t.Fatalf("document = %s", output)
	}
}

func TestUsageNamesTheBucketVerb(t *testing.T) {
	if !strings.Contains(usage, "bucket ") {
		t.Fatal("top-level usage does not name the bucket verb")
	}
	output := captureStdout(t, func() {
		if err := run([]string{"bucket", "--help"}); err != nil {
			t.Fatalf("bucket --help: %v", err)
		}
	})
	for _, want := range []string{"list", "rearm", "--reason", "--force", "--json"} {
		if !strings.Contains(output, want) {
			t.Fatalf("bucket help lacks %q: %s", want, output)
		}
	}
}
