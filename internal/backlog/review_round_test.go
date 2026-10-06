package backlog

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestReviewCollectorFullRoundWithMalformedAndTimeout(t *testing.T) {
	for _, bad := range []string{"", "malformed", "timeout"} {
		t.Run(bad, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			now := time.Now().UTC()
			digest := strings.Repeat("a", 64)
			round := review.Round{ID: "round-fake", WorkflowRunID: "run-fake", InputManifestDigest: digest, Deadline: now.Add(time.Hour)}
			records := sqlite.CoordinatorRecords{}
			payloads := map[string]string{}
			for i, name := range []string{"independent-a", "independent-b", "swarm-security", "swarm-errors", "swarm-tests", "judge"} {
				role := "independent"
				required := true
				if strings.HasPrefix(name, "swarm-") {
					role = "swarm:" + strings.TrimPrefix(name, "swarm-")
					required = false
				}
				if name == "judge" {
					role = "judge"
				}
				route := "a/full"
				if i%2 == 1 {
					route = "b/full"
				}
				round.Reviewers = append(round.Reviewers, review.Reviewer{ID: name, TaskID: name, Role: role, Route: route, Required: required})
				attempt := domain.Attempt{ID: "attempt-" + name, WorkflowRunID: "run-fake", TaskID: name, Number: 1, Progress: domain.ProgressSucceeded}
				if bad == "timeout" && name == "independent-b" {
					attempt.Progress = domain.ProgressActive
				}
				records.Attempts = append(records.Attempts, attempt)
				v := review.Verdict{Schema: review.Schema, Verdict: "accept", Findings: []review.Finding{}, InputManifestDigest: digest, ReviewerRoute: route}
				raw, _ := json.Marshal(v)
				if bad == "malformed" && name == "independent-b" {
					raw = []byte("{invalid")
				}
				for _, doc := range []string{"review.md", "verdict.json"} {
					id := name + "-" + doc
					content := "review accepted"
					if doc == "verdict.json" {
						content = string(raw)
					}
					records.Artifacts = append(records.Artifacts, domain.Artifact{ID: id, WorkflowRunID: "run-fake", TaskID: name, AttemptID: attempt.ID, Kind: domain.ArtifactOutput, Name: doc, Size: int64(len(content))})
					payloads[id] = content
				}
			}
			if _, err := store.CreateReviewRound(ctx, round); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			c := ReviewCollector{Store: store, Results: t.TempDir(), Open: func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
				return domain.Artifact{ID: id}, io.NopCloser(strings.NewReader(payloads[id])), nil
			}}
			if err := c.Tick(ctx, now); err != nil {
				t.Fatal(err)
			}
			got, err := store.GetReviewRound(ctx, round.ID)
			if err != nil {
				t.Fatal(err)
			}
			if bad == "timeout" {
				if got.Terminal() {
					t.Fatal("active reviewer settled before deadline")
				}
				if err := c.Tick(ctx, now.Add(2*time.Hour)); err != nil {
					t.Fatal(err)
				}
				got, err = store.GetReviewRound(ctx, round.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if !got.Terminal() {
				t.Fatal("round not collected")
			}
			expected := "accept"
			if bad != "" {
				expected = "reject"
			}
			if got.CombinedVerdict() != expected {
				t.Fatalf("combined %s", got.CombinedVerdict())
			}
			if bad == "malformed" && got.Reviewers[1].State != "invalid" {
				t.Fatal("malformed verdict accepted")
			}
			if bad == "timeout" && got.Reviewers[1].State != "timed-out" {
				t.Fatal("timeout accepted")
			}
			if got.ReplyText == "" || !strings.Contains(got.ReplyText, "summary.json") {
				t.Fatal("missing short wake reply")
			}
			if err := c.Tick(ctx, now.Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			replay, _ := store.GetReviewRound(ctx, round.ID)
			if replay.Revision != got.Revision {
				t.Fatal("collector changed terminal result on replay")
			}
		})
	}
}
