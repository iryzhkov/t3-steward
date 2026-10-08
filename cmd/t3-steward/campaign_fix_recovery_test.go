package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoveryTestUnit(t *testing.T) campaign.CompiledUnit {
	cli, _ := fixCommandFixture(t)
	r, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if e != nil {
		t.Fatal(e)
	}
	u, e := campaign.GenerateFixChain(r.Source, r.Options)
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func TestCampaignFixWriterCrashHelper(t *testing.T) {
	target := os.Getenv("STEWARD_H4_WRITER_TEST_TARGET")
	if target == "" {
		t.Skip("helper")
	}
	u := recoveryTestUnit(t)
	raw := make([]byte, 32<<20)
	for _, p := range []string{"crash1", "crash2", "crash3", "crash4"} {
		u.Files = append(u.Files, campaign.CompiledFile{Path: p, Content: raw})
	}
	limits := campaign.DefaultLimits
	limits.MaxBytes = 256 << 20
	_, e := campaign.WriteFixChain(target, u, limits)
	if e != nil {
		t.Fatal(e)
	}
}
func TestCampaignFixKilledWriterRetry(t *testing.T) {
	target := filepath.Join(t.TempDir(), "generated")
	child := exec.Command(os.Args[0], "-test.run=^TestCampaignFixWriterCrashHelper$")
	child.Env = append(os.Environ(), "STEWARD_H4_WRITER_TEST_TARGET="+target)
	if e := child.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		observed := false
		if _, e := os.Stat(target + ".fix-lock"); e == nil {
			observed = true
		}
		entries, _ := os.ReadDir(filepath.Dir(target))
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".fix-staging-") {
				observed = true
			}
		}
		if observed {
			break
		}
		if time.Now().After(deadline) {
			child.Process.Kill()
			child.Wait()
			t.Fatal("writer lock not observed")
		}
		time.Sleep(50 * time.Microsecond)
	}
	if e := child.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = child.Wait()
	if _, e := os.Stat(target); !os.IsNotExist(e) {
		t.Fatalf("must kill before publication: %v", e)
	}
	if _, e := campaign.WriteFixChain(target, recoveryTestUnit(t)); e != nil {
		t.Fatalf("retry after killed writer must recover without unpublished output: %v", e)
	}
}

func TestCampaignFixRefusesCarriedAttemptDrift(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	oldDetail, oldOpen := cli.detail, cli.open
	cli.detail = func(ctx context.Context, run string) (backlogadmin.WorkflowDetail, error) {
		d, e := oldDetail(ctx, run)
		d.Tasks[0].Attempt.ID = "pa-new"
		d.Tasks[0].Artifacts = []backlogadmin.Artifact{
			{Metadata: backlogadmin.ArtifactMetadata{ID: "commit", Name: "implementation", WorkflowRunID: "run", TaskID: "p", AttemptID: "pa-new", Kind: domain.ArtifactOutput}},
			{Metadata: backlogadmin.ArtifactMetadata{ID: "old-commit", Name: "implementation", WorkflowRunID: "run", TaskID: "p", AttemptID: "pa-old", Kind: domain.ArtifactOutput}},
		}
		d.Tasks[1].Task.DependencyInputs = nil
		d.Tasks[1].Task.CarriedInputs = []domain.CarriedInput{{SourceRunID: "run", ProducerTaskID: "p", SourceAttemptID: "pa-old", SourceArtifactID: "old-commit", ArtifactID: "carried-commit", Name: "implementation"}}
		return d, e
	}
	oldCommit := strings.Repeat("c", 40)
	cli.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		if id == "old-commit" || id == "carried-commit" {
			raw, e := json.Marshal(backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run", TaskID: "p", Name: "implementation", Repository: "project", Base: strings.Repeat("a", 40), Commit: oldCommit, Ref: backlog.CampaignRef("run", "p", "implementation")})
			if e != nil {
				t.Fatal(e)
			}
			return backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewReader(raw))}, nil
		}
		return oldOpen(ctx, id)
	}
	r, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if e == nil {
		t.Fatalf("stale carried source must be refused instead of resolving newer commit: %+v", r.Commit)
	}
	if !strings.Contains(e.Error(), "reviewed commit source changed") {
		t.Fatal(e)
	}
}
