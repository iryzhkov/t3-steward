package main

import (
	"bytes"
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCampaignFixGateOverridesAndMissing(t *testing.T) {
	cli := fixLineageFixture(t, 0, false)
	for _, tc := range []struct {
		a    campaignFixArgs
		want string
	}{{campaignFixArgs{noGate: true}, "no-gate"}, {campaignFixArgs{gate: []string{"custom"}, noGate: true}, "flag"}} {
		tc.a.node = domain.NodeRef{RunID: "run", TaskID: "review"}
		r, e := cli.resolve(context.Background(), tc.a)
		if e != nil || r.Gate.Source != tc.want {
			t.Fatalf("%+v %v", r.Gate, e)
		}
	}
	cli, _ = fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := old(ctx, s)
		d.Tasks[0].Task.Gate = nil
		return d, e
	}
	if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil || !strings.Contains(e.Error(), "no gate is resolvable") {
		t.Fatal(e)
	}
}
func TestCampaignFixRejectsFreshAndFailedProducer(t *testing.T) {
	for _, mode := range []string{"fresh", "failed"} {
		t.Run(mode, func(t *testing.T) {
			cli, _ := fixCommandFixture(t)
			old := cli.detail
			cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
				d, e := old(ctx, s)
				if mode == "fresh" {
					d.Summary.Workflow.Environment.Type = "fresh"
				} else {
					d.Tasks[0].Attempt.Progress = domain.ProgressFailed
				}
				return d, e
			}
			if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil {
				t.Fatal("accepted")
			}
		})
	}
}
func TestCampaignFixContextChecksBeforeCoordinator(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "local.md")
	if e := os.WriteFile(file, []byte("context"), 0600); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(root, "link.md")
	if e := os.Symlink(file, link); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{link, root, filepath.Join(root, "missing.md")} {
		cli, _ := fixCommandFixture(t)
		cli.detail = func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			t.Fatal("coordinator reached")
			return backlogadmin.WorkflowDetail{}, nil
		}
		if e := cli.run(context.Background(), []string{"run/review", "--idempotency-key", "key", "--context", name}); e == nil {
			t.Fatal("accepted")
		}
	}
}
func TestCampaignFixOutIsArbitraryDirectoryAndRefusesExisting(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	target := filepath.Join(t.TempDir(), "Fix campaign.DIR")
	if e := cli.run(context.Background(), []string{"run/review", "--idempotency-key", "key", "--out", target, "--dry-run"}); e != nil {
		t.Fatal(e)
	}
	cli.detail = func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
		t.Fatal("coordinator reached")
		return backlogadmin.WorkflowDetail{}, nil
	}
	if e := cli.run(context.Background(), []string{"run/review", "--idempotency-key", "key", "--out", target, "--dry-run"}); e == nil {
		t.Fatal("overwrote")
	}
}
func TestCampaignFixMissingFallbackArtifactRefused(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := old(ctx, s)
		d.Tasks[1].Attempt.ReviewVerdict = nil
		d.Tasks[1].Artifacts = nil
		return d, e
	}
	if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil {
		t.Fatal("accepted")
	}
}
func TestCampaignFixRejectsCorruptLineage(t *testing.T) {
	for _, raw := range []string{`{"schema":"steward.fix-lineage/v1","roundLimit":4,"roundsUsedBefore":-1,"roundsDeclared":2}`, `{"schema":"steward.fix-lineage/v1","roundLimit":4,"roundsUsedBefore":0.5,"roundsDeclared":2}`, `{"schema":"unknown"}`, `{"schema":"steward.fix-lineage/v1"} {}`} {
		cli := fixLineageFixture(t, 0, false)
		old := cli.open
		cli.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
			if id == "lineage" {
				return backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewBufferString(raw))}, nil
			}
			return old(ctx, id)
		}
		if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
