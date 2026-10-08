package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"strings"
	"testing"
)

func TestCampaignFixMissingLineageFields(t *testing.T) {
	for _, raw := range []string{
		`{"roundsDeclared":2}`,
		`{"schema":null,"rootRun":null,"roundLimit":null,"roundsDeclared":2}`,
	} {
		t.Run(raw, func(t *testing.T) {
			cli := fixLineageFixture(t, 0, false)
			old := cli.open
			cli.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
				if id == "lineage" {
					return backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader(raw))}, nil
				}
				return old(ctx, id)
			}
			r, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
			if e == nil {
				t.Fatalf("accepted malformed lineage with synthesized root=%q limit=%d schema=%q", r.Options.Lineage.RootRun, r.Options.Lineage.RoundLimit, r.Options.Lineage.Schema)
			}
		})
	}
}
