package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestCampaignFixExhaustionIdentifiesLatestReview(t *testing.T) {
	cli := fixLineageFixture(t, 2, false)
	_, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if err == nil || !strings.Contains(err.Error(), "latest review run/review requested changes") {
		t.Fatalf("latest review identity omitted: %v", err)
	}
}
