package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"strings"
	"testing"
)

func TestCampaignGroupArgumentsAndFixInheritance(t *testing.T) {
	for _, group := range []string{"wave-1", strings.Repeat("a", 64), "BAD", "a--b", "-a", "a-", strings.Repeat("a", 65), ""} {
		valid := group == "wave-1" || len(group) == 64
		_, submitErr := parseCampaignArgs("submit", []string{"dir", "--idempotency-key", "key", "--group", group}, false, true)
		_, fixErr := parseCampaignFixArgs([]string{"run/review", "--idempotency-key", "key", "--group", group})
		_, rerunErr := parseCampaignRerunArgs([]string{"run", "--from", "task", "--idempotency-key", "key", "--group", group})
		_, listErr := parseWorkflowFilters([]string{"--group", group})
		_, _, progressErr := parseProgressArgs([]string{"--group", group})
		for _, err := range []error{submitErr, fixErr, rerunErr, listErr, progressErr} {
			if (err == nil) != valid {
				t.Fatalf("group %q: %v", group, err)
			}
		}
	}
	for _, override := range []string{"", "wave-2"} {
		cli, _ := fixCommandFixture(t)
		original := cli.detail
		cli.detail = func(ctx context.Context, run string) (backlogadmin.WorkflowDetail, error) {
			d, err := original(ctx, run)
			d.Summary.Run.Group = "wave-1"
			return d, err
		}
		a, _ := parseCampaignFixArgs([]string{"run/review", "--idempotency-key", "key"})
		a.group = override
		resolved, err := cli.resolve(context.Background(), a)
		want := "wave-1"
		if override != "" {
			want = override
		}
		if err != nil || resolved.Group != want {
			t.Fatalf("fix group: %s %v", resolved.Group, err)
		}
	}
}
