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

func TestCampaignGroupAndVerboseScopes(t *testing.T) {
	submit, err := parseCampaignArgs("submit", []string{"dir", "--idempotency-key", "key", "--group", "wave-1"}, false, true)
	if err != nil || submit.group != "wave-1" {
		t.Fatalf("submit group: %+v, %v", submit, err)
	}
	registered, err := parseCampaignArgs("submit", []string{"dir", "--idempotency-key", "key", "--register-only"}, false, true)
	if err != nil || !registered.registerOnly {
		t.Fatalf("submit register-only: %+v, %v", registered, err)
	}
	check, err := parseCampaignArgs("check", []string{"dir", "--verbose"}, false, false)
	if err != nil || !check.verbose {
		t.Fatalf("check verbose: %+v, %v", check, err)
	}
	for _, command := range []string{"validate", "plan", "check"} {
		args := []string{"dir", "--group", "wave-1"}
		if command == "check" {
			args = append(args, "--verbose")
		}
		if _, err := parseCampaignArgs(command, args, false, false); err == nil {
			t.Fatalf("accepted group on %s", command)
		}
	}
	if _, err := parseCampaignArgs("submit", []string{"dir", "--idempotency-key", "key", "--group", "wave-1", "--verbose"}, false, true); err == nil {
		t.Fatal("accepted verbose on grouped submit")
	}
}
