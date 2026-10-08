package main

import (
	"strings"
	"testing"
)

func TestCampaignFixHelpAdmission(t *testing.T) {
	for _, full := range []bool{false, true} {
		args := []string{"campaign", "fix", "run-source/review", "--help"}
		if full {
			args = append(args, "full")
		}
		var out strings.Builder
		handled, err := admitHelp(&out, nil, args)
		if !handled || err != nil {
			t.Fatalf("help full=%v: handled=%v err=%v", full, handled, err)
		}
		for _, want := range []string{"campaign fix", "--idempotency-key", "--round-limit", "--gate", "--out", "--dry-run"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("help full=%v missing %q: %s", full, want, out.String())
			}
		}
		if full {
			for _, want := range []string{"round_limit", "review-round-limit-exhausted", "docs/campaign-fix.md", "worker-owned", "recorded"} {
				if !strings.Contains(out.String(), want) {
					t.Errorf("full help missing %q", want)
				}
			}
		}
	}
}

func TestCampaignFixHelpFlagsAndFamilySynopsis(t *testing.T) {
	page, found := helpPageFor("campaign fix")
	if !found {
		t.Fatal("campaign fix help page missing")
	}
	flags := map[string]bool{}
	for _, flag := range page.Flags {
		flags[flag.Name] = true
	}
	for _, name := range []string{"--idempotency-key", "--round-limit", "--gate", "--gate-timeout", "--no-gate", "--commit", "--context", "--out", "--dry-run", "--json", "--notify-thread", "--no-notify"} {
		if !flags[name] {
			t.Errorf("help does not document %s", name)
		}
		if !strings.Contains(campaignFamilyBlock(t, "fix"), name) {
			t.Errorf("family synopsis does not document %s", name)
		}
	}
}
