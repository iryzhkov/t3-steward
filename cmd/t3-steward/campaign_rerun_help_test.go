package main

import (
	"strings"
	"testing"
)

func TestCampaignRerunUseCommitCommandHelp(t *testing.T) {
	for _, mode := range []string{"", "full"} {
		args := []string{"campaign", "rerun", "--help"}
		if mode != "" {
			args = append(args, mode)
		}
		out, errOut := probeHelp(t, args)
		if errOut != "" {
			t.Fatalf("help stderr: %s", errOut)
		}
		for _, fragment := range []string{"--use-commit", "verification", "failed attempt"} {
			if !strings.Contains(out, fragment) {
				t.Fatalf("%s help missing %q: %s", mode, fragment, out)
			}
		}
	}
}
