package main

import (
	"strings"
	"testing"
)

func TestReviewCandidateExplicitEmptyFlags(t *testing.T) {
	for _, flag := range []string{"commit", "bundle", "base"} {
		if _, err := parseReviewArgs([]string{"--project", "scratch", "--" + flag + "="}); err == nil || !strings.Contains(err.Error(), "--"+flag) {
			t.Fatalf("empty %s silently accepted: %v", flag, err)
		}
	}
	for _, mode := range []string{"--bundle=", "--diff=", "--diff-file="} {
		if _, err := parseReviewArgs([]string{"--project", "scratch", "--commit=HEAD", mode}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("empty mode erases exclusion: %s %v", mode, err)
		}
	}
}
