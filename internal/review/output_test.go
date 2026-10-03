package review

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func outputRound() Round {
	return Round{ID: "round-1", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []Reviewer{
		{ID: "b", Route: "codex/sol", Role: "review", Required: true, State: "succeeded", Verdict: &Verdict{Verdict: "reject", Findings: []Finding{
			{ID: "L", Severity: "low", Title: "Low", Evidence: []string{"a.go:1"}},
			{ID: "H", Severity: "high", Blocking: true, Title: "High", Evidence: []string{"artifact:abc"}},
		}}},
		{ID: "a", Route: "claude/opus", Role: "review", Required: true, State: "succeeded", Verdict: &Verdict{Verdict: "accept-with-changes", Findings: []Finding{{ID: "M", Severity: "medium", Title: "Medium", Evidence: []string{"a.go:2"}}}}},
	}}
}
func TestOutputSortsAttributedFindingsAndRefusesSymlinks(t *testing.T) {
	base := t.TempDir()
	r := outputRound()
	reply, err := WriteOutput(base, r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(reply.SummaryPath)
	if err != nil {
		t.Fatal(err)
	}
	var summary Summary
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	if len(summary.Findings) != 3 || summary.Findings[0].ID != "H" || summary.Findings[1].Reviewer != "a" || summary.Findings[2].ID != "L" {
		t.Fatalf("%+v", summary.Findings)
	}
	external := filepath.Join(t.TempDir(), "untouched")
	if err := os.WriteFile(external, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(reply.SummaryPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, reply.SummaryPath); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteOutput(base, r); err == nil {
		t.Fatal("followed output link")
	}
	got, _ := os.ReadFile(external)
	if string(got) != "original" {
		t.Fatal("changed external file")
	}
}
func TestOutputNeverWritesIntoACheckout(t *testing.T) {
	checkout := t.TempDir()
	if err := os.Mkdir(filepath.Join(checkout, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteOutput(filepath.Join(checkout, "state", "results"), outputRound()); err == nil {
		t.Fatal("wrote results inside a checkout")
	}
}
