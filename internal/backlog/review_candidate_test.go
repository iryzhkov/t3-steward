package backlog

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/review"
)

func TestReviewCandidateManifestFences(t *testing.T) {
	head, base := strings.Repeat("a", 40), strings.Repeat("b", 40)
	makeManifest := func(mode string) Manifest {
		m := Manifest{PinnedInputs: true, Inputs: []string{"candidate.bundle"},
			Environment: ManifestEnvironment{Type: EnvironmentGit, Ref: head},
			Review:      &review.Round{CandidateMode: mode, HeadCommit: head}}
		if mode == "bundle" {
			m.Review.BundleInput = "candidate.bundle"
			m.Review.BaseCommit = base
			m.Environment.Ref = base
		}
		return m
	}
	for _, mode := range []string{"commit", "bundle"} {
		m := makeManifest(mode)
		if err := validateReviewCandidateManifest(m); err != nil {
			t.Fatal(mode, err)
		}
	}
	m := makeManifest("bundle")
	m.Review.BaseCommit = ""
	if err := validateReviewCandidateManifest(m); err != nil {
		t.Fatal("self-contained bundle", err)
	}
	for _, tc := range []struct {
		name, mode string
		mutate     func(*Manifest)
	}{
		{"unknown", "unexpected", func(m *Manifest) {}},
		{"commit-head", "commit", func(m *Manifest) { m.Review.HeadCommit = "HEAD" }},
		{"commit-ref", "commit", func(m *Manifest) { m.Environment.Ref = base }},
		{"commit-base", "commit", func(m *Manifest) { m.Review.BaseCommit = "HEAD" }},
		{"commit-bundle", "commit", func(m *Manifest) { m.Review.BundleInput = "candidate.bundle" }},
		{"bundle-head", "bundle", func(m *Manifest) { m.Review.HeadCommit = "HEAD" }},
		{"bundle-base", "bundle", func(m *Manifest) { m.Review.BaseCommit = "HEAD" }},
		{"bundle-ref", "bundle", func(m *Manifest) { m.Environment.Ref = head }},
		{"bundle-missing", "bundle", func(m *Manifest) { m.Inputs = nil }},
		{"bundle-unpinned", "bundle", func(m *Manifest) { m.PinnedInputs = false }},
		{"bundle-path", "bundle", func(m *Manifest) { m.Review.BundleInput = "../candidate.bundle" }},
		{"fresh", "bundle", func(m *Manifest) { m.Environment.Type = EnvironmentFresh }},
		{"legacy-bundle", "", func(m *Manifest) { m.Review.BundleInput = "candidate.bundle" }},
		{"legacy-no-base", "", func(m *Manifest) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := makeManifest(tc.mode)
			tc.mutate(&m)
			if err := validateReviewCandidateManifest(m); err == nil {
				t.Fatal("accepted invalid candidate")
			}
		})
	}
	// Preserve the original diff form.
	m = makeManifest("")
	m.Review.BaseCommit = base
	if err := validateReviewCandidateManifest(m); err != nil {
		t.Fatal(err)
	}
}
