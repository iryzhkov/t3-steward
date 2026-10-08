package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewCandidateBundleNonCommitHead(t *testing.T) {
	f := newCandidateFixture(t)
	candidateGit(t, "tag", "-am", "annotated", "review-tag", f.head)
	path := filepath.Join(f.root, "tag.bundle")
	candidateGit(t, "bundle", "create", path, "refs/tags/review-tag", "^"+f.base)
	dir, err := candidateBuild(t, f, "--bundle", path)
	if err == nil {
		os.RemoveAll(dir)
		t.Fatal("accepted tag object as round head commit")
	}
	if !strings.Contains(err.Error(), "head must name a commit") {
		t.Fatal(err)
	}
}
func TestReviewCandidateBundleLocalDefaultSpoof(t *testing.T) {
	f := newCandidateFixture(t)
	f.catalog[0].DefaultRef = "main"
	candidateGit(t, "branch", "main", f.base)
	candidateGit(t, "checkout", "--orphan", "foreign")
	candidateGit(t, "rm", "-f", "code.txt")
	candidateGit(t, "commit", "--allow-empty", "-qm", "foreign")
	foreign := candidateGit(t, "rev-parse", "HEAD")
	candidateGit(t, "branch", "-f", "main", foreign)
	path := filepath.Join(f.root, "foreign.bundle")
	candidateGit(t, "bundle", "create", path, "HEAD")
	dir, err := candidateBuild(t, f, "--bundle", path)
	if err == nil {
		os.RemoveAll(dir)
		t.Fatal("local default spoof accepted")
	}
	if !strings.Contains(err.Error(), "unrelated") {
		t.Fatal(err)
	}
}
