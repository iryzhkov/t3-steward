package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewCandidateUnpublishedPrerequisites(t *testing.T) {
	for _, multiple := range []bool{false, true} {
		t.Run(map[bool]string{false: "environment", true: "second-prerequisite"}[multiple], func(t *testing.T) {
			f := newCandidateFixture(t)
			path := filepath.Join(f.root, "unreachable.bundle")
			if !multiple {
				candidateGit(t, "commit", "--allow-empty", "-qm", "child")
				candidateGit(t, "bundle", "create", path, "HEAD", "^"+f.head)
			} else {
				candidateGit(t, "checkout", "-qb", "unpublished", f.base)
				candidateGit(t, "commit", "--allow-empty", "-qm", "unpublished")
				other := candidateGit(t, "rev-parse", "HEAD")
				candidateGit(t, "checkout", "--detach", f.head)
				candidateGit(t, "merge", "--no-ff", "-qm", "merge", other)
				candidateGit(t, "bundle", "create", path, "HEAD", "^"+f.head, "^"+other)
				headers := candidateGit(t, "bundle", "list-heads", path)
				if headers == "" {
					t.Fatal("missing head")
				}
				// Publish precisely the first prerequisite, so worker preparation could
				// succeed while the second prerequisite is still unavailable.
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				first := ""
				for _, line := range strings.Split(string(raw), "\n") {
					if strings.HasPrefix(line, "-") {
						first = strings.Fields(line[1:])[0]
						break
					}
				}
				if first == "" {
					t.Fatal("missing prerequisite")
				}
				candidateGit(t, "push", "-q", "origin", first+":refs/heads/available")
			}
			dir, err := candidateBuild(t, f, "--bundle", path)
			if err == nil {
				os.RemoveAll(dir)
				t.Fatal("accepted prerequisite absent from project remote")
			}
			if !strings.Contains(err.Error(), "prerequisite") || !strings.Contains(err.Error(), "project remote") {
				t.Fatal("unclear prerequisite refusal", err)
			}
		})
	}
}
