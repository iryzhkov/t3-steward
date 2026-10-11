package backlog

import (
	"context"
	"fmt"
	"strings"
)

// verifyExportAncestry checks the retained pack without the catalog objects.
// Traversal stops at advertised prerequisites, whose objects live at the consumer.
func verifyExportAncestry(ctx context.Context, repo string, p CommitProvenance, prerequisites []string) error {
	stops := map[string]bool{}
	for _, id := range prerequisites {
		stops[id] = true
	}
	walk := func(start string, boundary map[string]bool) (map[string]bool, error) {
		seen := map[string]bool{}
		pending := []string{start}
		for len(pending) > 0 {
			id := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if seen[id] {
				continue
			}
			seen[id] = true
			if boundary[id] {
				continue
			}
			raw, err := exportGit(ctx, "--git-dir", repo, "cat-file", "commit", id).Output()
			if err != nil {
				return nil, fmt.Errorf("commit export: incomplete retained ancestry at %s: %w", id, err)
			}
			for _, line := range strings.Split(string(raw), "\n") {
				if line == "" {
					break
				}
				if parent, ok := strings.CutPrefix(line, "parent "); ok {
					if !validGitObjectID(parent) {
						return nil, fmt.Errorf("commit export: malformed parent %q", parent)
					}
					pending = append(pending, parent)
				}
			}
		}
		return seen, nil
	}
	// The declared base must be reachable from the exported head.
	headStops := map[string]bool{p.Base: true}
	for id := range stops {
		headStops[id] = true
	}
	headAncestors, err := walk(p.Commit, headStops)
	if err != nil {
		return err
	}
	if !headAncestors[p.Base] {
		return fmt.Errorf("commit export: declared commit does not descend from declared base")
	}
	baseAncestors, err := walk(p.Base, stops)
	if err != nil {
		return err
	}
	for _, id := range prerequisites {
		if !baseAncestors[id] {
			return fmt.Errorf("commit export: prerequisite %s is not an ancestor of declared base", id)
		}
	}
	return nil
}
