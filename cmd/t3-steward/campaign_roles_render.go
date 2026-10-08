package main

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
)

func renderRoleSelection(out io.Writer, selection *domain.RoleSelection) {
	if selection == nil {
		return
	}
	fmt.Fprintf(out, "    role: %s; selected route: %s; effort: %s; policy digest: %s\n    %s\n", selection.Role, selection.Route, selection.Effort, selection.PolicyDigest, selection.Reason)
	for _, candidate := range selection.Candidates {
		if !candidate.Eligible {
			fmt.Fprintf(out, "    candidate %s: %s\n", candidate.Route, candidate.Reason)
		}
	}
	if selection.Diversity.Reason != "" {
		fmt.Fprintf(out, "    diversity: %s\n", selection.Diversity.Reason)
	}
}
