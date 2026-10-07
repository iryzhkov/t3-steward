package main

import (
	"fmt"
	"io"
	"os"

	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

// annotateCampaignLocalRoles adds advisory authoring evidence, never authority.
// It performs no coordinator query and does not read a policy for explicit routes.
func annotateCampaignLocalRoles(plan *campaign.Plan, path string, warnings io.Writer) {
	hasRole := plan.Role != ""
	for _, task := range plan.Tasks {
		hasRole = hasRole || task.Role != ""
	}
	if !hasRole || path == "" {
		return
	}
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, pinnedinput.MaxFileBytes+1))
	if err != nil {
		return
	}
	policy, err := parseRoutePolicy(raw)
	if err != nil {
		return
	}
	roles := make(map[string]policyRole, len(policy.Roles))
	for _, role := range policy.Roles {
		roles[role.Name] = role
	}
	warned := make(map[string]bool)
	warn := func(name string) {
		if name == "" || warned[name] {
			return
		}
		warned[name] = true
		if warnings != nil {
			fmt.Fprintf(warnings, "warning: role %s is absent from local policy %s; the coordinator's route policy is authoritative\n", name, path)
		}
	}
	if _, ok := roles[plan.Role]; !ok {
		warn(plan.Role)
	}
	for i := range plan.Tasks {
		task := &plan.Tasks[i]
		if task.Role == "" {
			continue
		}
		role, ok := roles[task.Role]
		if !ok {
			warn(task.Role)
			continue
		}
		for _, candidate := range role.Candidates {
			task.LocalPolicyCandidates = append(task.LocalPolicyCandidates, fmt.Sprintf("%s (effort %s)", candidate.Route, candidate.Effort))
		}
	}
}
