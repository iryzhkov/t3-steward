package main

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"sort"
	"strings"
	"time"
)

// selectPolicyRoute preserves the CLI's original selection and override semantics.
func selectPolicyRoute(p *routePolicy, role, model, effort, worker string, project backlogadmin.Project, accept func(string) bool) (policySelection, error) {
	selection, _, err := selectPolicyRouteDetailed(p, role, model, effort, worker, project, accept)
	return selection, err
}

// Each candidate uses the same eligibility calculation as an ordinary CLI call.
func selectPolicyRouteDetailed(p *routePolicy, role, model, effort, worker string, project backlogadmin.Project, accept func(string) bool) (policySelection, []domain.RoleCandidateVerdict, error) {
	if model != "" || role == "" {
		s, e := selectPolicyRouteCandidate(p, role, model, effort, worker, project, accept)
		return s, nil, e
	}
	var rr *policyRole
	for i := range p.Roles {
		if p.Roles[i].Name == role {
			rr = &p.Roles[i]
			break
		}
	}
	if rr == nil {
		s, e := selectPolicyRouteCandidate(p, role, model, effort, worker, project, accept)
		return s, nil, e
	}
	winner, selectionErr := selectPolicyRouteCandidate(p, role, model, effort, worker, project, accept)
	var verdicts []domain.RoleCandidateVerdict
	for _, candidate := range rr.Candidates {
		one := *p
		one.Roles = []policyRole{*rr}
		one.Roles[0].Candidates = []policyCandidate{candidate}
		_, err := selectPolicyRouteCandidate(&one, role, "", effort, worker, project, accept)
		v := domain.RoleCandidateVerdict{Route: candidate.Route, Eligible: err == nil}
		if err != nil {
			v.Reason = campaignPolicyCandidateReason(candidate, *rr, project, worker, accept, err)
		}
		verdicts = append(verdicts, v)
	}
	return winner, verdicts, selectionErr
}

type coordinatorRoleResolver struct {
	PolicyPath string
	Now        func() time.Time
}

func (r coordinatorRoleResolver) Resolve(ctx context.Context, tasks []backlogadmin.ViabilityTask, projects []backlogadmin.Project, eligible backlogadmin.RoleWorkerEligible) (map[string]domain.RoleSelection, map[string]backlogadmin.ViabilityReason) {
	return r.resolve(ctx, tasks, projects, eligible, nil)
}

func (r coordinatorRoleResolver) ResolveWithQuota(ctx context.Context, tasks []backlogadmin.ViabilityTask, projects []backlogadmin.Project, eligible backlogadmin.RoleWorkerEligible, snapshot backlogadmin.RoleQuotaSnapshot) (map[string]domain.RoleSelection, map[string]backlogadmin.ViabilityReason) {
	return r.resolve(ctx, tasks, projects, eligible, []backlogadmin.RoleQuotaSnapshot{snapshot})
}

func (r coordinatorRoleResolver) resolve(ctx context.Context, tasks []backlogadmin.ViabilityTask, projects []backlogadmin.Project, eligible backlogadmin.RoleWorkerEligible, snapshots []backlogadmin.RoleQuotaSnapshot) (map[string]domain.RoleSelection, map[string]backlogadmin.ViabilityReason) {
	_ = ctx
	hasRoles := false
	for _, task := range tasks {
		hasRoles = hasRoles || task.Role != ""
	}
	if !hasRoles {
		return nil, nil
	}
	path := r.PolicyPath
	if path == "" {
		path = defaultRoutePolicyPath()
	}
	raw, err := os.ReadFile(path)
	var policy *routePolicy
	if err == nil {
		policy, err = parseRoutePolicy(raw)
	}
	if err != nil {
		failures := map[string]backlogadmin.ViabilityReason{}
		for _, task := range tasks {
			if task.Role != "" {
				failures[task.Name] = campaignRoleReason("route-policy-unavailable", fmt.Sprintf("route policy unavailable on the coordinator: %s: %v", path, err))
			}
		}
		return nil, failures
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	return resolveCampaignPolicy(policy, tasks, projects, eligible, now, snapshots...)
}
func campaignRoleReason(code, detail string) backlogadmin.ViabilityReason {
	return backlogadmin.ViabilityReason{Code: code, Detail: detail, Permanent: true}
}
func resolveCampaignPolicy(p *routePolicy, tasks []backlogadmin.ViabilityTask, projects []backlogadmin.Project, eligible backlogadmin.RoleWorkerEligible, now time.Time, snapshots ...backlogadmin.RoleQuotaSnapshot) (map[string]domain.RoleSelection, map[string]backlogadmin.ViabilityReason) {
	selections := map[string]domain.RoleSelection{}
	failures := map[string]backlogadmin.ViabilityReason{}
	var snapshot *backlogadmin.RoleQuotaSnapshot
	if len(snapshots) > 0 {
		snapshot = &snapshots[0]
		now = snapshot.Now
	}
	selectRoute := func(task backlogadmin.ViabilityTask, project backlogadmin.Project, accept func(string) bool) (policySelection, []domain.RoleCandidateVerdict, error) {
		if snapshot == nil {
			return selectPolicyRouteDetailed(p, task.Role, "", "", "", project, accept)
		}
		ranked, err := selectPolicyRouteRanked(p, task.Role, "", "", "", project, accept, routeRankView{Now: now, Pools: snapshot.Pools})
		verdicts := make([]domain.RoleCandidateVerdict, 0, len(ranked.Candidates))
		for _, candidate := range ranked.Candidates {
			verdicts = append(verdicts, domain.RoleCandidateVerdict{Route: candidate.Route, Ordinal: candidate.Ordinal, Eligible: candidate.Eligible, Band: candidate.Band, Pool: candidate.Pool, Reason: candidate.Reason})
		}
		return ranked, verdicts, err
	}
	byName := map[string]backlogadmin.ViabilityTask{}
	for _, t := range tasks {
		byName[t.Name] = t
	}
	state := map[string]int{}
	var visit func(string)
	visit = func(name string) {
		if state[name] == 2 {
			return
		}
		task, exists := byName[name]
		if !exists {
			return
		}
		if state[name] == 1 {
			failures[name] = campaignRoleReason("role-no-eligible-candidate", "cyclic role dependencies; fix local needs")
			return
		}
		state[name] = 1
		producers := task.Producers
		if len(producers) == 0 {
			producers = task.Needs
		}
		localProducers := make([]string, 0, len(producers))
		for _, producer := range producers {
			if !strings.Contains(producer, "/") {
				localProducers = append(localProducers, producer)
				visit(producer)
			}
		}
		producers = localProducers
		state[name] = 2
		if task.Role == "" {
			return
		}
		var rr *policyRole
		var names []string
		for i := range p.Roles {
			names = append(names, p.Roles[i].Name)
			if p.Roles[i].Name == task.Role {
				rr = &p.Roles[i]
			}
		}
		if rr == nil {
			sort.Strings(names)
			failures[name] = campaignRoleReason("unknown-role", fmt.Sprintf("role %s is not in the coordinator's route policy %s; roles: %s; choose a listed role or update the coordinator policy", task.Role, p.Digest, strings.Join(names, ", ")))
			return
		}
		project := backlogadmin.Project{Name: task.Project}
		for _, candidate := range projects {
			if candidate.Name == task.Project {
				project = candidate
				break
			}
		}
		filtered := func(ready bool) backlogadmin.Project {
			result := project
			result.Workers = nil
			for _, w := range project.Workers {
				if !w.Advertises || (!ready && !w.Enrolled) || (ready && !w.Ready) {
					continue
				}
				if len(task.Hosts) > 0 && !contains(task.Hosts, w.Worker) {
					continue
				}
				if eligible != nil && !eligible(task, w.Worker, ready) {
					continue
				}
				w.Ready = true
				result.Workers = append(result.Workers, w)
			}
			return result
		}
		live := filtered(true)
		first, verdicts, err := selectRoute(task, live, nil)
		waiting := false
		if err != nil {
			live = filtered(false)
			first, verdicts, err = selectRoute(task, live, nil)
			waiting = err == nil
		}
		if err != nil {
			var reasons []string
			for _, v := range verdicts {
				reasons = append(reasons, v.Route+": "+v.Reason)
			}
			failures[name] = campaignRoleReason("role-no-eligible-candidate", err.Error()+"; "+strings.Join(reasons, "; "))
			return
		}
		reviewType := task.ReviewType || task.Role == "review" || task.Role == "critical-review"
		diversity := domain.RoleDiversity{}
		if reviewType && len(producers) > 0 {
			known := map[string]bool{}
			for _, producer := range producers {
				if strings.Contains(producer, "/") {
					continue
				}
				pt, ok := byName[producer]
				if !ok {
					continue
				}
				pair := ""
				if selection, ok := selections[producer]; ok {
					pair = selection.Route
				} else if pt.Role == "" && len(pt.Routes) > 0 {
					pair = pt.Routes[0].ProviderInstanceID + "/" + pt.Routes[0].Model
				}
				family := campaignRouteFamily(projects, pt.Project, pair)
				if family != "" {
					known[family] = true
				}
			}
			for family := range known {
				diversity.ProducerFamilies = append(diversity.ProducerFamilies, family)
			}
			sort.Strings(diversity.ProducerFamilies)
			diversity.Reason = "not applied: producer provider unknown"
			if len(known) > 0 {
				diversity.Reason = "fallback: no eligible candidate outside " + strings.Join(diversity.ProducerFamilies, ", ")
				if snapshot != nil {
					diversity.Reason = "fallback: no usable candidate in the winning quota band outside " + strings.Join(diversity.ProducerFamilies, ", ")
				}
				for _, verdict := range verdicts {
					if !verdict.Eligible {
						continue
					}
					if snapshot != nil && !campaignDiversityUsable(verdict, first, *snapshot) {
						continue
					}
					family := campaignRouteFamily(projects, task.Project, verdict.Route)
					if family == "" || known[family] {
						continue
					}
					chosen, _, e := selectRoute(task, live, func(pair string) bool { return pair == verdict.Route })
					if e == nil {
						first = chosen
						diversity.CrossProvider = true
						diversity.Reason = "cross-provider: " + family + " differs from producer providers " + strings.Join(diversity.ProducerFamilies, ", ")
						break
					}
				}
			}
		}
		if task.RoleEffort != "" {
			levels := map[string]int{"low": 1, "medium": 2, "high": 3}
			if err := validPolicyEffort(task.RoleEffort); err != nil || levels[task.RoleEffort] > levels[first.Effort] {
				failures[name] = campaignRoleReason("role-effort-raised", fmt.Sprintf("effort override %s exceeds policy effort %s for route %s; lower the override", task.RoleEffort, first.Effort, first.Route))
				return
			}
			first.Effort = task.RoleEffort
		}
		if waiting {
			if snapshot == nil {
				first.Reason = "no ready worker now; first advertised candidate selected, the run waits for capacity"
			} else {
				first.Reason += "; no ready worker now; ranked advertised candidate selected, the run waits for capacity"
			}
		}
		selections[name] = domain.RoleSelection{Role: task.Role, Route: first.Route, Effort: first.Effort, PolicyDigest: p.Digest, Reason: first.Reason, Ranking: first.Ranking, Candidates: verdicts, Diversity: diversity, ResolvedAt: now}
	}
	for _, t := range tasks {
		visit(t.Name)
	}
	return selections, failures
}
func campaignRouteFamily(projects []backlogadmin.Project, project, pair string) string {
	if pair == "" {
		return ""
	}
	family := ""
	for _, p := range projects {
		if p.Name != project {
			continue
		}
		for _, w := range p.Workers {
			for _, r := range w.Routes {
				if r.Instance+"/"+r.Model == pair && r.ProviderFamily != "" {
					if family != "" && family != r.ProviderFamily {
						return ""
					}
					family = r.ProviderFamily
				}
			}
		}
	}
	return family
}
