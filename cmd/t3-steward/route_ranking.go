package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type policyRankCandidate struct {
	Route    string `json:"route"`
	Ordinal  int    `json:"ordinal"`
	Eligible bool   `json:"eligible"`
	Band     string `json:"band,omitempty"`
	Pool     string `json:"pool,omitempty"`
	Reason   string `json:"reason"`
}
type routeRankView struct {
	Now   time.Time
	Pools map[string]domain.RouteRankPool
	// PoolGateReasons explains task-class admission gates normalized to v1's gated band.
	PoolGateReasons map[string]string
	// Freshness reports each pool's reading state and age in every receipt.
	Freshness map[string]backlogadmin.QuotaPoolFreshness
	// Unavailable says why no quota view could be read at all.
	Unavailable string
}

// Observations are fleet-wide, even when a project advertises on only one
// worker. Pool membership and completeness remain owned by M17-1. The view is
// backlogadmin.BuildRoleQuotaSnapshot, the one the coordinator ranks campaign
// and schedule roles with.
func buildRouteRankView(workers []backlogadmin.Worker, quotas []backlogadmin.Quota, staleAfter time.Duration) backlogadmin.RoleQuotaSnapshot {
	return backlogadmin.RoleQuotaSnapshotFromAnswers(modelsNow().UTC(), staleAfter, quotas, workers)
}
func (c taskRunCLI) policyCatalogWorkers(ctx context.Context, p *routePolicy) ([]backlogadmin.Worker, error) {
	if c.query == nil {
		return nil, fmt.Errorf("configured route catalog unavailable; configure coordinator-client.json")
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
	if err != nil {
		return nil, fmt.Errorf("configured route catalog unavailable: %w; check t3-steward coordinator identity", err)
	}
	if err := validatePolicyCatalog(p, response.Workers); err != nil {
		return nil, err
	}
	return response.Workers, nil
}

// routeRankingView composes the class admission gate exactly as campaign role
// resolution does, so one snapshot ranks the same through both.
func (c taskRunCLI) routeRankingView(ctx context.Context, workers []backlogadmin.Worker, class domain.TaskClass) routeRankView {
	if c.query == nil {
		return routeRankView{Unavailable: "quota view unavailable: no coordinator query transport"}
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuota})
	if err != nil {
		return routeRankView{Unavailable: "quota view unavailable: " + err.Error()}
	}
	snapshot := buildRouteRankView(workers, response.Quotas, c.quotaStaleAfter)
	return campaignTaskRankView(backlogadmin.ViabilityTask{Class: class}, snapshot)
}
func selectPolicyRouteRanked(p *routePolicy, role, model, effort, worker string, project backlogadmin.Project, accept func(string) bool, view routeRankView) (policySelection, error) {
	if model != "" {
		return selectPolicyRoute(p, role, model, effort, worker, project, accept)
	}
	_, verdicts, err := evaluatePolicyCandidates(p, role, model, effort, worker, project, accept)
	if err != nil {
		return policySelection{}, err
	}
	input := domain.RouteRankInput{Version: domain.RouteRankingV1, Now: view.Now}
	receipt := make([]policyRankCandidate, 0, len(verdicts))
	eligible := map[string]policyCandidateVerdict{}
	for _, v := range verdicts {
		// Candidate-local errors remain in the verdict; they cannot prevent
		// another eligible route from ranking.
		receipt = append(receipt, policyRankCandidate{Route: v.Route, Ordinal: v.Ordinal, Eligible: v.Eligible, Reason: v.Reason})
		if !v.Eligible {
			continue
		}
		candidate := domain.RouteRankCandidate{Route: v.Route, Ordinal: v.Ordinal}
		for _, id := range v.Pools {
			if pool, ok := view.Pools[id]; ok && id != "" {
				candidate.Pools = append(candidate.Pools, pool)
			} else {
				candidate.Pools = append(candidate.Pools, domain.RouteRankPool{ID: id, Windows: domain.QuotaWindowSet{Pool: id, Unknown: true}})
			}
		}
		input.Candidates = append(input.Candidates, candidate)
		eligible[v.Route] = v
	}
	if len(input.Candidates) == 0 {
		return policySelection{Ranking: domain.RouteRankingV1, Candidates: receipt}, noEligiblePolicyRoute(role, model, project)
	}
	ranked, err := domain.RankRoutes(input)
	if err != nil {
		return policySelection{}, err
	}
	for index := range ranked {
		entry := &ranked[index]
		if reason := view.PoolGateReasons[entry.Pool]; entry.Band == "gated" && reason != "" {
			entry.Reason = domain.RouteRankingV1 + ": " + entry.Pool + " " + reason + " (gated)"
		}
		entry.Reason += "; " + view.freshness(entry.Pool)
		for i := range receipt {
			if receipt[i].Route == entry.Route {
				receipt[i].Band, receipt[i].Pool, receipt[i].Reason = entry.Band, entry.Pool, entry.Reason
			}
		}
	}
	chosen := ranked[0]
	reason := chosen.Reason
	// Show why a preferred route lost to the gate in the one-line choice.
	for _, entry := range ranked {
		if entry.Ordinal < chosen.Ordinal && entry.Band == "gated" {
			reason += "; " + entry.Reason
		}
	}
	return policySelection{Schema: "route-selection/v1", Role: role, Route: chosen.Route, PolicyDigest: p.Digest, Reason: reason, Effort: eligible[chosen.Route].Effort, Ranking: domain.RouteRankingV1, Candidates: receipt}, nil
}

// freshness is one pool's reading state and age, or why it has none; a pool is
// never unknown without the receipt saying so.
func (view routeRankView) freshness(pool string) string {
	if view.Unavailable != "" {
		return view.Unavailable
	}
	if pool == "" {
		return "no quota pool bound to this route"
	}
	if f, ok := view.Freshness[pool]; ok {
		return f.String()
	}
	return pool + " quota missing: the pool is not in the coordinator's quota view"
}

func renderPolicySelection(w io.Writer, s policySelection) {
	fmt.Fprintf(w, "role %s\npolicy %s\nreason %s\neffort %s\n", s.Role, s.PolicyDigest, s.Reason, s.Effort)
	if s.Ranking != "" {
		fmt.Fprintf(w, "ranking %s\nchosen %s\n", s.Ranking, s.Route)
		for _, c := range s.Candidates {
			fmt.Fprintf(w, "candidate %s eligible=%t %s\n", c.Route, c.Eligible, strings.TrimSpace(c.Reason))
		}
	}
}
