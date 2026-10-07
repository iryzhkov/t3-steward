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
}

// Observations are fleet-wide, even when a project advertises on only one
// worker. Pool membership and completeness remain owned by M17-1.
func buildRouteRankView(workers []backlogadmin.Worker, quotas []backlogadmin.Quota, staleAfter time.Duration) routeRankView {
	snapshots := make([]domain.WorkerSnapshot, 0, len(workers))
	for _, w := range workers {
		snapshots = append(snapshots, w.Snapshot)
	}
	states := domain.MergeQuotaObservations(nil, snapshots)
	view := routeRankView{Now: modelsNow(), Pools: map[string]domain.RouteRankPool{}}
	for _, q := range quotas {
		admission := q.Pool.Admission
		if q.Admission != nil {
			admission = q.Admission.Admission
		}
		view.Pools[q.Pool.ID] = domain.RouteRankPool{ID: q.Pool.ID, Admission: admission, Windows: domain.ReadQuotaWindows(q.Pool, states, view.Now, staleAfter)}
	}
	return view
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
func (c taskRunCLI) routeRankingView(ctx context.Context, workers []backlogadmin.Worker) routeRankView {
	if c.query == nil {
		return routeRankView{}
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuota})
	if err != nil {
		return routeRankView{}
	}
	return buildRouteRankView(workers, response.Quotas, c.quotaStaleAfter)
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
		if v.err != nil {
			return policySelection{}, v.err
		}
		receipt = append(receipt, policyRankCandidate{Route: v.Route, Ordinal: v.Ordinal, Eligible: v.Eligible, Reason: v.Reason})
		if !v.Eligible {
			continue
		}
		candidate := domain.RouteRankCandidate{Route: v.Route, Ordinal: v.Ordinal}
		for _, id := range v.Pools {
			if pool, ok := view.Pools[id]; ok && id != "" {
				candidate.Pools = append(candidate.Pools, pool)
			} else {
				candidate.Pools = append(candidate.Pools, domain.RouteRankPool{ID: id, Windows: domain.QuotaWindowSet{Unknown: true}})
			}
		}
		input.Candidates = append(input.Candidates, candidate)
		eligible[v.Route] = v
	}
	if len(input.Candidates) == 0 {
		return policySelection{}, noEligiblePolicyRoute(role, model, project)
	}
	ranked, err := domain.RankRoutes(input)
	if err != nil {
		return policySelection{}, err
	}
	for _, entry := range ranked {
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
func renderPolicySelection(w io.Writer, s policySelection) {
	fmt.Fprintf(w, "role %s\npolicy %s\nreason %s\neffort %s\n", s.Role, s.PolicyDigest, s.Reason, s.Effort)
	if s.Ranking != "" {
		fmt.Fprintf(w, "ranking %s\nchosen %s\n", s.Ranking, s.Route)
		for _, c := range s.Candidates {
			fmt.Fprintf(w, "candidate %s eligible=%t %s\n", c.Route, c.Eligible, strings.TrimSpace(c.Reason))
		}
	}
}
