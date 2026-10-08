package main

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func roleRankingFixture() (*routePolicy, []backlogadmin.Project, []backlogadmin.ViabilityTask, backlogadmin.RoleQuotaSnapshot) {
	now := time.Unix(1000, 0).UTC()
	p := &routePolicy{Digest: "quota-digest", Roles: []policyRole{{Name: "execute", Candidates: []policyCandidate{{Route: "a/m", Tier: "standard", Effort: "medium"}, {Route: "b/m", Tier: "standard", Effort: "low"}}}, {Name: "review", Candidates: []policyCandidate{{Route: "a/m", Tier: "standard", Effort: "medium"}, {Route: "b/m", Tier: "standard", Effort: "low"}}}}}
	projects := []backlogadmin.Project{{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Enrolled: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{{Instance: "a", Model: "m", Tier: "executor", ProviderFamily: "anthropic", QuotaPool: "a"}, {Instance: "b", Model: "m", Tier: "executor", ProviderFamily: "openai", QuotaPool: "b"}}}}}}
	snapshot := backlogadmin.RoleQuotaSnapshot{Now: now, Pools: map[string]domain.RouteRankPool{}, ChecksDisabled: map[string]bool{}}
	for _, id := range []string{"a", "b"} {
		snapshot.Pools[id] = domain.RouteRankPool{ID: id, Admission: domain.AdmissionOpen, Windows: domain.QuotaWindowSet{Pool: id, Windows: []domain.QuotaWindowReading{{Window: "primary", UsedPercent: 10, ObservedAt: now}}}}
	}
	return p, projects, []backlogadmin.ViabilityTask{{Name: "t", Project: "p", Role: "execute", Class: domain.TaskClassRequired}}, snapshot
}

func TestCampaignQuotaRanking(t *testing.T) {
	for _, mode := range []string{"closed", "draining", "exhausted", "stale", "missing", "invalid", "unbound", "reset-soon", "waiting", "unauthorized", "disabled", "equal-band", "reversed", "tier", "placement", "no-eligible"} {
		t.Run(mode, func(t *testing.T) {
			p, projects, tasks, snapshot := roleRankingFixture()
			a, b := snapshot.Pools["a"], snapshot.Pools["b"]
			want := "b/m"
			switch mode {
			case "closed":
				a.Admission = domain.AdmissionClosed
			case "draining":
				a.Admission = domain.AdmissionDraining
			case "exhausted":
				a.Windows.Windows[0].UsedPercent = 100
				a.Windows.Windows[0].Exhausted = true
			case "stale":
				a.Windows.Windows[0].Stale = true
			case "missing":
				a.Windows.Windows = nil
			case "invalid":
				a.Windows.Windows[0].UsedPercent = -1
			case "unbound":
				delete(snapshot.Pools, "a")
			case "reset-soon":
				reset := snapshot.Now.Add(time.Hour)
				b.Windows.Windows[0].Window = "seven_day"
				b.Windows.Windows[0].ResetsAt = &reset
			case "waiting":
				a.Admission = domain.AdmissionClosed
				projects[0].Workers[0].Ready = false
			case "unauthorized":
				a.Admission = domain.AdmissionClosed
				p.Roles[0].Constraints.ProviderFamilies = []string{"anthropic"}
				want = "a/m"
			case "disabled":
				a.Windows.Unknown = true
				b.Windows.Unknown = true
				snapshot.ChecksDisabled["a"] = true
				snapshot.ChecksDisabled["b"] = true
				want = "a/m"
			case "equal-band":
				want = "a/m"
			case "tier":
				a.Admission = domain.AdmissionClosed
				projects[0].Workers[0].Routes[1].Tier = "critical"
				want = "a/m"
			case "placement":
				tasks[0].Hosts = []string{"elsewhere"}
			case "no-eligible":
				projects[0].Workers[0].Routes = nil
			case "reversed":
				p.Roles[0].Candidates[0], p.Roles[0].Candidates[1] = p.Roles[0].Candidates[1], p.Roles[0].Candidates[0]
			}
			if mode != "unbound" {
				snapshot.Pools["a"] = a
			}
			snapshot.Pools["b"] = b
			selected, failures := resolveCampaignPolicy(p, tasks, projects, nil, snapshot.Now, snapshot)
			got := selected["t"]
			if mode == "placement" || mode == "no-eligible" {
				if len(selected) != 0 || failures["t"].Code != "role-no-eligible-candidate" || !strings.Contains(failures["t"].Detail, "b/m") {
					t.Fatalf("candidate rejection evidence lost: selected=%+v failures=%+v", selected, failures)
				}
				return
			}
			if mode == "tier" && got.Candidates[1].Eligible {
				t.Fatal("quota bypassed tier constraint")
			}
			if len(failures) != 0 || got.Route != want || got.Ranking != domain.RouteRankingV1 || got.PolicyDigest != "quota-digest" || len(got.Candidates) != 2 || !strings.Contains(got.Reason, domain.RouteRankingV1) {
				t.Fatalf("selected=%+v failures=%+v want=%s", got, failures, want)
			}
			if mode == "unauthorized" && got.Candidates[1].Eligible {
				t.Fatal("quota authorized a forbidden provider")
			}
		})
	}
}

func TestCampaignQuotaClassGateUsesCopiedSnapshot(t *testing.T) {
	for _, state := range []domain.AdmissionState{domain.AdmissionConstrained, domain.AdmissionRecovering} {
		t.Run(string(state), func(t *testing.T) {
			p, projects, tasks, snapshot := roleRankingFixture()
			pool := snapshot.Pools["a"]
			pool.Admission = state
			snapshot.Pools["a"] = pool
			tasks[0].Class = domain.TaskClassSurplus
			selected, failures := resolveCampaignPolicy(p, tasks, projects, nil, snapshot.Now, snapshot)
			got := selected["t"]
			if len(failures) != 0 || got.Route != "b/m" || got.Candidates[0].Band != "gated" || !strings.Contains(got.Candidates[0].Reason, "admission "+string(state)+" for surplus") || strings.Contains(got.Candidates[0].Reason, "admission closed") {
				t.Fatalf("surplus class gate lost: selected=%+v failures=%+v", got, failures)
			}
			if snapshot.Pools["a"].Admission != state {
				t.Fatal("class gate mutated request snapshot")
			}
			tasks[0].Class = domain.TaskClassRequired
			selected, failures = resolveCampaignPolicy(p, tasks, projects, nil, snapshot.Now, snapshot)
			if len(failures) != 0 || selected["t"].Route != "a/m" || selected["t"].Candidates[0].Band != "healthy" {
				t.Fatalf("surplus gate leaked into required task: %+v %+v", selected, failures)
			}
		})
	}
}

func TestCampaignQuotaDiversityIsSoftWithinUsableBand(t *testing.T) {
	for _, mode := range []string{"healthy", "gated-other", "unknown-other", "reset-first", "unknown-producer", "multi-producer", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			p, projects, _, snapshot := roleRankingFixture()
			tasks := []backlogadmin.ViabilityTask{{Name: "producer", Project: "p", Routes: []domain.ProviderRoute{{ProviderInstanceID: "a", Model: "m"}}}, {Name: "t", Project: "p", Role: "review", Needs: []string{"producer"}}}
			b := snapshot.Pools["b"]
			want := "b/m"
			cross := true
			switch mode {
			case "gated-other":
				b.Admission = domain.AdmissionClosed
				want = "a/m"
				cross = false
			case "unknown-other":
				b.Windows.Unknown = true
				want = "a/m"
				cross = false
			case "reset-first":
				a := snapshot.Pools["a"]
				reset := snapshot.Now.Add(time.Hour)
				a.Windows.Windows[0].Window = "seven_day"
				a.Windows.Windows[0].ResetsAt = &reset
				snapshot.Pools["a"] = a
				want = "a/m"
				cross = false
			case "unknown-producer":
				tasks[0].Routes[0].ProviderInstanceID = "missing"
				want = "a/m"
				cross = false
			case "multi-producer":
				tasks = append(tasks, backlogadmin.ViabilityTask{Name: "other", Project: "p", Routes: []domain.ProviderRoute{{ProviderInstanceID: "b", Model: "m"}}})
				tasks[1].Needs = append(tasks[1].Needs, "other")
				want = "a/m"
				cross = false
			case "disabled":
				for _, id := range []string{"a", "b"} {
					pool := snapshot.Pools[id]
					pool.Windows.Unknown = true
					snapshot.Pools[id] = pool
					snapshot.ChecksDisabled[id] = true
				}
			}
			if mode != "disabled" {
				snapshot.Pools["b"] = b
			}
			selected, failures := resolveCampaignPolicy(p, tasks, projects, nil, snapshot.Now, snapshot)
			got := selected["t"]
			if len(failures) != 0 || got.Route != want || got.Diversity.CrossProvider != cross || got.Diversity.Reason == "" {
				t.Fatalf("selected=%+v failures=%+v", got, failures)
			}
		})
	}
}
