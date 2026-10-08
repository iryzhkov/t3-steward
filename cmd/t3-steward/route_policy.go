package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"gopkg.in/yaml.v3"
)

type routePolicy struct {
	Schema string       `yaml:"schema" json:"schema"`
	Roles  []policyRole `yaml:"roles" json:"roles"`
	Digest string       `yaml:"-" json:"digest"`
	raw    []byte
}
type policyRole struct {
	Name        string            `yaml:"name" json:"name"`
	Candidates  []policyCandidate `yaml:"candidates" json:"candidates"`
	Constraints policyConstraints `yaml:"constraints,omitempty" json:"constraints,omitempty"`
}
type policyCandidate struct {
	Route  string `yaml:"route" json:"route"`
	Effort string `yaml:"effort" json:"effort"`
	Tier   string `yaml:"tier" json:"tier"`
}
type policyConstraints struct {
	ProviderFamilies        []string `yaml:"provider_families,omitempty" json:"providerFamilies,omitempty"`
	ExcludeProviderFamilies []string `yaml:"exclude_provider_families,omitempty" json:"excludeProviderFamilies,omitempty"`
	Tiers                   []string `yaml:"tiers,omitempty" json:"tiers,omitempty"`
}
type policySelection struct {
	Schema       string                `json:"schema"`
	Role         string                `json:"role,omitempty"`
	Route        string                `json:"route"`
	PolicyDigest string                `json:"policyDigest"`
	Reason       string                `json:"reason"`
	Effort       string                `json:"effort,omitempty"`
	Ranking      string                `json:"ranking,omitempty"`
	Candidates   []policyRankCandidate `json:"candidates,omitempty"`
}

func validPolicyEffort(e string) error {
	switch e {
	case "", "low", "medium", "high":
		return nil
	}
	return fmt.Errorf("effort %q is refused; use low, medium or high (max and higher are forbidden)", e)
}
func parseRoutePolicy(raw []byte) (*routePolicy, error) {
	if int64(len(raw)) > pinnedinput.MaxFileBytes {
		return nil, errors.New("policy exceeds 1 MiB; reduce the policy")
	}
	var p routePolicy
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("invalid route policy: %w; run t3-steward policy validate --file PATH", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("policy must contain exactly one YAML document")
	}
	if p.Schema != "route-policy/v1" {
		return nil, fmt.Errorf("unsupported policy schema %q; use route-policy/v1", p.Schema)
	}
	if len(p.Roles) == 0 {
		return nil, errors.New("policy needs at least one role")
	}
	seen := map[string]bool{}
	for _, r := range p.Roles {
		if !review.IDPattern.MatchString(r.Name) || seen[r.Name] {
			return nil, fmt.Errorf("duplicate or invalid role %q; use unique safe role names", r.Name)
		}
		seen[r.Name] = true
		if len(r.Candidates) == 0 {
			return nil, fmt.Errorf("role %s has no candidates; add an instance/model candidate", r.Name)
		}
		routes := map[string]bool{}
		for _, c := range r.Candidates {
			if !review.ValidRoute(c.Route) || routes[c.Route] {
				return nil, fmt.Errorf("role %s has invalid or duplicate candidate %q; use unique INSTANCE/MODEL routes", r.Name, c.Route)
			}
			routes[c.Route] = true
			if c.Effort == "" {
				return nil, fmt.Errorf("candidate %s needs effort: low, medium or high", c.Route)
			}
			if err := validPolicyEffort(c.Effort); err != nil {
				return nil, err
			}
			if !contains([]string{"economy", "standard", "premium"}, c.Tier) {
				return nil, fmt.Errorf("candidate %s needs tier economy, standard or premium", c.Route)
			}
			if len(r.Constraints.Tiers) > 0 && !contains(r.Constraints.Tiers, c.Tier) {
				return nil, fmt.Errorf("candidate %s violates role %s tier constraint", c.Route, r.Name)
			}
		}
		for _, list := range [][]string{r.Constraints.ProviderFamilies, r.Constraints.ExcludeProviderFamilies, r.Constraints.Tiers} {
			unique := map[string]bool{}
			for _, v := range list {
				if !review.IDPattern.MatchString(v) || unique[v] {
					return nil, fmt.Errorf("role %s has invalid or duplicate constraint %q", r.Name, v)
				}
				unique[v] = true
			}
		}
		for _, v := range r.Constraints.Tiers {
			if !contains([]string{"economy", "standard", "premium"}, v) {
				return nil, fmt.Errorf("unknown cost tier %s", v)
			}
		}
		for _, v := range r.Constraints.ProviderFamilies {
			if contains(r.Constraints.ExcludeProviderFamilies, v) {
				return nil, fmt.Errorf("role %s both allows and excludes provider %s", r.Name, v)
			}
		}
	}
	sum := sha256.Sum256(raw)
	p.Digest = hex.EncodeToString(sum[:])
	p.raw = append([]byte(nil), raw...)
	return &p, nil
}
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
func validatePolicyCatalog(p *routePolicy, workers []backlogadmin.Worker) error {
	models := map[string][][]string{}
	reported := false
	for _, w := range workers {
		if len(w.Providers) > 0 {
			reported = true
		}
		for _, a := range w.Providers {
			if a.Dropped != "" {
				continue
			}
			models[a.Instance] = append(models[a.Instance], a.Models)
		}
	}
	if !reported {
		return errors.New("configured route catalog unavailable; upgrade/configure the coordinator and retry t3-steward policy validate")
	}
	for _, r := range p.Roles {
		for _, c := range r.Candidates {
			instance, model, _ := strings.Cut(c.Route, "/")
			authorized := false
			for _, allowed := range models[instance] {
				if domain.ModelAuthorized(allowed, model) {
					authorized = true
					break
				}
			}
			if !authorized {
				return fmt.Errorf("policy candidate %s is not catalog-authorized; correct the policy or configure its model authorization (t3-steward worker list)", c.Route)
			}
		}
	}
	return nil
}
func defaultRoutePolicyPath() string {
	// Resolve without DefaultPaths' legacy migration: policy show is read-only.
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "t3-steward", "route-policy.yaml")
}
func (c taskRunCLI) loadPolicy(ctx context.Context, path, role string) (*routePolicy, error) {
	explicitFile := path != "" || c.policyPath != ""
	if path == "" {
		path = c.policyPath
	}
	if path == "" {
		path = defaultRoutePolicyPath()
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) && role == "" && !explicitFile {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read route policy %s: %w; install a policy or pass --policy-file PATH (t3-steward policy validate --file PATH)", path, err)
	}
	p, err := parseRoutePolicy(raw)
	if err != nil {
		return nil, err
	}
	// Explicit routes use policy bytes only for effort and provenance. Catalog
	// authorization is required when the policy chooses a route automatically.
	return p, nil
}
func policyEffort(p *routePolicy, route string) (string, error) {
	effort := ""
	for _, r := range p.Roles {
		for _, c := range r.Candidates {
			if c.Route == route {
				if effort != "" && effort != c.Effort {
					return "", fmt.Errorf("ambiguous policy effort for %s; pass --effort explicitly or align candidate efforts", route)
				}
				effort = c.Effort
			}
		}
	}
	return effort, nil
}

// policyCandidateVerdict is the shared policy eligibility seam. Pools come
// only from workers eligible for this project and this invocation.
type policyCandidateVerdict struct {
	Route          string
	Ordinal        int
	Eligible       bool
	Reason         string
	ProviderFamily string
	Tier           string
	Effort         string
	Pools          []string
	err            error
}

func evaluatePolicyCandidates(p *routePolicy, role, model, effort, worker string, project backlogadmin.Project, accept func(string) bool) (*policyRole, []policyCandidateVerdict, error) {
	if err := validPolicyEffort(effort); err != nil {
		return nil, nil, err
	}
	var rr *policyRole
	for i := range p.Roles {
		if p.Roles[i].Name == role {
			rr = &p.Roles[i]
		}
	}
	if role != "" && rr == nil {
		return nil, nil, fmt.Errorf("unknown role %q; inspect t3-steward policy show", role)
	}
	if model != "" && rr == nil {
		if !review.ValidRoute(model) {
			return nil, nil, fmt.Errorf("explicit route %q must be INSTANCE/MODEL", model)
		}
		s, err := explicitPolicySelection(p, role, model, effort)
		if err != nil {
			return nil, nil, err
		}
		return nil, []policyCandidateVerdict{{Route: model, Eligible: true, Reason: s.Reason, Effort: s.Effort}}, nil
	}
	candidates := []policyCandidate{}
	if model != "" {
		candidates = append(candidates, policyCandidate{Route: model})
	} else if rr != nil {
		candidates = rr.Candidates
	}
	verdicts := make([]policyCandidateVerdict, 0, len(candidates))
	for ordinal, candidate := range candidates {
		v := policyCandidateVerdict{Route: candidate.Route, Ordinal: ordinal}
		eligible := project
		eligible.Workers = nil
		for _, w := range project.Workers {
			if (model != "" || w.Ready && w.Advertises) && (worker == "" || w.Worker == worker) {
				eligible.Workers = append(eligible.Workers, w)
			}
		}
		route, err := deriveTaskRunRoute(candidate.Route, worker, "", eligible)
		if err != nil {
			v.Reason = err.Error()
			verdicts = append(verdicts, v)
			continue
		}
		pair := route.Instance + "/" + route.Model
		v.Route = pair
		if accept != nil && !accept(pair) {
			v.Reason = "caller selection constraints refused route"
			verdicts = append(verdicts, v)
			continue
		}
		metadata := backlogadmin.ProjectRoute{}
		poolSet := map[string]bool{}
		for _, w := range eligible.Workers {
			for _, r := range w.Routes {
				if r.Instance+"/"+r.Model == pair {
					if metadata.ProviderFamily != "" && (metadata.ProviderFamily != r.ProviderFamily || metadata.Tier != r.Tier) {
						v.err = fmt.Errorf("conflicting catalog metadata for %s", pair)
					}
					metadata = r
					// Explicit pins retain old readiness behaviour; automatic ranking only
					// sees ready, advertising workers.
					if w.Ready && w.Advertises {
						poolSet[r.QuotaPool] = true
					}
				}
			}
		}
		v.ProviderFamily = metadata.ProviderFamily
		switch metadata.Tier {
		case "executor":
			v.Tier = "standard"
		case "critical":
			v.Tier = "premium"
		case "economy":
			v.Tier = "economy"
		}
		for pool := range poolSet {
			v.Pools = append(v.Pools, pool)
		}
		sort.Strings(v.Pools)
		switch {
		case v.err != nil:
			v.Reason = v.err.Error()
		case v.Tier == "" && rr != nil && len(rr.Constraints.Tiers) > 0:
			v.Reason = "catalog tier unavailable"
		case model == "" && v.Tier != "" && v.Tier != candidate.Tier:
			v.Reason = "catalog tier does not match policy candidate"
		case rr != nil && len(rr.Constraints.ProviderFamilies) > 0 && (metadata.ProviderFamily == "" || !contains(rr.Constraints.ProviderFamilies, metadata.ProviderFamily)):
			v.Reason = "provider family outside role allow list"
		case rr != nil && len(rr.Constraints.ExcludeProviderFamilies) > 0 && (metadata.ProviderFamily == "" || contains(rr.Constraints.ExcludeProviderFamilies, metadata.ProviderFamily)):
			v.Reason = "provider family excluded by role"
		case rr != nil && len(rr.Constraints.Tiers) > 0 && !contains(rr.Constraints.Tiers, v.Tier):
			v.Reason = "tier outside role constraints"
		default:
			v.Effort = effort
			if v.Effort == "" {
				if model == "" {
					v.Effort = candidate.Effort
				} else {
					v.Effort, v.err = policyEffort(p, pair)
				}
			}
			if v.err != nil {
				v.Reason = v.err.Error()
			} else {
				v.Eligible = true
				v.Reason = "first eligible candidate in policy order"
				if model != "" {
					v.Reason = "explicit model override"
				}
			}
		}
		verdicts = append(verdicts, v)
	}
	return rr, verdicts, nil
}

// selectPolicyRouteCandidate picks the first eligible candidate in policy
// order. selectPolicyRoute in campaign_roles.go wraps it for role receipts.
func selectPolicyRouteCandidate(p *routePolicy, role, model, effort, worker string, project backlogadmin.Project, accept func(string) bool) (policySelection, error) {
	_, verdicts, err := evaluatePolicyCandidates(p, role, model, effort, worker, project, accept)
	if err != nil {
		return policySelection{}, err
	}
	for _, v := range verdicts {
		if v.err != nil {
			return policySelection{}, v.err
		}
		if v.Eligible {
			return policySelection{Schema: "route-selection/v1", Role: role, Route: v.Route, PolicyDigest: p.Digest, Reason: v.Reason, Effort: v.Effort}, nil
		}
	}
	return policySelection{}, noEligiblePolicyRoute(role, model, project)
}
func noEligiblePolicyRoute(role, model string, project backlogadmin.Project) error {
	return fmt.Errorf("no eligible candidate for role %q and model %q; check t3-steward models --project %s and policy show; explicit models remain pinned", role, model, project.Name)
}

// explicitPolicySelection adds effort and provenance without route resolution.
func explicitPolicySelection(p *routePolicy, role, pair, effort string) (policySelection, error) {
	if effort == "" {
		var err error
		effort, err = policyEffort(p, pair)
		if err != nil {
			return policySelection{}, err
		}
	}
	return policySelection{Schema: "route-selection/v1", Role: role, Route: pair, PolicyDigest: p.Digest, Reason: "explicit model override", Effort: effort}, nil
}

// Merge only immutable snapshots; never re-read the source policy after resolution.
func policySnapshotInputs(existing pinnedinput.Snapshot, p *routePolicy, selections []policySelection) (pinnedinput.Snapshot, error) {
	dir, err := os.MkdirTemp("", "t3-route-policy-")
	if err != nil {
		return pinnedinput.Snapshot{}, err
	}
	defer os.RemoveAll(dir)
	if err := os.MkdirAll(filepath.Join(dir, "inputs"), 0700); err != nil {
		return pinnedinput.Snapshot{}, err
	}
	if err := existing.Write(dir); err != nil {
		return pinnedinput.Snapshot{}, err
	}
	paths := []string{}
	for _, e := range existing.Manifest.Entries {
		paths = append(paths, filepath.Join(dir, e.Name))
	}
	// Quota is live telemetry. Pin only the ranking version alongside stable
	// provenance and the chosen route, never live reasons or candidate bands.
	pinned := append([]policySelection(nil), selections...)
	for i := range pinned {
		if pinned[i].Ranking != "" {
			pinned[i].Candidates = nil
			pinned[i].Reason = pinned[i].Ranking
		}
	}
	raw, err := json.Marshal(pinned)
	if err != nil {
		return pinnedinput.Snapshot{}, err
	}
	for _, f := range []struct {
		name string
		data []byte
	}{{"route-policy.yaml", p.raw}, {"route-selection.json", raw}} {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.data, 0600); err != nil {
			return pinnedinput.Snapshot{}, err
		}
		paths = append(paths, path)
	}
	return pinnedinput.SnapshotFiles(paths)
}
func policyInputs(files []string, p *routePolicy, selections []policySelection) (pinnedinput.Snapshot, error) {
	snap, err := pinnedinput.SnapshotFiles(files)
	if err != nil {
		return snap, err
	}
	if p == nil {
		return snap, nil
	}
	return policySnapshotInputs(snap, p, selections)
}

const policyUsage = `t3-steward policy - inspect a route-policy/v1 file without installing it.

Usage:
  t3-steward policy show [--file PATH] [--json]
  t3-steward policy validate [--file PATH] [--json]

Flags:
  --file PATH   Policy file; default $XDG_CONFIG_HOME/t3-steward/route-policy.yaml,
                beside coordinator-client.json.
  --json        Print route-policy/v1 with the SHA-256 digest of the original bytes.
  --config PATH Coordinator client configuration for catalog validation.

show parses and prints the policy in role/candidate order. validate also checks
configured model authorizations at the coordinator, independently of worker
availability. Neither command writes, installs or replaces a policy.
Exit: 0 valid; 1 malformed/refused; coordinator transport exits apply.
`

func parsePolicyArgs(args []string) (string, bool, error) {
	f := flag.NewFlagSet("policy", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("file", defaultRoutePolicyPath(), "")
	asJSON := f.Bool("json", false, "")
	if err := f.Parse(args); err != nil {
		return "", false, err
	}
	if f.NArg() != 0 {
		return "", false, errors.New("policy accepts flags only; try t3-steward policy --help")
	}
	return *path, *asJSON, nil
}

func resolveReviewPolicy(a reviewArgs, project backlogadmin.Project) (reviewArgs, error) {
	a.efforts = map[string]string{}
	if a.policy == nil {
		for _, route := range append(append(append([]string{}, a.reviewers...), a.swarmModels...), a.judge) {
			a.efforts[route] = a.effort
		}
		return a, nil
	}
	routes, families, err := reviewRoutes(project)
	if err != nil {
		return a, err
	}
	// A role contributes exactly one independent reviewer when no route is pinned.
	if len(a.reviewers) == 0 && a.role != "" {
		accept := func(pair string) bool {
			m := routes[pair]
			members := []review.Reviewer{{ID: "independent-1", Role: "independent", Route: pair, ProviderFamily: m.ProviderFamily, Tier: m.Tier, Required: true}}
			if a.judge != "" {
				j := routes[a.judge]
				members = append(members, review.Reviewer{ID: "judge", Role: "judge", Route: a.judge, ProviderFamily: j.ProviderFamily, Tier: j.Tier, Required: true})
			}
			if a.swarm != "" {
				for i, s := range a.swarmModels {
					m := routes[s]
					members = append(members, review.Reviewer{ID: fmt.Sprintf("swarm-%d", i), Role: "swarm:test", Route: s, ProviderFamily: m.ProviderFamily, Tier: m.Tier})
				}
			}
			return review.ValidateSelection(review.Round{Risk: a.risk, Reviewers: members}, families) == nil
		}
		s, err := selectPolicyRouteRanked(a.policy, a.role, "", a.effort, "", project, accept, a.rankView)
		if err != nil {
			return a, fmt.Errorf("%w; Phase A selects one reviewer and never fills a judge or fans out a role. %s", err, reviewPolicyRemedy(routes, families, a.risk))
		}
		a.reviewers = []string{s.Route}
		a.selections = append(a.selections, s)
		a.efforts[s.Route] = s.Effort
	} else {
		for _, pair := range a.reviewers {
			s, err := selectPolicyRoute(a.policy, a.role, pair, a.effort, "", project, nil)
			if err != nil {
				return a, err
			}
			a.selections = append(a.selections, s)
			a.efforts[s.Route] = s.Effort
		}
	}
	// Judge and swarm stay explicit. They inherit only unambiguous effort, not the independent role.
	for _, pair := range append(append([]string{}, a.swarmModels...), a.judge) {
		if pair == "" {
			continue
		}
		s, err := selectPolicyRoute(a.policy, "", pair, a.effort, "", project, nil)
		if err != nil {
			return a, err
		}
		a.selections = append(a.selections, s)
		a.efforts[s.Route] = s.Effort
	}
	return a, nil
}

// Offer a complete explicit round, without implying that one extra flag
// extends an automatic role. Use declared families and existing tier authority.
func reviewPolicyRemedy(routes map[string]backlogadmin.ProjectRoute, families int, risk string) string {
	keys := make([]string, 0, len(routes))
	for pair := range routes {
		keys = append(keys, pair)
	}
	sort.Strings(keys)
	for i, first := range keys {
		for _, second := range keys[i+1:] {
			members := []review.Reviewer{}
			for j, pair := range []string{first, second} {
				m := routes[pair]
				members = append(members, review.Reviewer{ID: fmt.Sprintf("independent-%d", j+1), Role: "independent", Route: pair, ProviderFamily: m.ProviderFamily, Tier: m.Tier, Required: true})
			}
			if routes[first].ProviderFamily != routes[second].ProviderFamily && review.ValidateSelection(review.Round{Risk: risk, Reviewers: members}, families) == nil {
				return fmt.Sprintf("Use explicit independent routes from two catalog families, without --role: t3-steward review --independent %s --independent %s (retain your project/input/notify flags)", first, second)
			}
		}
	}
	return "Use two explicit --independent INSTANCE/MODEL routes from distinct catalog provider families with executor or critical tier, without --role; configure backlog_v2.review_routes if needed (t3-steward models --project P)"
}

func cmdPolicy(g globalFlags, args []string) error {
	if len(args) == 0 {
		return errors.New("use t3-steward policy show or validate")
	}
	if args[0] != "show" && args[0] != "validate" {
		return errors.New("unknown policy command; use show or validate")
	}
	path, asJSON, err := parsePolicyArgs(args[1:])
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read policy: %w; pass --file PATH", err)
	}
	p, err := parseRoutePolicy(raw)
	if err != nil {
		return err
	}
	if args[0] == "validate" {
		cfg, err := loadConfig(g)
		if err != nil {
			return err
		}
		c := newTaskRunCLI(cfg)
		response, err := c.query(context.Background(), backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
		if err != nil {
			return err
		}
		if err := validatePolicyCatalog(p, response.Workers); err != nil {
			return err
		}
	}
	if asJSON {
		return encodeCampaignJSON(os.Stdout, p)
	}
	fmt.Printf("policy %s\ndigest %s\n", path, p.Digest)
	_, err = os.Stdout.Write(raw)
	return err
}
