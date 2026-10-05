package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"path/filepath"
	"slices"
	"strings"
)

type ManifestReviewRequirements struct {
	Version             int                    `yaml:"version"`
	Risk                string                 `yaml:"risk"`
	CriteriaFile        string                 `yaml:"criteria_file"`
	RequiredReviewers   int                    `yaml:"required_reviewers"`
	MinProviderFamilies int                    `yaml:"min_provider_families"`
	RoundLimit          int                    `yaml:"round_limit"`
	Members             []ManifestReviewMember `yaml:"members"`
}
type ManifestReviewMember struct {
	ID       string `yaml:"id"`
	Role     string `yaml:"role"`
	Route    string `yaml:"route"`
	Required bool   `yaml:"required"`
}

func HasTaskReviewRequirements(m Manifest) bool {
	for _, task := range m.Tasks {
		if task.ReviewRequirements != nil {
			return true
		}
	}
	return false
}
func validateTaskReviewDeclaration(m Manifest, name string, r *ManifestReviewRequirements) error {
	if r == nil {
		return nil
	}
	fail := func(msg string) error { return fmt.Errorf("task %s review_requirements: %s", name, msg) }
	if r.Version != 1 {
		return fail("version must be 1")
	}
	if m.Environment.Type != EnvironmentGit || !review.FullCommitID(m.Environment.Ref) {
		return fail("requires environment.type git and an immutable full 40/64 lowercase hex source ref; branch/default/fresh sources are unsupported")
	}
	if err := validateRelativePath(r.CriteriaFile, false); err != nil {
		return fail("criteria_file: " + err.Error())
	}
	if filepath.ToSlash(filepath.Clean(r.CriteriaFile)) != r.CriteriaFile || r.CriteriaFile == "workflow.yaml" {
		return fail("criteria_file must be a canonical submitted input path")
	}
	declared := false
	for _, pattern := range m.Inputs {
		match, err := filepath.Match(pattern, r.CriteriaFile)
		if err == nil && match {
			declared = true
		}
	}
	if !declared {
		return fail("criteria_file must match explicitly declared inputs")
	}
	ceiling := 2
	if r.Risk == "risky" {
		ceiling = 3
	} else if r.Risk != "routine" {
		return fail("risk must be routine or risky")
	}
	if r.RoundLimit < 0 || r.RoundLimit > ceiling {
		return fail("round_limit exceeds risk ceiling or is negative")
	}
	if r.RequiredReviewers < 2 || r.MinProviderFamilies < 2 || r.MinProviderFamilies > 32 || len(r.Members) > 32 || len(r.Members) < r.RequiredReviewers {
		return fail("invalid required_reviewers, min_provider_families or member count")
	}
	seen := map[string]bool{}
	required := 0
	for _, member := range r.Members {
		if !review.IDPattern.MatchString(member.ID) || seen[member.ID] || strings.TrimSpace(member.Role) == "" || len(member.Role) > 64 || strings.ContainsAny(member.Role, "\n\r\x00") || !review.ValidRoute(member.Route) || strings.Contains(member.Route, "*") {
			return fail("invalid/duplicate member identity, role or exact route")
		}
		seen[member.ID] = true
		if member.Required {
			required++
		}
	}
	if required != r.RequiredReviewers || required < r.MinProviderFamilies {
		return fail("required member count/diversity declaration mismatch")
	}
	return nil
}
func (r ManifestReviewRequirements) policy(criteriaID string) AdmissionPolicy {
	p := AdmissionPolicy{Risk: r.Risk, CriteriaArtifactID: criteriaID, RequiredReviewers: r.RequiredReviewers, MinProviderFamilies: r.MinProviderFamilies, RoundLimit: r.RoundLimit}
	for _, m := range r.Members {
		p.Members = append(p.Members, AdmissionMember(m))
	}
	return p
}

// ValidateTaskReviewAdmission is the permanent configured part of acceptance.
// It ignores live availability/quota; selected static configuration must authorize every route.
func ValidateTaskReviewAdmission(ctx context.Context, m Manifest, catalog AdmissionCatalogSource) error {
	for name, t := range m.Tasks {
		if t.ReviewRequirements == nil {
			continue
		}
		if err := validateTaskReviewDeclaration(m, name, t.ReviewRequirements); err != nil {
			return err
		}
		if catalog == nil {
			return fmt.Errorf("%w: task %s review_requirements needs a configured admission catalog; repair coordinator configuration", ErrValidationUnavailable, name)
		}
		c, err := catalog.ReviewAdmissionCatalog(ctx, m.Environment.Project)
		if err != nil {
			return err
		}
		p := t.ReviewRequirements.policy("")
		spec := review.RequirementsSpec{Risk: p.Risk, CriteriaDigest: strings.Repeat("0", 64), PolicyDigest: strings.Repeat("0", 64), RequiredReviewers: p.RequiredReviewers, MinProviderFamilies: p.MinProviderFamilies, RoundLimit: p.RoundLimit}
		for _, member := range p.Members {
			metadata, _, err := admissionRouteMetadata(c, m.Environment.Project, member.Route)
			if err != nil {
				return fmt.Errorf("task %s configured admission: %w", name, err)
			}
			spec.Members = append(spec.Members, review.MemberRequirement{ID: member.ID, Role: member.Role, Route: member.Route, Required: member.Required, ProviderFamily: metadata.ProviderFamily, Tier: metadata.Tier})
		}
		if _, err := review.NewRequirements(spec); err != nil {
			return fmt.Errorf("task %s configured admission: %w", name, err)
		}
	}
	return nil
}
func compileTaskReview(r *ManifestReviewRequirements, workflowID, runID, taskID string, a domain.Artifact) *domain.TaskReviewRequirements {
	if r == nil {
		return nil
	}
	limit := r.RoundLimit
	if limit == 0 {
		limit = 2
		if r.Risk == "risky" {
			limit = 3
		}
	}
	c := &domain.TaskReviewRequirements{Version: r.Version, Risk: r.Risk, RequiredReviewers: r.RequiredReviewers, MinProviderFamilies: r.MinProviderFamilies, RoundLimit: limit, Criteria: domain.ReviewCriteria{ArtifactID: a.ID, RunID: runID, WorkflowID: workflowID, TaskID: taskID, Name: a.Name, Size: a.Size, SHA256: a.SHA256}}
	for _, m := range r.Members {
		c.Members = append(c.Members, domain.TaskReviewMember{ID: m.ID, Role: m.Role, Route: m.Route, Required: m.Required})
	}
	return c
}
func validateCompiledCriteria(r *domain.TaskReviewRequirements, workflow domain.Workflow, run domain.WorkflowRun, task domain.Task, c CriteriaProvenance) error {
	if r == nil || r.Version != 1 || r.Criteria != (domain.ReviewCriteria{ArtifactID: c.ArtifactID, RunID: run.ID, WorkflowID: workflow.ID, TaskID: task.ID, Name: c.Name, Size: c.Size, SHA256: c.SHA256}) || !slices.Contains(task.InputArtifactIDs, c.ArtifactID) {
		return fmt.Errorf("review admission: compiled retained criteria provenance changed")
	}
	return nil
}
