package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// DeclaredAdmissionRequest accepts identity only. Authority comes from saved declaration and configured adapters.
type DeclaredAdmissionRequest struct{ RunID, TaskID, AttemptID string }
type declaredAdmissionStore interface {
	admissionRecords
	GetFrozenReviewAuthority(context.Context, string, string) (review.FrozenAuthority, bool, error)
	FreezeDeclaredReviewAuthority(context.Context, review.FrozenAuthority) (review.FrozenAuthority, error)
}

func declaredPolicy(r *domain.TaskReviewRequirements) AdmissionPolicy {
	p := AdmissionPolicy{Risk: r.Risk, CriteriaArtifactID: r.Criteria.ArtifactID, RequiredReviewers: r.RequiredReviewers, MinProviderFamilies: r.MinProviderFamilies, RoundLimit: r.RoundLimit}
	for _, m := range r.Members {
		p.Members = append(p.Members, AdmissionMember{m.ID, m.Role, m.Route, m.Required})
	}
	return p
}
func (s ReviewAdmissionService) ResolveDeclared(ctx context.Context, request DeclaredAdmissionRequest) (AdmissionSnapshot, error) {
	store, ok := s.Store.(declaredAdmissionStore)
	if !ok {
		return AdmissionSnapshot{}, fmt.Errorf("review admission: declaration-backed storage required")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	run, err := admissionOne(records.WorkflowRuns, func(r domain.WorkflowRun) bool { return r.ID == request.RunID })
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	task, err := admissionOne(domain.TasksForRun(run, records.Tasks), func(t domain.Task) bool { return t.ID == request.TaskID })
	if err != nil || task.ReviewRequirements == nil || task.ReviewRequirements.Version != 1 {
		return AdmissionSnapshot{}, fmt.Errorf("review admission: saved version 1 review_requirements required")
	}
	workflow, err := admissionOne(records.Workflows, func(w domain.Workflow) bool { return w.ID == run.WorkflowID })
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	manifest, criteria, err := admissionCriteria(ctx, s.Artifacts, records, workflow, run, task, task.ReviewRequirements.Criteria.ArtifactID)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	if err = validateCompiledCriteria(task.ReviewRequirements, workflow, run, task, criteria); err != nil {
		return AdmissionSnapshot{}, err
	}
	frozen, found, err := store.GetFrozenReviewAuthority(ctx, run.ID, task.ID)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	if found {
		// A later checkpoint keeps the original issued revision and trusted policy.
		if frozen.DeclarationDigest != domain.TaskDigest(task) || frozen.Parent.AttemptID != request.AttemptID {
			return AdmissionSnapshot{}, fmt.Errorf("review admission: issued declaration/parent changed")
		}
		var provenance AdmissionProvenance
		if err := json.Unmarshal(frozen.AdmissionProvenance, &provenance); err != nil {
			return AdmissionSnapshot{}, fmt.Errorf("review admission: retained admission provenance: %w", err)
		}
		if provenance.Criteria != criteria || provenance.InputManifest.Digest != manifest.Digest || provenance.PolicyDigest != frozen.Requirements.PolicyDigest {
			return AdmissionSnapshot{}, fmt.Errorf("review admission: retained provenance changed")
		}
		return AdmissionSnapshot{Authority: frozen, Provenance: provenance}, nil
	}
	snapshot, err := s.Resolve(ctx, AdmissionRequest{RunID: request.RunID, TaskID: request.TaskID, AttemptID: request.AttemptID, Policy: declaredPolicy(task.ReviewRequirements)})
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	snapshot.Authority.DeclarationDigest = domain.TaskDigest(task)
	snapshot.Authority.AdmissionProvenance, err = json.Marshal(snapshot.Provenance)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	return snapshot, nil
}
func (s ReviewAdmissionService) FreezeDeclared(ctx context.Context, request DeclaredAdmissionRequest) (AdmissionSnapshot, error) {
	snapshot, err := s.ResolveDeclared(ctx, request)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	store := s.Store.(declaredAdmissionStore)
	snapshot.Authority, err = store.FreezeDeclaredReviewAuthority(ctx, snapshot.Authority)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	return snapshot, nil
}
