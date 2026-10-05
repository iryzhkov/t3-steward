package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// AdmissionPolicy is trusted internal declaration, not authenticated public
// submission input. It deliberately cannot carry digests, family, tier or base.
type AdmissionPolicy struct {
	Risk                string
	CriteriaArtifactID  string
	RequiredReviewers   int
	MinProviderFamilies int
	RoundLimit          int
	Members             []AdmissionMember
}
type AdmissionMember struct {
	ID, Role, Route string
	Required        bool
	Execution       *domain.ReviewExecutionProfile `json:",omitempty"`
}

// AdmissionCatalogSource must return coordinator-authored configuration, never
// worker observations. A later wiring adapter must establish that provenance.
// Projects in AuthoredWorkers are authored project eligibility; availability,
// connection, quota and health are deliberately ignored.
type AdmissionCatalogSource interface {
	ReviewAdmissionCatalog(context.Context, string) (AdmissionCatalog, error)
}
type AdmissionCatalog struct {
	AuthoredWorkers []domain.WorkerInventory
	Classifications []AdmissionClassification
}
type AdmissionClassification struct{ Route, ProviderFamily, Tier string }

type admissionRecords interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	FreezeReviewAuthority(context.Context, review.FrozenAuthority) (review.FrozenAuthority, error)
}
type AdmissionRequest struct {
	RunID, TaskID, AttemptID string
	Policy                   AdmissionPolicy
}
type CriteriaProvenance struct {
	ArtifactID, RunID, WorkflowID, TaskID, Name string
	Size                                        int64
	SHA256                                      string
}
type AdmissionProvenance struct {
	Version       string
	InputManifest pinnedinput.Manifest
	Criteria      CriteriaProvenance
	RepositoryURL string
	Environment   ResolvedEnvironment
	// PolicyDigest is versioned admission policy, NOT a generated role-policy file hash.
	PolicyDigest string
}
type AdmissionSnapshot struct {
	Authority  review.FrozenAuthority
	Provenance AdmissionProvenance
}

// ReviewAdmissionService is inert until a trusted coordinator caller supplies
// its adapters. It does not submit children or enforce public task acceptance.
type ReviewAdmissionService struct {
	Store     admissionRecords
	Artifacts CoordinatorArtifactStore
	Projects  *ProjectCatalog
	Catalog   AdmissionCatalogSource
}

func (s ReviewAdmissionService) Freeze(ctx context.Context, request AdmissionRequest) (AdmissionSnapshot, error) {
	snapshot, err := s.Resolve(ctx, request)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	frozen, err := s.Store.FreezeReviewAuthority(ctx, snapshot.Authority)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	snapshot.Authority = frozen
	return snapshot, nil
}

func (s ReviewAdmissionService) Resolve(ctx context.Context, request AdmissionRequest) (AdmissionSnapshot, error) {
	fail := func(msg string) (AdmissionSnapshot, error) {
		return AdmissionSnapshot{}, fmt.Errorf("review admission: %s", msg)
	}
	if s.Store == nil || s.Projects == nil || s.Catalog == nil || s.Artifacts.Catalog == nil {
		return fail("trusted records, project, artifact and authored catalog sources required")
	}
	records, err := s.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	run, err := admissionOne(records.WorkflowRuns, func(v domain.WorkflowRun) bool { return v.ID == request.RunID })
	if err != nil || run.Progress.Terminal() {
		return fail("run missing, duplicate or terminal")
	}
	task, err := admissionOne(domain.TasksForRun(run, records.Tasks), func(v domain.Task) bool { return v.ID == request.TaskID })
	if err != nil || task.WorkflowID != run.WorkflowID || task.RunID != run.ID {
		return fail("task is not an immutable declaration in this run")
	}
	workflow, err := admissionOne(records.Workflows, func(v domain.Workflow) bool { return v.ID == run.WorkflowID })
	if run.Graph != nil {
		workflow.TaskIDs = nil
		for _, t := range run.Graph.Tasks {
			workflow.TaskIDs = append(workflow.TaskIDs, t.ID)
		}
	}
	if err != nil || !admissionUnique(workflow.TaskIDs) || !slices.Contains(workflow.TaskIDs, task.ID) {
		return fail("workflow task membership mismatch")
	}
	attempt, err := admissionOne(records.Attempts, func(v domain.Attempt) bool { return v.ID == request.AttemptID })
	if err != nil || attempt.WorkflowRunID != run.ID || attempt.TaskID != task.ID || !attempt.TurnLive() || attempt.SupervisionActivationID != "" {
		return fail("attempt is not live declared parent")
	}
	for _, other := range records.Attempts {
		if other.WorkflowRunID == run.ID && other.TaskID == task.ID && other.SupervisionActivationID == "" && other.ID != attempt.ID && other.Number >= attempt.Number {
			return fail("attempt is not uniquely latest")
		}
	}
	assignment, err := admissionOne(records.Assignments, func(v domain.Assignment) bool { return v.ID == attempt.AssignmentID })
	if err != nil || assignment.AttemptID != attempt.ID || assignment.Project != workflow.Project || assignment.State != domain.AssignmentClaimed || assignment.ThreadID != attempt.ThreadID || assignment.ActivationID != "" {
		return fail("claimed assignment identity mismatch")
	}
	if assignment.TaskDigest != "" && assignment.TaskDigest != domain.TaskDigest(task) ||
		assignment.TaskRevision != 0 && assignment.TaskRevision != task.DefinitionRevision ||
		assignment.GraphRevision != 0 && assignment.GraphRevision != run.GraphRevision {
		return fail("assignment no longer names loaded immutable task definition")
	}
	if workflow.Environment.Type != EnvironmentGit || !review.FullCommitID(workflow.Environment.Ref) {
		return fail("stored immutable git base required; branch/default/fresh resolution is pending")
	}
	environment, err := s.Projects.Resolve(workflow, task)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	if environment.ProjectName != workflow.Project || environment.Ref != workflow.Environment.Ref || environment.Repository == "" {
		return fail("resolved project/base mismatch")
	}
	manifest, criteria, err := admissionCriteria(ctx, s.Artifacts, records, workflow, run, task, request.Policy.CriteriaArtifactID)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	catalog, err := s.Catalog.ReviewAdmissionCatalog(ctx, workflow.Project)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	policy := request.Policy
	policy.Members = append([]AdmissionMember(nil), policy.Members...)
	for i := range policy.Members {
		policy.Members[i].Execution = domain.CloneReviewExecution(policy.Members[i].Execution)
	}
	sort.Slice(policy.Members, func(i, j int) bool { return policy.Members[i].ID < policy.Members[j].ID })
	if policy.RoundLimit == 0 {
		policy.RoundLimit = 2
		if policy.Risk == "risky" {
			policy.RoundLimit = 3
		}
	}
	spec := review.RequirementsSpec{Risk: policy.Risk, CriteriaDigest: criteria.SHA256, RequiredReviewers: policy.RequiredReviewers, MinProviderFamilies: policy.MinProviderFamilies, RoundLimit: policy.RoundLimit}
	var selected []admissionRoute
	for _, member := range policy.Members {
		metadata, grants, err := admissionMemberMetadata(catalog, workflow.Project, member)
		if err != nil {
			return AdmissionSnapshot{}, err
		}
		spec.Members = append(spec.Members, review.MemberRequirement{ID: member.ID, Role: member.Role, Route: member.Route, Required: member.Required, ProviderFamily: metadata.ProviderFamily, Tier: metadata.Tier, Execution: domain.CloneReviewExecution(member.Execution)})
		selected = append(selected, admissionRoute{metadata, grants})
	}
	// Project collections are sets. Preserve command order, which is meaningful.
	environment.ResourceLocks = admissionSorted(environment.ResourceLocks)
	environment.RequiredCredentials = admissionSorted(environment.RequiredCredentials)
	sort.Slice(environment.DirectoryBindings, func(i, j int) bool {
		return admissionDigest(environment.DirectoryBindings[i]) < admissionDigest(environment.DirectoryBindings[j])
	})
	spec.PolicyDigest = admissionDigest(struct {
		Version     string
		Policy      AdmissionPolicy
		Environment ResolvedEnvironment
		InputDigest string
		Criteria    CriteriaProvenance
		Routes      []admissionRoute
	}{"review-admission/v1", policy, environment, manifest.Digest, criteria, selected})
	requirements, err := review.NewRequirements(spec)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	parent := review.ParentBinding{RunID: run.ID, TaskID: task.ID, AttemptID: attempt.ID, ThreadID: attempt.ThreadID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, IssuedRevision: attempt.Revision, Repository: workflow.Project, BaseCommit: workflow.Environment.Ref, ExecutorRoute: assignment.Route.ProviderInstanceID + "/" + assignment.Route.Model}
	authority, err := review.NewFrozenAuthority(parent, requirements)
	if err != nil {
		return AdmissionSnapshot{}, err
	}
	return AdmissionSnapshot{authority, AdmissionProvenance{"review-admission/v1", manifest, criteria, environment.Repository, environment, spec.PolicyDigest}}, nil
}

func admissionOne[T any](values []T, matches func(T) bool) (T, error) {
	var result T
	n := 0
	for _, v := range values {
		if matches(v) {
			result = v
			n++
		}
	}
	if n != 1 {
		return result, errors.New("record must exist exactly once")
	}
	return result, nil
}
func admissionUnique(values []string) bool {
	seen := map[string]bool{}
	for _, v := range values {
		if v == "" || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}
func admissionSorted(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
func admissionDigest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func admissionCriteria(ctx context.Context, store CoordinatorArtifactStore, records sqlite.CoordinatorRecords, workflow domain.Workflow, run domain.WorkflowRun, task domain.Task, id string) (pinnedinput.Manifest, CriteriaProvenance, error) {
	fail := func(msg string) (pinnedinput.Manifest, CriteriaProvenance, error) {
		return pinnedinput.Manifest{}, CriteriaProvenance{}, fmt.Errorf("review criteria: %s", msg)
	}
	// Clone/rerun inputs are verified reference artifacts owned by the new graph.
	referenced := run.Graph != nil && run.Graph.ClonedFrom != nil
	if referenced {
		workflow.InputArtifactIDs = append([]string(nil), task.InputArtifactIDs...)
		run.InputArtifactIDs = append([]string(nil), task.InputArtifactIDs...)
	}
	if workflow.InputManifest == nil || !admissionUnique(workflow.InputArtifactIDs) || !admissionUnique(task.InputArtifactIDs) || !admissionUnique(run.InputArtifactIDs) || !reflect.DeepEqual(admissionSorted(workflow.InputArtifactIDs), admissionSorted(run.InputArtifactIDs)) {
		return fail("missing manifest or invalid input membership")
	}
	manifest, err := pinnedinput.NewManifest(workflow.InputManifest.Entries)
	if err != nil || manifest.Digest != workflow.InputManifest.Digest {
		return fail("canonical input manifest mismatch")
	}
	if id == "" || id == task.PromptArtifactID || !slices.Contains(task.InputArtifactIDs, id) || !slices.Contains(workflow.InputArtifactIDs, id) {
		return fail("criteria not declared by task and workflow")
	}
	// Every canonical pin must map to exactly one retained submission artifact.
	var criteria domain.Artifact
	for _, entry := range manifest.Entries {
		matches := []domain.Artifact{}
		for _, aid := range workflow.InputArtifactIDs {
			artifact, err := admissionOne(records.Artifacts, func(v domain.Artifact) bool { return v.ID == aid })
			if err != nil {
				return fail("missing or duplicate retained artifact")
			}
			if artifact.Name == entry.Name {
				matches = append(matches, artifact)
			}
		}
		if len(matches) != 1 {
			return fail("pin name missing or ambiguous")
		}
		artifact := matches[0]
		if artifact.WorkflowRunID != run.ID || (artifact.TaskID != "" && (!referenced || artifact.TaskID != task.ID)) || artifact.AttemptID != "" || artifact.Kind != domain.ArtifactInput || artifact.Producer != "submission" || artifact.Name == "workflow.yaml" || artifact.Size != entry.Size || artifact.SHA256 != entry.SHA256 {
			return fail("retained input lineage/size/hash mismatch")
		}
		if artifact.ID == id {
			criteria = artifact
		}
	}
	if criteria.ID == "" {
		return fail("criteria not a canonical pinned input")
	}
	// Size was bounded by NewManifest before Open hashes the retained object.
	opened, file, err := store.Open(ctx, id)
	if err != nil {
		return fail("retained criteria open failed: " + err.Error())
	}
	defer file.Close()
	if !reflect.DeepEqual(opened, criteria) {
		return fail("opened metadata differs from loaded input")
	}
	raw, err := io.ReadAll(io.LimitReader(file, pinnedinput.MaxFileBytes+1))
	if err != nil || int64(len(raw)) != criteria.Size {
		return fail("criteria bytes changed or exceed bound")
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if digest != criteria.SHA256 {
		return fail("criteria content hash changed")
	}
	return manifest, CriteriaProvenance{criteria.ID, run.ID, workflow.ID, task.ID, criteria.Name, criteria.Size, digest}, nil
}

type admissionGrant struct {
	WorkerID, CatalogRevision, InstanceID, QuotaPoolID string
	Models                                             []string
}
type admissionRoute struct {
	Classification AdmissionClassification
	Grants         []admissionGrant
}

func admissionRouteMetadata(catalog AdmissionCatalog, project, route string) (AdmissionClassification, []admissionGrant, error) {
	fail := func(msg string) (AdmissionClassification, []admissionGrant, error) {
		return AdmissionClassification{}, nil, fmt.Errorf("review route %q: %s", route, msg)
	}
	instance, model, ok := strings.Cut(route, "/")
	if !ok || !review.ValidRoute(route) || model == "*" {
		return fail("exact concrete route required")
	}
	var metadata AdmissionClassification
	count := 0
	for _, c := range catalog.Classifications {
		if c.Route == route {
			metadata = c
			count++
		}
	}
	if count != 1 || metadata.ProviderFamily == "" || metadata.Tier == "" {
		return fail("missing or conflicting configured classification")
	}
	var grants []admissionGrant
	seen := map[string]bool{}
	for _, worker := range catalog.AuthoredWorkers {
		if worker.ID == "" || seen[worker.ID] {
			return fail("invalid duplicate authored worker")
		}
		seen[worker.ID] = true
		eligible := false
		for _, p := range worker.Projects {
			if p.Name == project {
				eligible = true
			}
		}
		if !eligible {
			continue
		}
		providers := map[string]bool{}
		for _, provider := range worker.Providers {
			if provider.InstanceID != instance {
				continue
			}
			key := provider.InstanceID + "\x00" + provider.QuotaPoolID
			if providers[key] {
				return fail("duplicate authored provider")
			}
			providers[key] = true
			// Mixed wildcard policies are malformed, not expanded.
			if slices.Contains(provider.Models, "*") && len(provider.Models) != 1 {
				return fail("mixed wildcard authorization")
			}
			if provider.InstanceID == instance && domain.ModelAuthorized(provider.Models, model) {
				grants = append(grants, admissionGrant{worker.ID, worker.CatalogRevision, instance, provider.QuotaPoolID, admissionSorted(provider.Models)})
			}
		}
	}
	if len(grants) == 0 {
		return fail("no authored eligible worker authorizes model; configure an eligible project binding, exact model authorization and quota pool binding")
	}
	sort.Slice(grants, func(i, j int) bool { return admissionDigest(grants[i]) < admissionDigest(grants[j]) })
	return metadata, grants, nil
}
