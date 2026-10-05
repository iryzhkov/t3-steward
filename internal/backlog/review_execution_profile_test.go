package backlog

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func executionManifestYAML() string {
	raw := strings.Replace(declaredManifestYAML(), "version: 1", "version: 2", 1)
	return strings.ReplaceAll(raw, "required: true}", "required: true, execution: {effort: medium, quota_pool: review-pool, max_turns: 9, resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}}}")
}

func TestReviewExecutionProfilePolicyDigestAndExactGrantCustody(t *testing.T) {
	f := executionFixture(t)
	ctx := context.Background()
	extra := domain.WorkerInventory{ID: "alt-worker", CatalogRevision: "alt-v1", Projects: []domain.WorkerProjectInventory{{Name: "t3-steward"}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"org/sol"}, QuotaPoolID: "alt-pool"}}}
	f.catalog.catalog.AuthoredWorkers = append(f.catalog.catalog.AuthoredWorkers, extra)
	original, err := f.service.Resolve(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	f.catalog.catalog.AuthoredWorkers[1].CatalogRevision = "alt-v2"
	unchanged, err := f.service.Resolve(ctx, f.request)
	if err != nil || original.Provenance.PolicyDigest != unchanged.Provenance.PolicyDigest {
		t.Fatal("unselected quota grants affected frozen policy", err)
	}
	changes := map[string]func(*domain.ReviewExecutionProfile){
		"effort":     func(p *domain.ReviewExecutionProfile) { p.Effort = "high" },
		"pool":       func(p *domain.ReviewExecutionProfile) { p.QuotaPoolID = "alt-pool" },
		"turns":      func(p *domain.ReviewExecutionProfile) { p.MaxTurns++ },
		"floor":      func(p *domain.ReviewExecutionProfile) { p.Resources.MinCPUClass = domain.CPUClassLow },
		"preference": func(p *domain.ReviewExecutionProfile) { p.Resources.PreferredCPUClass = domain.CPUClassMedium },
		"cpu":        func(p *domain.ReviewExecutionProfile) { p.Resources.CPUUnits++ },
		"memory":     func(p *domain.ReviewExecutionProfile) { p.Resources.MemoryMB++ },
		"scratch":    func(p *domain.ReviewExecutionProfile) { p.Resources.ScratchMB++ },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			request := f.request
			request.Policy.Members = append([]AdmissionMember(nil), request.Policy.Members...)
			for i := range request.Policy.Members {
				request.Policy.Members[i].Execution = domain.CloneReviewExecution(request.Policy.Members[i].Execution)
			}
			change(request.Policy.Members[0].Execution)
			next, err := f.service.Resolve(ctx, request)
			if err != nil || next.Provenance.PolicyDigest == original.Provenance.PolicyDigest || next.Authority.RequirementsDigest == original.Authority.RequirementsDigest {
				t.Fatal("profile field missing from frozen policy", err)
			}
		})
	}
}
func TestReviewExecutionProfileLegacyPolicyAndDeclarationGolden(t *testing.T) {
	// Preserve the old policy/member capitalization and canonical layout exactly.
	p := AdmissionPolicy{Risk: "routine", CriteriaArtifactID: "criteria", RequiredReviewers: 2, MinProviderFamilies: 2, RoundLimit: 2, Members: []AdmissionMember{{ID: "one", Role: "independent", Route: "codex/sol", Required: true}}}
	expected := `{"Risk":"routine","CriteriaArtifactID":"criteria","RequiredReviewers":2,"MinProviderFamilies":2,"RoundLimit":2,"Members":[{"ID":"one","Role":"independent","Route":"codex/sol","Required":true}]}`
	raw, err := json.Marshal(p)
	if err != nil || string(raw) != expected {
		t.Fatalf("legacy admission policy changed: %s %v", raw, err)
	}
	if admissionDigest(p) != admissionDigest(json.RawMessage(expected)) {
		t.Fatal("legacy policy digest changed")
	}
	m, err := ParseManifest([]byte(declaredManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	c := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{ID: "criteria"})
	raw, err = json.Marshal(c.Members)
	expected = `[{"id":"one","role":"independent","route":"codex/org/sol","required":true},{"id":"two","role":"independent","route":"other/model","required":true}]`
	if err != nil || string(raw) != expected {
		t.Fatalf("legacy declaration member bytes changed: %s %v", raw, err)
	}
	d := domain.CloneTaskReview(c)
	d.Members[0].Execution = &domain.ReviewExecutionProfile{}
	if domain.ValidateTaskReviewExecution(d) == nil {
		t.Fatal("version 1 domain execution accepted")
	}
}

func executionFixture(t *testing.T) admissionFixture {
	t.Helper()
	f := newAdmissionFixture(t)
	for i := range f.catalog.catalog.AuthoredWorkers[0].Providers {
		f.catalog.catalog.AuthoredWorkers[0].Providers[i].QuotaPoolID = "review-pool"
	}
	rewriteBundleManifest(t, f.source, executionManifestYAML())
	ingested, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	f.records = ingested.Records
	a := &f.records.Attempts[0]
	a.Revision = 1
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	a.ThreadID = "profile-thread"
	a.AssignmentID = "profile-assignment"
	task, run := f.records.Tasks[0], f.records.WorkflowRuns[0]
	f.records.Assignments = []domain.Assignment{{ID: a.AssignmentID, DispatchToken: "profile-token", AttemptID: a.ID, Project: "t3-steward", ThreadID: a.ThreadID, Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "org/sol"}, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision}}
	if err = f.store.SaveCoordinatorRecords(context.Background(), f.records); err != nil {
		t.Fatal(err)
	}
	f.request = AdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: a.ID, Policy: declaredPolicy(task.ReviewRequirements)}
	return f
}
func TestReviewExecutionProfileParserCompiler(t *testing.T) {
	m, err := ParseManifest([]byte(executionManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	r := m.Tasks["inspect"].ReviewRequirements
	c := compileTaskReview(r, "w", "r", "t", domain.Artifact{ID: "criteria"})
	expected := domain.ReviewExecutionProfile{Effort: "medium", QuotaPoolID: "review-pool", MaxTurns: 9, Resources: domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 1.5, MemoryMB: 512, ScratchMB: 64}}
	if c.Version != 2 || *c.Members[0].Execution != expected {
		t.Fatalf("wrong compiled profile: %+v", c)
	}
	*r.Members[0].Execution.Resources.CPUUnits = 99
	if *c.Members[0].Execution != expected || *c.Members[1].Execution != expected {
		t.Fatal("compiler aliases declaration or siblings")
	}
	copied := m
	applyManifestDefaults(&copied)
	copied.Tasks["inspect"].ReviewRequirements.Members[0].Execution.Effort = "high"
	// The map is shared by this artificial copy, but the original pointer is detached.
	if r.Members[0].Execution.Effort != "medium" {
		t.Fatal("manifest defaults alias source pointers")
	}
	light := strings.ReplaceAll(executionManifestYAML(), "preset: build", "preset: light")
	lm, err := ParseManifest([]byte(light))
	if err != nil {
		t.Fatal(err)
	}
	lc := compileTaskReview(lm.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
	if lc.Members[0].Execution.Resources.MinCPUClass != domain.CPUClassLow || lc.Members[0].Execution.Resources.PreferredCPUClass != "" {
		t.Fatal("light preset wrong")
	}
}
func TestReviewExecutionProfileUnsafeMissingMixed(t *testing.T) {
	raw := executionManifestYAML()
	cases := map[string]string{
		"v1":             strings.Replace(raw, "version: 2\n      risk", "version: 1\n      risk", 1),
		"v1-null":        strings.Replace(declaredManifestYAML(), "required: true}", "required: true, execution: null}", 1),
		"missing":        strings.Replace(raw, ", execution: {effort: medium, quota_pool: review-pool, max_turns: 9, resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}}", "", 1),
		"empty":          strings.ReplaceAll(raw, "effort: medium", "effort: ''"),
		"max":            strings.ReplaceAll(raw, "effort: medium", "effort: max"),
		"whitespace":     strings.ReplaceAll(raw, "effort: medium", "effort: ' medium'"),
		"control":        strings.ReplaceAll(raw, "effort: medium", "effort: \"medium\\n\""),
		"pool":           strings.ReplaceAll(raw, "quota_pool: review-pool", "quota_pool: '../bad'"),
		"missing-pool":   strings.ReplaceAll(raw, "quota_pool: review-pool, ", ""),
		"zero-turn":      strings.ReplaceAll(raw, "max_turns: 9", "max_turns: 0"),
		"big-turn":       strings.ReplaceAll(raw, "max_turns: 9", "max_turns: 33"),
		"negative-turn":  strings.ReplaceAll(raw, "max_turns: 9", "max_turns: -1"),
		"missing-turn":   strings.ReplaceAll(raw, "max_turns: 9, ", ""),
		"null-resources": strings.ReplaceAll(raw, "resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}", "resources: null"),
		"floor":          strings.ReplaceAll(raw, "preset: build, ", ""),
		"preset":         strings.ReplaceAll(raw, "preset: build", "preset: magic"),
		"class":          strings.ReplaceAll(raw, "preset: build", "min_cpu_class: bogus"),
		"preference":     strings.ReplaceAll(raw, "preset: build", "min_cpu_class: high, preferred_cpu_class: low"),
		"cpu-zero":       strings.ReplaceAll(raw, "cpu_units: 1.5", "cpu_units: 0"),
		"cpu-negative":   strings.ReplaceAll(raw, "cpu_units: 1.5", "cpu_units: -1"),
		"nan":            strings.ReplaceAll(raw, "cpu_units: 1.5", "cpu_units: .nan"),
		"inf":            strings.ReplaceAll(raw, "cpu_units: 1.5", "cpu_units: .inf"),
		"memory":         strings.ReplaceAll(raw, "memory_mb: 512", "memory_mb: 0"),
		"scratch":        strings.ReplaceAll(raw, "scratch_mb: 64", "scratch_mb: -1"),
		"option-map":     strings.ReplaceAll(raw, "effort: medium", "effort: medium, options: {unsafe: true}"),
	}
	for name, candidate := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(candidate)); err == nil {
				t.Fatal("unsafe declaration accepted")
			}
		})
	}
	for _, turns := range []string{"1", "32"} {
		candidate := strings.ReplaceAll(raw, "max_turns: 9", "max_turns: "+turns)
		candidate = strings.ReplaceAll(candidate, "effort: medium", "effort: high")
		if _, err := ParseManifest([]byte(candidate)); err != nil {
			t.Fatal(err)
		}
	}
	// Frozen domain input also refuses nonfinite values.
	p := domain.ReviewExecutionProfile{Effort: "medium", QuotaPoolID: "pool", MaxTurns: 1, Resources: domain.ResourceDemand{MinCPUClass: domain.CPUClassLow, CPUUnits: math.NaN()}}
	if p.Validate() == nil {
		t.Fatal("frozen NaN accepted")
	}
}
func TestReviewExecutionProfileStaticGrantAndRefusal(t *testing.T) {
	for _, kind := range []string{"pool", "project", "instance", "model", "other-project-pool", "other-instance-pool", "other-model-pool", "missing-pool"} {
		t.Run(kind, func(t *testing.T) {
			f := newAdmissionFixture(t)
			for i := range f.catalog.catalog.AuthoredWorkers[0].Providers {
				f.catalog.catalog.AuthoredWorkers[0].Providers[i].QuotaPoolID = "review-pool"
			}
			w := &f.catalog.catalog.AuthoredWorkers[0]
			switch kind {
			case "pool":
				w.Providers[0].QuotaPoolID = "different"
			case "project":
				w.Projects = nil
			case "instance":
				w.Providers[0].InstanceID = "different"
			case "model":
				w.Providers[0].Models = []string{"different"}
			case "missing-pool":
				w.Providers[0].QuotaPoolID = ""
			default:
				w.Providers[0].QuotaPoolID = "different"
				extra := domain.WorkerInventory{ID: "wrong-worker", Projects: []domain.WorkerProjectInventory{{Name: "t3-steward"}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"org/sol"}, QuotaPoolID: "review-pool"}}}
				if kind == "other-project-pool" {
					extra.Projects[0].Name = "different"
				}
				if kind == "other-instance-pool" {
					extra.Providers[0].InstanceID = "different"
				}
				if kind == "other-model-pool" {
					extra.Providers[0].Models = []string{"different"}
				}
				f.catalog.catalog.AuthoredWorkers = append(f.catalog.catalog.AuthoredWorkers, extra)
			}
			rewriteBundleManifest(t, f.source, executionManifestYAML())
			db := stagingSQL(t, f)
			beforeSQL := independentDeclaredTables(t, db)
			before, err := f.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source); err == nil {
				t.Fatal("bad static grant accepted")
			}
			after, err := f.store.LoadCoordinatorRecords(context.Background())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("refusal mutated records", err)
			}
			if beforeSQL != independentDeclaredTables(t, db) {
				t.Fatal("refusal mutated logical SQL/native audit")
			}
		})
	}
	// No live availability, draining or quota state is required by the static catalog.
	f := executionFixture(t)
	f.catalog.catalog.AuthoredWorkers[0].AcceptBacklog = false
	for i := range f.catalog.catalog.AuthoredWorkers[0].Providers {
		f.catalog.catalog.AuthoredWorkers[0].Providers[i].Available = false
	}
	first, err := f.service.FreezeDeclared(context.Background(), declaredRequest(f))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range first.Authority.Requirements.Members {
		if m.Execution == nil || m.Execution.QuotaPoolID != "review-pool" {
			t.Fatal("lost grant")
		}
	}
}
func TestReviewExecutionProfileOriginalAuthorityCatalogChangeAndCopies(t *testing.T) {
	f := executionFixture(t)
	ctx := context.Background()
	first, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(f.store.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	f.service.Store = reopened
	f.catalog.catalog = AdmissionCatalog{}
	a := f.records.Attempts[0]
	a.Revision++
	if err = reopened.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	next, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil || !reflect.DeepEqual(first, next) || next.Authority.Parent.IssuedRevision != 1 {
		t.Fatal("catalog changed original authority", err)
	}
	first.Authority.Requirements.Members[0].Execution.Effort = "high"
	again, err := f.service.ResolveDeclared(ctx, declaredRequest(f))
	if err != nil || !reflect.DeepEqual(next, again) {
		t.Fatal("snapshot mutated stored authority", err)
	}
	d := f.records.Tasks[0]
	digest := domain.TaskDigest(d)
	dag := cloneDAGState(DAGState{Tasks: []domain.Task{d}})
	planned := clonePlanningTask(d)
	runTasks := domain.TasksForRun(domain.WorkflowRun{ID: d.RunID, WorkflowID: d.WorkflowID}, []domain.Task{d})
	for _, p := range []*domain.Task{&dag.Tasks[0], &planned, &runTasks[0]} {
		p.ReviewRequirements.Members[0].Execution.Resources.MemoryMB++
	}
	if domain.TaskDigest(d) != digest {
		t.Fatal("graph clone aliases source")
	}
	// Exact JSON round-trip used by graph clone/rerun retains all execution fields.
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var copied domain.Task
	if err = json.Unmarshal(raw, &copied); err != nil || domain.TaskDigest(d) != domain.TaskDigest(copied) || !reflect.DeepEqual(d.ReviewRequirements, copied.ReviewRequirements) {
		t.Fatal("clone/rerun JSON custody", err)
	}
	copied.ReviewRequirements.Members[0].Execution.MaxTurns++
	if domain.TaskDigest(d) != digest || domain.TaskDigest(copied) == digest {
		t.Fatal("clone aliases or digest omits profile")
	}
}
