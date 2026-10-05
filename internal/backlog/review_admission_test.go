package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type admissionFixtureCatalog struct{ catalog AdmissionCatalog }

func (c *admissionFixtureCatalog) ReviewAdmissionCatalog(context.Context, string) (AdmissionCatalog, error) {
	return c.catalog, nil
}

type admissionCountingStore struct {
	*sqlite.Store
	freezes int
	dbPath  string // Disposable fixture path for narrow retained child corruption tests.
}

func (s *admissionCountingStore) FreezeReviewAuthority(ctx context.Context, a review.FrozenAuthority) (review.FrozenAuthority, error) {
	s.freezes++
	return s.Store.FreezeReviewAuthority(ctx, a)
}

type admissionFixture struct {
	service ReviewAdmissionService
	request AdmissionRequest
	records sqlite.CoordinatorRecords
	store   *admissionCountingStore
	catalog *admissionFixtureCatalog
	source  string
}

func newAdmissionFixture(t *testing.T) admissionFixture {
	t.Helper()
	ctx := context.Background()
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "inputs/criteria.md", "retained criteria")
	writeBundleFile(t, bundle, "inputs/context.md", "context")
	rewriteBundleManifest(t, bundle, "version: 2\nname: admission\npinned_inputs: true\nenvironment: {project: t3-steward, ref: "+strings.Repeat("c", 40)+"}\ninputs: [inputs/*.md]\ntasks:\n  inspect: {prompt_file: prompts/inspect.md}\n")
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	root := filepath.Join(t.TempDir(), "retained")
	t.Cleanup(func() { _ = removeIngestedTree(root) })
	ingested, err := (BundleIngester{Store: db, StorageRoot: root}).Ingest(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	records := ingested.Records
	a := &records.Attempts[0]
	a.Revision = 1
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	a.ThreadID = "thread"
	a.AssignmentID = "assignment"
	records.Assignments = []domain.Assignment{{ID: a.AssignmentID, AttemptID: a.ID, Project: "t3-steward", ThreadID: a.ThreadID, Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, WorkerID: "worker", CreatedAt: time.Now(), UpdatedAt: time.Now()}}
	if err := db.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectCatalog([]ProjectDefinition{{Name: "t3-steward", Repository: "https://example.org/steward.git", DefaultRef: "main", SetupProfile: "setup"}}, []SetupProfile{{Name: "setup", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	catalog := &admissionFixtureCatalog{AdmissionCatalog{AuthoredWorkers: []domain.WorkerInventory{{ID: "worker", CatalogRevision: "catalog-v1", Projects: []domain.WorkerProjectInventory{{Name: "t3-steward"}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"org/sol"}}, {InstanceID: "other", Models: []string{"model"}}}}}, Classifications: []AdmissionClassification{{"codex/org/sol", "openai", "executor"}, {"other/model", "other-family", "critical"}}}}
	criteria := ""
	for _, artifact := range records.Artifacts {
		if artifact.Name == "inputs/criteria.md" {
			criteria = artifact.ID
		}
	}
	store := &admissionCountingStore{Store: db, dbPath: dbPath}
	service := ReviewAdmissionService{Store: store, Artifacts: CoordinatorArtifactStore{Catalog: db, SubmissionRoot: root}, Projects: projects, Catalog: catalog}
	request := AdmissionRequest{RunID: ingested.RunID, TaskID: records.Tasks[0].ID, AttemptID: a.ID, Policy: AdmissionPolicy{Risk: "routine", CriteriaArtifactID: criteria, RequiredReviewers: 2, MinProviderFamilies: 2, Members: []AdmissionMember{{ID: "one", Role: "independent", Route: "codex/org/sol", Required: true}, {ID: "two", Role: "independent", Route: "other/model", Required: true}}}}
	return admissionFixture{service, request, records, store, catalog, bundle}
}

func TestReviewAdmissionRetainedSQLiteFreezeReplayAndCopies(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	first, err := f.service.Freeze(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Authority.Requirements.CriteriaDigest != admissionDigestBytes([]byte("retained criteria")) || first.Provenance.InputManifest.Digest != f.records.Workflows[0].InputManifest.Digest || first.Provenance.RepositoryURL != "https://example.org/steward.git" {
		t.Fatalf("wrong provenance: %#v", first)
	}
	if first.Authority.Parent.IssuedRevision != f.records.Attempts[0].Revision || first.Authority.Parent.ExecutorRoute != "codex/sol" {
		t.Fatal("caller identity used")
	}
	// Mutating original submitted source cannot alter retained authority.
	writeBundleFile(t, f.source, "inputs/criteria.md", "mutated source")
	request := f.request
	request.Policy.Members = append([]AdmissionMember(nil), request.Policy.Members...)
	request.Policy.Members[0], request.Policy.Members[1] = request.Policy.Members[1], request.Policy.Members[0]
	f.catalog.catalog.AuthoredWorkers[0].Providers[0], f.catalog.catalog.AuthoredWorkers[0].Providers[1] = f.catalog.catalog.AuthoredWorkers[0].Providers[1], f.catalog.catalog.AuthoredWorkers[0].Providers[0]
	second, err := f.service.Freeze(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("canonical replay changed")
	}
	first.Authority.Requirements.Members[0].ProviderFamily = "fake"
	first.Provenance.InputManifest.Entries[0].SHA256 = strings.Repeat("0", 64)
	third, err := f.service.Resolve(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, third) {
		t.Fatal("returned slices alias trusted records")
	}
}

func admissionDigestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Insert a different immutable artifact and refer to it from the fixture.
// Existing retained metadata remains immutable under the real SQLite API.
func admissionReplaceCriteria(f *admissionFixture, mutate func(*domain.Artifact)) {
	old := f.request.Policy.CriteriaArtifactID
	for _, artifact := range f.records.Artifacts {
		if artifact.ID != old {
			continue
		}
		artifact.ID += "-replacement"
		mutate(&artifact)
		f.records.Artifacts = append(f.records.Artifacts, artifact)
		f.request.Policy.CriteriaArtifactID = artifact.ID
		for _, ids := range [][]string{f.records.Tasks[0].InputArtifactIDs, f.records.Workflows[0].InputArtifactIDs, f.records.WorkflowRuns[0].InputArtifactIDs} {
			for i, id := range ids {
				if id == old {
					ids[i] = artifact.ID
				}
			}
		}
		return
	}
}
func TestReviewAdmissionRejectsProvenanceBeforeFreeze(t *testing.T) {
	cases := map[string]func(*admissionFixture){
		"missing retained file": func(f *admissionFixture) {
			for _, a := range f.records.Artifacts {
				if a.ID == f.request.Policy.CriteriaArtifactID {
					object := filepath.Join(f.service.Artifacts.SubmissionRoot, a.StoragePath)
					if err := os.Chmod(filepath.Dir(object), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.Remove(object); err != nil {
						t.Fatal(err)
					}
				}
			}
		},
		"missing tier":           func(f *admissionFixture) { f.catalog.catalog.Classifications[0].Tier = "" },
		"missing family":         func(f *admissionFixture) { f.catalog.catalog.Classifications[0].ProviderFamily = "" },
		"fake classification":    func(f *admissionFixture) { f.catalog.catalog.Classifications[0].Tier = "fake" },
		"changed task digest":    func(f *admissionFixture) { f.records.Assignments[0].TaskDigest = strings.Repeat("0", 64) },
		"wrong run identity":     func(f *admissionFixture) { f.request.RunID = "foreign" },
		"wrong task identity":    func(f *admissionFixture) { f.request.TaskID = "foreign" },
		"wrong attempt identity": func(f *admissionFixture) { f.request.AttemptID = "foreign" },
		"undeclared task":        func(f *admissionFixture) { f.records.Workflows[0].TaskIDs = nil },
		"duplicate input name": func(f *admissionFixture) {
			for _, a := range f.records.Artifacts {
				if a.ID == f.request.Policy.CriteriaArtifactID {
					a.ID += "-duplicate"
					f.records.Artifacts = append(f.records.Artifacts, a)
					f.records.Workflows[0].InputArtifactIDs = append(f.records.Workflows[0].InputArtifactIDs, a.ID)
					f.records.WorkflowRuns[0].InputArtifactIDs = append(f.records.WorkflowRuns[0].InputArtifactIDs, a.ID)
					break
				}
			}
		},
		"missing criteria": func(f *admissionFixture) { f.request.Policy.CriteriaArtifactID = "missing" },
		"prompt":           func(f *admissionFixture) { f.request.Policy.CriteriaArtifactID = f.records.Tasks[0].PromptArtifactID },
		"workflow manifest": func(f *admissionFixture) {
			for _, a := range f.records.Artifacts {
				if a.Name == "workflow.yaml" {
					f.request.Policy.CriteriaArtifactID = a.ID
					f.records.Tasks[0].InputArtifactIDs = append(f.records.Tasks[0].InputArtifactIDs, a.ID)
				}
			}
		},
		"not task declared":     func(f *admissionFixture) { f.records.Tasks[0].InputArtifactIDs = nil },
		"not workflow declared": func(f *admissionFixture) { f.records.Workflows[0].InputArtifactIDs = nil },
		"foreign run": func(f *admissionFixture) {
			admissionReplaceCriteria(f, func(a *domain.Artifact) { a.WorkflowRunID = "foreign" })
		},
		"output": func(f *admissionFixture) {
			admissionReplaceCriteria(f, func(a *domain.Artifact) { a.Kind = domain.ArtifactOutput })
		},
		"missing manifest": func(f *admissionFixture) { f.records.Workflows[0].InputManifest = nil },
		"fake digest":      func(f *admissionFixture) { f.records.Workflows[0].InputManifest.Digest = strings.Repeat("0", 64) },
		"duplicate pins": func(f *admissionFixture) {
			m := f.records.Workflows[0].InputManifest
			m.Entries = append(m.Entries, m.Entries[0])
		},
		"duplicate membership": func(f *admissionFixture) {
			f.records.Tasks[0].InputArtifactIDs = append(f.records.Tasks[0].InputArtifactIDs, f.request.Policy.CriteriaArtifactID)
		},
		"oversized": func(f *admissionFixture) {
			m := f.records.Workflows[0].InputManifest
			m.Entries[0].Size = pinnedinput.MaxFileBytes + 1
		},
		"manifest entry mismatch": func(f *admissionFixture) {
			m := f.records.Workflows[0].InputManifest
			m.Entries[0].Size++
			canonical, _ := pinnedinput.NewManifest(m.Entries)
			*m = canonical
		},
		"branch":      func(f *admissionFixture) { f.records.Workflows[0].Environment.Ref = "main" },
		"default ref": func(f *admissionFixture) { f.records.Workflows[0].Environment.Ref = "" },
		"fresh":       func(f *admissionFixture) { f.records.Workflows[0].Environment.Type = EnvironmentFresh },
		"stale attempt": func(f *admissionFixture) {
			other := f.records.Attempts[0]
			other.ID = "new-attempt"
			other.Number++
			f.records.Attempts = append(f.records.Attempts, other)
		},
		"terminal":               func(f *admissionFixture) { f.records.WorkflowRuns[0].Progress = domain.ProgressSucceeded },
		"unclaimed":              func(f *admissionFixture) { f.records.Assignments[0].State = domain.AssignmentOffered },
		"thread mismatch":        func(f *admissionFixture) { f.records.Assignments[0].ThreadID = "different" },
		"missing classification": func(f *admissionFixture) { f.catalog.catalog.Classifications = nil },
		"conflicting classification": func(f *admissionFixture) {
			f.catalog.catalog.Classifications = append(f.catalog.catalog.Classifications, f.catalog.catalog.Classifications[0])
		},
		"unauthorized": func(f *admissionFixture) {
			f.catalog.catalog.AuthoredWorkers[0].Providers[0].Models = []string{"wrong"}
		},
		"no authored project": func(f *admissionFixture) { f.catalog.catalog.AuthoredWorkers[0].Projects = nil },
		"wildcard route":      func(f *admissionFixture) { f.request.Policy.Members[0].Route = "codex/*" },
		"mixed wildcard": func(f *admissionFixture) {
			f.catalog.catalog.AuthoredWorkers[0].Providers[0].Models = []string{"*", "org/sol"}
		},
		"changed retained": func(f *admissionFixture) {
			for _, a := range f.records.Artifacts {
				if a.ID == f.request.Policy.CriteriaArtifactID {
					p := filepath.Join(f.service.Artifacts.SubmissionRoot, a.StoragePath)
					if err := os.Chmod(p, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(p, []byte("tampered criteria"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			mutate(&f)
			// Save altered coordinator fixture records, never a live database.
			if err := f.store.SaveCoordinatorRecords(context.Background(), f.records); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.Freeze(context.Background(), f.request); err == nil {
				t.Fatal("invalid provenance accepted")
			}
			if f.store.freezes != 0 {
				t.Fatal("failed provenance reached freeze")
			}
		})
	}
}

func TestReviewAdmissionAuthoredWildcardAndRiskRules(t *testing.T) {
	f := newAdmissionFixture(t)
	f.catalog.catalog.AuthoredWorkers[0].Providers[0].Models = []string{"*"}
	// Unavailable authored workers still authorize; availability cannot classify.
	f.request.Policy.Risk = "risky"
	snapshot, err := f.service.Resolve(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Authority.Requirements.RoundLimit != 3 || snapshot.Authority.Requirements.Members[0].Route != "codex/org/sol" {
		t.Fatal("slash route or risk ceiling changed")
	}
	f.catalog.catalog.Classifications[1].Tier = "executor"
	if _, err := f.service.Resolve(context.Background(), f.request); err == nil {
		t.Fatal("risky admission without critical independent member")
	}
	f.catalog.catalog.Classifications[1].Tier = "critical"
	f.catalog.catalog.Classifications[1].ProviderFamily = "openai"
	if _, err := f.service.Resolve(context.Background(), f.request); err == nil {
		t.Fatal("one-family waiver")
	}
}

func TestReviewAdmissionFrozenConflicts(t *testing.T) {
	for _, name := range []string{"policy", "catalog", "criteria", "identity"} {
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			ctx := context.Background()
			if _, err := f.service.Freeze(ctx, f.request); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "policy":
				f.request.Policy.RoundLimit = 1
			case "catalog":
				f.catalog.catalog.AuthoredWorkers[0].CatalogRevision = "v2"
			case "criteria":
				for _, a := range f.records.Artifacts {
					if a.Name == "inputs/context.md" {
						f.request.Policy.CriteriaArtifactID = a.ID
					}
				}
			case "identity":
				f.records.Attempts[0].ThreadID = "new-thread"
				f.records.Assignments[0].ThreadID = "new-thread"
				if err := f.store.SaveCoordinatorRecords(ctx, f.records); err != nil {
					t.Fatal(err)
				}
			}
			_, err := f.service.Freeze(ctx, f.request)
			if !errors.Is(err, sqlite.ErrReviewAuthorityConflict) {
				t.Fatalf("want conflict, got %v", err)
			}
		})
	}
}
