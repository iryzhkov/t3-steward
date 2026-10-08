package campaign

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func fixFixture() (FixSource, FixOptions) {
	return FixSource{
		RunID:          "run-source",
		Workflow:       domain.Workflow{Name: "source", Project: "project", Class: domain.TaskClassSurplus, Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: strings.Repeat("a", 40)}},
		Producer:       domain.Task{Name: "implement", Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "model", QuotaPoolID: "pool", Options: map[string]string{"effort": "medium"}}}, Placement: domain.Placement{Hosts: []string{"worker"}}, ResourcePreset: "build", MaxTurns: 8, Verification: []string{"go test ./..."}, Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{Revision: "HEAD"}}, {Name: "handoff.md"}}},
		Review:         domain.Task{Name: "review", Routes: []domain.ProviderRoute{{ProviderInstanceID: "claude", Model: "review-model"}}, MaxTurns: 5, Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}}},
		ReviewedCommit: strings.Repeat("b", 40),
	}, FixOptions{Name: "fix-source", Lineage: FixLineage{Schema: FixLineageSchema, RootRun: "run-source", RootProducingTask: "implement", RootReviewTask: "review", RoundLimit: 4, RoundsDeclared: 2}, Gate: &domain.TaskGate{Commands: []string{"make check-review"}, Timeout: 30 * time.Minute}, Brief: []CompiledFile{{Path: "inputs/fix/brief/prompt.md", Content: []byte("Original brief\n")}}}
}

func fixManifest(t *testing.T, unit CompiledUnit) backlog.Manifest {
	t.Helper()
	for _, f := range unit.Files {
		if f.Path == "workflow.yaml" {
			m, e := backlog.ParseManifest(f.Content)
			if e != nil {
				t.Fatal(e)
			}
			return m
		}
	}
	t.Fatal("missing workflow")
	return backlog.Manifest{}
}
func TestFixChainTwoRoundTemplate(t *testing.T) {
	s, o := fixFixture()
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range u.Files {
		want, err := os.ReadFile(filepath.Join("testdata/fix/two-round", filepath.FromSlash(f.Path)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, f.Content) {
			t.Fatalf("golden differs: %s", f.Path)
		}
	}
	m := fixManifest(t, u)
	if len(m.Tasks) != 4 {
		t.Fatal(m.Tasks)
	}
	f := m.Tasks["fix1"]
	if !reflect.DeepEqual([]string(f.Needs), []string{"run-source/implement", "run-source/review"}) {
		t.Fatal(f.Needs)
	}
	if !reflect.DeepEqual(f.InputsFrom["run-source/implement"], []string{"implementation", "handoff.md"}) {
		t.Fatal(f.InputsFrom)
	}
	if f.Gate != nil || m.Tasks["fix2"].Gate == nil {
		t.Fatal("gate placement")
	}
	if f.MaxTurns != 8 || f.Resources.Preset != "build" || !reflect.DeepEqual(f.Placement.Hosts, []string{"worker"}) || f.Routes[0].Options["effort"] != "medium" {
		t.Fatal(f)
	}
	if f.Commits[0].Name != "fix" || f.Verify[len(f.Verify)-1] != "git diff --quiet && git diff --cached --quiet" {
		t.Fatal(f)
	}
	for _, n := range []string{"review2", "review3"} {
		if m.Tasks[n].ReviewOutput.Verdict != "verdict.json" || m.Tasks[n].MaxTurns != 5 {
			t.Fatal(m.Tasks[n])
		}
	}
	if !reflect.DeepEqual(m.Tasks["review3"].InputsFrom["fix2"], []string{"fix", "handoff.md", "verification.log", "gate/log.txt"}) {
		t.Fatal(m.Tasks["review3"])
	}
	for _, f := range u.Files {
		if strings.HasSuffix(f.Path, ".md") && strings.HasPrefix(f.Path, "prompts/") && !bytes.Contains(f.Content, []byte(".t3/dependencies/")) {
			t.Fatal(f.Path)
		}
	}
}
func TestFixChainSingleRoundWhenOneRemains(t *testing.T) {
	s, o := fixFixture()
	o.Lineage.RoundsUsedBefore = 3
	o.Lineage.RoundsDeclared = 1
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	m := fixManifest(t, u)
	if len(m.Tasks) != 2 || m.Tasks["fix1"].Gate == nil {
		t.Fatal(m.Tasks)
	}
}
func writeFixTest(t *testing.T, u CompiledUnit) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range u.Files {
		p := filepath.Join(dir, filepath.FromSlash(f.Path))
		if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(p, f.Content, 0600); e != nil {
			t.Fatal(e)
		}
	}
	return dir
}
func TestFixChainParsesAndPacks(t *testing.T) {
	for _, rounds := range []int{1, 2} {
		for _, gate := range []bool{false, true} {
			s, o := fixFixture()
			o.Lineage.RoundsDeclared = rounds
			if !gate {
				o.Gate = nil
			}
			u, e := GenerateFixChain(s, o)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = Prepare(writeFixTest(t, u), DefaultLimits); e != nil {
				t.Fatal(e)
			}
		}
	}
}
func TestFixChainIsDeterministic(t *testing.T) {
	s, o := fixFixture()
	a, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	b, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("files differ")
	}
	x, e := Prepare(writeFixTest(t, a), DefaultLimits)
	if e != nil {
		t.Fatal(e)
	}
	y, e := Prepare(writeFixTest(t, b), DefaultLimits)
	if e != nil {
		t.Fatal(e)
	}
	if x.ContentDigest != y.ContentDigest {
		t.Fatal("digest differs")
	}
}
func TestFixChainCarriesBriefAndLineageForward(t *testing.T) {
	s, o := fixFixture()
	o.Brief = append(o.Brief, CompiledFile{Path: "inputs/fix/brief/inputs/rules.md", Content: []byte("original rules")})
	o.Lineage.History = []FixLineageEntry{{Run: "run-old", ReviewTask: "review3", ReviewedCommit: strings.Repeat("c", 40), Verdict: "changes-requested", BlockingFindings: 2}}
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range o.Brief {
		found := false
		for _, got := range u.Files {
			if got.Path == want.Path {
				found = bytes.Equal(got.Content, want.Content)
			}
		}
		if !found {
			t.Fatal(want.Path)
		}
	}
	found := false
	for _, f := range u.Files {
		if f.Path == "inputs/fix/lineage.json" {
			found = bytes.Contains(f.Content, []byte("run-old"))
		}
	}
	if !found {
		t.Fatal("missing history")
	}
}
func TestWriteFixChainRefusesExistingAndUnsafePaths(t *testing.T) {
	s, o := fixFixture()
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "Arbitrary Output")
	got, e := WriteFixChain(target, u)
	if e != nil || got != target {
		t.Fatalf("%s %v", got, e)
	}
	if _, e = WriteFixChain(target, u); e == nil {
		t.Fatal("existing accepted")
	}
	bad := filepath.Join(t.TempDir(), "bad")
	u.Files = append(u.Files, CompiledFile{Path: "../escape", Content: []byte("bad")})
	if _, e = WriteFixChain(bad, u); e == nil {
		t.Fatal("unsafe accepted")
	}
	if _, e = os.Stat(bad); !os.IsNotExist(e) {
		t.Fatal("invalid unit wrote output")
	}
}

func TestFixChainIngestsAgainstSucceededSource(t *testing.T) {
	ctx := context.Background()
	store, e := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() {
		_ = filepath.Walk(storage, func(p string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				_ = os.Chmod(p, 0700)
			}
			return nil
		})
	})
	svc := backlog.SubmissionService{Store: store, StorageRoot: storage, MaxFiles: DefaultLimits.MaxFiles, MaxBytes: DefaultLimits.MaxBytes}
	s, o := fixFixture()
	manifest := backlog.Manifest{Version: 2, Name: "source", Class: s.Workflow.Class, Environment: backlog.ManifestEnvironment{Project: s.Workflow.Project, Type: "git", Scope: "task", Ref: s.Workflow.Environment.Ref}, Tasks: map[string]backlog.ManifestTask{
		"implement": {PromptFile: "prompts/implement.md", Outputs: []string{"handoff.md"}, Commits: []backlog.ManifestCommit{{Name: "implementation", Revision: "HEAD"}}, Routes: []backlog.ManifestRoute{{Instance: "codex", Model: "model"}}},
		"review":    {PromptFile: "prompts/review.md", Needs: []string{"implement"}, InputsFrom: map[string][]string{"implement": {"implementation", "handoff.md"}}, Outputs: []string{"review.md", "verdict.json"}, Routes: []backlog.ManifestRoute{{Instance: "claude", Model: "model"}}},
	}}
	raw, e := yaml.Marshal(manifest)
	if e != nil {
		t.Fatal(e)
	}
	dir := writeFixTest(t, CompiledUnit{Files: []CompiledFile{{Path: "workflow.yaml", Content: raw}, {Path: "prompts/implement.md", Content: []byte("brief")}, {Path: "prompts/review.md", Content: []byte("review")}}})
	source, e := svc.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "fix-source-fixture", BundleDir: dir})
	if e != nil {
		t.Fatal(e)
	}
	records, e := store.LoadCoordinatorRecords(ctx)
	if e != nil {
		t.Fatal(e)
	}
	s.RunID = source.Record.RunID
	now := time.Now().UTC()
	for i := range records.WorkflowRuns {
		records.WorkflowRuns[i].Progress = domain.ProgressSucceeded
	}
	for _, task := range records.Tasks {
		var attempt domain.Attempt
		for i := range records.Attempts {
			if records.Attempts[i].TaskID == task.ID {
				records.Attempts[i].Progress = domain.ProgressSucceeded
				records.Attempts[i].Control = domain.ControlStopped
				attempt = records.Attempts[i]
			}
		}
		if attempt.ID == "" {
			t.Fatal("missing source attempt")
		}
		if task.Name == "implement" {
			s.Producer = task
		} else {
			s.Review = task
		}
		for _, out := range task.Outputs {
			payload := []byte("retained fixture output")
			file := filepath.Join(storage, "fixture", out.Name)
			if e = os.MkdirAll(filepath.Dir(file), 0700); e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(file, payload, 0600); e != nil {
				t.Fatal(e)
			}
			sum := sha256.Sum256(payload)
			records.Artifacts = append(records.Artifacts, domain.Artifact{ID: "output-" + task.Name + "-" + out.Name, WorkflowRunID: s.RunID, TaskID: task.ID, AttemptID: attempt.ID, Kind: domain.ArtifactOutput, Name: out.Name, Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:]), StoragePath: "fixture/" + out.Name, CreatedAt: now})
		}
	}
	if e = store.SaveCoordinatorRecords(ctx, records); e != nil {
		t.Fatal(e)
	}
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	req := backlog.DirectorySubmission{IdempotencyKey: "fix-ingestion-fixture", BundleDir: writeFixTest(t, u)}
	first, e := svc.SubmitDirectory(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	second, e := svc.SubmitDirectory(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	if !second.Replay || first.Record.RunID != second.Record.RunID {
		t.Fatal("replay failed")
	}
	records, e = store.LoadCoordinatorRecords(ctx)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, task := range records.Tasks {
		if task.RunID == first.Record.RunID && task.Name == "fix1" {
			found = true
			if !slices.Contains(task.Placement.Capabilities, workerproto.PackageCapabilityCommitBundle) {
				t.Fatal(task.Placement)
			}
			names := map[string]bool{}
			for _, in := range task.CarriedInputs {
				if in.SourceRunID != s.RunID {
					t.Fatal(in)
				}
				names[in.Name] = true
			}
			for _, name := range []string{"implementation", "handoff.md", "review.md", "verdict.json"} {
				if !names[name] {
					t.Fatal(name)
				}
			}
		}
	}
	if !found {
		t.Fatal("missing fix1")
	}
}

func TestFixChainRefusesNameCollisions(t *testing.T) {
	for _, name := range []string{"rules.md", "lineage.json", "fix1.md", "workflow.yaml"} {
		s, o := fixFixture()
		o.Context = []CompiledFile{{Path: name, Content: []byte("context")}}
		if _, e := GenerateFixChain(s, o); e == nil {
			t.Fatal(name)
		}
	}
	s, o := fixFixture()
	o.Context = []CompiledFile{{Path: "notes.md"}, {Path: "notes.md"}}
	if _, e := GenerateFixChain(s, o); e == nil {
		t.Fatal("duplicate")
	}
}
