package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// CoordinatorRecordStore persists a complete ingestion snapshot atomically.
type CoordinatorRecordStore interface {
	SaveCoordinatorRecords(context.Context, sqlite.CoordinatorRecords) error
}

// ErrValidationUnavailable marks a refusal the submission did not earn: the
// coordinator could not run its own readiness validation at all.
//
// It is a distinct classification because the two refusals mean opposite things
// to whoever reads them. "This campaign can never run" is about the campaign
// and is fixed by changing it. "Validation was unavailable" is about this
// coordinator and is fixed by repairing it, after which the same idempotency
// key makes the retry safe and returns the same run.
//
// Accepting the submission instead was the earlier behaviour and was wrong. A
// permanent verdict must not come from the coordinator failing to ask itself a
// question, but silent acceptance is not the alternative to a wrong refusal: it
// creates the run the gate exists to prevent, with no record of why the gate
// did not run.
var ErrValidationUnavailable = errors.New("readiness validation unavailable")

// PermanentValidator refuses a manifest that can never run as written. It is
// applied inside ingestion rather than only on the client, because a client
// that skipped its own check, or a fleet that changed after the client checked,
// must still not be able to create a structurally impossible run.
type PermanentValidator interface {
	ValidatePermanent(context.Context, Manifest) error
}

// BundleIngester copies a validated version 2 submission into coordinator-owned
// storage and persists the corresponding immutable domain records.
type BundleIngester struct {
	DirectoryCatalogs map[string][]directoryresource.Binding
	StorageRoot       string
	Store             CoordinatorRecordStore
	// Permanent, when set, refuses a permanently impossible manifest before any
	// record is built or any file is staged.
	Permanent  PermanentValidator
	Now        func() time.Time
	NewID      func() string
	NewTypedID func(string) string
}

// IngestedBundle identifies a successfully committed workflow submission.
type IngestedBundle struct {
	WorkflowID string
	RunID      string
	StorageDir string
	Records    sqlite.CoordinatorRecords
}

type ingestedFile struct {
	relative    string
	size        int64
	sha256      string
	storagePath string
}

func (i BundleIngester) Ingest(ctx context.Context, bundleDir string) (IngestedBundle, error) {
	if i.StorageRoot == "" {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: storage root is required")
	}
	if i.Store == nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: coordinator store is required")
	}

	root, sourceRoot, manifest, manifestBytes, err := openIngestionBundle(bundleDir)
	if err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: %w", err)
	}
	defer sourceRoot.Close()
	// The permanent checks are repeated here, transactionally, before anything
	// exists. A campaign that can never run must consume no run ID and occupy
	// no place in the graph, whether or not the client checked first.
	if i.Permanent != nil {
		if err := i.Permanent.ValidatePermanent(ctx, manifest); err != nil {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: %w", err)
		}
	}
	relativePaths, inputPaths, err := ingestionPaths(root, manifest)
	if err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: %w", err)
	}

	directoryBindings := make(map[string][]directoryresource.Binding)
	for name, task := range manifest.Tasks {
		if len(task.Directories) == 0 {
			continue
		}
		bindings, err := directoryresource.Resolve(i.DirectoryCatalogs[manifest.Environment.Project], task.Directories)
		if err != nil {
			return IngestedBundle{}, fmt.Errorf("ingest task %q directories: %w", name, err)
		}
		directoryBindings[name] = bindings
	}
	workflowID := i.newID("workflow")
	runID := i.newID("run")
	workflowsRoot := filepath.Join(i.StorageRoot, "workflows")
	if err := os.MkdirAll(workflowsRoot, 0o700); err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: create storage: %w", err)
	}
	stageDir, err := os.MkdirTemp(workflowsRoot, ".ingest-")
	if err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: create staging directory: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			removeIngestedTree(stageDir)
		}
	}()

	files := make(map[string]ingestedFile, len(relativePaths))
	for _, relative := range relativePaths {
		if err := ctx.Err(); err != nil {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: %w", err)
		}
		storagePath := filepath.ToSlash(filepath.Join("workflows", workflowID, "files", relative))
		var file ingestedFile
		if relative == "workflow.yaml" {
			file, err = writeIngestedFile(bytes.NewReader(manifestBytes), filepath.Join(stageDir, "files", relative), relative, storagePath)
		} else {
			resolved, resolveErr := safeBundleFile(root, relative)
			if resolveErr != nil {
				err = resolveErr
			} else {
				sourceRelative, relativeErr := filepath.Rel(root, resolved)
				if relativeErr != nil {
					err = relativeErr
				} else {
					file, err = copyIngestedFile(sourceRoot, sourceRelative, filepath.Join(stageDir, "files", relative), relative, storagePath)
				}
			}
		}
		if err != nil {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle file %q: %w", relative, err)
		}
		files[relative] = file
	}
	if err := makeIngestedTreeImmutable(stageDir); err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: protect staged files: %w", err)
	}

	now := i.now()
	records, supervision, err := i.buildRecords(manifest, workflowID, runID, inputPaths, files, directoryBindings, now)
	if err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest sink: %w", err)
	}
	if err = i.retainExternalInputs(ctx, manifest, &records, runID); err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest external inputs: %w", err)
	}
	finalDir := filepath.Join(workflowsRoot, workflowID)
	if err := os.Rename(stageDir, finalDir); err != nil {
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: publish files: %w", err)
	}
	keepStage = true

	// Supervision is materialized before the run exists. A gate row belonging to
	// a run that was never created is inert, because a supervision snapshot of a
	// run with no run row is unsupervised; the reverse order would leave a real
	// run supervised in name with no gate to withhold anything.
	if supervision != nil {
		materializer, ok := i.Store.(SupervisionMaterializer)
		if !ok {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: this coordinator store records no supervision")
		}
		if _, err := materializer.PutSupervision(ctx, *supervision); err != nil {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: materialize supervision: %w", err)
		}
	}
	if err := i.Store.SaveCoordinatorRecords(ctx, records); err != nil {
		if cleanupErr := removeIngestedTree(finalDir); cleanupErr != nil {
			return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: persist metadata: %w (cleanup failed: %v)", err, cleanupErr)
		}
		return IngestedBundle{}, fmt.Errorf("ingest workflow bundle: persist metadata: %w", err)
	}
	return IngestedBundle{
		WorkflowID: workflowID,
		RunID:      runID,
		StorageDir: finalDir,
		Records:    records,
	}, nil
}

func openIngestionBundle(bundleDir string) (string, *os.Root, Manifest, []byte, error) {
	var manifest Manifest
	root, err := filepath.Abs(bundleDir)
	if err != nil {
		return "", nil, manifest, nil, fmt.Errorf("resolve workflow bundle: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", nil, manifest, nil, fmt.Errorf("resolve workflow bundle: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", nil, manifest, nil, fmt.Errorf("stat workflow bundle: %w", err)
	}
	if !info.IsDir() {
		return "", nil, manifest, nil, fmt.Errorf("workflow bundle is not a directory")
	}
	sourceRoot, err := os.OpenRoot(root)
	if err != nil {
		return "", nil, manifest, nil, fmt.Errorf("open workflow bundle: %w", err)
	}
	fail := func(err error) (string, *os.Root, Manifest, []byte, error) {
		sourceRoot.Close()
		return "", nil, Manifest{}, nil, err
	}
	manifestPath, err := safeBundleFile(root, "workflow.yaml")
	if err != nil {
		return fail(fmt.Errorf("workflow.yaml: %w", err))
	}
	manifestRelative, err := filepath.Rel(root, manifestPath)
	if err != nil {
		return fail(fmt.Errorf("workflow.yaml: %w", err))
	}
	raw, err := sourceRoot.ReadFile(manifestRelative)
	if err != nil {
		return fail(fmt.Errorf("read workflow.yaml: %w", err))
	}
	manifest, err = ParseManifest(raw)
	if err != nil {
		return fail(err)
	}
	if err := validateManifestFiles(root, manifest); err != nil {
		return fail(err)
	}
	return root, sourceRoot, manifest, raw, nil
}

func (i BundleIngester) buildRecords(manifest Manifest, workflowID, runID string, inputPaths []string, files map[string]ingestedFile, directoryBindings map[string][]directoryresource.Binding, now time.Time) (sqlite.CoordinatorRecords, *sqlite.SupervisionMaterialization, error) {
	records := sqlite.CoordinatorRecords{}
	artifactIDByPath := make(map[string]string, len(files))
	manifestArtifact := i.artifact(runID, "", files["workflow.yaml"], now)
	records.Artifacts = append(records.Artifacts, manifestArtifact)
	artifactIDByPath["workflow.yaml"] = manifestArtifact.ID
	workflowInputIDs := []string{manifestArtifact.ID}
	taskInputIDs := make([]string, 0, len(inputPaths))
	for _, relative := range inputPaths {
		artifact := i.artifact(runID, "", files[relative], now)
		records.Artifacts = append(records.Artifacts, artifact)
		artifactIDByPath[relative] = artifact.ID
		workflowInputIDs = append(workflowInputIDs, artifact.ID)
		taskInputIDs = append(taskInputIDs, artifact.ID)
	}
	// The overseer prompt and every gate rubric are retained as run inputs, so a
	// review reads the exact bytes the submission declared.
	for _, relative := range supervisionBundleFiles(manifest) {
		relative = filepath.Clean(relative)
		if _, retained := artifactIDByPath[relative]; retained {
			continue
		}
		artifact := i.artifact(runID, "", files[relative], now)
		records.Artifacts = append(records.Artifacts, artifact)
		artifactIDByPath[relative] = artifact.ID
	}

	taskNames := make([]string, 0, len(manifest.Tasks))
	for name := range manifest.Tasks {
		taskNames = append(taskNames, name)
	}
	sort.Strings(taskNames)

	taskIDs := make([]string, 0, len(taskNames))
	for range taskNames {
		taskIDs = append(taskIDs, i.newID("task"))
	}
	records.Workflows = []domain.Workflow{{
		ID:      workflowID,
		Version: manifest.Version,
		Name:    manifest.Name,
		Project: manifest.Environment.Project,
		Environment: domain.ExecutionEnvironment{
			Type: manifest.Environment.Type, Scope: manifest.Environment.Scope, Ref: manifest.Environment.Ref,
		},
		Class:            manifest.Class,
		TaskIDs:          append([]string(nil), taskIDs...),
		InputArtifactIDs: append([]string(nil), workflowInputIDs...),
		CreatedAt:        now,
	}}
	records.WorkflowRuns = []domain.WorkflowRun{{
		ID: runID, WorkflowID: workflowID, Progress: domain.ProgressQueued,
		InputArtifactIDs: append([]string(nil), workflowInputIDs...), Revision: 1, CreatedAt: now, UpdatedAt: now,
	}}

	taskIDsByName := make(map[string]string, len(taskNames))
	for index, name := range taskNames {
		taskManifest := manifest.Tasks[name]
		taskID := taskIDs[index]
		promptArtifact := i.artifact(runID, taskID, files[filepath.Clean(taskManifest.PromptFile)], now)
		records.Artifacts = append(records.Artifacts, promptArtifact)
		taskIDsByName[name] = taskID
		var outputs []domain.ArtifactDeclaration
		if len(taskManifest.Outputs) != 0 {
			outputs = make([]domain.ArtifactDeclaration, 0, len(taskManifest.Outputs))
		}
		for _, output := range taskManifest.Outputs {
			outputs = append(outputs, domain.ArtifactDeclaration{Name: output, MediaType: mediaType(output)})
		}
		for _, commit := range taskManifest.Commits {
			outputs = append(outputs, domain.ArtifactDeclaration{
				Name: commit.Name, MediaType: "application/json",
				Commit: &domain.CommitOutput{Revision: commit.Revision},
			})
		}
		routes := make([]domain.ProviderRoute, 0, len(taskManifest.Routes))
		for _, route := range taskManifest.Routes {
			routes = append(routes, domain.ProviderRoute{
				WorkerID: route.Host, ProviderInstanceID: route.Instance, Model: route.Model,
				Options: cloneStringMap(route.Options), QuotaPoolID: route.QuotaPool,
			})
		}
		var localNeeds []string
		var externalNeeds []domain.NodeRef
		for _, need := range taskManifest.Needs {
			if strings.Contains(need, "/") {
				ref, _ := domain.ParseNodeRef(need)
				externalNeeds = append(externalNeeds, ref)
			} else {
				localNeeds = append(localNeeds, need)
			}
		}
		records.Tasks = append(records.Tasks, domain.Task{
			DirectoryBindings: directoryresource.CloneBindings(directoryBindings[name]),
			ID:                taskID, RunID: runID, WorkflowID: workflowID, Name: name, Class: taskManifest.Class,
			Needs: localNeeds, ExternalNeeds: externalNeeds, PromptArtifactID: promptArtifact.ID,
			InputArtifactIDs: append([]string(nil), taskInputIDs...), DependencyInputs: cloneStringSlices(taskManifest.InputsFrom),
			Context: taskManifest.Context, Outputs: outputs, Verification: append([]string(nil), taskManifest.Verify...),
			Placement: domain.Placement{
				Hosts:        append([]string(nil), taskManifest.Placement.Hosts...),
				Capabilities: append([]string(nil), taskManifest.Placement.Requires...),
			},
			ResourceDemand: resourceDemandFor(taskManifest.Resources),
			Preflight:      PackagePreflightSteps(taskManifest.Preflight.Steps),
			Routes:         routes, ResourceLocks: append([]string(nil), taskManifest.ResourceLocks...),
			Importance: taskManifest.Importance, Difficulty: taskManifest.Difficulty,
			EstimatedCost: taskManifest.EstimatedCost, MaxTurns: taskManifest.MaxTurns,
			NotBefore: taskManifest.NotBefore, Deadline: taskManifest.Deadline, ExpiresAt: taskManifest.ExpiresAt,
		})
		progress := domain.ProgressReady
		if len(taskManifest.Needs) != 0 {
			progress = domain.ProgressBlocked
		}
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: i.newID("attempt"), WorkflowRunID: runID, TaskID: taskID, Number: 1,
			Progress: progress, Control: domain.ControlUnassigned, UpdatedAt: now,
		})
	}
	run, err := domain.BindRunSink(records.WorkflowRuns[0], records.Tasks)
	if err != nil {
		return sqlite.CoordinatorRecords{}, nil, err
	}
	supervision, err := buildSupervision(manifest, runID, taskIDsByName, artifactIDByPath, run.GraphRevision, now)
	if err != nil {
		return sqlite.CoordinatorRecords{}, nil, err
	}
	if supervision != nil {
		// The run carries its declared record from creation, so a supervised run
		// is supervised before any supervision transaction has run. The gate rows
		// are materialized separately, and before the run exists, so no window
		// leaves a supervised run with no gates.
		declared := supervision.Record
		run.Supervision = &declared
	}
	records.WorkflowRuns[0] = run
	return records, supervision, nil
}

func (i BundleIngester) artifact(runID, taskID string, file ingestedFile, now time.Time) domain.Artifact {
	return domain.Artifact{
		ID: i.newID("artifact"), WorkflowRunID: runID, TaskID: taskID,
		Kind: domain.ArtifactInput, Name: filepath.ToSlash(file.relative), MediaType: mediaType(file.relative),
		Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
		Producer: "submission", CreatedAt: now,
	}
}

func (i BundleIngester) now() time.Time {
	if i.Now != nil {
		return i.Now().UTC()
	}
	return time.Now().UTC()
}

func (i BundleIngester) newID(kind string) string {
	if i.NewTypedID != nil {
		return i.NewTypedID(kind)
	}
	if i.NewID != nil {
		return kind + "-" + i.NewID()
	}
	return kind + "-" + uuid.NewString()
}

func ingestionPaths(root string, manifest Manifest) ([]string, []string, error) {
	all := map[string]struct{}{"workflow.yaml": {}}
	for _, task := range manifest.Tasks {
		all[filepath.Clean(task.PromptFile)] = struct{}{}
	}
	// The overseer prompt and the gate rubrics are retained exactly like a task
	// prompt: they are the evidence a later reader needs to know what the review
	// was asked to apply.
	for _, relative := range supervisionBundleFiles(manifest) {
		all[filepath.Clean(relative)] = struct{}{}
	}
	inputSet := make(map[string]struct{})
	for _, pattern := range manifest.Inputs {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			return nil, nil, fmt.Errorf("input pattern %q: %w", pattern, err)
		}
		for _, match := range matches {
			relative, err := filepath.Rel(root, match)
			if err != nil {
				return nil, nil, fmt.Errorf("input pattern %q: %w", pattern, err)
			}
			relative = filepath.Clean(relative)
			inputSet[relative] = struct{}{}
			all[relative] = struct{}{}
		}
	}
	paths := sortedKeys(all)
	inputs := sortedKeys(inputSet)
	return paths, inputs, nil
}

func copyIngestedFile(sourceRoot *os.Root, sourceRelative, destination, relative, storagePath string) (ingestedFile, error) {
	input, err := sourceRoot.Open(sourceRelative)
	if err != nil {
		return ingestedFile{}, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return ingestedFile{}, err
	}
	if !info.Mode().IsRegular() {
		return ingestedFile{}, fmt.Errorf("source is not a regular file")
	}
	return writeIngestedFile(input, destination, relative, storagePath)
}

func writeIngestedFile(input io.Reader, destination, relative, storagePath string) (ingestedFile, error) {
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return ingestedFile{}, err
	}
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return ingestedFile{}, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	closeErr := output.Close()
	if copyErr != nil {
		return ingestedFile{}, copyErr
	}
	if closeErr != nil {
		return ingestedFile{}, closeErr
	}
	return ingestedFile{
		relative: filepath.Clean(relative), size: size, sha256: fmt.Sprintf("%x", hash.Sum(nil)), storagePath: storagePath,
	}, nil
}

func makeIngestedTreeImmutable(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o500)
		}
		return os.Chmod(path, 0o400)
	})
}

func removeIngestedTree(root string) error {
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err == nil && entry.IsDir() {
			_ = os.Chmod(path, 0o700)
		}
		return nil
	})
	return os.RemoveAll(root)
}

func mediaType(name string) string {
	if value := mime.TypeByExtension(filepath.Ext(name)); value != "" {
		return value
	}
	return "application/octet-stream"
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// resourceDemandFor converts a parsed manifest resource declaration into the
// internal demand a placement decision consumes.
//
// The manifest layer and the domain layer each declare their own CPU class
// type on purpose. The manifest type is the wire shape an author writes in
// YAML; the domain type is what placement reasons about. They currently share
// the same three spellings, and this is the single point that depends on that,
// so a future divergence — an alias an author may write, or a class the fleet
// gains — is absorbed here rather than leaking into either layer.
//
// The manifest's numeric fields are pointers so that an explicit zero can be
// rejected during validation; by this point validation has already run, so an
// absent field and a zero mean the same thing to placement and both become the
// zero value. A task that declares nothing produces a zero demand, which
// constrains no worker.
func resourceDemandFor(resources ManifestResources) domain.ResourceDemand {
	demand := domain.ResourceDemand{
		MinCPUClass:       domain.CPUClass(resources.MinCPUClass),
		PreferredCPUClass: domain.CPUClass(resources.PreferredCPUClass),
	}
	if resources.CPUUnits != nil {
		demand.CPUUnits = *resources.CPUUnits
	}
	if resources.MemoryMB != nil {
		demand.MemoryMB = *resources.MemoryMB
	}
	if resources.ScratchMB != nil {
		demand.ScratchMB = *resources.ScratchMB
	}
	return demand
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	maps.Copy(result, values)
	return result
}

func cloneStringSlices(values map[string][]string) map[string][]string {
	if values == nil {
		return nil
	}
	result := make(map[string][]string, len(values))
	for key, value := range values {
		result[key] = append([]string(nil), value...)
	}
	return result
}
