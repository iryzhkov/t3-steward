package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type coordinatorRecordLoader interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
}

type projectContextAcceptanceLoader interface {
	LoadSupervisionProjection(context.Context, string) (sqlite.SupervisionProjection, error)
}

// retainExternalInputs resolves cross-run inputs while accepting the campaign.
// The consumer owns immutable reference metadata; the source bytes and provenance
// remain under the successful producer and are never selected from a mutable cache.
func (i BundleIngester) retainExternalInputs(ctx context.Context, manifest Manifest, records *sqlite.CoordinatorRecords, targetRunID string) error {
	needsExternal := false
	for _, task := range manifest.Tasks {
		for producer := range task.InputsFrom {
			if strings.Contains(producer, "/") {
				needsExternal = true
			}
		}
	}
	if !needsExternal {
		return nil
	}
	loader, ok := i.Store.(coordinatorRecordLoader)
	if !ok {
		return errors.New("external input custody requires coordinator record reads")
	}
	source, err := loader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return fmt.Errorf("load external input custody: %w", err)
	}
	for consumerName, authored := range manifest.Tasks {
		consumer, consumerRunID, err := exactExternalInputConsumer(records, targetRunID, consumerName)
		if err != nil {
			return fmt.Errorf("external input consumer %q: %w", consumerName, err)
		}
		for producer, names := range authored.InputsFrom {
			if !strings.Contains(producer, "/") {
				continue
			}
			ref, err := domain.ParseNodeRef(producer)
			if err != nil {
				return err
			}
			observation, err := domain.ResolveNode(ref, source.WorkflowRuns, source.Tasks, source.Attempts, source.Assignments)
			if err != nil {
				return fmt.Errorf("external input %s: %w", producer, err)
			}
			if observation.Progress != domain.ProgressSucceeded || observation.AttemptID == "" {
				return fmt.Errorf("external input %s is %s, not a completed successful producer", producer, observation.Progress)
			}
			var producerTask domain.Task
			for _, run := range source.WorkflowRuns {
				if run.ID != ref.RunID {
					continue
				}
				for _, candidate := range domain.TasksForRun(run, source.Tasks) {
					if candidate.ID == observation.Target.TaskID {
						producerTask = candidate
						break
					}
				}
			}
			if producerTask.ID == "" {
				return fmt.Errorf("external input %s producer definition is unavailable", producer)
			}
			declared := map[string]bool{}
			for _, output := range producerTask.Outputs {
				declared[output.Name] = true
			}
			for _, name := range names {
				name = filepath.ToSlash(name)
				if !declared[name] {
					return fmt.Errorf("external input %s references undeclared artifact %q", producer, name)
				}
				var found []domain.Artifact
				for _, artifact := range source.Artifacts {
					if artifact.WorkflowRunID == ref.RunID && artifact.TaskID == producerTask.ID &&
						artifact.AttemptID == observation.AttemptID && artifact.Kind == domain.ArtifactOutput &&
						artifact.Name == name && artifact.SHA256 != "" && artifact.StoragePath != "" {
						found = append(found, artifact)
					}
				}
				if len(found) != 1 {
					return fmt.Errorf("external input %s/%s has %d retained successful artifacts; want exactly one", producer, name, len(found))
				}
				if err := i.verifyExternalArtifact(found[0]); err != nil {
					return fmt.Errorf("external input %s/%s is not retrievable: %w", producer, name, err)
				}
				reference := found[0]
				reference.ID = i.newID("input")
				reference.WorkflowRunID = consumerRunID
				reference.TaskID = consumer.ID
				reference.AttemptID = ""
				reference.Kind = domain.ArtifactInput
				namespace := externalProducerNamespace(ref.RunID, producerTask)
				acceptance, err := i.resolveExternalAcceptance(ctx, consumer.Context, ref.RunID, producerTask.ID, observation.AttemptID, found[0])
				if err != nil {
					return fmt.Errorf("external input %s/%s acceptance: %w", producer, name, err)
				}
				if err := bindExternalProjectContext(consumer.Context, ref.RunID, producerTask.ID, observation.AttemptID, found[0], reference, namespace, acceptance); err != nil {
					return fmt.Errorf("external input %s/%s context: %w", producer, name, err)
				}
				records.Artifacts = append(records.Artifacts, reference)
				consumer.CarriedInputs = append(consumer.CarriedInputs, domain.CarriedInput{
					Producer: producerTask.Name, ProducerNamespace: namespace,
					ProducerTaskID: producerTask.ID, SourceRunID: ref.RunID,
					SourceAttemptID: observation.AttemptID, SourceArtifactID: found[0].ID,
					Name: name, ArtifactID: reference.ID,
				})
			}
			delete(consumer.DependencyInputs, producer)
		}
		if len(consumer.DependencyInputs) == 0 {
			consumer.DependencyInputs = nil
		}
	}
	return nil
}

func exactExternalInputConsumer(records *sqlite.CoordinatorRecords, runID, name string) (*domain.Task, string, error) {
	var run *domain.WorkflowRun
	for index := range records.WorkflowRuns {
		if records.WorkflowRuns[index].ID == runID {
			if run != nil {
				return nil, "", errors.New("exact target run identity is duplicated")
			}
			run = &records.WorkflowRuns[index]
		}
	}
	if run == nil {
		return nil, "", errors.New("exact target run is unavailable")
	}
	var consumer *domain.Task
	if run.Graph != nil {
		for index := range run.Graph.Tasks {
			if run.Graph.Tasks[index].Name == name {
				if consumer != nil {
					return nil, "", errors.New("exact target graph has duplicate task names")
				}
				consumer = &run.Graph.Tasks[index]
			}
		}
	} else {
		for index := range records.Tasks {
			candidate := &records.Tasks[index]
			if candidate.WorkflowID == run.WorkflowID && candidate.RunID == run.ID && candidate.Name == name {
				if consumer != nil {
					return nil, "", errors.New("exact target run has duplicate task names")
				}
				consumer = candidate
			}
		}
	}
	if consumer == nil {
		return nil, "", errors.New("task is unavailable in exact target run projection")
	}
	return consumer, run.ID, nil
}

func projectContextConsumerRunID(task domain.Task, runs []domain.WorkflowRun) (string, error) {
	if task.RunID != "" {
		for _, run := range runs {
			if run.ID == task.RunID && run.WorkflowID == task.WorkflowID {
				return run.ID, nil
			}
		}
		return "", errors.New("exact target run is unavailable")
	}
	var matched string
	for _, run := range runs {
		if run.WorkflowID != task.WorkflowID {
			continue
		}
		if matched != "" && matched != run.ID {
			return "", errors.New("target run identity is ambiguous")
		}
		matched = run.ID
	}
	if matched == "" {
		return "", errors.New("target run is unavailable")
	}
	return matched, nil
}

func (i BundleIngester) resolveExternalAcceptance(
	ctx context.Context,
	index *domain.ProjectContext,
	runID, taskID, attemptID string,
	artifact domain.Artifact,
) (*domain.ProjectContextAcceptance, error) {
	if index == nil {
		return nil, nil
	}
	uri := "execution:" + runID + "/" + taskID + "/" + attemptID + "/" + artifact.ID
	needed := false
	for _, ref := range index.References {
		if ref.Kind == domain.ContextReferenceExecution && ref.URI == uri {
			needed = true
			break
		}
	}
	if !needed {
		return nil, nil
	}
	loader, ok := i.Store.(projectContextAcceptanceLoader)
	if !ok {
		return nil, errors.New("accepted gate receipt custody is unavailable")
	}
	projection, err := loader.LoadSupervisionProjection(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("load accepted gate receipt: %w", err)
	}
	digestMatches := func(recorded string) bool {
		recorded = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(recorded)), "sha256:")
		return recorded == strings.ToLower(artifact.SHA256)
	}
	var matches []domain.ProjectContextAcceptance
	for _, gate := range projection.Gates {
		if gate.State != domain.GateAccepted || gate.EvidenceSnapshotID == "" || !gate.Definition.Observes(taskID) {
			continue
		}
		for _, decision := range projection.Decisions {
			if decision.RunID != runID || decision.GateID != gate.Definition.ID ||
				decision.Outcome != domain.GateDecisionAccept ||
				decision.Evidence.ID != gate.EvidenceSnapshotID {
				continue
			}
			for _, producer := range decision.Evidence.Producers {
				if producer.TaskID != taskID || producer.AttemptID != attemptID {
					continue
				}
				for _, digest := range producer.ArtifactDigests {
					if digest.ArtifactID == artifact.ID && digestMatches(digest.Digest) {
						matches = append(matches, domain.ProjectContextAcceptance{
							GateID: gate.Definition.ID, DecisionID: decision.ID,
							EvidenceSnapshotID: decision.Evidence.ID,
						})
					}
				}
			}
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("source artifact has %d current accepted gate receipts; want exactly one", len(matches))
	}
	return &matches[0], nil
}

func bindExternalProjectContext(index *domain.ProjectContext, runID, taskID, attemptID string, source, retained domain.Artifact, namespace string, acceptance *domain.ProjectContextAcceptance) error {
	if index == nil {
		return nil
	}
	uri := "execution:" + runID + "/" + taskID + "/" + attemptID + "/" + source.ID
	matched := false
	for n := range index.References {
		ref := &index.References[n]
		if ref.Kind != domain.ContextReferenceExecution || ref.URI != uri {
			continue
		}
		if matched {
			return fmt.Errorf("execution reference %q is duplicated", uri)
		}
		if !strings.EqualFold(ref.Revision, source.SHA256) {
			return fmt.Errorf("execution reference %q revision %q does not match retained digest %q", uri, ref.Revision, source.SHA256)
		}
		if acceptance == nil {
			return fmt.Errorf("execution reference %q has no accepted gate receipt", uri)
		}
		ref.Status = domain.ProjectContextAccepted
		ref.Authority = "gate-decision:" + acceptance.DecisionID
		ref.Acceptance = acceptance
		ref.Binding = &domain.ProjectContextArtifactBinding{
			ArtifactID: retained.ID, Path: "dependencies/" + namespace + "/" + filepath.ToSlash(source.Name), SHA256: source.SHA256,
			SourceRunID: runID, SourceTaskID: taskID, SourceAttemptID: attemptID, SourceArtifactID: source.ID,
		}
		index.Status = domain.ProjectContextAccepted
		index.Authority = []string{"gate-decision:" + acceptance.DecisionID}
		matched = true
	}
	return nil
}

func externalProducerNamespace(runID string, task domain.Task) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + task.ID))
	return fmt.Sprintf("%s--%x", task.Name, sum[:6])
}

func (i BundleIngester) verifyExternalArtifact(artifact domain.Artifact) error {
	if i.StorageRoot == "" {
		return nil
	}
	path, err := safeBundleFile(i.StorageRoot, filepath.FromSlash(artifact.StoragePath))
	if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return err
	}
	if size != artifact.Size || fmt.Sprintf("%x", digest.Sum(nil)) != strings.ToLower(artifact.SHA256) {
		return errors.New("retained artifact size or digest mismatch")
	}
	return nil
}
