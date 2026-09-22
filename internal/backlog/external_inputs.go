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

// retainExternalInputs resolves cross-run inputs while accepting the campaign.
// The consumer owns immutable reference metadata; the source bytes and provenance
// remain under the successful producer and are never selected from a mutable cache.
func (i BundleIngester) retainExternalInputs(ctx context.Context, manifest Manifest, records *sqlite.CoordinatorRecords) error {
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
	taskByName := make(map[string]*domain.Task, len(records.Tasks))
	for index := range records.Tasks {
		taskByName[records.Tasks[index].Name] = &records.Tasks[index]
	}
	for consumerName, authored := range manifest.Tasks {
		consumer := taskByName[consumerName]
		if consumer == nil {
			return fmt.Errorf("external input consumer %q is unavailable", consumerName)
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
				reference.WorkflowRunID = records.WorkflowRuns[0].ID
				reference.TaskID = consumer.ID
				reference.AttemptID = ""
				reference.Kind = domain.ArtifactInput
				records.Artifacts = append(records.Artifacts, reference)
				consumer.CarriedInputs = append(consumer.CarriedInputs, domain.CarriedInput{
					Producer: producerTask.Name, ProducerTaskID: producerTask.ID,
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
