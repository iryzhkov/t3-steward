package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// SupervisionMaterializer is the durable half of ingestion's supervision step.
//
// It is an optional interface on the ingester's store rather than a method of
// CoordinatorRecordStore, so a store that predates supervision keeps ingesting
// unsupervised submissions unchanged. An unsupervised submission never reaches
// it at all: absence of the record is the unsupervised case, and no empty record
// is ever created.
type SupervisionMaterializer interface {
	PutSupervision(context.Context, sqlite.SupervisionMaterialization) (domain.SupervisionRecord, error)
}

// buildSupervision produces the run's declared supervision from the manifest and
// the identities this ingestion assigned.
//
// Two rewrites happen here and nowhere else. The prompt and rubric paths the
// manifest carries are bundle-relative, which is the only identity the authoring
// side has; they become the IDs of the artifacts the ingester retained, so a
// later review reads the exact retained bytes rather than a directory that no
// longer exists. And the observed and protected sets are declared as manifest
// task names, which become the task IDs of this run, because the readiness
// predicate is asked about a task ID.
//
// It reports a nil materialization for an unsupervised manifest.
func buildSupervision(
	manifest Manifest,
	runID string,
	taskIDsByName map[string]string,
	artifactIDByPath map[string]string,
	graphRevision int64,
	now time.Time,
) (*sqlite.SupervisionMaterialization, error) {
	config, supervised := manifest.SupervisionConfig()
	if !supervised {
		return nil, nil
	}
	artifactFor := func(purpose, relative string) (string, error) {
		id, found := artifactIDByPath[filepath.Clean(relative)]
		if !found {
			return "", fmt.Errorf("supervision %s %q was not retained by this ingestion", purpose, relative)
		}
		return id, nil
	}
	promptID, err := artifactFor("prompt", config.PromptArtifactID)
	if err != nil {
		return nil, err
	}
	config.PromptArtifactID = promptID
	if err := config.Validate(); err != nil {
		return nil, err
	}

	resolve := func(gate string, names []string) ([]string, error) {
		if len(names) == 0 {
			return nil, nil
		}
		ids := make([]string, 0, len(names))
		for _, name := range names {
			id, found := taskIDsByName[name]
			if !found {
				return nil, fmt.Errorf("gate %q names task %q, which this workflow does not declare", gate, name)
			}
			ids = append(ids, id)
		}
		return ids, nil
	}
	definitions := manifest.GateDefinitions()
	gates := make([]domain.Gate, 0, len(definitions))
	for _, definition := range definitions {
		observed, err := resolve(definition.Name, definition.ObservedTaskIDs)
		if err != nil {
			return nil, err
		}
		protected, err := resolve(definition.Name, definition.ProtectedTaskIDs)
		if err != nil {
			return nil, err
		}
		definition.ObservedTaskIDs, definition.ProtectedTaskIDs = observed, protected
		// The gate row is keyed by this ID, so it must name the run. The manifest
		// only knows the authored name, which two runs of the same campaign, and
		// two different campaigns using the same word, both share. The clone and
		// rerun path mints the same run-scoped identity; Name stays the authored
		// word so blockers and CLI output remain readable.
		definition.ID = runID + ":" + definition.Name
		if definition.RubricArtifactID != "" {
			rubricID, err := artifactFor("rubric", definition.RubricArtifactID)
			if err != nil {
				return nil, err
			}
			definition.RubricArtifactID = rubricID
		}
		if err := definition.Validate(); err != nil {
			return nil, err
		}
		gates = append(gates, domain.Gate{
			Definition:    definition,
			RunID:         runID,
			State:         domain.GatePendingEvidence,
			GraphRevision: graphRevision,
			UpdatedAt:     now,
		})
	}
	return &sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{
			RunID:  runID,
			Config: config,
			// The first epoch is one, not zero: a supervisor names the epoch it
			// acts under, and zero is the value an operator uses to mean "not
			// fencing on an epoch at all".
			ActivationEpoch:          1,
			BudgetGrantedActivations: config.MaxActivations,
			CreatedAt:                now,
			UpdatedAt:                now,
		},
		Gates: gates,
	}, nil
}
