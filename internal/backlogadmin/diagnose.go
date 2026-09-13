package backlogadmin

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Diagnosis joins coordinator evidence at the reported graph revision. Worker
// snapshots and waits are separately timestamped observations, not an atomic
// distributed snapshot. Lease tokens and dispatch credentials are excluded.
type GraphRevisionEvidence struct {
	Revision  int64     `json:"revision"`
	Parent    int64     `json:"parent"`
	Actor     string    `json:"actor"`
	Reason    string    `json:"reason"`
	RequestID string    `json:"requestId"`
	Digest    string    `json:"digest"`
	CreatedAt time.Time `json:"createdAt"`
}

type Diagnosis struct {
	Revisions     []GraphRevisionEvidence `json:"revisions"`
	GraphRevision int64                   `json:"graphRevision"`
	GeneratedAt   time.Time               `json:"generatedAt"`
	Status        Status                  `json:"status"`
	Workflow      WorkflowDetail          `json:"workflow"`
	Graph         Graph                   `json:"graph"`
	Events        []Event                 `json:"events"`
	Explanations  []Explanation           `json:"explanations"`
	Assignments   []Assignment            `json:"assignments"`
	Commands      []Command               `json:"commands"`
	Waits         []domain.NodeWait       `json:"waits"`
	Workers       []DiagnosticWorker      `json:"workers"`
	Unavailable   []string                `json:"unavailable,omitempty"`
}

type DiagnosticWorker struct {
	WorkerID    string                               `json:"workerId"`
	WorkerEpoch string                               `json:"workerEpoch"`
	Sequence    int64                                `json:"sequence"`
	ObservedAt  time.Time                            `json:"observedAt"`
	ValidUntil  time.Time                            `json:"validUntil"`
	Assignments []domain.WorkerAssignmentObservation `json:"assignments"`
}

func (s *Service) diagnose(ctx context.Context, v view, runID string) (Diagnosis, error) {
	detail, ok := v.workflowDetail(runID)
	if !ok {
		return Diagnosis{}, notFound("workflow run", runID)
	}
	graph, _ := v.graph(runID)
	result := Diagnosis{GraphRevision: graph.GraphRevision, GeneratedAt: v.now,
		Status: v.status(), Workflow: detail, Graph: graph, Events: v.events(runID),
		Commands: v.commands(Query{WorkflowRunID: runID})}
	attempts := map[string]bool{}
	assignments := map[string]bool{}
	for _, a := range v.records.Attempts {
		if a.WorkflowRunID == runID {
			attempts[a.ID] = true
		}
	}
	for _, a := range v.records.Assignments {
		if attempts[a.AttemptID] {
			result.Assignments = append(result.Assignments, assignmentDTO(a))
			assignments[a.ID] = true
		}
	}
	for _, task := range detail.Tasks {
		explanation, _ := v.explanation(runID, task.Task.ID)
		result.Explanations = append(result.Explanations, explanation)
	}
	for _, worker := range v.workers {
		evidence := DiagnosticWorker{WorkerID: worker.WorkerID, WorkerEpoch: worker.WorkerEpoch,
			Sequence: worker.Sequence, ObservedAt: worker.ObservedAt, ValidUntil: worker.ValidUntil}
		for _, a := range worker.Assignments {
			if assignments[a.AssignmentID] {
				evidence.Assignments = append(evidence.Assignments, a)
			}
		}
		if len(evidence.Assignments) > 0 {
			result.Workers = append(result.Workers, evidence)
		}
	}
	if reader, ok := s.reader.(interface {
		ListNodeWaits(context.Context) ([]domain.NodeWait, error)
	}); ok {
		waits, err := reader.ListNodeWaits(ctx)
		if err != nil {
			return Diagnosis{}, fmt.Errorf("diagnose waits: %w", err)
		}
		for _, wait := range waits {
			if wait.Request.Target.RunID == runID {
				result.Waits = append(result.Waits, wait)
			}
		}
	} else {
		result.Unavailable = append(result.Unavailable, "native wait registry")
	}
	if reader, ok := s.reader.(interface {
		LoadGraphRevisions(context.Context, string) ([]domain.GraphDefinition, error)
	}); ok {
		revisions, err := reader.LoadGraphRevisions(ctx, runID)
		if err != nil {
			return Diagnosis{}, err
		}
		for _, g := range revisions {
			if g.Revision <= result.GraphRevision {
				result.Revisions = append(result.Revisions, GraphRevisionEvidence{Revision: g.Revision, Parent: g.Parent, Actor: g.Actor, Reason: g.Reason, RequestID: g.RequestID, Digest: g.Digest, CreatedAt: g.CreatedAt})
			}
		}
	}
	for id := range assignments {
		found := false
		for _, worker := range result.Workers {
			for _, a := range worker.Assignments {
				if a.AssignmentID == id && a.Journal != nil {
					found = true
				}
			}
		}
		if !found {
			result.Unavailable = append(result.Unavailable, "worker journal excerpt for "+id)
		}
	}
	return result, nil
}
