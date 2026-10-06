package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The coordinator's half of the continuation.md checkpoint contract. A
// worker's result may carry the attempt's latest snapshot of continuation.md
// and the metadata describing it; the coordinator keeps the snapshot as a
// checkpoint artifact dated by its capture, and hands the latest one of a
// task to the task's next attempt as an input.

// isContinuationResultObject reports whether a result object is the snapshot
// or its metadata under the attempt's own fixed identity.
func isContinuationResultObject(object workerproto.ArtifactObject, name string, attempt domain.Attempt) bool {
	if object.Kind != string(domain.ArtifactCheckpoint) || attempt.IsSupervisionActivation() {
		return false
	}
	switch name {
	case domain.ContinuationArtifactName:
		return object.ID == domain.ContinuationArtifactID(attempt.ID) && object.MediaType == "text/markdown"
	case domain.ContinuationMetadataArtifactName:
		return object.ID == domain.ContinuationMetadataArtifactID(attempt.ID) && object.MediaType == "application/json"
	}
	return false
}

// keepContinuationCheckpoint checks the snapshot a result carries against its
// metadata and dates the snapshot artifact by its capture. A snapshot whose
// metadata does not describe it, or either half alone, is dropped with a
// warning: a checkpoint is evidence for a later attempt, and it never costs
// the result that carried it its import.
func keepContinuationCheckpoint(artifacts []domain.Artifact, payloads [][]byte, attempt domain.Attempt, uploadedAt time.Time) ([]domain.Artifact, [][]byte) {
	snapshot, metadata := -1, -1
	for index, artifact := range artifacts {
		if artifact.Kind != domain.ArtifactCheckpoint {
			continue
		}
		switch artifact.ID {
		case domain.ContinuationArtifactID(attempt.ID):
			snapshot = index
		case domain.ContinuationMetadataArtifactID(attempt.ID):
			metadata = index
		}
	}
	if snapshot < 0 && metadata < 0 {
		return artifacts, payloads
	}
	reason := ""
	var checkpoint domain.ContinuationCheckpoint
	switch {
	case snapshot < 0 || metadata < 0:
		reason = "the snapshot and its metadata must arrive together"
	case json.Unmarshal(payloads[metadata], &checkpoint) != nil:
		reason = "the metadata is not a checkpoint description"
	case checkpoint.AttemptID != attempt.ID || !strings.EqualFold(checkpoint.SHA256, artifacts[snapshot].SHA256) ||
		checkpoint.Size != artifacts[snapshot].Size || checkpoint.Size > domain.ContinuationSnapshotLimit:
		reason = "the metadata does not describe the snapshot"
	case checkpoint.CapturedAt.IsZero() || checkpoint.CapturedAt.After(uploadedAt):
		reason = "the capture time is missing or later than the upload"
	}
	if reason != "" {
		slog.Warn("continuation checkpoint dropped from a worker result", "attempt", attempt.ID, "reason", reason)
		keptArtifacts := make([]domain.Artifact, 0, len(artifacts))
		keptPayloads := make([][]byte, 0, len(payloads))
		for index := range artifacts {
			if index != snapshot && index != metadata {
				keptArtifacts = append(keptArtifacts, artifacts[index])
				keptPayloads = append(keptPayloads, payloads[index])
			}
		}
		return keptArtifacts, keptPayloads
	}
	artifacts[snapshot].CreatedAt = checkpoint.CapturedAt.UTC()
	return artifacts, payloads
}

// LatestContinuationArtifact returns the newest continuation snapshot the
// coordinator holds for a task of a run, by capture time, leaving out the
// named attempt's own. Ties break on the attempt ID so the answer never
// depends on storage order.
func LatestContinuationArtifact(artifacts []domain.Artifact, runID, taskID, excludeAttemptID string) *domain.Artifact {
	var latest *domain.Artifact
	for index := range artifacts {
		artifact := artifacts[index]
		if artifact.Kind != domain.ArtifactCheckpoint || artifact.Name != domain.ContinuationArtifactName ||
			artifact.WorkflowRunID != runID || artifact.TaskID != taskID || artifact.AttemptID == "" ||
			artifact.AttemptID == excludeAttemptID || artifact.ID != domain.ContinuationArtifactID(artifact.AttemptID) {
			continue
		}
		if latest == nil || artifact.CreatedAt.After(latest.CreatedAt) ||
			(artifact.CreatedAt.Equal(latest.CreatedAt) && artifact.AttemptID > latest.AttemptID) {
			copy := artifact
			latest = &copy
		}
	}
	return latest
}

// assignmentContinuationStore freezes an assignment's continuation decision;
// see sqlite.Store.FreezeAssignmentContinuation.
type assignmentContinuationStore interface {
	FreezeAssignmentContinuation(context.Context, int64, string, domain.Assignment, workerproto.ExecutionIdentity, sqlite.ContinuationDecision) (sqlite.ContinuationDecision, error)
}

// continuationInputs offers the continuation checkpoint contract to a worker
// that advertises it: the package then declares the capability, so the worker
// returns its own snapshot with the result, and a retry carries the latest
// snapshot an earlier attempt of the task left. A worker that does not
// advertise it gets exactly the package it got before.
//
// The decision is frozen with the assignment's first offer, as the session
// display is: a replayed offer must be the same package, whether or not the
// worker's inventory can still be read and whatever checkpoint arrived since.
// A store that cannot freeze it offers nothing.
func (b CoordinatorOfferBuilder) continuationInputs(ctx context.Context, state executionPackageState, assignment domain.Assignment, identity workerproto.ExecutionIdentity, inputs []workerproto.ArtifactObject) ([]workerproto.ArtifactObject, *workerproto.ContinuationInput, bool, error) {
	store, ok := b.Store.(assignmentContinuationStore)
	if !ok {
		return inputs, nil, false, nil
	}
	for _, input := range inputs {
		if strings.HasPrefix(filepath.ToSlash(input.Path), "inputs/continuation/") {
			return nil, nil, false, fmt.Errorf("execution package builder: static input path %q collides with the reserved continuation input", input.Path)
		}
	}
	advertised, known, inventoryErr := b.advertisedCapabilities(ctx, assignment.WorkerID)
	supported := inventoryErr == nil && known && slices.Contains(advertised, workerproto.PackageCapabilityContinuationCheckpoint)
	var proposed sqlite.ContinuationDecision
	if supported {
		proposed.Offered = true
		retained := make([]domain.Artifact, 0, len(state.artifacts))
		for _, artifact := range state.artifacts {
			retained = append(retained, artifact)
		}
		if latest := LatestContinuationArtifact(retained, state.run.ID, state.task.ID, state.attempt.ID); latest != nil {
			proposed.ArtifactID = latest.ID
			proposed.Input = &workerproto.ContinuationInput{
				Path: workerproto.ContinuationInputPath, AttemptID: latest.AttemptID,
				Size: latest.Size, CapturedAt: latest.CreatedAt.UTC(),
			}
		}
	}
	decision, err := store.FreezeAssignmentContinuation(ctx, b.CoordinatorEpoch, b.CoordinatorID, assignment, identity, proposed)
	if err != nil {
		return nil, nil, false, fmt.Errorf("execution package builder: freeze continuation decision: %w", err)
	}
	// A known incapable worker cannot execute a previously negotiated package.
	// An unavailable inventory is uncertainty, not evidence of capability loss.
	if decision.Offered && inventoryErr == nil && known && !supported {
		return nil, nil, false, fmt.Errorf("execution package builder: worker %q no longer supports frozen %q",
			assignment.WorkerID, workerproto.PackageCapabilityContinuationCheckpoint)
	}
	if !decision.Offered || decision.Input == nil {
		return inputs, nil, decision.Offered, nil
	}
	artifact, ok := state.artifacts[decision.ArtifactID]
	if !ok || artifact.WorkflowRunID != state.run.ID || artifact.TaskID != state.task.ID {
		return nil, nil, false, fmt.Errorf("execution package builder: frozen continuation checkpoint %q is not retained by this task", decision.ArtifactID)
	}
	object, err := packageArtifact(artifact, workerproto.ContinuationInputPath, "input")
	if err != nil {
		return nil, nil, false, fmt.Errorf("execution package builder: continuation checkpoint: %w", err)
	}
	input := *decision.Input
	return append(inputs, object), &input, true, nil
}
