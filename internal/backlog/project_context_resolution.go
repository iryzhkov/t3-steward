package backlog

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type resolvedProjectContextAcceptance struct {
	Receipt  domain.ProjectContextAcceptance
	Decision domain.GateDecision
}

// resolveAuthoredProjectContext makes the per-run coordinator copy. The authored
// template remains authority-free; Git and Jocasta become pinned only by an
// explicitly selected immutable submission input.
func resolveAuthoredProjectContext(index *domain.ProjectContext, inputs map[string]domain.Artifact, now time.Time) (*domain.ProjectContext, error) {
	if index == nil {
		return nil, nil
	}
	resolved := cloneProjectContext(index)
	if resolved.Freshness.ObservedAt.After(now) {
		return nil, errors.New("project context freshness observation is in the future")
	}
	if resolved.Freshness.FreshThrough != nil && resolved.Freshness.FreshThrough.Before(now) {
		return nil, fmt.Errorf("project context is stale: freshness ended at %s", resolved.Freshness.FreshThrough.UTC().Format(time.RFC3339))
	}
	for n := range resolved.References {
		ref := &resolved.References[n]
		switch ref.Kind {
		case domain.ContextReferenceGit, domain.ContextReferenceJocasta:
			selector := filepath.ToSlash(filepath.Clean(ref.Artifact))
			if selector == "." || selector != ref.Artifact {
				return nil, fmt.Errorf("project context reference %q has invalid artifact selector %q", ref.ID, ref.Artifact)
			}
			artifact, ok := inputs[selector]
			if !ok {
				return nil, fmt.Errorf("project context reference %q selects missing retained input %q", ref.ID, selector)
			}
			ref.Status = domain.ProjectContextPinned
			ref.Authority = "artifact-custody:" + artifact.ID
			ref.Binding = &domain.ProjectContextArtifactBinding{
				ArtifactID: artifact.ID,
				Path:       "inputs/" + selector,
				SHA256:     artifact.SHA256,
			}
		case domain.ContextReferenceExecution:
			// Cross-run custody and review acceptance are resolved later from
			// the exact retained producer selected by inputs_from.
		default:
			return nil, fmt.Errorf("project context reference %q has unsupported kind %q", ref.ID, ref.Kind)
		}
	}
	return resolved, nil
}

func finalizeProjectContexts(records *sqlite.CoordinatorRecords, runID string) error {
	var run *domain.WorkflowRun
	for n := range records.WorkflowRuns {
		if records.WorkflowRuns[n].ID == runID {
			if run != nil {
				return errors.New("project context finalization found duplicate target run")
			}
			run = &records.WorkflowRuns[n]
		}
	}
	if run == nil {
		return errors.New("project context finalization cannot find target run")
	}
	if run.Graph != nil {
		for n := range run.Graph.Tasks {
			if err := finalizeProjectContext(run.Graph.Tasks[n].Context); err != nil {
				return fmt.Errorf("task %q: %w", run.Graph.Tasks[n].Name, err)
			}
		}
		for n := range records.Tasks {
			if records.Tasks[n].RunID != runID {
				continue
			}
			for _, exact := range run.Graph.Tasks {
				if records.Tasks[n].ID == exact.ID {
					records.Tasks[n].Context = cloneProjectContext(exact.Context)
				}
			}
		}
		return nil
	}
	for n := range records.Tasks {
		if records.Tasks[n].RunID != runID {
			continue
		}
		if err := finalizeProjectContext(records.Tasks[n].Context); err != nil {
			return fmt.Errorf("task %q: %w", records.Tasks[n].Name, err)
		}
	}
	return nil
}

func finalizeProjectContext(index *domain.ProjectContext) error {
	if index == nil {
		return nil
	}
	authorities := make([]string, 0, len(index.References))
	hasExecution := false
	for _, ref := range index.References {
		if ref.Binding == nil {
			return fmt.Errorf("project context reference %q was not resolved to immutable artifact custody", ref.ID)
		}
		switch ref.Kind {
		case domain.ContextReferenceExecution:
			hasExecution = true
			if ref.Status != domain.ProjectContextAccepted || ref.Acceptance == nil {
				return fmt.Errorf("execution reference %q has no exact accepted gate decision", ref.ID)
			}
		case domain.ContextReferenceGit, domain.ContextReferenceJocasta:
			if ref.Status != domain.ProjectContextPinned {
				return fmt.Errorf("reference %q was not pinned from its selected retained input", ref.ID)
			}
		}
		authorities = append(authorities, ref.Authority)
	}
	sort.Strings(authorities)
	index.Authority = compactStrings(authorities)
	if hasExecution {
		index.Status = domain.ProjectContextAccepted
	} else {
		index.Status = domain.ProjectContextPinned
	}
	sort.Slice(index.Decisions, func(a, b int) bool { return index.Decisions[a].ID < index.Decisions[b].ID })
	sort.Strings(index.CheckpointDelta)
	index.CheckpointDelta = compactStrings(index.CheckpointDelta)
	return domain.ValidateProjectContext(index)
}

func appendResolvedDecision(index *domain.ProjectContext, decision domain.GateDecision) {
	authority := string(decision.Actor.Kind) + ":" + decision.Actor.Principal
	if decision.Actor.ActivationEpoch > 0 {
		authority += "@" + strconv.FormatInt(decision.Actor.ActivationEpoch, 10)
	}
	for _, current := range index.Decisions {
		if current.ID == decision.ID {
			return
		}
	}
	index.Decisions = append(index.Decisions, domain.ProjectContextDecision{
		ID: decision.ID, Status: "accepted", Summary: decision.Reason, Authority: authority,
	})
	for _, producer := range decision.Evidence.Producers {
		artifacts := make([]string, 0, len(producer.ArtifactDigests))
		for _, digest := range producer.ArtifactDigests {
			artifacts = append(artifacts, digest.ArtifactID+"="+digest.Digest)
		}
		sort.Strings(artifacts)
		commits := make([]string, 0, len(producer.CommitIdentities))
		for _, identity := range producer.CommitIdentities {
			commits = append(commits, identity.Repository+"@"+identity.Commit)
		}
		sort.Strings(commits)
		index.CheckpointDelta = append(index.CheckpointDelta, fmt.Sprintf(
			"evidence=%s graphRevision=%d task=%s attempt=%s resultRevision=%d artifacts=[%s] commits=[%s]",
			decision.Evidence.ID, decision.Evidence.GraphRevision, producer.TaskID, producer.AttemptID,
			producer.ResultRevision, strings.Join(artifacts, ","), strings.Join(commits, ","),
		))
	}
}

func cloneProjectContext(index *domain.ProjectContext) *domain.ProjectContext {
	if index == nil {
		return nil
	}
	clone := *index
	clone.Authority = append([]string(nil), index.Authority...)
	clone.Outputs = append([]string(nil), index.Outputs...)
	clone.RequiredReferences = append([]string(nil), index.RequiredReferences...)
	clone.References = append([]domain.ProjectContextReference(nil), index.References...)
	for n := range clone.References {
		clone.References[n].Topics = append([]string(nil), index.References[n].Topics...)
		if index.References[n].Binding != nil {
			binding := *index.References[n].Binding
			clone.References[n].Binding = &binding
		}
		if index.References[n].Acceptance != nil {
			acceptance := *index.References[n].Acceptance
			clone.References[n].Acceptance = &acceptance
		}
	}
	clone.Decisions = append([]domain.ProjectContextDecision(nil), index.Decisions...)
	for n := range clone.Decisions {
		clone.Decisions[n].Topics = append([]string(nil), index.Decisions[n].Topics...)
	}
	clone.CheckpointDelta = append([]string(nil), index.CheckpointDelta...)
	clone.CodeLocations = append([]domain.ProjectContextLocation(nil), index.CodeLocations...)
	for n := range clone.CodeLocations {
		clone.CodeLocations[n].Topics = append([]string(nil), index.CodeLocations[n].Topics...)
	}
	clone.InputLocations = append([]domain.ProjectContextLocation(nil), index.InputLocations...)
	for n := range clone.InputLocations {
		clone.InputLocations[n].Topics = append([]string(nil), index.InputLocations[n].Topics...)
	}
	if index.Freshness.FreshThrough != nil {
		freshThrough := *index.Freshness.FreshThrough
		clone.Freshness.FreshThrough = &freshThrough
	}
	clone.Setup = append([]string(nil), index.Setup...)
	clone.Checks = append([]string(nil), index.Checks...)
	clone.CapabilityRefs = append([]string(nil), index.CapabilityRefs...)
	return &clone
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := values[:0]
	for _, value := range values {
		if value != "" && (len(result) == 0 || result[len(result)-1] != value) {
			result = append(result, value)
		}
	}
	return result
}
