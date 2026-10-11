package backlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Select only from the retained, digest-verified dependency view. Objects in
// the cache and records in ordinary outputs cannot satisfy environment.ref.
func chainedWorkspaceCommit(stage string, request WorkspacePreparation) (*CommitProvenance, error) {
	if !strings.HasPrefix(request.Environment.Ref, "campaign:") {
		return nil, nil
	}
	ref, name, err := ParseCampaignReference(request.Environment.Ref)
	if err != nil {
		return nil, err
	}
	var result *CommitProvenance
	for _, task := range request.DependencyTasks {
		namespace := task.Name
		source, ok := request.DependencySources[namespace]
		if !ok || source.WorkflowRunID != ref.RunID {
			continue
		}
		expectedNamespace := externalProducerNamespace(ref.RunID, domain.Task{ID: source.TaskID, Name: ref.TaskID})
		if source.TaskID != ref.TaskID && namespace != expectedNamespace {
			continue
		}
		if !DeclaresCommit(task, name) {
			return nil, fmt.Errorf("campaign ref %s is not a declared commit", request.Environment.Ref)
		}
		path, err := safeBundleFile(filepath.Join(stage, "dependencies"), filepath.Join(namespace, name))
		if err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		provenance, err := ParseCommitProvenance(raw)
		if err != nil {
			return nil, err
		}
		if provenance.WorkflowRunID != source.WorkflowRunID || provenance.TaskID != source.TaskID ||
			provenance.Name != name || provenance.Repository != request.Environment.Repository || provenance.FailedAttempt != nil {
			return nil, fmt.Errorf("campaign ref %s provenance producer or repository mismatch", request.Environment.Ref)
		}
		if result != nil {
			return nil, fmt.Errorf("campaign ref %s is ambiguous", request.Environment.Ref)
		}
		result = &provenance
	}
	if result == nil {
		return nil, fmt.Errorf("campaign ref %s has no retained successful commit", request.Environment.Ref)
	}
	return result, nil
}
