package workerproto

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func validatePackageProjectContext(pkg ExecutionPackage) error {
	if err := domain.ValidateProjectContext(pkg.Context); err != nil {
		return err
	}
	if pkg.Context == nil {
		return nil
	}
	index := pkg.Context
	if index.Freshness.ObservedAt.After(pkg.CreatedAt) {
		return errors.New("project context: freshness observation is newer than the assignment")
	}
	if index.Freshness.FreshThrough != nil {
		requiredThrough := pkg.CreatedAt
		if pkg.ExpiresAt != nil {
			requiredThrough = *pkg.ExpiresAt
		}
		if index.Freshness.FreshThrough.Before(requiredThrough) {
			return fmt.Errorf("project context: stale at %s before assignment validity ends at %s",
				index.Freshness.FreshThrough.UTC().Format("2006-01-02T15:04:05Z"),
				requiredThrough.UTC().Format("2006-01-02T15:04:05Z"))
		}
	}
	refs := make(map[string]domain.ProjectContextReference, len(index.References))
	for _, ref := range index.References {
		refs[ref.ID] = ref
	}
	validated := make(map[string]bool, len(index.RequiredReferences))
	for _, id := range index.RequiredReferences {
		ref := refs[id]
		if ref.Binding == nil {
			return fmt.Errorf("project context: required reference %q has no retained artifact binding", id)
		}
		if err := validateContextBinding(pkg, ref); err != nil {
			return fmt.Errorf("project context: required reference %q: %w", id, err)
		}
		validated[id] = true
	}
	for _, ref := range index.References {
		if ref.Kind != domain.ContextReferenceExecution || validated[ref.ID] {
			continue
		}
		if ref.Binding == nil {
			return fmt.Errorf("project context: execution reference %q has no retained artifact binding", ref.ID)
		}
		if err := validateContextBinding(pkg, ref); err != nil {
			return fmt.Errorf("project context: execution reference %q: %w", ref.ID, err)
		}
	}
	return nil
}

func validateContextBinding(pkg ExecutionPackage, ref domain.ProjectContextReference) error {
	binding := ref.Binding
	matches := 0
	var matchedProvenance *DependencyProvenance
	objects := append([]ArtifactObject(nil), pkg.StaticInputs...)
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			if object.ID == binding.ArtifactID {
				matchedProvenance = dependency.Provenance
			}
			objects = append(objects, object)
		}
	}
	for _, object := range objects {
		if object.ID != binding.ArtifactID {
			continue
		}
		matches++
		if filepath.ToSlash(object.Path) != filepath.ToSlash(binding.Path) ||
			!strings.EqualFold(object.SHA256, binding.SHA256) {
			return errors.New("retained artifact path or digest does not match the pinned binding")
		}
	}
	if matches != 1 {
		return fmt.Errorf("retained artifact %q has %d package matches; want exactly one", binding.ArtifactID, matches)
	}
	hasSource := binding.SourceRunID != ""
	if ref.Kind == domain.ContextReferenceExecution && (!hasSource || matchedProvenance == nil) {
		return errors.New("execution reference requires retained dependency provenance")
	}
	if !hasSource {
		if matchedProvenance != nil {
			return errors.New("source provenance is omitted for a cross-run dependency")
		}
		return nil
	}
	if matchedProvenance == nil ||
		matchedProvenance.RunID != binding.SourceRunID ||
		matchedProvenance.TaskID != binding.SourceTaskID ||
		matchedProvenance.AttemptID != binding.SourceAttemptID ||
		matchedProvenance.SourceArtifacts[binding.ArtifactID] != binding.SourceArtifactID {
		return errors.New("source run/task/attempt/artifact provenance does not match the retained binding")
	}
	expectedURI := "execution:" + binding.SourceRunID + "/" + binding.SourceTaskID + "/" +
		binding.SourceAttemptID + "/" + binding.SourceArtifactID
	if ref.Kind == domain.ContextReferenceExecution && ref.URI != expectedURI {
		return fmt.Errorf("execution URI %q does not match bound provenance %q", ref.URI, expectedURI)
	}
	return nil
}
