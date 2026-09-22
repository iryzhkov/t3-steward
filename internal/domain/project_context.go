package domain

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ProjectContextVersion = 1
	ProjectContextFile    = ".t3/context/index.json"
)

type ProjectContextStatus string

const (
	ProjectContextPinned   ProjectContextStatus = "pinned"
	ProjectContextAccepted ProjectContextStatus = "accepted"
)

type ProjectContextReferenceKind string

const (
	ContextReferenceGit       ProjectContextReferenceKind = "git"
	ContextReferenceJocasta   ProjectContextReferenceKind = "jocasta"
	ContextReferenceExecution ProjectContextReferenceKind = "execution"
)

type ProjectContextFreshness struct {
	ObservedAt   time.Time  `json:"observedAt" yaml:"observed_at"`
	FreshThrough *time.Time `json:"freshThrough,omitempty" yaml:"fresh_through,omitempty"`
}

type ProjectContextArtifactBinding struct {
	ArtifactID       string `json:"artifactId" yaml:"artifact_id"`
	Path             string `json:"path" yaml:"path"`
	SHA256           string `json:"sha256" yaml:"sha256"`
	SourceRunID      string `json:"sourceRunId,omitempty" yaml:"source_run_id,omitempty"`
	SourceTaskID     string `json:"sourceTaskId,omitempty" yaml:"source_task_id,omitempty"`
	SourceAttemptID  string `json:"sourceAttemptId,omitempty" yaml:"source_attempt_id,omitempty"`
	SourceArtifactID string `json:"sourceArtifactId,omitempty" yaml:"source_artifact_id,omitempty"`
}

type ProjectContextReference struct {
	ID        string                         `json:"id" yaml:"id"`
	Kind      ProjectContextReferenceKind    `json:"kind" yaml:"kind"`
	URI       string                         `json:"uri" yaml:"uri"`
	Revision  string                         `json:"revision" yaml:"revision"`
	Status    ProjectContextStatus           `json:"status" yaml:"status"`
	Authority string                         `json:"authority" yaml:"authority"`
	Topics    []string                       `json:"topics,omitempty" yaml:"topics,omitempty"`
	Binding   *ProjectContextArtifactBinding `json:"binding,omitempty" yaml:"binding,omitempty"`
}

type ProjectContextDecision struct {
	ID        string   `json:"id" yaml:"id"`
	Status    string   `json:"status" yaml:"status"`
	Summary   string   `json:"summary" yaml:"summary"`
	Authority string   `json:"authority" yaml:"authority"`
	Topics    []string `json:"topics,omitempty" yaml:"topics,omitempty"`
}

type ProjectContextLocation struct {
	Path     string   `json:"path" yaml:"path"`
	Revision string   `json:"revision" yaml:"revision"`
	Topics   []string `json:"topics,omitempty" yaml:"topics,omitempty"`
}

type ProjectContext struct {
	Version            int                       `json:"version" yaml:"version"`
	Revision           string                    `json:"revision" yaml:"revision"`
	Status             ProjectContextStatus      `json:"status" yaml:"status"`
	Objective          string                    `json:"objective" yaml:"objective"`
	Authority          []string                  `json:"authority" yaml:"authority"`
	Budget             string                    `json:"budget" yaml:"budget"`
	Outputs            []string                  `json:"outputs" yaml:"outputs"`
	RequiredReferences []string                  `json:"requiredReferences" yaml:"required_references"`
	References         []ProjectContextReference `json:"references" yaml:"references"`
	Decisions          []ProjectContextDecision  `json:"decisions,omitempty" yaml:"decisions,omitempty"`
	CheckpointDelta    []string                  `json:"checkpointDelta,omitempty" yaml:"checkpoint_delta,omitempty"`
	CodeLocations      []ProjectContextLocation  `json:"codeLocations,omitempty" yaml:"code_locations,omitempty"`
	InputLocations     []ProjectContextLocation  `json:"inputLocations,omitempty" yaml:"input_locations,omitempty"`
	Setup              []string                  `json:"setup" yaml:"setup"`
	Checks             []string                  `json:"checks" yaml:"checks"`
	CapabilityRefs     []string                  `json:"capabilityRefs" yaml:"capability_refs"`
	Freshness          ProjectContextFreshness   `json:"freshness" yaml:"freshness"`
}

type ProjectContextMatch struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Summary  string `json:"summary"`
	Revision string `json:"revision,omitempty"`
}

var (
	projectContextIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	projectContextSHA       = regexp.MustCompile(`^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$`)
	projectContextJocasta   = regexp.MustCompile(`^jocasta:[0-9a-f]{32}@[1-9][0-9]*$`)
)

func ValidateProjectContext(index *ProjectContext) error {
	if index == nil {
		return nil
	}
	if index.Version != ProjectContextVersion {
		return fmt.Errorf("project context: unsupported version %d", index.Version)
	}
	if !projectContextIDPattern.MatchString(index.Revision) || index.Status != ProjectContextPinned && index.Status != ProjectContextAccepted {
		return errors.New("project context: invalid revision or status")
	}
	for name, value := range map[string]string{"objective": index.Objective, "budget": index.Budget} {
		if strings.TrimSpace(value) == "" || strings.ContainsRune(value, 0) {
			return fmt.Errorf("project context: %s is required", name)
		}
	}
	if len(index.Authority) == 0 || len(index.Outputs) == 0 || len(index.Setup) == 0 || len(index.Checks) == 0 || index.Freshness.ObservedAt.IsZero() {
		return errors.New("project context: authority, outputs, setup, checks, and observed freshness are required")
	}
	if index.Freshness.FreshThrough != nil && index.Freshness.FreshThrough.Before(index.Freshness.ObservedAt) {
		return errors.New("project context: freshness expires before it was observed")
	}
	refs := make(map[string]ProjectContextReference, len(index.References))
	for _, ref := range index.References {
		if !projectContextIDPattern.MatchString(ref.ID) || strings.TrimSpace(ref.Authority) == "" {
			return errors.New("project context: reference has invalid id or missing authority")
		}
		if _, duplicate := refs[ref.ID]; duplicate {
			return fmt.Errorf("project context: duplicate reference %q", ref.ID)
		}
		if err := validateProjectContextReference(ref); err != nil {
			return fmt.Errorf("project context: reference %q: %w", ref.ID, err)
		}
		refs[ref.ID] = ref
	}
	required := map[string]bool{}
	for _, id := range index.RequiredReferences {
		ref, ok := refs[id]
		if !ok {
			return fmt.Errorf("project context: required reference %q is missing", id)
		}
		if required[id] {
			return fmt.Errorf("project context: required reference %q is repeated", id)
		}
		if ref.Status != ProjectContextAccepted && ref.Status != ProjectContextPinned {
			return fmt.Errorf("project context: required reference %q is not accepted or pinned", id)
		}
		required[id] = true
	}
	for _, id := range index.CapabilityRefs {
		if _, ok := refs[id]; !ok {
			return fmt.Errorf("project context: capability reference %q is missing", id)
		}
	}
	for _, decision := range index.Decisions {
		if !projectContextIDPattern.MatchString(decision.ID) || decision.Status != "accepted" ||
			strings.TrimSpace(decision.Summary) == "" || strings.TrimSpace(decision.Authority) == "" {
			return fmt.Errorf("project context: decision %q is not an accepted authoritative decision", decision.ID)
		}
	}
	for _, location := range append(append([]ProjectContextLocation(nil), index.CodeLocations...), index.InputLocations...) {
		if strings.TrimSpace(location.Path) == "" || strings.ContainsRune(location.Path, 0) || strings.TrimSpace(location.Revision) == "" {
			return errors.New("project context: code/input locations require path and revision")
		}
	}
	return nil
}

func validateProjectContextReference(ref ProjectContextReference) error {
	if ref.Status != ProjectContextPinned && ref.Status != ProjectContextAccepted {
		return errors.New("invalid status")
	}
	switch ref.Kind {
	case ContextReferenceGit:
		if strings.TrimSpace(ref.URI) == "" || !projectContextSHA.MatchString(ref.Revision) {
			return errors.New("git reference requires a repository URI and exact 40- or 64-hex revision")
		}
	case ContextReferenceJocasta:
		if !projectContextJocasta.MatchString(ref.URI) || !strings.HasSuffix(ref.URI, "@"+ref.Revision) {
			return errors.New("jocasta reference must be jocasta:ID@REVISION and match revision")
		}
	case ContextReferenceExecution:
		if !strings.HasPrefix(ref.URI, "execution:") || strings.Count(strings.TrimPrefix(ref.URI, "execution:"), "/") != 3 ||
			strings.TrimSpace(ref.Revision) == "" {
			return errors.New("execution reference requires run/task/attempt/artifact URI and revision")
		}
	default:
		return errors.New("unsupported kind")
	}
	if ref.Binding != nil {
		b := ref.Binding
		if strings.TrimSpace(b.ArtifactID) == "" || strings.TrimSpace(b.Path) == "" || !regexp.MustCompile(`^[0-9a-fA-F]{64}$`).MatchString(b.SHA256) {
			return errors.New("artifact binding requires id, path, and sha256")
		}
		source := []string{b.SourceRunID, b.SourceTaskID, b.SourceAttemptID, b.SourceArtifactID}
		present := 0
		for _, value := range source {
			if value != "" {
				present++
			}
		}
		if present != 0 && present != len(source) {
			return errors.New("artifact binding source provenance is incomplete")
		}
	}
	return nil
}

// LookupProjectContext performs bounded deterministic lexical/topic lookup. Required
// references are still selected explicitly; this helper only supplements them.
func LookupProjectContext(index ProjectContext, query string, limit int) ([]ProjectContextMatch, error) {
	if err := ValidateProjectContext(&index); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 64 {
		return nil, errors.New("project context lookup limit must be between 1 and 64")
	}
	terms := strings.Fields(strings.ToLower(query))
	if len(terms) == 0 || len(terms) > 16 {
		return nil, errors.New("project context lookup requires 1 to 16 lexical terms")
	}
	var matches []ProjectContextMatch
	add := func(kind, id, summary, revision string, topics []string) {
		haystack := strings.ToLower(strings.Join(append([]string{id, summary, revision}, topics...), " "))
		for _, term := range terms {
			if !strings.Contains(haystack, term) {
				return
			}
		}
		matches = append(matches, ProjectContextMatch{Kind: kind, ID: id, Summary: summary, Revision: revision})
	}
	for _, ref := range index.References {
		add("reference", ref.ID, ref.URI, ref.Revision, ref.Topics)
	}
	for _, decision := range index.Decisions {
		add("decision", decision.ID, decision.Summary, "", decision.Topics)
	}
	for _, location := range index.CodeLocations {
		add("code", location.Path, location.Path, location.Revision, location.Topics)
	}
	for _, location := range index.InputLocations {
		add("input", location.Path, location.Path, location.Revision, location.Topics)
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Kind != matches[j].Kind {
			return matches[i].Kind < matches[j].Kind
		}
		return matches[i].ID < matches[j].ID
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}
