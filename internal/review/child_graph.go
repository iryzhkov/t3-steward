package review

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

// RetainedFile is a trusted coordinator snapshot, never public submission data.
// Bytes must come from verified retained descriptors, not executor paths.
type RetainedFile struct {
	Artifact domain.Artifact
	Bytes    []byte
}

// ChildPreparation can only be constructed after content validation. Storage
// rebuilds policy and identities from its own issued authority, not this value.
type ChildPreparation struct {
	files        []RetainedFile
	criteriaName string
	deadline     time.Time
}

type ChildGraph struct {
	Workflow  domain.Workflow
	Run       domain.WorkflowRun
	Tasks     []domain.Task
	Attempts  []domain.Attempt
	Artifacts []domain.Artifact
}

func ChildWorkflowID(c CheckpointAuthority) string { return "rw-" + authorityDigest(c.Key()) }
func ChildAttemptID(c CheckpointAuthority, member string) string {
	return "rt-" + authorityDigest([]string{c.Key(), member})
}
func ChildInputID(c CheckpointAuthority, name string) string {
	return "ri-" + authorityDigest([]string{c.Key(), name})
}
func ChildPromptID(c CheckpointAuthority, member string) string {
	return "rp-" + authorityDigest([]string{c.Key(), member})
}
func ChildPromptName(member string) string { return "prompts/" + member + ".md" }

// ChildPrompt freezes the existing reviewer output contract and exact diff and criteria.
func ChildPrompt(f FrozenAuthority, c CheckpointAuthority, m MemberRequirement, manifest pinnedinput.Manifest, criteriaName string) (string, error) {
	p, err := Prompt(m.Reviewer(c.MemberTaskID(m.ID)), manifest)
	if err != nil {
		return "", err
	}
	return p + "Base commit: " + f.Parent.BaseCommit + "\nHead commit: " + c.Checkpoint.HeadCommit + "\nCriteria input: " + criteriaName + "\nCriteria SHA256: " + f.Requirements.CriteriaDigest + "\n", nil
}

func PrepareChild(f FrozenAuthority, c CheckpointAuthority, criteriaName string, deadline time.Time, files []RetainedFile) (ChildPreparation, error) {
	p := ChildPreparation{criteriaName: criteriaName, deadline: deadline.UTC()}
	for _, file := range files {
		p.files = append(p.files, RetainedFile{Artifact: file.Artifact, Bytes: bytes.Clone(file.Bytes)})
	}
	sort.Slice(p.files, func(i, j int) bool { return p.files[i].Artifact.Name < p.files[j].Artifact.Name })
	if _, err := p.Build(f, c, time.Time{}); err != nil {
		return ChildPreparation{}, err
	}
	return p, nil
}

// Build is pure. It rechecks retained metadata/bytes and derives all policy from
// the supplied authority; the SQLite caller supplies only durable stored values.
func (p ChildPreparation) Build(f FrozenAuthority, c CheckpointAuthority, created time.Time) (ChildGraph, error) {
	var g ChildGraph
	canonical, err := f.Canonical()
	if err != nil {
		return g, err
	}
	if !reflect.DeepEqual(f, canonical) || c.AuthorityKey != f.Key() || c.RoundID != c.Key() || c.Number < 1 || c.Number > f.Requirements.RoundLimit {
		return g, errors.New("child authority/checkpoint mismatch")
	}
	if err := c.Checkpoint.Validate(); err != nil {
		return g, err
	}
	if p.deadline.IsZero() || p.deadline.Year() > 9999 || p.deadline.Year() < 1 || !created.IsZero() && !p.deadline.After(created) {
		return g, errors.New("child requires finite deadline after allocation")
	}
	prompts := map[string]MemberRequirement{}
	for _, m := range f.Requirements.Members {
		prompts[ChildPromptName(m.ID)] = m
	}
	seen := map[string]bool{}
	var entries []pinnedinput.Entry
	var inputs []string
	criteriaFound := false
	for _, file := range p.files {
		a := file.Artifact
		if seen[a.Name] || a.WorkflowRunID != c.RoundID || a.AttemptID != "" || a.Kind != domain.ArtifactInput || a.Producer != "submission" || !pinnedinput.ValidName(a.StoragePath) || a.Size > pinnedinput.MaxFileBytes || a.CreatedAt.IsZero() ||
			a.Size != int64(len(file.Bytes)) || a.SHA256 != authorityBytesDigest(file.Bytes) {
			return g, errors.New("child retained artifact mismatch")
		}
		seen[a.Name] = true
		if m, ok := prompts[a.Name]; ok {
			if a.ID != ChildPromptID(c, m.ID) || a.TaskID != c.MemberTaskID(m.ID) {
				return g, errors.New("child prompt ownership mismatch")
			}
		} else {
			if a.ID != ChildInputID(c, a.Name) || a.TaskID != "" {
				return g, errors.New("child input ownership mismatch")
			}
			entries = append(entries, pinnedinput.Entry{Name: a.Name, Size: a.Size, SHA256: a.SHA256})
			inputs = append(inputs, a.ID)
			if a.Name == p.criteriaName {
				criteriaFound = true
				if a.SHA256 != f.Requirements.CriteriaDigest {
					return g, errors.New("child criteria digest mismatch")
				}
			}
		}
		g.Artifacts = append(g.Artifacts, a)
	}
	manifest, err := pinnedinput.NewManifest(entries)
	if err != nil {
		return g, err
	}
	if manifest.Digest != c.Checkpoint.InputDigest || !criteriaFound {
		return g, errors.New("child manifest/criteria mismatch")
	}
	for _, m := range f.Requirements.Members {
		name := ChildPromptName(m.ID)
		if !seen[name] {
			return g, errors.New("child prompt missing")
		}
		expected, err := ChildPrompt(f, c, m, manifest, p.criteriaName)
		if err != nil {
			return g, err
		}
		for _, file := range p.files {
			if file.Artifact.Name == name && string(file.Bytes) != expected {
				return g, errors.New("child prompt bytes mismatch")
			}
		}
		taskID := c.MemberTaskID(m.ID)
		instance, model, _ := strings.Cut(m.Route, "/")
		deadline := p.deadline
		class := domain.TaskClassRequired
		if !m.Required {
			class = domain.TaskClassSurplus
		}
		task := domain.Task{ID: taskID, RunID: c.RoundID, WorkflowID: ChildWorkflowID(c), Name: m.ID, Class: class, MaxTurns: 12, PromptArtifactID: ChildPromptID(c, m.ID), InputArtifactIDs: append([]string(nil), inputs...), Deadline: &deadline,
			Routes: []domain.ProviderRoute{{ProviderInstanceID: instance, Model: model}}, Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}}}
		if m.Execution != nil {
			task.MaxTurns = m.Execution.MaxTurns
			task.ResourceDemand = m.Execution.Resources
			task.Routes[0].QuotaPoolID = m.Execution.QuotaPoolID
			task.Routes[0].Options = map[string]string{"effort": m.Execution.Effort}
		}
		progress := domain.ProgressReady
		if m.Role == "judge" {
			task.ReviewJudge = true
			task.DependencyInputs = map[string][]string{}
			for _, lens := range f.Requirements.Members {
				if strings.HasPrefix(lens.Role, "swarm:") {
					task.Needs = append(task.Needs, lens.ID)
					task.DependencyInputs[lens.ID] = []string{"verdict.json"}
				}
			}
			progress = domain.ProgressBlocked
		}
		g.Tasks = append(g.Tasks, task)
		g.Attempts = append(g.Attempts, domain.Attempt{ID: ChildAttemptID(c, m.ID), WorkflowRunID: c.RoundID, TaskID: taskID, Number: 1, Revision: 1, Progress: progress, Control: domain.ControlUnassigned, UpdatedAt: created})
	}
	var taskIDs []string
	for _, t := range g.Tasks {
		taskIDs = append(taskIDs, t.ID)
	}
	g.Workflow = domain.Workflow{ID: ChildWorkflowID(c), Version: 2, Name: "checkpoint-review", Project: f.Parent.Repository, Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: c.Checkpoint.HeadCommit}, Class: domain.TaskClassRequired, TaskIDs: taskIDs, InputArtifactIDs: append([]string(nil), inputs...), InputManifest: &manifest, CreatedAt: created}
	g.Run, err = domain.BindRunSink(domain.WorkflowRun{ID: c.RoundID, WorkflowID: g.Workflow.ID, Progress: domain.ProgressQueued, Revision: 1, CreatedAt: created, UpdatedAt: created, InputArtifactIDs: append([]string(nil), inputs...)}, g.Tasks)
	return g, err
}
func authorityBytesDigest(raw []byte) string {
	// Canonical JSON hashing is intentionally separate from retained-byte hashing.
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (p ChildPreparation) Deadline() time.Time { return p.deadline }
