package review

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

// RequirementsSpec is coordinator input, never an executor artifact or public wire field.
// Members freeze exact role/count/route/catalog metadata; no route substitutions or waivers.
type RequirementsSpec struct {
	Risk                string
	CriteriaDigest      string
	PolicyDigest        string
	RequiredReviewers   int
	MinProviderFamilies int
	RoundLimit          int
	Members             []MemberRequirement
}
type MemberRequirement struct {
	ID             string
	Role           string
	Route          string
	ProviderFamily string
	Tier           string
	Required       bool
}
type Requirements struct {
	spec   RequirementsSpec
	digest string
}

func NewRequirements(spec RequirementsSpec) (Requirements, error) {
	spec.Members = append([]MemberRequirement(nil), spec.Members...)
	ceiling := 2
	switch spec.Risk {
	case "routine":
	case "risky":
		ceiling = 3
	default:
		return Requirements{}, errors.New("risk must be routine or risky")
	}
	if spec.RoundLimit == 0 {
		spec.RoundLimit = ceiling
	}
	if spec.RoundLimit < 1 || spec.RoundLimit > ceiling || !pinnedinput.ValidDigest(spec.CriteriaDigest) || !pinnedinput.ValidDigest(spec.PolicyDigest) {
		return Requirements{}, errors.New("invalid frozen limits or criteria/policy digest")
	}
	// M16 has no diversity waiver, even when only one family is currently available.
	if spec.MinProviderFamilies < 2 || spec.MinProviderFamilies > 32 || len(spec.Members) > 32 {
		return Requirements{}, errors.New("invalid provider diversity or member bound")
	}
	sort.Slice(spec.Members, func(i, j int) bool { return spec.Members[i].ID < spec.Members[j].ID })
	round := Round{Risk: spec.Risk}
	required, critical := 0, 0
	families := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range spec.Members {
		if !IDPattern.MatchString(m.ID) || seen[m.ID] || len(m.Role) > 64 || !safeFindingText(m.Role) || !safeFindingText(m.Route) {
			return Requirements{}, errors.New("invalid or duplicate member identity/role/route")
		}
		seen[m.ID] = true
		round.Reviewers = append(round.Reviewers, m.Reviewer(""))
		if m.Required {
			required++
			families[m.ProviderFamily] = true
			if m.Role == "independent" && m.Tier == "critical" {
				critical++
			}
		}
	}
	if err := ValidateSelection(round, spec.MinProviderFamilies); err != nil {
		return Requirements{}, err
	}
	if required != spec.RequiredReviewers || required < 2 || len(families) < spec.MinProviderFamilies || spec.Risk == "risky" && critical == 0 {
		return Requirements{}, errors.New("required count, diversity or risky critical tier unmet")
	}
	return Requirements{spec: spec, digest: authorityDigest(spec)}, nil
}
func (r Requirements) Snapshot() RequirementsSpec {
	s := r.spec
	s.Members = append([]MemberRequirement(nil), s.Members...)
	return s
}
func (r Requirements) Digest() string { return r.digest }
func (m MemberRequirement) Reviewer(taskID string) Reviewer {
	return Reviewer{ID: m.ID, TaskID: taskID, Role: m.Role, Route: m.Route, ProviderFamily: m.ProviderFamily, Tier: m.Tier, Required: m.Required}
}
func authorityDigest(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func FullCommitID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ParentBinding names the exact coordinator-held executor identity. Repository is
// the immutable workflow project identity; BaseCommit is supplied by trusted admission.
type ParentBinding struct {
	RunID           string
	TaskID          string
	AttemptID       string
	ThreadID        string
	AssignmentID    string
	AssignmentEpoch int64
	IssuedRevision  int64
	Repository      string
	BaseCommit      string
	ExecutorRoute   string
}

func (p ParentBinding) Validate() error {
	for _, id := range []string{p.RunID, p.TaskID, p.AttemptID, p.ThreadID, p.AssignmentID, p.Repository} {
		if !IDPattern.MatchString(id) {
			return errors.New("invalid parent identity")
		}
	}
	if p.AssignmentEpoch < 1 || p.IssuedRevision < 1 || !FullCommitID(p.BaseCommit) || !ValidRoute(p.ExecutorRoute) || !safeFindingText(p.ExecutorRoute) {
		return errors.New("invalid parent epoch/revision/base/route")
	}
	return nil
}

// Checkpoint is a trusted coordinator snapshot. The same ID must retain head/input.
type Checkpoint struct {
	ID          string
	HeadCommit  string
	InputDigest string
}

func (c Checkpoint) Validate() error {
	if !IDPattern.MatchString(c.ID) || !FullCommitID(c.HeadCommit) || !pinnedinput.ValidDigest(c.InputDigest) {
		return errors.New("invalid checkpoint identity/head/input")
	}
	return nil
}

type FrozenAuthority struct {
	Parent             ParentBinding
	Requirements       RequirementsSpec
	RequirementsDigest string
}

func NewFrozenAuthority(parent ParentBinding, r Requirements) (FrozenAuthority, error) {
	if err := parent.Validate(); err != nil {
		return FrozenAuthority{}, err
	}
	checked, err := NewRequirements(r.Snapshot())
	if err != nil || checked.Digest() != r.Digest() {
		return FrozenAuthority{}, errors.New("requirements must be constructed and validated")
	}
	return FrozenAuthority{Parent: parent, Requirements: checked.Snapshot(), RequirementsDigest: checked.Digest()}, nil
}
func (a FrozenAuthority) Canonical() (FrozenAuthority, error) {
	r, err := NewRequirements(a.Requirements)
	if err != nil {
		return FrozenAuthority{}, err
	}
	if r.Digest() != a.RequirementsDigest {
		return FrozenAuthority{}, errors.New("frozen requirements digest mismatch")
	}
	return NewFrozenAuthority(a.Parent, r)
}
func (a FrozenAuthority) Key() string {
	return "ra-" + authorityDigest([]string{a.Parent.RunID, a.Parent.TaskID})
}

type CheckpointAuthority struct {
	AuthorityKey string
	Checkpoint   Checkpoint
	Number       int
	RoundID      string
}

func (a CheckpointAuthority) Key() string {
	return "rc-" + authorityDigest([]string{a.AuthorityKey, a.Checkpoint.ID})
}

func (a CheckpointAuthority) MemberTaskID(memberID string) string {
	return "rm-" + authorityDigest([]string{a.Key(), memberID})
}

// ValidateBoundEvidence is a validator, not a completion gate. Storage callers must
// load the durable round themselves and supply a coordinator-trusted final head.
func ValidateBoundEvidence(frozen FrozenAuthority, checkpoint CheckpointAuthority, round Round, trustedHead string) (string, error) {
	f, err := frozen.Canonical()
	if err != nil {
		return "", err
	}
	if err := checkpoint.Checkpoint.Validate(); err != nil {
		return "", err
	}
	if !FullCommitID(trustedHead) || trustedHead != checkpoint.Checkpoint.HeadCommit || checkpoint.AuthorityKey != f.Key() || checkpoint.Number < 1 || checkpoint.Number > f.Requirements.RoundLimit || checkpoint.RoundID != checkpoint.Key() ||
		round.ID != checkpoint.RoundID || round.WorkflowRunID != checkpoint.RoundID || round.Risk != f.Requirements.Risk ||
		round.BaseCommit != f.Parent.BaseCommit || round.HeadCommit != trustedHead || round.InputManifestDigest != checkpoint.Checkpoint.InputDigest ||
		len(round.Reviewers) != len(f.Requirements.Members) {
		return "", errors.New("bound review identity/head/policy mismatch")
	}
	// Re-run existing strict validation from retained raw results. Never trust a
	// stored Combined or typed Verdict alone, even if corrupted into an accept.
	rebuilt := Round{InputManifestDigest: round.InputManifestDigest}
	for i, m := range f.Requirements.Members {
		actual := round.Reviewers[i]
		taskID := checkpoint.MemberTaskID(m.ID)
		if actual.ID != m.ID || actual.TaskID != taskID || actual.TaskID == f.Parent.TaskID || actual.Role != m.Role || actual.Route != m.Route || actual.ProviderFamily != m.ProviderFamily || actual.Tier != m.Tier || actual.Required != m.Required {
			return "", errors.New("bound review member mismatch")
		}
		member := m.Reviewer(taskID)
		member.State = "pending"
		rebuilt.Reviewers = append(rebuilt.Reviewers, member)
		if !m.Required {
			continue
		}
		if actual.State != "succeeded" || actual.Failure != "" || strings.TrimSpace(actual.ReviewMD) == "" {
			return "", errors.New("required reviewer has no successful validated result")
		}
		if err := rebuilt.ApplyResult(m.ID, Result{State: actual.State, ReviewMD: actual.ReviewMD, VerdictJSON: actual.VerdictJSON}, round.UpdatedAt); err != nil {
			return "", err
		}
	}
	verdict := rebuilt.CombinedVerdict()
	if verdict != "accept" && verdict != "accept-with-changes" {
		return "", errors.New("required review evidence does not qualify")
	}
	return verdict, nil
}
