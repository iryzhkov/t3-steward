package domain

import (
	"strings"
	"time"
)

// RoleSelection is the coordinator's immutable selection receipt.
type RoleSelection struct {
	Role         string                 `json:"role"`
	Route        string                 `json:"route"`
	Effort       string                 `json:"effort"`
	PolicyDigest string                 `json:"policyDigest"`
	Reason       string                 `json:"reason"`
	Candidates   []RoleCandidateVerdict `json:"candidates,omitempty"`
	Diversity    RoleDiversity          `json:"diversity"`
	ResolvedAt   time.Time              `json:"resolvedAt"`
}

type RoleCandidateVerdict struct {
	Route    string `json:"route"`
	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}

type RoleDiversity struct {
	ProducerFamilies []string `json:"producerFamilies,omitempty"`
	CrossProvider    bool     `json:"crossProvider"`
	Reason           string   `json:"reason"`
}

// Valid checks the concrete route and bounded effort before a receipt becomes executable.
func (selection RoleSelection) Valid() bool {
	instance, model, concrete := strings.Cut(selection.Route, "/")
	return len(selection.Route) <= 256 && concrete && instance != "" && model != "" && !strings.ContainsAny(selection.Route, " \t\r\n") && (selection.Effort == "low" || selection.Effort == "medium" || selection.Effort == "high")
}
func CloneRoleSelection(selection RoleSelection) RoleSelection {
	selection.Candidates = append([]RoleCandidateVerdict(nil), selection.Candidates...)
	selection.Diversity.ProducerFamilies = append([]string(nil), selection.Diversity.ProducerFamilies...)
	return selection
}

func (selection RoleSelection) ProviderRoute() ProviderRoute {
	instance, model, _ := strings.Cut(selection.Route, "/")
	return ProviderRoute{ProviderInstanceID: instance, Model: model, Options: map[string]string{"effort": selection.Effort}}
}

// ApplyRoleSelection returns a run-local task without changing its shared template.
func ApplyRoleSelection(task Task, selection RoleSelection) Task {
	selection = CloneRoleSelection(selection)
	task.RoleSelection = &selection
	instance, model, ok := strings.Cut(selection.Route, "/")
	if ok && instance != "" && model != "" {
		task.Routes = []ProviderRoute{{ProviderInstanceID: instance, Model: model, Options: map[string]string{"effort": selection.Effort}}}
	}
	return task
}
