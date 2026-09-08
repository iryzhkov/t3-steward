package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	t3control "github.com/iryzhkov/t3-quota-watchdog/internal/control/t3"
)

// providerCache mirrors the fields of <data_dir>/caches/<instance>.json that
// T3 writes for every provider instance it knows.
type providerCache struct {
	InstanceID string `json:"instanceId"`
	Driver     string `json:"driver"`
	Enabled    bool   `json:"enabled"`
	Installed  bool   `json:"installed"`
	Status     string `json:"status"`
	Auth       struct {
		Status string `json:"status"`
	} `json:"auth"`
	Models []struct {
		Slug         string `json:"slug"`
		Capabilities *struct {
			OptionDescriptors []struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Options []struct {
					ID string `json:"id"`
				} `json:"options"`
			} `json:"optionDescriptors"`
		} `json:"capabilities"`
	} `json:"models"`
}

// Finding is one validation result.
type Finding struct {
	Level   string // "ok", "warn", "fail"
	Message string
}

// Validation is the outcome of checking one task.
type Validation struct {
	Findings []Finding
	// Selection is the resolved model selection when the task is valid.
	Selection map[string]any
	Project   *t3control.Project
}

// OK reports whether nothing failed.
func (v Validation) OK() bool {
	for _, f := range v.Findings {
		if f.Level == "fail" {
			return false
		}
	}
	return true
}

func (v *Validation) add(level, format string, args ...any) {
	v.Findings = append(v.Findings, Finding{Level: level, Message: fmt.Sprintf(format, args...)})
}

// Validator checks tasks against the local T3 installation.
type Validator struct {
	Control Control
	// DataDir is T3's base directory (the parent of userdata and caches).
	DataDir string
	// SeenInstances are provider instance ids observed in use (from the
	// thread list), accepted when no cache file describes them.
	SeenInstances map[string]bool
	// Projects, when set, is used instead of asking Control.
	Projects []t3control.Project
	Now      time.Time
}

// Validate checks a task's fields, project, provider instance, model and
// options. Host is not checked here; a remote task is validated on its
// host.
func (val Validator) Validate(ctx context.Context, t Task) Validation {
	var v Validation
	now := val.Now
	if now.IsZero() {
		now = time.Now()
	}
	if t.NotBefore != nil && t.Deadline != nil && !t.Deadline.After(*t.NotBefore) {
		v.add("fail", "deadline %s is not after not_before %s", t.Deadline.Format(time.RFC3339), t.NotBefore.Format(time.RFC3339))
	}
	if t.Deadline != nil && t.Deadline.Before(now) {
		v.add("warn", "deadline %s is already in the past; the task runs as soon as quota is healthy", t.Deadline.Format(time.RFC3339))
	}
	if t.MaxTurns < 1 || t.MaxTurns > 20 {
		v.add("fail", "max_turns must be between 1 and 20 (got %d)", t.MaxTurns)
	}
	if len(t.Prompt) < 40 {
		v.add("warn", "the prompt is very short (%d characters); an unattended agent needs to know what done looks like", len(t.Prompt))
	}

	// Project.
	projects := val.Projects
	if projects == nil {
		var err error
		projects, err = val.Control.ListProjects(ctx)
		if err != nil {
			v.add("fail", "cannot list T3 projects: %v", err)
			return v
		}
	}
	project, ok := findProject(projects, t.Project)
	if !ok {
		names := make([]string, 0, len(projects))
		for _, p := range projects {
			names = append(names, p.Title)
		}
		v.add("fail", "project %q not found or ambiguous on this host; projects here: %s", t.Project, strings.Join(names, ", "))
		return v
	}
	v.Project = &project
	v.add("ok", "project %q (%s)", project.Title, project.WorkspaceRoot)

	// Model selection.
	selection := modelSelection(t, project)
	if selection == nil {
		v.add("fail", "no model: set model and instance in the task, or a default model on project %q", project.Title)
		return v
	}
	instance, _ := selection["instanceId"].(string)
	model, _ := selection["model"].(string)
	cache, cerr := val.loadCache(instance)
	switch {
	case cerr == nil:
		if !cache.Enabled || !cache.Installed || cache.Status == "disabled" {
			v.add("fail", "provider instance %q is %s on this host (enabled=%v installed=%v)", instance, cache.Status, cache.Enabled, cache.Installed)
		} else if cache.Status == "error" {
			v.add("fail", "provider instance %q reports status error on this host", instance)
		} else {
			v.add("ok", "provider instance %q is %s", instance, cache.Status)
		}
		switch cache.Auth.Status {
		case "authenticated":
		case "unauthenticated":
			v.add("fail", "provider instance %q is not signed in on this host", instance)
		default:
			v.add("warn", "provider instance %q auth status is %q", instance, cache.Auth.Status)
		}
		if len(cache.Models) > 0 {
			found := -1
			for i, m := range cache.Models {
				if m.Slug == model {
					found = i
					break
				}
			}
			if found < 0 {
				slugs := make([]string, 0, len(cache.Models))
				for _, m := range cache.Models {
					slugs = append(slugs, m.Slug)
				}
				v.add("fail", "model %q is not offered by %q on this host; available: %s", model, instance, strings.Join(slugs, ", "))
			} else {
				v.add("ok", "model %q", model)
				val.checkOptions(&v, t, cache, found)
			}
		} else {
			v.add("warn", "provider %q lists no models in its cache; model %q not checked", instance, model)
		}
	case val.SeenInstances[instance]:
		v.add("warn", "provider instance %q has no cache file on this host; accepted because threads use it, model %q not checked", instance, model)
	default:
		v.add("fail", "provider instance %q is unknown on this host (no %s and no thread uses it)", instance, filepath.Join("caches", instance+".json"))
	}
	if v.OK() {
		v.Selection = selection
	}
	return v
}

func (val Validator) loadCache(instance string) (*providerCache, error) {
	if val.DataDir == "" || instance == "" {
		return nil, os.ErrNotExist
	}
	raw, err := os.ReadFile(filepath.Join(val.DataDir, "caches", instance+".json"))
	if err != nil {
		return nil, err
	}
	var c providerCache
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// checkOptions validates the task's options against the model's option
// descriptors when the cache carries them.
func (val Validator) checkOptions(v *Validation, t Task, cache *providerCache, modelIndex int) {
	if len(t.Options) == 0 {
		return
	}
	caps := cache.Models[modelIndex].Capabilities
	if caps == nil || len(caps.OptionDescriptors) == 0 {
		v.add("warn", "options given but the model advertises none; T3 may ignore them")
		return
	}
	for id, value := range t.Options {
		matched := false
		for _, d := range caps.OptionDescriptors {
			if d.ID != id {
				continue
			}
			matched = true
			if d.Type == "select" && len(d.Options) > 0 {
				okValue := false
				choices := make([]string, 0, len(d.Options))
				for _, o := range d.Options {
					choices = append(choices, o.ID)
					if o.ID == value {
						okValue = true
					}
				}
				if !okValue {
					v.add("fail", "option %s=%q is not one of %s", id, value, strings.Join(choices, ", "))
				}
			}
		}
		if !matched {
			ids := make([]string, 0, len(caps.OptionDescriptors))
			for _, d := range caps.OptionDescriptors {
				ids = append(ids, d.ID)
			}
			v.add("fail", "option %q is unknown for this model; known: %s", id, strings.Join(ids, ", "))
		}
	}
}
