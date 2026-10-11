package backlog

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ParseCampaignReference names a declared commit, never a remote Git branch.
func ParseCampaignReference(value string) (domain.NodeRef, string, error) {
	parts := strings.Split(strings.TrimPrefix(value, "campaign:"), "/")
	if !strings.HasPrefix(value, "campaign:") || len(parts) != 3 {
		return domain.NodeRef{}, "", fmt.Errorf("campaign ref requires campaign:<run>/<task>/<name>")
	}
	ref, err := domain.ParseNodeRef(parts[0] + "/" + parts[1])
	if err != nil {
		return ref, "", err
	}
	if ref.TaskID == domain.SinkTaskName {
		return ref, "", fmt.Errorf("campaign ref cannot name a run sink")
	}
	if err := validateCommitTarget(ref.RunID, ref.TaskID, parts[2]); err != nil {
		return ref, "", err
	}
	return ref, parts[2], nil
}

func validateEnvironmentRef(ref string) error {
	if strings.HasPrefix(ref, "campaign:") {
		_, _, err := ParseCampaignReference(ref)
		return err
	}
	return validateGitRef(ref)
}

func validateChaining(manifest Manifest) error {
	if strings.HasPrefix(manifest.Environment.Ref, "campaign:") {
		if _, _, err := ParseCampaignReference(manifest.Environment.Ref); err != nil {
			return err
		}
	}
	for name, task := range manifest.Tasks {
		for producer := range task.InputsFromRun {
			if !strings.Contains(producer, "/") {
				return fmt.Errorf("task %s inputs_from_run requires <run>/<task>, got %q", name, producer)
			}
			if _, err := domain.ParseNodeRef(producer); err != nil {
				return err
			}
		}
	}
	return nil
}

// Lower the new spellings to the existing retained cross-run dependency contract.
// That contract pins each successful producer's artifact digest at acceptance.
func applyChainingDefaults(manifest *Manifest) {
	ref, commit, refErr := ParseCampaignReference(manifest.Environment.Ref)
	for name, task := range manifest.Tasks {
		bind := func(producer string, artifacts []string) {
			if !slices.Contains(task.Needs, producer) {
				task.Needs = append(task.Needs, producer)
			}
			if task.InputsFrom == nil {
				task.InputsFrom = map[string][]string{}
			}
			for _, artifact := range artifacts {
				if !slices.Contains(task.InputsFrom[producer], artifact) {
					task.InputsFrom[producer] = append(task.InputsFrom[producer], artifact)
				}
			}
		}
		for _, producer := range slices.Sorted(maps.Keys(task.InputsFromRun)) {
			bind(producer, task.InputsFromRun[producer])
		}
		if refErr == nil {
			bind(ref.RunID+"/"+ref.TaskID, []string{commit})
		}
		manifest.Tasks[name] = task
	}
}
