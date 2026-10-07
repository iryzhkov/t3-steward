package main

import (
	"path"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// taskRunOutputsLine is the line the start record and the dry run print after
// the route: what the task's run will keep. Findings were lost twice because
// nothing said that only the final message is kept when no output is declared,
// so the line is printed with no outputs too, and says so.
func taskRunOutputsLine(outputs []string) string {
	if len(outputs) == 0 {
		return "outputs none: only " + finalMessageArtifactName +
			" is kept; declare files the task writes with --output FILE"
	}
	return "outputs " + strings.Join(outputs, ", ") + " (" + finalMessageArtifactName + " is always kept)"
}

// missingDeclaredOutputs names the files a task declared and did not leave
// behind, given the output artifacts collected for it. A task that has not
// ended may still write them and a skipped one never ran, so neither is
// missing anything. A declared commit is not a file and is not checked here.
// Names are compared after path.Clean, because the manifest accepts
// "reports/./a.md" and two spellings of one path are one file.
func missingDeclaredOutputs(task backlogadmin.TaskDetail, collected []string) []string {
	if task.Attempt == nil || !task.Attempt.Progress.Terminal() || task.Attempt.Progress == domain.ProgressSkipped {
		return nil
	}
	retained := make(map[string]bool, len(collected))
	for _, name := range collected {
		retained[path.Clean(name)] = true
	}
	var missing []string
	for _, declared := range task.Task.Outputs {
		if declared.Commit != nil || retained[path.Clean(declared.Name)] {
			continue
		}
		missing = append(missing, declared.Name)
	}
	return missing
}
