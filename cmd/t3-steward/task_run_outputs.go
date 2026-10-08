package main

import (
	"path"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

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
	printed := make([]string, len(outputs))
	for i, output := range outputs {
		printed[i] = printedOutputName(output)
	}
	return "outputs " + strings.Join(printed, ", ") + " (" + finalMessageArtifactName + " is always kept)"
}

// printedOutputName is a declared name as a terminal line carries it. The
// validator admits control and format characters and newlines, and a name read
// back from a stored manifest is not the caller's own, so such a name is
// quoted: it can neither drive the terminal nor forge a line. JSON carries the
// name exactly.
func printedOutputName(name string) string {
	unsafe := strings.ContainsFunc(name, func(symbol rune) bool {
		return symbol == utf8.RuneError || unicode.IsControl(symbol) || unicode.Is(unicode.Cf, symbol)
	})
	if unsafe {
		return strconv.Quote(name)
	}
	return name
}

// missingDeclaredOutputs names the files a task declared and did not leave
// behind, given only the selected attempt's collected output artifacts. A task that has not
// ended may still write them and a skipped one never ran, so neither is
// missing anything. A declared commit is not a file and is not checked here.
// Names are compared after path.Clean, because the manifest accepts
// "reports/./a.md" and two spellings of one path are one file, named once.
func missingDeclaredOutputs(task backlogadmin.TaskDetail, collected []string) []string {
	if task.Attempt == nil || !task.Attempt.Progress.Terminal() || task.Attempt.Progress == domain.ProgressSkipped {
		return nil
	}
	retained := make(map[string]bool, len(collected))
	for _, name := range collected {
		retained[path.Clean(name)] = true
	}
	var missing []string
	reported := make(map[string]bool, len(task.Task.Outputs))
	for _, declared := range task.Task.Outputs {
		clean := path.Clean(declared.Name)
		if declared.Commit != nil || retained[clean] || reported[clean] {
			continue
		}
		reported[clean] = true
		missing = append(missing, declared.Name)
	}
	return missing
}
