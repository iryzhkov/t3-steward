package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// A command family that answers to one name but keeps more than one
// dispatcher behind it has to decide, per command word, which dispatcher gets
// it. That decision lives in one place, the words it decides about are
// documented in a second (the family's help) and implemented in a third (the
// parser), and nothing made the three agree. "t3-steward backlog projects"
// was documented and implemented for a whole release while the router sent it
// to the legacy task-file dispatcher, which answered
// `unknown backlog command "projects"`; "backlog rewake" was dead the same
// way and nobody had noticed at all.
//
// The tests here take the command words out of the help text itself and drive
// every one of them through the family's own entry point, asserting which
// dispatcher it reaches. A verb documented in future is covered the moment
// its help line is written, with nobody remembering to extend a list here.

// dispatchRoute names one dispatcher of one command family.
type dispatchRoute string

const (
	routeCoordinatorAdmin dispatchRoute = "coordinator admin"
	routeBackup           dispatchRoute = "stopped-coordinator backup"
	routeLegacyTaskFiles  dispatchRoute = "legacy task files"
	routeTaskRun          dispatchRoute = "task run"
	routeTaskResult       dispatchRoute = "task result"
	routeTaskEnv          dispatchRoute = "task env"
	// routeNotACommand marks a help section that documents no command word,
	// such as a closing example.
	routeNotACommand dispatchRoute = "not a command"
	// routeAnyDispatcher marks a section whose commands are known to be
	// routed but not all to the same dispatcher; the test then asserts only
	// that exactly one dispatcher was reached.
	routeAnyDispatcher dispatchRoute = "any dispatcher of the family"
)

// backlogHelpSections says which dispatcher the command words under each
// section of the backlog help belong to. It is keyed by section heading
// because that is the one judgement a help scan cannot make for itself: only
// a person knows that the backup commands run against a stopped coordinator
// and that the task-file helpers are offline. A heading the map does not know
// fails the test, so a new section forces the decision to be recorded rather
// than letting its verbs route wherever they land.
var backlogHelpSections = map[string]dispatchRoute{
	"Coordinator submission command":      routeCoordinatorAdmin,
	"Coordinator read commands":           routeCoordinatorAdmin,
	"Graph amendments":                    routeCoordinatorAdmin,
	"Revision-fenced controls":            routeCoordinatorAdmin,
	"Stopped coordinator backup commands": routeBackup,
	"Legacy task-file helpers":            routeLegacyTaskFiles,
	"Example":                             routeNotACommand,
}

// taskHelpSections classifies the one section of the task help. Its three
// commands go to three different dispatchers, so the section says only that
// they are commands and taskCommandDispatchers below pins the three by name.
var taskHelpSections = map[string]dispatchRoute{"Commands": routeAnyDispatcher}

// taskCommandDispatchers pins the dispatcher each documented task command is
// expected to reach. A command the map does not name still has to reach
// exactly one dispatcher; it is only the identity that is left open.
var taskCommandDispatchers = map[string]dispatchRoute{
	"run":            routeTaskRun,
	"result":         routeTaskResult,
	"env":            routeTaskEnv,
	"env --get NAME": routeTaskEnv,
}

// helpCommands reads one usage text and returns every command form it
// documents, mapped to the route its section declares.
func helpCommands(t *testing.T, usage string, sections map[string]dispatchRoute) map[string]dispatchRoute {
	t.Helper()
	commands := map[string]dispatchRoute{}
	section := ""
	for _, line := range strings.Split(usage, "\n") {
		if heading, ok := helpSectionHeading(line); ok {
			if _, known := sections[heading]; !known {
				t.Fatalf("the help grew the section %q; classify it in the router table so its commands are routed", heading)
			}
			section = heading
			continue
		}
		if section == "" || sections[section] == routeNotACommand {
			continue
		}
		words := helpCommandWords(line)
		if len(words) == 0 {
			continue
		}
		for _, form := range expandAlternativeWords(words) {
			commands[strings.Join(form, " ")] = sections[section]
		}
	}
	return commands
}

// helpSectionHeading recognises a section heading: a line in the first column
// that ends in a colon. A parenthetical is dropped from the name, so that
// rewording it does not fail the test while renaming the section does.
func helpSectionHeading(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " ")
	if trimmed == "" || strings.HasPrefix(trimmed, " ") || !strings.HasSuffix(trimmed, ":") {
		return "", false
	}
	name := strings.TrimSuffix(trimmed, ":")
	if open := strings.Index(name, " ("); open >= 0 {
		name = name[:open]
	}
	return name, true
}

// helpCommandWords takes the command form off one help line. The line has to
// be indented exactly two spaces and to start with a letter, which leaves out
// both the continuation lines of a description and a prose note about a flag;
// the description after the first run of two or more spaces is dropped; and
// the form stops at the first placeholder. What is left is the literal words
// the command is written with: "task show", "list --all", or the alternation
// "start|resume|cancel|retry|skip".
func helpCommandWords(line string) []string {
	if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
		return nil
	}
	form := strings.TrimSpace(line[2:])
	if gap := strings.Index(form, "  "); gap >= 0 {
		form = form[:gap]
	}
	fields := strings.Fields(form)
	if len(fields) == 0 || !isASCIILetter(fields[0][0]) {
		return nil
	}
	var words []string
	for _, field := range fields {
		if strings.ContainsAny(field, "<>[]/") {
			break
		}
		words = append(words, field)
	}
	return words
}

func isASCIILetter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// expandAlternativeWords turns a form written with alternatives, such as
// "edge add|remove", into one command form per alternative.
func expandAlternativeWords(words []string) [][]string {
	forms := [][]string{{}}
	for _, word := range words {
		var next [][]string
		for _, form := range forms {
			for _, alternative := range strings.Split(word, "|") {
				next = append(next, append(append([]string{}, form...), alternative))
			}
		}
		forms = next
	}
	return forms
}

func sortedCommands(commands map[string]dispatchRoute) []string {
	out := make([]string, 0, len(commands))
	for command := range commands {
		out = append(out, command)
	}
	sort.Strings(out)
	return out
}

// recordBacklogRoutes replaces the three dispatchers behind "backlog" with
// recorders for one test. Nothing runs: the point is to see where cmdBacklog
// goes, not what the dispatcher does, so no coordinator is reached and no
// state database or backlog directory is opened.
func recordBacklogRoutes(t *testing.T) *[]dispatchRoute {
	t.Helper()
	admin, backup, legacy := backlogAdminRoute, backlogBackupRoute, backlogLegacyRoute
	var reached []dispatchRoute
	backlogAdminRoute = func(config.Config, []string, bool) error {
		reached = append(reached, routeCoordinatorAdmin)
		return nil
	}
	backlogBackupRoute = func(context.Context, config.Config, []string) error {
		reached = append(reached, routeBackup)
		return nil
	}
	backlogLegacyRoute = func(config.Config, []string) error {
		reached = append(reached, routeLegacyTaskFiles)
		return nil
	}
	t.Cleanup(func() {
		backlogAdminRoute, backlogBackupRoute, backlogLegacyRoute = admin, backup, legacy
	})
	return &reached
}

// recordTaskRoutes does the same for the three dispatchers behind "task".
func recordTaskRoutes(t *testing.T) *[]dispatchRoute {
	t.Helper()
	env, run, result := taskFamilyEnvRoute, taskFamilyRunRoute, taskFamilyResultRoute
	var reached []dispatchRoute
	taskFamilyEnvRoute = func([]string, func(string) string, io.Writer) error {
		reached = append(reached, routeTaskEnv)
		return nil
	}
	taskFamilyRunRoute = func(globalFlags, []string) error {
		reached = append(reached, routeTaskRun)
		return nil
	}
	taskFamilyResultRoute = func(globalFlags, []string) error {
		reached = append(reached, routeTaskResult)
		return nil
	}
	t.Cleanup(func() {
		taskFamilyEnvRoute, taskFamilyRunRoute, taskFamilyResultRoute = env, run, result
	})
	return &reached
}

// throwawayBacklogFlags is a configuration that exists only so that cmdBacklog
// gets past loadConfig. It names a state path under the test's temporary
// directory and nothing else; with the dispatchers recorded, neither that file
// nor any coordinator client is opened.
func throwawayBacklogFlags(t *testing.T) globalFlags {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(path, []byte("state_path: "+filepath.Join(root, "state.db")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return globalFlags{configPath: path}
}

func TestEveryDocumentedBacklogCommandReachesItsDispatcher(t *testing.T) {
	// The shared coordinator transport help is appended to this family's usage
	// and documents no command of its own; its indented prose would be read as
	// command words, so the scan stops where that help begins.
	usage := strings.TrimSuffix(backlogUsage, coordinatorTransportHelp)
	if usage == backlogUsage {
		t.Fatal("backlogUsage no longer ends with coordinatorTransportHelp; the scan below would read the transport prose as commands")
	}
	commands := helpCommands(t, usage, backlogHelpSections)
	// A scan that quietly stopped finding help lines would pass every
	// assertion below, so the two forms that carried the defect and a sample
	// of each route have to be present for the table to mean anything.
	for _, required := range []string{
		"submit", "projects", "status", "workers", "list", "show", "task show", "artifact get",
		"task add", "edge add", "edge remove", "rewake", "cancel", "quarantine release", "recover",
		"backup create", "backup verify", "backup restore",
		"new", "path", "check", "receive", "list --all",
	} {
		if _, found := commands[required]; !found {
			t.Fatalf("the help scan did not find %q, so this table proves nothing; it found %v", required, sortedCommands(commands))
		}
	}
	flags := throwawayBacklogFlags(t)
	for _, command := range sortedCommands(commands) {
		want := commands[command]
		t.Run(command, func(t *testing.T) {
			reached := recordBacklogRoutes(t)
			if err := cmdBacklog(flags, strings.Fields(command)); err != nil {
				t.Fatalf("t3-steward backlog %s: %v", command, err)
			}
			if len(*reached) != 1 {
				t.Fatalf("t3-steward backlog %s reached %d dispatchers, want exactly one: %v", command, len(*reached), *reached)
			}
			if got := (*reached)[0]; got != want {
				t.Fatalf("t3-steward backlog %s reached the %s dispatcher, want the %s one", command, got, want)
			}
		})
	}
}

func TestEveryDocumentedTaskCommandReachesADispatcher(t *testing.T) {
	commands := helpCommands(t, taskUsage, taskHelpSections)
	for required := range taskCommandDispatchers {
		if _, found := commands[required]; !found {
			t.Fatalf("the help scan did not find %q, so this table proves nothing; it found %v", required, sortedCommands(commands))
		}
	}
	for _, command := range sortedCommands(commands) {
		t.Run(command, func(t *testing.T) {
			reached := recordTaskRoutes(t)
			if err := cmdTask(globalFlags{}, strings.Fields(command)); err != nil {
				t.Fatalf("t3-steward task %s: %v", command, err)
			}
			if len(*reached) != 1 {
				t.Fatalf("t3-steward task %s reached %d dispatchers, want exactly one: %v", command, len(*reached), *reached)
			}
			if want, pinned := taskCommandDispatchers[command]; pinned && (*reached)[0] != want {
				t.Fatalf("t3-steward task %s reached the %s dispatcher, want the %s one", command, (*reached)[0], want)
			}
		})
	}
}

// The defect's own shape, through the real dispatchers rather than recorders:
// a coordinator verb must not be refused by the legacy task-file dispatcher.
// The package's TestMain installs a scratch HOME, so no coordinator client is
// discoverable and the admin route refuses with its own client-configuration
// message; that refusal is proof the router sent the command there, and
// "unknown backlog command" is proof it did not.
func TestTheDeadBacklogVerbsNoLongerDieInTheLegacyDispatcher(t *testing.T) {
	flags := throwawayBacklogFlags(t)
	for _, command := range [][]string{
		{"projects", "--project", "dogfood-sum", "--json"},
		{"rewake", "run-x/task-y", "--reason", "test"},
	} {
		t.Run(command[0], func(t *testing.T) {
			line := strings.Join(command, " ")
			err := cmdBacklog(flags, command)
			if err == nil {
				t.Fatalf("t3-steward backlog %s answered with no coordinator configured", line)
			}
			if strings.Contains(err.Error(), "unknown backlog command") {
				t.Fatalf("t3-steward backlog %s was refused by the legacy dispatcher: %v", line, err)
			}
		})
	}
}
