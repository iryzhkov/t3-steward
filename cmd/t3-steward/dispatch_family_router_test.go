package main

import (
	"regexp"
	"strings"
	"testing"
)

// The rc.71 router test, extended from backlog and task to every family.
//
// Its shape is kept: the command words come out of the family's own help text,
// every one of them is driven through the family's real entry point, and the
// test asserts where it landed. Two families keep the stronger assertion --
// backlog and task have recorder seams, so their tests above pin the exact
// dispatcher -- and the rest assert the property the seams exist to protect:
// a documented verb is routed somewhere, rather than dying in a sibling
// dispatcher as `unknown <family> command "<verb>"`, which is how "backlog
// projects" and "backlog rewake" were dead for a release.
//
// Each verb must also have a page of its own, which is the B-1 half: a verb
// the family help documents and whose --help cannot answer is exactly the
// hole this stage closes.

// unknownCommandRefusal matches the refusals a family makes when it is handed
// a word it does not route.
var unknownCommandRefusal = regexp.MustCompile(`unknown [a-z-]+( admin)?( schedules)? command`)

// familyRouterCase is one command family: the help text its verbs are read
// out of, the section classification that scan needs, and the entry point the
// words are driven through.
type familyRouterCase struct {
	name     string
	usage    string
	sections map[string]dispatchRoute
	drive    func(t *testing.T, flags globalFlags, words []string) error
	// unexecuted names a verb that is routed but must not be run here,
	// with the reason. Its routing is asserted from the family's own verb
	// list instead.
	unexecuted map[string]string
	routed     func(verb string) bool
}

func familyRouterCases() []familyRouterCase {
	return []familyRouterCase{
		{
			name:  "campaign",
			usage: campaignCommandUsage,
			sections: map[string]dispatchRoute{
				"Offline, reaches no coordinator":                                     routeAnyDispatcher,
				"Read-only and live, asks the coordinator and creates nothing":        routeAnyDispatcher,
				"Mutating, checks first and creates one workflow and one run":         routeAnyDispatcher,
				"Mutating recovery, creates a second run and never changes the first": routeAnyDispatcher,
				"Lifecycle": routeAnyDispatcher,
				"Supervised runs, structured decisions only and never prose": routeAnyDispatcher,
				// Prose that happens to end in a colon in the first column. It
				// documents no command word of its own.
				"check reports one outcome per task and per worker":                 routeNotACommand,
				"A complete example, from an empty directory to a running campaign": routeNotACommand,
			},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdCampaign(flags, words)
			},
		},
		{
			name:  "wait",
			usage: waitCommandUsage,
			sections: map[string]dispatchRoute{
				"Commands": routeAnyDispatcher,
				// add flags documents the options of one command, not commands;
				// the rest is prose that ends in a colon in the first column.
				"add flags":                         routeNotACommand,
				"task-bound, is one parseable line": routeNotACommand,
				"Verifying the caller's thread for an interactive wait needs the T3 API token": routeNotACommand,
				"On failure or timeout the wait still wakes you, with structured evidence":     routeNotACommand,
				"Examples, inside a task": routeNotACommand,
			},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdWait(flags, words)
			},
		},
		{
			name:     "thread",
			usage:    threadUsage,
			sections: map[string]dispatchRoute{"Commands": routeAnyDispatcher},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdThread(flags, words)
			},
		},
		{
			name:     "bucket",
			usage:    bucketUsage,
			sections: map[string]dispatchRoute{"Commands": routeAnyDispatcher},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdBucket(flags, words)
			},
		},
		{
			name:     "archive",
			usage:    archiveUsage,
			sections: map[string]dispatchRoute{"Commands": routeAnyDispatcher},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdArchive(flags, words)
			},
		},
		{
			name:     "coordinator",
			usage:    coordinatorUsage,
			sections: map[string]dispatchRoute{"Commands": routeAnyDispatcher, "Configuration on a host that is not the coordinator": routeNotACommand, "Common failures": routeNotACommand, "Recovery": routeNotACommand, "Example": routeNotACommand},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdCoordinator(flags, words)
			},
		},
		{
			name:     "schedules",
			usage:    strings.TrimSuffix(schedulesUsage, coordinatorTransportHelp),
			sections: map[string]dispatchRoute{"Definition administration": routeAnyDispatcher, "Read commands": routeAnyDispatcher, "Revision-fenced controls": routeAnyDispatcher, "Example": routeNotACommand},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdSchedules(flags, words)
			},
		},
		{
			name:  "worker",
			usage: workerUsage,
			sections: map[string]dispatchRoute{
				"Worker daemon":                                 routeAnyDispatcher,
				"On the coordinator host":                       routeAnyDispatcher,
				"Operator diagnostics and provider containment": routeAnyDispatcher,
			},
			drive: func(_ *testing.T, flags globalFlags, words []string) error {
				return cmdWorker(flags, words)
			},
			unexecuted: map[string]string{
				// It starts the contained T3 server in this process. Its routing
				// is asserted from the family's own verb list instead.
				"contained-t3": "it starts a server",
			},
			routed: func(verb string) bool {
				for _, known := range workerVerbList {
					if known == verb {
						return true
					}
				}
				return false
			},
		},
	}
}

func TestEveryDocumentedVerbOfEveryFamilyIsRoutedAndDocumentedAtDepth(t *testing.T) {
	for _, family := range familyRouterCases() {
		t.Run(family.name, func(t *testing.T) {
			commands := helpCommands(t, family.usage, family.sections)
			if len(commands) == 0 {
				t.Fatalf("the %s help scan found no command at all, so this table proves nothing", family.name)
			}
			flags := throwawayBacklogFlags(t)
			for _, command := range sortedCommands(commands) {
				t.Run(command, func(t *testing.T) {
					words := strings.Fields(command)
					path := family.name + " " + command
					assertVerbDocumentedAtDepth(t, family.name, words)
					if reason, skip := family.unexecuted[command]; skip {
						if family.routed == nil || !family.routed(words[0]) {
							t.Fatalf("%q is not executed here because %s, and the family's own verb list does not name it either", path, reason)
						}
						return
					}
					err := family.drive(t, flags, words)
					if err != nil && unknownCommandRefusal.MatchString(err.Error()) {
						t.Fatalf("t3-steward %s was refused as an unknown command: %v", path, err)
					}
				})
			}
		})
	}
}

// The backlog and task families already derive their verbs from help text and
// drive them through recorder seams, above. This asserts the other half of the
// contract for them: every verb those scans find is documented at depth.
func TestEveryDocumentedBacklogAndTaskVerbIsDocumentedAtDepth(t *testing.T) {
	for family, commands := range map[string]map[string]dispatchRoute{
		"backlog": helpCommands(t, strings.TrimSuffix(backlogUsage, coordinatorTransportHelp), backlogHelpSections),
		"task":    helpCommands(t, taskUsage, taskHelpSections),
	} {
		for _, command := range sortedCommands(commands) {
			assertVerbDocumentedAtDepth(t, family, strings.Fields(command))
		}
	}
}

// assertVerbDocumentedAtDepth requires the command form the help documents to
// resolve to a page below the family's own. The resolution is the one the
// program performs for a real --help, so a form written with its flags, such
// as "list --all" or "contained-exec --spec FILE", is answered by the page of
// the verb it names rather than falling back to the family overview.
func assertVerbDocumentedAtDepth(t *testing.T, family string, words []string) {
	t.Helper()
	line := family + " " + strings.Join(words, " ")
	path, requested := helpRequest([]string{family}, append(append([]string{}, words...), "--help"))
	if !requested {
		t.Errorf("%q is documented in the %s help and \"t3-steward %s --help\" is not even a help request", line, family, line)
		return
	}
	if len(path) < 2 {
		t.Errorf("%q is documented in the %s help and has no page of its own; \"t3-steward %s --help\" falls back to the family overview", line, family, line)
	}
}
