package main

import (
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
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
//
// What "not refused as unknown" is worth differs by family, and saying so is
// the point of the routed field below. campaign, thread, bucket, coordinator
// and worker reach their routing decision under this test's throwaway
// configuration, so the clause bites for them. wait, archive and schedules do
// not: under that configuration wait dies opening the state database, archive
// dies on "archive.destination is not configured" and schedules dies on the
// coordinator transport, all before any verb is looked at, so for those three
// the refusal that satisfies the clause is an unrelated one and the clause on
// its own proves nothing. They therefore carry a routed predicate read out of
// their own router's source, which is environment-independent and is the
// property the execution was standing in for.

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
	// routed reports whether the family's own router knows this verb word.
	// It is required of every documented verb of the families that set it,
	// and it is the only assertion that bites for the three families whose
	// entry point dies before it reaches a routing decision.
	routed func(verb string) bool
}

// routedBy turns a derived verb set into the predicate familyRouterCase wants.
func routedBy(verbs map[string]bool) func(string) bool {
	return func(verb string) bool { return verbs[verb] }
}

// verbsRoutedIn reads the verb words a family's own router decides on: the
// string literals of its case clauses and of its equality comparisons. It is
// deliberately generous, because the property asserted is "the router still
// knows this word", and a set that is too wide can only miss a dead verb,
// never invent a failure. A named router that no longer exists is a failure,
// so a rename is reported rather than silently proving nothing.
func verbsRoutedIn(t *testing.T, files map[string]*ast.File, routers ...string) map[string]bool {
	t.Helper()
	verbs := map[string]bool{}
	for _, name := range routers {
		matched := false
		for _, file := range files {
			for _, decl := range file.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok || function.Body == nil || function.Name.Name != name {
					continue
				}
				matched = true
				ast.Inspect(function.Body, func(n ast.Node) bool {
					switch node := n.(type) {
					case *ast.CaseClause:
						for _, expression := range node.List {
							collectStringLiteral(verbs, expression)
						}
					case *ast.BinaryExpr:
						if node.Op == token.EQL || node.Op == token.NEQ {
							collectStringLiteral(verbs, node.X)
							collectStringLiteral(verbs, node.Y)
						}
					}
					return true
				})
			}
		}
		if !matched {
			t.Fatalf("the router %q no longer exists; point this case at the router that took its place", name)
		}
	}
	if len(verbs) == 0 {
		t.Fatalf("the scan of %v found no verb word, so this predicate would prove nothing", routers)
	}
	return verbs
}

func collectStringLiteral(into map[string]bool, expression ast.Expr) {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return
	}
	if value, err := strconv.Unquote(literal.Value); err == nil && value != "" {
		into[value] = true
	}
}

func familyRouterCases(t *testing.T, files map[string]*ast.File) []familyRouterCase {
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
			// cmdWait opens the state database before it looks at the verb, so
			// the execution above cannot tell a routed verb from an unrouted
			// one here. Its own router is what says.
			routed: routedBy(verbsRoutedIn(t, files, "cmdWait")),
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
			// cmdArchive refuses on an unconfigured archive.destination before
			// it looks at the verb, so its own router is what says.
			routed: routedBy(verbsRoutedIn(t, files, "cmdArchive")),
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
			// cmdSchedules builds the coordinator transport before it looks at
			// the verb, so its own router is what says. put is decided in
			// runSchedules and the four controls in isScheduleMutation.
			routed: routedBy(verbsRoutedIn(t, files, "runSchedules", "isScheduleMutation")),
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
	files := packageSource(t)
	for _, family := range familyRouterCases(t, files) {
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
					if family.routed != nil && !family.routed(words[0]) {
						t.Fatalf("t3-steward %s is documented and the %s router does not know the word %q", path, family.name, words[0])
					}
					if reason, skip := family.unexecuted[command]; skip {
						if family.routed == nil {
							t.Fatalf("%q is not executed here because %s, and the family has no verb list to assert its routing from", path, reason)
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
