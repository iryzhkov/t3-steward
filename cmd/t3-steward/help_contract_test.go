package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The help contract, enforced rather than described.
//
// Three findings sit behind these tests. Three verbs out of eighteen answered
// --help at all; "backlog show --help" and "diagnose --help" sent the word
// --help to the coordinator as a workflow run id and came back with exit 8;
// "backlog artifacts --help" printed an empty table and exit 0, which is an
// ambiguous zero in answer to a documentation request; and "backlog new
// --help" created a file called --help.md. The overview, separately, named
// worker-exchange but not worker.
//
// What makes these tests worth having is where they get their facts. The verb
// set comes out of the dispatcher's own switch, the flag set out of the
// parsers that consume the flags, and the probe drives the real command line.
// A test that pinned a hand-written list of verbs or flags would pass while
// the documentation went stale again, which is the defect itself.

// flagLiteral matches a long option spelled out in source.
var flagLiteral = regexp.MustCompile(`^--[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// flagSetDeclarations are the flag.FlagSet methods that declare an option. The
// index is the argument holding the name, which is spelled without the dashes.
var flagSetDeclarations = map[string]int{
	"Bool": 0, "String": 0, "Int": 0, "Int64": 0, "Uint": 0, "Duration": 0, "Float64": 0,
	"Func": 0, "BoolFunc": 0,
	"BoolVar": 1, "StringVar": 1, "IntVar": 1, "Int64Var": 1, "UintVar": 1,
	"DurationVar": 1, "Float64Var": 1, "Var": 1, "TextVar": 1,
}

// packageSource parses this command's own source, without its tests.
func packageSource(t *testing.T) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the command's source directory: %v", err)
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files[name] = file
	}
	if len(files) == 0 {
		t.Fatal("parsed no source files; the derivations below would prove nothing")
	}
	return files
}

// flagsIn collects every option a region of source mentions: a long option
// written as a string literal, and a flag.FlagSet declaration. The help
// spellings are dropped, because every verb accepts them by definition.
func flagsIn(node ast.Node) map[string]bool {
	found := map[string]bool{}
	ast.Inspect(node, func(n ast.Node) bool {
		if literal, ok := n.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if value, err := strconv.Unquote(literal.Value); err == nil && flagLiteral.MatchString(value) {
				found[value] = true
			}
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		index, declares := flagSetDeclarations[selector.Sel.Name]
		if !declares || len(call.Args) <= index {
			return true
		}
		literal, ok := call.Args[index].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		if value, err := strconv.Unquote(literal.Value); err == nil && value != "" && !strings.HasPrefix(value, "-") {
			found["--"+value] = true
		}
		return true
	})
	delete(found, "--help")
	return found
}

// declaredFlags reads the options out of one parser site: a function, or the
// case clause of a switch inside it when one function serves several verbs.
// A site that matches nothing is a failure, not an empty answer: a renamed
// parser must break the test rather than silently stop proving anything.
func declaredFlags(t *testing.T, files map[string]*ast.File, site parserSite) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	matchedFunction := false
	matchedCase := false
	for _, file := range files {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil || function.Name.Name != site.Func {
				continue
			}
			matchedFunction = true
			if site.Case == "" {
				for flag := range flagsIn(function.Body) {
					found[flag] = true
				}
				continue
			}
			ast.Inspect(function.Body, func(n ast.Node) bool {
				clause, ok := n.(*ast.CaseClause)
				if !ok || !caseClauseCovers(clause, site.Case) {
					return true
				}
				matchedCase = true
				for _, statement := range clause.Body {
					for flag := range flagsIn(statement) {
						found[flag] = true
					}
				}
				return true
			})
		}
	}
	if !matchedFunction {
		t.Fatalf("the help page names the parser %q, which no longer exists; point it at the parser that took its place", site.Func)
	}
	if site.Case != "" && !matchedCase {
		t.Fatalf("the help page names the case %q of the parser %q, which no longer exists", site.Case, site.Func)
	}
	return found
}

// caseClauseCovers reports whether a case clause handles this word. A clause
// that lists several words, such as `case "cancel", "run-now":`, covers each
// of them.
func caseClauseCovers(clause *ast.CaseClause, word string) bool {
	for _, expression := range clause.List {
		literal, ok := expression.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			continue
		}
		if value, err := strconv.Unquote(literal.Value); err == nil && value == word {
			return true
		}
	}
	return false
}

// dispatchRoutedVerbs reads the verb set out of the dispatcher's own switch
// statements, which is the only place in this program that decides what a verb
// is. The alias spellings of help and version are left out: they are the same
// two verbs written differently, and the overview is not a list of spellings.
func dispatchRoutedVerbs(t *testing.T, files map[string]*ast.File) []string {
	t.Helper()
	verbs := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Name.Name != "dispatch" || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(n ast.Node) bool {
				statement, ok := n.(*ast.SwitchStmt)
				if !ok {
					return true
				}
				identifier, ok := statement.Tag.(*ast.Ident)
				if !ok || identifier.Name != "cmd" {
					return true
				}
				for _, item := range statement.Body.List {
					clause, ok := item.(*ast.CaseClause)
					if !ok {
						continue
					}
					for _, expression := range clause.List {
						literal, ok := expression.(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							continue
						}
						value, err := strconv.Unquote(literal.Value)
						if err != nil || value == "" || strings.HasPrefix(value, "-") || value == "help" {
							continue
						}
						verbs[value] = true
					}
				}
				return true
			})
		}
	}
	if len(verbs) < 20 {
		t.Fatalf("the dispatch scan found only %d verbs, so this table proves nothing: %v", len(verbs), sortedKeys(verbs))
	}
	return sortedKeys(verbs)
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// overviewCommands reads the command list out of the top-level usage: the
// entries between the Commands heading and the flags that follow it.
func overviewCommands(text string) []string {
	var commands []string
	inside := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimRight(line, " ")
		if trimmed == "Commands:" {
			inside = true
			continue
		}
		if inside && trimmed != "" && !strings.HasPrefix(trimmed, " ") {
			break
		}
		if !inside || !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || !isASCIILetter(fields[0][0]) {
			continue
		}
		commands = append(commands, fields[0])
	}
	return commands
}

// Test 1. B-9's general form: the overview names every verb the dispatcher
// routes, and no verb it does not, and every one of them has a page at depth.
// Stage 1 added worker and ui-archive to the list by hand; this derives the
// list from the switch, so the next verb cannot be added without its entry.
func TestTheOverviewNamesExactlyTheVerbsDispatchRoutes(t *testing.T) {
	files := packageSource(t)
	routed := dispatchRoutedVerbs(t, files)
	for _, verb := range routed {
		if !usageListsCommand(usage, verb) {
			t.Errorf("the overview does not name %q, which dispatch routes", verb)
		}
		if _, documented := helpPageFor(verb); !documented {
			t.Errorf("%q is routed and has no help page, so \"t3-steward %s --help\" cannot answer", verb, verb)
		}
	}
	routedSet := map[string]bool{}
	for _, verb := range routed {
		routedSet[verb] = true
	}
	for _, listed := range overviewCommands(usage) {
		if !routedSet[listed] {
			t.Errorf("the overview names %q, which dispatch does not route", listed)
		}
	}
}

// Test 3. Every verb, driven with --help on the real command line, answers on
// standard output with exit 0, writes nothing to standard error, creates no
// file, and names every flag its parser accepts.
//
// The verbs under test are the ones the dispatcher routes plus every
// second-level page; test 2 is what requires a page to exist for each verb the
// family help documents, so the three together close the loop from help prose
// to page to answered command line.
func TestEveryVerbAnswersHelpWithItsOwnFlags(t *testing.T) {
	files := packageSource(t)
	verbs := map[string]bool{}
	for _, verb := range dispatchRoutedVerbs(t, files) {
		verbs[verb] = true
	}
	for _, path := range helpPagePaths() {
		if path != "" {
			verbs[path] = true
		}
	}
	if len(verbs) < 80 {
		t.Fatalf("only %d verbs under test; the probe proves too little", len(verbs))
	}
	for _, verb := range sortedKeys(verbs) {
		t.Run(verb, func(t *testing.T) {
			for _, spelling := range []string{"--help", "-h", "help"} {
				stdout, stderr := probeHelp(t, append(strings.Fields(verb), spelling))
				if strings.TrimSpace(stdout) == "" {
					t.Fatalf("t3-steward %s %s printed nothing on stdout", verb, spelling)
				}
				if stderr != "" {
					t.Fatalf("t3-steward %s %s wrote to stderr: %q", verb, spelling, stderr)
				}
				if spelling == "--help" {
					assertHelpNamesItsFlags(t, files, verb, stdout)
				}
			}
		})
	}
}

// assertHelpNamesItsFlags is the half of the contract that cannot be satisfied
// by prose: the page has to name every option the verb's own parser accepts,
// and may not name one it does not.
func assertHelpNamesItsFlags(t *testing.T, files map[string]*ast.File, verb, help string) {
	t.Helper()
	page, found := helpPageFor(verb)
	if !found {
		t.Fatalf("%q answered --help with no registered page", verb)
	}
	sites := page.Parsers
	// A verb below a command family is entered through that family's clause in
	// the dispatcher, and that clause takes --config out of the arguments for
	// the whole family. The site is derived from the verb's own path, not read
	// off the page, so no page can leave it out -- which is what makes "this
	// verb takes no flags" impossible to write about a verb of a family.
	if family, _, isFamilyVerb := strings.Cut(verb, " "); isFamilyVerb {
		sites = append(append([]parserSite{}, sites...), familyDispatchSite(family))
	}
	if len(sites) == 0 {
		// Only a page in pagesWithoutAParserSite reaches this, and that table is
		// asserted exact by TestEveryPageDeclaresAParserSite.
		return
	}
	accepted := map[string]bool{}
	for _, site := range sites {
		for flag := range declaredFlags(t, files, site) {
			accepted[flag] = true
		}
	}
	if len(accepted) == 0 && len(page.Flags) > 0 {
		t.Fatalf("%q documents flags and names parsers that declare none; point the sites at the parser that takes them", verb)
	}
	undocumented := map[string]bool{}
	for _, flag := range page.Undocumented {
		if !accepted[flag] {
			t.Errorf("%q excuses %s from its help, and the parser no longer accepts it; drop the exception", verb, flag)
		}
		undocumented[flag] = true
	}
	for _, flag := range sortedKeys(accepted) {
		if undocumented[flag] {
			continue
		}
		if !namesFlag(help, flag) {
			t.Errorf("the parser of %q accepts %s and its help does not name it", verb, flag)
		}
	}
	for _, flag := range page.Flags {
		if !accepted[flag.Name] {
			t.Errorf("the help of %q names %s and no parser of that verb accepts it", verb, flag.Name)
		}
	}
}

// namesFlag reports whether the help text mentions the option as a whole word,
// so that --task does not answer for --task-run.
func namesFlag(help, flag string) bool {
	for index := 0; ; {
		offset := strings.Index(help[index:], flag)
		if offset < 0 {
			return false
		}
		end := index + offset + len(flag)
		if end == len(help) || !isFlagWordByte(help[end]) {
			return true
		}
		index = index + offset + 1
	}
}

func isFlagWordByte(b byte) bool {
	return b == '-' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9'
}

// probeHelp runs one command line through the real entry point with standard
// output and standard error captured, from an empty working directory that is
// checked afterwards: a help page that reads or writes a file is the defect
// "backlog new --help" had, which created a task file called --help.md.
func probeHelp(t *testing.T, args []string) (string, string) {
	t.Helper()
	directory := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(directory); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatal(err)
		}
	}()

	outFile := filepath.Join(t.TempDir(), "stdout")
	errFile := filepath.Join(t.TempDir(), "stderr")
	out, err := os.Create(outFile)
	if err != nil {
		t.Fatal(err)
	}
	errOut, err := os.Create(errFile)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = out, errOut
	runErr := run(args)
	os.Stdout, os.Stderr = stdout, stderr
	out.Close()
	errOut.Close()
	if runErr != nil {
		t.Fatalf("t3-steward %s: %v (a help request must exit 0)", strings.Join(args, " "), runErr)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("t3-steward %s created %d file(s) in the working directory; a help page writes nothing", strings.Join(args, " "), len(entries))
	}

	outBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	errBytes, err := os.ReadFile(errFile)
	if err != nil {
		t.Fatal(err)
	}
	return string(outBytes), string(errBytes)
}

// Every page has to carry the four things the contract asks of it. A page with
// a Body carries a reference this package already held before the contract
// existed, and those are checked for the same four things by their own tests.
func TestEveryRenderedPageCarriesTheContract(t *testing.T) {
	for _, path := range helpPagePaths() {
		page, _ := helpPageFor(path)
		if page.Body != "" {
			continue
		}
		name := path
		if name == "" {
			name = "(overview)"
		}
		if strings.TrimSpace(page.Purpose) == "" {
			t.Errorf("%s has no purpose line", name)
		}
		if len(page.Usage) == 0 {
			t.Errorf("%s has no usage line", name)
		}
		if len(page.Exits) == 0 {
			t.Errorf("%s states no exit codes", name)
		}
		if len(page.JSONKeys) == 0 && strings.TrimSpace(page.JSONNote) == "" {
			t.Errorf("%s says nothing about what it prints under --json", name)
		}
		rendered := page.render()
		for _, flag := range page.Flags {
			if flag.Required || flag.Default != "" {
				continue
			}
			t.Errorf("%s documents %s without a default and without marking it required", name, flag.Name)
		}
		for _, heading := range []string{"Usage:", "Flags:", "Exit codes:", "--json keys:"} {
			if !strings.Contains(rendered, heading) {
				t.Errorf("%s renders without the %q section", name, heading)
			}
		}
	}
}

// pagesWithoutAParserSite are the pages allowed to declare no parser site,
// each with the reason it is one. TestEveryPageDeclaresAParserSite asserts the
// table exact in both directions, so it cannot grow by accident and cannot
// keep an entry for a page that has since gained a site.
//
// It exists because the derivation used to return silently for any page whose
// Parsers list was empty, which made deleting one line the way to take a verb
// out of the flag contract with the suite still green. That is not a
// hypothetical refactor: it is how nineteen pages came to state that their
// verb takes no flags while accepting --config, worker serve among them, which
// the repository's own systemd unit invokes with --config.
var pagesWithoutAParserSite = map[string]string{
	"": "the top-level overview: it names the families and parses nothing of its own",

	// A family page summarises its verbs and the flags live one level down,
	// which is what the two depths are for.
	"archive":     "family page",
	"backlog":     "family page",
	"bucket":      "family page",
	"campaign":    "family page",
	"coordinator": "family page",
	"models":      "family page",
	"schedules":   "family page",
	"task":        "family page",
	"thread":      "family page",
	"wait":        "family page",
	"worker":      "family page",

	// version is answered inside the dispatcher before a flag set is built,
	// and it is the one page whose no-flags claim is true.
	"version": "answered before any parser exists; it takes no option at all",

	// A hub page is a family page one level down: it summarises the verbs
	// below it, and each of those carries the sites.
	"backlog artifact": "hub page; the sites are on artifact show and artifact get",
	"backlog backup":   "hub page; the sites are on backup create, verify and restore",
	"backlog command":  "hub page; the site is on command show",
	"backlog edge":     "hub page; the sites are on edge add and edge remove",
	"backlog run":      "hub page; the site is on run clone",
	"backlog task":     "hub page; the sites are on task show, task add and task set",
}

// Test 4. The floor under the derivation. A page that declares no parser site
// derives nothing, so declaring none has to be a decision somebody made and
// wrote down rather than the default for an empty list, and a page that loses
// the sites it had fails the build.
func TestEveryPageDeclaresAParserSite(t *testing.T) {
	for _, path := range helpPagePaths() {
		page, _ := helpPageFor(path)
		name := path
		if name == "" {
			name = "(overview)"
		}
		reason, excused := pagesWithoutAParserSite[path]
		switch {
		case len(page.Parsers) > 0 && excused:
			t.Errorf("%s declares a parser site and is still excused from the derivation as %q; drop the entry", name, reason)
		case len(page.Parsers) == 0 && !excused:
			t.Errorf("%s declares no parser site, so nothing derives its flags and nothing would notice if it stopped naming them; point it at the parser that consumes its arguments, or add it to pagesWithoutAParserSite with the reason", name)
		case excused && strings.TrimSpace(reason) == "":
			t.Errorf("%s is excused from the derivation with no reason recorded", name)
		}
	}
	for path := range pagesWithoutAParserSite {
		if _, found := helpPageFor(path); !found {
			t.Errorf("pagesWithoutAParserSite names %q, which is not a registered page", path)
		}
	}
}

// A help request must never be mistaken for a shell check's own arguments:
// "wait add -- ./deployed.sh --help" asks for a wait on a script that takes
// --help, not for this program's documentation.
func TestHelpIsNotTakenFromBehindTheEndOfOptions(t *testing.T) {
	if _, requested := helpRequest([]string{"wait"}, []string{"add", "--task", "current", "--", "./deployed.sh", "--help"}); requested {
		t.Error("a --help inside a shell check's own command was read as a help request")
	}
	if path, requested := helpRequest([]string{"wait"}, []string{"add", "--help", "--", "./deployed.sh"}); !requested || strings.Join(path, " ") != "wait add" {
		t.Errorf("a --help before the separator resolved to %v, requested=%v", path, requested)
	}
}

// A mistyped top-level verb is still refused by name rather than answered with
// the overview and exit 0, which would be the same ambiguous zero the finding
// is about.
func TestAMistypedTopLevelVerbIsNotAHelpRequest(t *testing.T) {
	if _, requested := helpRequest(nil, []string{"backlgo", "--help"}); requested {
		t.Error("a word that names no verb was answered as a help request")
	}
	if path, requested := helpRequest(nil, []string{"backlog", "show", "run-123", "--help"}); !requested || strings.Join(path, " ") != "backlog show" {
		t.Errorf("backlog show run-123 --help resolved to %v, requested=%v", path, requested)
	}
}

// The same reasoning one level down. "t3-steward backlog shwo --help" used to
// print the backlog family page and exit 0, which answers a question about a
// verb by describing a family and reports success for a verb that does not
// exist. A word standing where a verb should stand is refused by name, and the
// refusal names the verbs that do exist.
func TestAMistypedSecondLevelVerbIsRefusedRatherThanAnswered(t *testing.T) {
	for _, line := range [][]string{
		{"backlog", "shwo", "--help"},
		{"wait", "bogus", "--help"},
		{"worker", "bogus", "--help"},
		{"backlog", "task", "shwo", "--help"},
	} {
		written := strings.Join(line, " ")
		mistyped := line[len(line)-2]
		if path, requested := helpRequest(nil, line); requested {
			t.Errorf("t3-steward %s was answered with the %q page and exit 0", written, strings.Join(path, " "))
		}
		var out bytes.Buffer
		answered, err := admitHelp(&out, nil, line)
		if answered || err == nil {
			t.Fatalf("t3-steward %s: answered=%v, err=%v; a documentation request about a verb that does not exist must fail", written, answered, err)
		}
		if out.Len() != 0 {
			t.Errorf("t3-steward %s printed %d bytes before refusing", written, out.Len())
		}
		if !strings.Contains(err.Error(), mistyped) {
			t.Errorf("the refusal of t3-steward %s does not name the word it refused: %v", written, err)
		}
		parent := line[:len(line)-2]
		for _, real := range helpPageChildren(parent) {
			if !strings.Contains(err.Error(), real) {
				t.Errorf("the refusal of t3-steward %s does not name the real verb %q: %v", written, real, err)
			}
		}
	}
	// What must keep working: the family's own page, and a verb page reached
	// past the identifier the verb takes.
	for _, line := range [][]string{
		{"backlog", "--help"},
		{"backlog", "show", "run-123", "--help"},
		{"backlog", "task", "show", "run-123/build", "--help"},
	} {
		if _, requested := helpRequest(nil, line); !requested {
			t.Errorf("t3-steward %s is no longer a help request", strings.Join(line, " "))
		}
	}
}
