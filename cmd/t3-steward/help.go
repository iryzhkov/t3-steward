package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// One help contract, in one place.
//
// Every verb the dispatcher routes, and every second-level verb of every
// command family, answers --help, -h and help the same way: on standard
// output, with exit status 0, before any argument parsing has happened. The
// admission is here rather than in the eighteen parsers because a parser that
// has to remember to test for help forgets: "backlog show --help" used to send
// the word --help to the coordinator as a workflow run id and come back with
// exit 8, and "backlog new --help" used to create a task file called
// --help.md. Neither is possible from here, because a help page is rendered
// from data in this package and reaches no network, no store and no file.
//
// The rule for a page is: one purpose line, a usage line, every flag the
// verb's parser accepts with its default, the exit codes the verb can return
// and what each one means, and the keys it prints under --json.

// helpFlag is one option of one verb, exactly as that verb's parser accepts
// it. The value placeholder is empty for a boolean flag, and Default is empty
// only when the flag has no default worth stating (a required flag, or one
// whose absence is the documented behaviour).
type helpFlag struct {
	Name     string
	Value    string
	Default  string
	Text     string
	Required bool
}

// helpExit is one exit status the verb can return, and what it means.
type helpExit struct {
	Code    int
	Meaning string
}

// parserSite names where a verb's options are actually parsed: a function in
// this package and, when one function serves several verbs through a switch,
// the case clause that belongs to this verb. The help contract test reads the
// flags out of those regions of the source and requires the page to name every
// one of them, so a flag added to a parser fails the build until it is
// documented. The site is a location, never a list of flags.
type parserSite struct {
	Func string
	Case string
}

// helpPage is one verb's reference.
type helpPage struct {
	// Path is the verb, without the program name: "backlog show". The empty
	// path is the top-level overview.
	Path    string
	Purpose string
	Usage   []string
	Flags   []helpFlag
	Exits   []helpExit
	// JSONKeys are the top-level keys the verb prints under --json. An empty
	// list with a JSONNote explains a verb that prints no JSON document.
	JSONKeys []string
	JSONNote string
	Notes    string
	// Body replaces the rendered skeleton with a reference this package
	// already carries in full, such as taskRunUsage. A page has Body or the
	// structured fields, never both.
	Body string
	// Parsers are the source regions that consume this verb's arguments.
	// Family pages carry none: a family summarises its verbs and the flags
	// live one level down.
	Parsers []parserSite
	// Undocumented names a flag the parser accepts and the help deliberately
	// does not advertise -- a superseded spelling kept for the callers that
	// already use it. Each entry needs a reason in the comment beside it, and
	// the contract test fails when the parser stops accepting one, so the list
	// cannot quietly rot either.
	Undocumented []string
}

// helpTokens are the three spellings of a help request.
func isHelp(arg string) bool {
	return arg == "help" || arg == "--help" || arg == "-h"
}

// helpResolution is what a command line asking for documentation resolves to:
// either the page that answers it, or the word that stands where a verb should
// and names none.
type helpResolution struct {
	// Path is the verb path whose page answers, when Answered.
	Path     []string
	Answered bool
	// Parent is the page whose verb list the unknown word should have named,
	// and Unknown is that word. Both are empty unless the request named a verb
	// that does not exist.
	Parent  []string
	Unknown string
}

// resolveHelp resolves a command line into the verb path whose page answers
// it. family is the verb words already consumed by the caller (nil at the top
// level), args is what the caller received.
//
// Scanning stops at a bare "--": everything after it is the caller's own
// command or prompt, and "t3-steward wait add -- ./deployed.sh --help" asks
// for a wait on a script, not for this program's documentation.
//
// The words before the help token name the verb, so "backlog show --help"
// resolves to the backlog show page; the words after it do when help comes
// first, so "backlog help show" resolves to the same page. Trailing words that
// name no page are dropped, which is how "backlog show run-123 --help" answers
// about backlog show. At the top level a first word that names no verb is not
// a help request at all, so a mistyped command is still refused by name.
//
// A word is dropped only when it can be an operand. "backlog show" has no
// verbs below it, so "backlog show run-123" names a run and the page for
// backlog show is the answer; "backlog" does have verbs below it, so
// "backlog shwo" names no run and no verb, and answering it with the family
// page and exit 0 would be an ambiguous zero in answer to a documentation
// request. That word is reported as unknown instead, and the caller refuses it
// by name.
func resolveHelp(family, args []string) helpResolution {
	index := -1
	for position, argument := range args {
		if argument == "--" {
			break
		}
		if isHelp(argument) {
			index = position
			break
		}
	}
	if index < 0 {
		return helpResolution{}
	}
	var words []string
	if index == 0 {
		words = append(words, args[1:]...)
	} else {
		words = append(words, args[:index]...)
	}
	// A word that is not a verb -- a flag, an identifier -- ends the path.
	for position, word := range words {
		if strings.HasPrefix(word, "-") {
			words = words[:position]
			break
		}
	}
	minimum := 0
	if len(family) == 0 && len(words) > 0 {
		minimum = 1
	}
	for length := len(words); length >= minimum; length-- {
		path := append(append([]string{}, family...), words[:length]...)
		if _, found := helpPages[strings.Join(path, " ")]; !found {
			continue
		}
		if length == len(words) || len(helpPageChildren(path)) == 0 {
			return helpResolution{Path: path, Answered: true}
		}
		return helpResolution{Parent: path, Unknown: words[length]}
	}
	return helpResolution{}
}

// helpRequest is resolveHelp for the callers that only need the page.
func helpRequest(family, args []string) ([]string, bool) {
	resolution := resolveHelp(family, args)
	return resolution.Path, resolution.Answered
}

// helpPageChildren names the verbs registered directly below a page, sorted.
// It is what tells an operand apart from a mistyped verb, and what a refusal
// lists, so the list of a family's verbs is derived from the registry every
// --help is answered from rather than written out a second time.
func helpPageChildren(path []string) []string {
	prefix := strings.Join(path, " ")
	if prefix == "" {
		return nil
	}
	prefix += " "
	var children []string
	for registered := range helpPages {
		if !strings.HasPrefix(registered, prefix) {
			continue
		}
		rest := registered[len(prefix):]
		if rest == "" || strings.Contains(rest, " ") {
			continue
		}
		children = append(children, rest)
	}
	sort.Strings(children)
	return children
}

// unknownHelpVerb is the refusal for "t3-steward backlog shwo --help": a
// documentation request about a verb that does not exist. It names the verbs
// that do, which is the standard the program's other refusals already meet,
// and it fails, because exit 0 with the family page says the verb was
// documented.
func unknownHelpVerb(parent []string, verb string) error {
	name := strings.Join(parent, " ")
	return fmt.Errorf("unknown %s command %q; the %s commands are %s (try \"t3-steward %s --help\")",
		name, verb, name, strings.Join(helpPageChildren(parent), ", "), name)
}

// admitHelp is the one place a help request is answered. A command family
// calls it once, at its entry point, before its parser sees the arguments; it
// reports whether it printed a page, and the caller returns its error when it
// did not print one and the request named a verb that does not exist.
//
// It reads no configuration, opens no store and reaches no coordinator, which
// is what makes a help page that returns a transport failure impossible by
// construction rather than by review.
func admitHelp(out io.Writer, family []string, args []string) (bool, error) {
	resolution := resolveHelp(family, args)
	if resolution.Unknown != "" {
		return false, unknownHelpVerb(resolution.Parent, resolution.Unknown)
	}
	if !resolution.Answered {
		return false, nil
	}
	page, found := helpPages[strings.Join(resolution.Path, " ")]
	if !found {
		return false, nil
	}
	_, err := fmt.Fprint(out, page.render())
	return true, err
}

// admitFamilyHelp is admitHelp for a command family, where the bare family
// name is itself a request for the family's page.
func admitFamilyHelp(out io.Writer, family []string, args []string) (bool, error) {
	if len(args) == 0 {
		args = []string{"--help"}
	}
	return admitHelp(out, family, args)
}

// helpPageFor returns the page for a verb path, for the contract tests.
func helpPageFor(path string) (helpPage, bool) {
	page, found := helpPages[path]
	return page, found
}

// helpPagePaths lists every registered verb path, sorted.
func helpPagePaths() []string {
	paths := make([]string, 0, len(helpPages))
	for path := range helpPages {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// renderedFlags are the options this page actually prints: its own, plus the
// --config every verb below a command family accepts because the dispatcher's
// clause for that family takes it out of the argument list before the family
// is entered.
//
// It is injected here rather than written on ninety-odd pages because it is
// the dispatcher's behaviour and not the verb's, and because writing it by
// hand is how nineteen pages came to say "This verb takes no flags" about
// verbs that take this one -- among them worker serve, which the packaged unit
// invokes with it. It is a method rather than a few lines inside render so
// that the contract test asserts against what the page prints: the injected
// flag never reached Flags, so deleting --config from one family's clause in
// the dispatcher would have left every page of that family documenting an
// option nothing accepted, with the suite green.
func (p helpPage) renderedFlags() []helpFlag {
	if family, _, isFamilyVerb := strings.Cut(p.Path, " "); isFamilyVerb {
		return append(append([]helpFlag{}, p.Flags...), familyConfigFlag(family))
	}
	return p.Flags
}

// render writes the page. A page with a Body carries a reference this package
// already holds in full and is printed as it stands.
func (p helpPage) render() string {
	if p.Body != "" {
		return p.Body
	}
	var b strings.Builder
	name := strings.TrimSpace("t3-steward " + p.Path)
	fmt.Fprintf(&b, "%s - %s\n", name, p.Purpose)
	b.WriteString("\nUsage:\n")
	for _, line := range p.Usage {
		fmt.Fprintf(&b, "  %s\n", line)
	}
	b.WriteString("\nFlags:\n")
	flags := p.renderedFlags()
	if len(flags) == 0 {
		b.WriteString("  This verb takes no flags.\n")
	}
	for _, flag := range flags {
		spelling := flag.Name
		if flag.Value != "" {
			spelling += " " + flag.Value
		}
		state := "default: " + flag.Default
		switch {
		case flag.Required:
			state = "required"
		case flag.Default == "":
			state = "no default"
		}
		fmt.Fprintf(&b, "  %-34s (%s)\n", spelling, state)
		for _, line := range wrapHelpText(flag.Text, 68) {
			fmt.Fprintf(&b, "      %s\n", line)
		}
	}
	b.WriteString("\nExit codes:\n")
	for _, exit := range p.Exits {
		fmt.Fprintf(&b, "  %-3d %s\n", exit.Code, exit.Meaning)
	}
	b.WriteString("\n--json keys:\n")
	switch {
	case len(p.JSONKeys) > 0:
		for _, line := range wrapHelpText(strings.Join(p.JSONKeys, ", "), 72) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
		if p.JSONNote != "" {
			for _, line := range wrapHelpText(p.JSONNote, 72) {
				fmt.Fprintf(&b, "  %s\n", line)
			}
		}
	case p.JSONNote != "":
		for _, line := range wrapHelpText(p.JSONNote, 72) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	default:
		b.WriteString("  This verb prints no JSON document.\n")
	}
	if notes := strings.TrimSpace(p.Notes); notes != "" {
		b.WriteString("\n")
		for _, paragraph := range strings.Split(notes, "\n") {
			if strings.TrimSpace(paragraph) == "" {
				b.WriteString("\n")
				continue
			}
			for _, line := range wrapHelpText(paragraph, 76) {
				b.WriteString(line + "\n")
			}
		}
	}
	return b.String()
}

// wrapHelpText breaks a sentence at word boundaries. A word longer than the
// width, such as a long flag spelling, is left on its own line rather than cut.
func wrapHelpText(text string, width int) []string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return nil
	}
	var lines []string
	current := fields[0]
	for _, word := range fields[1:] {
		if len(current)+1+len(word) > width {
			lines = append(lines, current)
			current = word
			continue
		}
		current += " " + word
	}
	return append(lines, current)
}
