package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/campaign"
)

// These rules are descriptive tool guidance, never runtime admission evidence.
// The same owned text feeds full help, JSON metadata and the resident fragment.
type agentGuidanceRule struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

var agentGuidanceRules = []agentGuidanceRule{
	{"workflow", "Use campaigns for agreed planned dependent work; use task run for one independent unattended outcome. Bounded subagents never replace declared tasks or reviews."},
	{"review", "Use the supported review interface for independent plan or diff review within session/provider constraints. Use declared independent campaign review when restrictions prevent standalone diversity; same-provider review is not diversity. Never silently switch provider."},
	{"routes", "Obey explicit model, effort and provider restrictions and advertised route admission; compiled commands do not establish runtime eligibility."},
	{"authority", "Large cost, operator and fleet actions need their actual authority. Help grants no permissions, credentials, quota, execution profile or backend enforcement; central policy remains authoritative."},
	{"notify", "Resolve notify identity before submission. Follow the closing wake instruction; task-bound waits end the task turn. Never poll in an agent loop."},
	{"decisions", "Inside a task use ask for decisions: approval-required asks have no default and fail unanswered. Read the matching skill/reference before the first command."},
	{"retirement", "Markdown intake and wrappers remain retired. Supported diagnostics and fenced operator controls are distinct; quarantine cleanup never creates or retries intake. Evidence custody and immutable rollback stay with the rollout lead."},
	{"ownership", "Do not resurrect deleted production APIs or advertise nested M16 public review before its owner publishes it. Source ownership, independent review, exact source CI, installer accessibility, release and deployment are separate gates."},
}

func guidanceFor(ids []string) []agentGuidanceRule {
	rules := make([]agentGuidanceRule, 0, len(ids))
	for _, rule := range agentGuidanceRules {
		for _, id := range ids {
			if id == rule.ID {
				rules = append(rules, rule)
				break
			}
		}
	}
	return rules
}

func guidanceText(rules []agentGuidanceRule) string {
	var b strings.Builder
	b.WriteString("t3-steward: unattended tasks and campaigns. Full reference: t3-steward --help full; scoped reference: t3-steward <path> --help full. Exports: --help json or --help agent-md.\n")
	for _, rule := range rules {
		fmt.Fprintf(&b, "- [%s] %s\n", rule.ID, rule.Text)
	}
	return b.String()
}

func (p helpPage) render() string {
	body := p.renderReference()
	rules := guidanceFor(p.RuleIDs)
	// Root full help carries the shared guidance. Scoped references retain
	// their exact legacy bytes; JSON binds their rule IDs without rewriting
	// body-backed essays or command contracts.
	if p.Path != "" || len(rules) == 0 {
		return body
	}
	return body + "\nAgent guidance (descriptive only):\n" + guidanceText(rules)
}

// helpExportRequest is pure argv normalization shared by admission and the
// special new-format instrumentation exemption. It stops at the first bare --.
// Only modifiers of an actual help token count; operands never do.
type helpExportRequest struct {
	active bool
	export bool
	format string
	clean  []string
	err    error
}

func parseHelpExport(family, args []string) helpExportRequest {
	r := helpExportRequest{clean: append([]string(nil), args...)}
	at, end := -1, len(args)
	for i, word := range args {
		if word == "--" {
			end = i
			break
		}
		if at < 0 && isHelp(word) {
			at = i
		}
	}
	if at < 0 {
		return r
	}
	modifiers := 0
	var tail []string
	stop := end
	for i := at + 1; i < end; i++ {
		word := args[i]
		// The first flag ends the modifier/path segment, including assignment
		// spellings. Its values and all following operands are legacy arguments,
		// not formats. No runtime flag parser is needed for this boundary.
		if strings.HasPrefix(word, "-") {
			stop = i
			break
		}
		switch word {
		case "json", "agent-md":
			r.export, r.active, r.format = true, true, word
			modifiers++
		case "full":
			modifiers++
		default:
			path := append(append([]string(nil), family...), tail...)
			_, page := helpPageFor(strings.Join(path, " "))
			_, nextPage := helpPageFor(strings.Join(append(path, word), " "))
			// A legacy full request can still name a help-first path, but an
			// ordinary non-path word ends that path before later format words.
			if !r.export && modifiers > 0 && !nextPage {
				stop = i
				break
			}
			// Help-first paths may precede the format. Once a registered leaf
			// is reached, an ordinary word starts operands instead. A word
			// after verb --help cannot extend that verb's path either.
			if at > 0 || (len(tail) > 0 && page && len(helpPageChildren(path)) == 0) {
				stop = i
				if r.export || (at > 0 && modifiers == 0) {
					r.active, r.export = true, true
					r.err = fmt.Errorf("unknown help format %q; use full, json or agent-md", word)
				}
				break
			}
			tail = append(tail, word)
		}
		if stop != end {
			break
		}
	}
	if r.err != nil && modifiers == 0 {
		return r
	}
	// A word after verb --help is a format, while help <path> preserves the
	// existing help-first path grammar. Unknown campaign topics retain their
	// legacy diagnostics and instrumentation.
	if !r.active && modifiers == 0 && len(tail) > 0 {
		unknown := at > 0
		if at == 0 && len(family) == 0 {
			resolution := resolveHelp(family, args)
			unknown = !resolution.Answered && resolution.Unknown == ""
		}
		if unknown {
			r.active, r.export = true, true
			r.err = fmt.Errorf("unknown help format %q; use full, json or agent-md", tail[0])
			return r
		}
	}
	if !r.active {
		if modifiers > 1 {
			r.active = true
			r.err = fmt.Errorf("repeated help format; use one of full, json or agent-md")
		}
		return r
	}
	r.clean = append(append(append([]string(nil), args[:at+1]...), tail...), args[stop:]...)
	if modifiers != 1 {
		r.err = fmt.Errorf("repeated or conflicting help formats; use one of full, json or agent-md")
	} else if at > 0 && len(tail) > 0 {
		r.err = fmt.Errorf("unknown help format %q; use full, json or agent-md", tail[0])
	} else if len(family) == 1 && family[0] == "campaign" && at == 0 && len(args) > 1 {
		// Topic-first essays have priority over colliding verbs. Do not label an
		// essay as the verb's structured reference.
		word := args[1]
		topic := word == "supervision"
		for _, t := range campaign.HelpTopics() {
			topic = topic || t.Name == word
		}
		if topic {
			r.err = fmt.Errorf("campaign help topic %q has no structured export; use campaign <verb> --help %s for a registered verb", word, r.format)
		}
	}
	return r
}

// campaignHelpArguments retains raw post-help boundaries for classification.
// Only pre-help config pairs are normalized in that view. The separate legacy
// view mirrors dispatch's old config removal, but is never classified again.
func campaignHelpArguments(args []string) (helpExportRequest, []string, bool) {
	var raw, legacy []string
	help, ended := false, false
	for i := 0; i < len(args); i++ {
		word := args[i]
		if word == "--config" && i+1 < len(args) {
			if help || ended || args[i+1] == "--" {
				raw = append(raw, word, args[i+1])
			}
			// A bare terminator remains a boundary even when legacy dispatch
			// would consume it as the config value.
			ended = ended || args[i+1] == "--"
			i++
			continue
		}
		legacy = append(legacy, word)
		raw = append(raw, word)
		if word == "--" {
			ended = true
		}
		if !ended && isHelp(word) {
			help = true
		}
	}
	// If old config stripping exposed a help token, answer that legacy route
	// here too; it must not reach a second export classifier downstream.
	legacyHelp := false
	for _, word := range legacy {
		if word == "--" {
			break
		}
		legacyHelp = legacyHelp || isHelp(word)
	}
	return parseHelpExport([]string{"campaign"}, raw), legacy, help || legacyHelp || len(legacy) == 0
}

// Instrumentation uses the same raw campaign classification as admission,
// without consulting configuration, environment, storage or transports.
func isHelpExportInvocation(args []string) bool {
	if len(args) > 0 && args[0] == "campaign" {
		r, _, _ := campaignHelpArguments(args[1:])
		return r.export
	}
	return parseHelpExport(nil, args).export
}

type guidanceFlag struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Default  string `json:"default"`
	Text     string `json:"text"`
	Required bool   `json:"required"`
}
type guidanceExit struct {
	Code    int    `json:"code"`
	Meaning string `json:"meaning"`
}
type guidancePage struct {
	Path          string         `json:"path"`
	Purpose       string         `json:"purpose"`
	HelpArgv      []string       `json:"help_argv"`
	Flags         []guidanceFlag `json:"flags"`
	Exits         []guidanceExit `json:"exits"`
	JSONKeys      []string       `json:"json_keys"`
	JSONNote      string         `json:"json_note"`
	ReferenceHelp string         `json:"reference_help"`
	FullHelp      string         `json:"full_help"`
	RuleIDs       []string       `json:"rule_ids"`
}
type guidanceDocument struct {
	Schema           string              `json:"schema"`
	SchemaVersion    int                 `json:"schema_version"`
	ToolID           string              `json:"tool_id"`
	SelectedPath     string              `json:"selected_path"`
	Authority        string              `json:"authority"`
	Rules            []agentGuidanceRule `json:"rules"`
	ResidentFragment string              `json:"resident_fragment"`
	Pages            []guidancePage      `json:"pages"`
}

func renderHelpExport(out io.Writer, page helpPage, format string) error {
	rules := guidanceFor(page.RuleIDs)
	fragment := guidanceText(rules)
	if format == "agent-md" {
		_, err := io.WriteString(out, fragment)
		return err
	}
	doc := guidanceDocument{"steward-help-guidance/v1", 1, "t3-steward", page.Path, "descriptive-only", rules, fragment, []guidancePage{}}
	for _, path := range helpPagePaths() {
		if page.Path != "" && path != page.Path && !strings.HasPrefix(path, page.Path+" ") {
			continue
		}
		p := helpPages[path]
		flags, exits := []guidanceFlag{}, []guidanceExit{}
		// Body-backed references have no inferred structured capability metadata.
		if p.Body == "" {
			for _, f := range p.renderedFlags() {
				flags = append(flags, guidanceFlag(f))
			}
			for _, e := range p.Exits {
				exits = append(exits, guidanceExit(e))
			}
		}
		argv := append([]string{"t3-steward"}, strings.Fields(path)...)
		argv = append(argv, "--help", "full")
		doc.Pages = append(doc.Pages, guidancePage{path, p.Purpose, argv, flags, exits, append([]string{}, p.JSONKeys...), p.JSONNote, p.renderReference(), p.render(), append([]string{}, p.RuleIDs...)})
	}
	// Marshal before writing: format/refusal errors never leave partial JSON.
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = out.Write(data)
	return err
}

func admitHelpExport(out io.Writer, family, args []string) (bool, error) {
	return admitHelpExportRequest(out, family, parseHelpExport(family, args))
}

func admitHelpExportRequest(out io.Writer, family []string, r helpExportRequest) (bool, error) {
	if !r.active {
		return false, nil
	}
	// Suppress the ordinary --json runtime error envelope: export errors own
	// stderr only, including refusals that produced no document.
	if r.err != nil {
		return true, documentPrinted{r.err}
	}
	resolution := resolveHelp(family, r.clean)
	if resolution.Unknown != "" {
		return true, documentPrinted{unknownHelpVerb(resolution.Parent, resolution.Unknown)}
	}
	if !resolution.Answered {
		return true, documentPrinted{fmt.Errorf("unknown help path; use t3-steward --help full")}
	}
	page := helpPages[strings.Join(resolution.Path, " ")]
	err := renderHelpExport(out, page, r.format)
	if err != nil {
		return true, documentPrinted{err}
	}
	return true, nil
}
