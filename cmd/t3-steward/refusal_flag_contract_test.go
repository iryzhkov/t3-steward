package main

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"regexp"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// Test 2 of the stage. No refusal names a flag its own verb rejects.
//
// A-4 was one message: "task run" refused without a T3 identity and advised
// --thread and --notify-thread, both of which its parser rejected, so the only
// refusal an agent could not act on sent it to a second refusal. The general
// form is what is tested here, because the wording of one message is not what
// broke: a shared resolver wrote advice for a verb it could not see.
//
// Every flag a refusal names is attributed to a verb -- the one invoked, or the
// one named in a "t3-steward ..." command on the same line -- and has to be a
// flag that verb's own parser accepts.
func TestNoRefusalNamesAFlagItsVerbRejects(t *testing.T) {
	files := packageSource(t)
	for _, refusal := range collectedRefusals(t) {
		t.Run(refusal.name, func(t *testing.T) {
			if strings.TrimSpace(refusal.message) == "" {
				t.Fatal("the path under test produced no message")
			}
			named := 0
			for _, mention := range flagMentions(refusal.verb, refusal.message) {
				named++
				accepted := verbAcceptedFlags(t, files, mention.verb)
				if len(accepted) == 0 {
					t.Errorf("%q names %s for %q, and no parser of that verb declares any flag",
						refusal.name, mention.flag, mention.verb)
					continue
				}
				if !accepted[mention.flag] {
					t.Errorf("%q tells the caller to pass %s to %q, whose parser rejects it.\n%s",
						refusal.name, mention.flag, mention.verb, refusal.message)
				}
			}
			if refusal.mustNameAFlag && named == 0 {
				t.Fatalf("%q names no flag at all, so it tells the caller nothing to do:\n%s",
					refusal.name, refusal.message)
			}
		})
	}
}

// refusalUnderTest is one message a real path produced, with the verb that was
// invoked to produce it.
type refusalUnderTest struct {
	name          string
	verb          string
	message       string
	mustNameAFlag bool
}

// collectedRefusals drives every refusal and remedy this stage touches that can
// be reached without a coordinator. Each is produced by the real code path, not
// by quoting its text here: a message asserted against its own literal proves
// only that somebody copied it twice.
func collectedRefusals(t *testing.T) []refusalUnderTest {
	t.Helper()
	unresolved := unresolvedThread("no T3 thread could be resolved from the caller's provider session")
	collected := []refusalUnderTest{
		{name: "wait add without an identity", verb: "wait add", mustNameAFlag: true,
			message: errorText(refuseWaitThread("wait add", "<the rest of this call>", unresolved))},
	}

	// "task run" from a shell with no T3 session identity: A-4's own path,
	// driven end to end through the harness.
	h := newTaskRunHarness()
	h.threadErr = unresolved
	collected = append(collected, refusalUnderTest{
		name: "task run without an identity", verb: "task run", mustNameAFlag: true,
		message: errorText(h.run("--model", "opus", "--", "work")),
	})

	// "campaign submit" reaches the same resolver and must not inherit the
	// other verb's advice.
	submit := campaignCLI{resolveThread: func(string) (string, error) { return "", unresolved }}
	_, submitErr := submit.campaignNotifyThread("campaign submit", "current")
	collected = append(collected, refusalUnderTest{
		name: "campaign submit without an identity", verb: "campaign submit", mustNameAFlag: true,
		message: errorText(submitErr),
	})

	// The run was submitted and the wait was not: the one refusal that has to
	// hand the caller a second command, which is a different verb's.
	unregistered := campaignCLI{
		notify: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			return backlogadmin.NodeWaitResponse{}, errors.New("coordinator unavailable")
		},
	}
	_, registerErr := unregistered.registerCampaignNotification(context.Background(), "key-1", "run-1", "thread-1")
	collected = append(collected, refusalUnderTest{
		name: "the wait could not be registered", verb: "task run", mustNameAFlag: true,
		message: errorText(registerErr),
	})

	// The record's own remedies. They are not refusals, but an agent acts on
	// them exactly as it acts on one, so they answer to the same rule.
	for name, record := range map[string]taskRunRecord{
		"a replay whose progress could not be read": {
			Run: "run-1", Replayed: true, ProgressUnavailable: "coordinator unavailable",
			Result: "t3-steward task result run-1",
		},
		"a live run with no wake attached": {
			Run: "run-1", Replayed: true, Progress: "active", Result: "t3-steward task result run-1",
			Notify: &campaignNotification{WaitID: "nw-1", ThreadID: "thread-1", Delivery: "delivered"},
		},
		"a run whose wake cannot reach this host": {
			Run: "run-1", Check: "ready", Result: "t3-steward task result run-1",
			Notify: &campaignNotification{WaitID: "nw-1", ThreadID: "thread-1", Delivery: "pending",
				Host: "normandy", Undeliverable: "the wake is sent into the T3 of normandy"},
		},
	} {
		var out bytes.Buffer
		if err := renderTaskRunRecord(&out, record); err != nil {
			t.Fatal(err)
		}
		collected = append(collected, refusalUnderTest{name: name, verb: "task run", message: out.String()})
	}
	return collected
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// flagMention is one option a message named, with the verb it was named for.
type flagMention struct {
	verb string
	flag string
}

var (
	flagPattern    = regexp.MustCompile(`--[a-z][a-z0-9-]*`)
	commandPattern = regexp.MustCompile(`t3-steward ((?:[a-z][a-z0-9-]* ?){1,3})`)
)

// flagMentions attributes every option a message names to the verb it is meant
// for: the verb named by a "t3-steward ..." command on that line, or the verb
// that was invoked when the line names no command. That distinction is the
// whole of the rule. Pointing at another verb's flag inside that verb's own
// command line is correct and is how a refusal hands off; naming it bare, as
// advice to the caller of this verb, is A-4.
func flagMentions(invoked, message string) []flagMention {
	var mentions []flagMention
	seen := map[flagMention]bool{}
	for _, line := range strings.Split(message, "\n") {
		for _, segment := range commandSegments(invoked, line) {
			for _, flag := range flagPattern.FindAllString(segment.text, -1) {
				mention := flagMention{verb: segment.verb, flag: flag}
				if seen[mention] {
					continue
				}
				seen[mention] = true
				mentions = append(mentions, mention)
			}
		}
	}
	return mentions
}

type commandSegment struct {
	verb string
	text string
}

// commandSegments splits one line into the stretches that belong to each verb:
// everything before the first "t3-steward" belongs to the invoked verb, and
// each "t3-steward <verb>" starts a stretch belonging to the longest registered
// verb path it names.
func commandSegments(invoked, line string) []commandSegment {
	positions := commandPattern.FindAllStringSubmatchIndex(line, -1)
	if len(positions) == 0 {
		return []commandSegment{{verb: invoked, text: line}}
	}
	segments := []commandSegment{{verb: invoked, text: line[:positions[0][0]]}}
	for index, position := range positions {
		end := len(line)
		if index+1 < len(positions) {
			end = positions[index+1][0]
		}
		verb := longestRegisteredVerb(strings.Fields(line[position[2]:position[3]]))
		if verb == "" {
			verb = invoked
		}
		segments = append(segments, commandSegment{verb: verb, text: line[position[1]:end]})
	}
	return segments
}

// longestRegisteredVerb is the longest help-page path the words spell, so that
// "wait add --run" resolves to "wait add" and not to "wait".
func longestRegisteredVerb(words []string) string {
	for length := len(words); length > 0; length-- {
		candidate := strings.Join(words[:length], " ")
		if _, found := helpPageFor(candidate); found {
			return candidate
		}
	}
	return ""
}

// verbAcceptedFlags is every option the parsers of one verb declare, read from
// the source rather than from its help: the help is what test 3 of the help
// contract checks against the same parsers, and a refusal must answer to the
// parser itself.
func verbAcceptedFlags(t *testing.T, files map[string]*ast.File, verb string) map[string]bool {
	t.Helper()
	page, found := helpPageFor(verb)
	if !found {
		t.Fatalf("a message names %q, which is not a verb this command has", verb)
	}
	accepted := map[string]bool{"--json": true, "--help": true}
	for _, site := range page.Parsers {
		for flag := range declaredFlags(t, files, site) {
			accepted[flag] = true
		}
	}
	if len(accepted) == 2 && len(page.Parsers) == 0 {
		return nil
	}
	return accepted
}
