package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
)

const campaignFixtureManifest = `version: 2
name: example-campaign
environment:
  project: t3-steward
inputs:
  - inputs/plan.md
tasks:
  review:
    prompt_file: prompts/review.md
    outputs: [review.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [review]
    inputs_from:
      review: [review.md]
`

var campaignTestLimits = campaign.Limits{MaxFiles: 1000, MaxBytes: 4 << 20}

// campaignFixture writes a small but complete campaign directory.
func campaignFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"workflow.yaml":        campaignFixtureManifest,
		"inputs/plan.md":       "the plan\n",
		"prompts/review.md":    "review the plan\n",
		"prompts/implement.md": "implement the plan\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// campaignTestCLI is an authoring CLI with both coordinator seams disarmed: a
// test that reaches either of them has found a command touching the
// coordinator when it should not.
func campaignTestCLI(t *testing.T, out io.Writer) campaignCLI {
	t.Helper()
	return campaignCLI{
		limits: campaignTestLimits,
		stdout: out,
		submissions: func() (adminSubmissionService, error) {
			t.Fatal("an authoring command resolved the submission transport")
			return nil, nil
		},
		admin: func(args []string) error {
			t.Fatalf("an authoring command called the coordinator admin path with %v", args)
			return nil
		},
		viability: func(context.Context, backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
			t.Fatal("an authoring command asked the coordinator whether the campaign could run")
			return backlogadmin.ViabilityMatrix{}, nil
		},
	}
}

// campaignReadyMatrix is what a coordinator answers for a campaign every task
// of which has a viable candidate now.
func campaignReadyMatrix(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
	matrix := backlogadmin.ViabilityMatrix{
		SchemaVersion: backlogadmin.ViabilityMatrixSchemaVersion,
		Outcome:       backlogadmin.ViabilityReady,
	}
	for _, task := range request.Tasks {
		matrix.Tasks = append(matrix.Tasks, backlogadmin.ViabilityTaskResult{
			Task: task.Name, Outcome: backlogadmin.ViabilityReady,
			Candidates: []backlogadmin.ViabilityCandidate{{
				Worker: "homelab", Outcome: backlogadmin.ViabilityReady,
			}},
		})
	}
	return matrix
}

func TestCampaignArgumentParsing(t *testing.T) {
	tests := []struct {
		name       string
		command    string
		args       []string
		allowDOT   bool
		requireKey bool
		want       campaignArgs
		wantError  string
	}{
		{
			name: "validate takes a source", command: "validate", args: []string{"./demo"},
			want: campaignArgs{source: "./demo"},
		},
		{
			name: "validate takes json", command: "validate", args: []string{"./demo", "--json"},
			want: campaignArgs{source: "./demo", asJSON: true},
		},
		{
			name: "source may follow the flag", command: "validate", args: []string{"--json", "./demo"},
			want: campaignArgs{source: "./demo", asJSON: true},
		},
		{
			name: "validate refuses dot", command: "validate", args: []string{"./demo", "--dot"},
			wantError: "does not accept --dot",
		},
		{
			name: "validate refuses a key", command: "validate", args: []string{"./demo", "--idempotency-key", "k"},
			wantError: "does not accept --idempotency-key",
		},
		{
			name: "json may not repeat", command: "validate", args: []string{"./demo", "--json", "--json"},
			wantError: "only once",
		},
		{
			name: "one source only", command: "validate", args: []string{"./demo", "./other"},
			wantError: "exactly one campaign directory",
		},
		{
			name: "a source is required", command: "validate", args: nil,
			wantError: "needs a campaign directory",
		},
		{
			name: "unknown option", command: "validate", args: []string{"./demo", "--verbose"},
			wantError: "unknown campaign validate option",
		},
		{
			name: "plan takes dot", command: "plan", args: []string{"./demo", "--dot"}, allowDOT: true,
			want: campaignArgs{source: "./demo", asDOT: true},
		},
		{
			name: "dot may not repeat", command: "plan", args: []string{"./demo", "--dot", "--dot"}, allowDOT: true,
			wantError: "only once",
		},
		{
			name: "json and dot are exclusive", command: "plan", args: []string{"./demo", "--json", "--dot"}, allowDOT: true,
			wantError: "mutually exclusive",
		},
		{
			name: "submit takes a key", command: "submit", args: []string{"./demo", "--idempotency-key", "key-1"}, requireKey: true,
			// notify defaults to current on submit: the calling thread is woken
			// unless --no-notify says otherwise, the same as on "task run".
			want: campaignArgs{source: "./demo", key: "key-1", notify: "current"},
		},
		{
			name: "submit requires a key", command: "submit", args: []string{"./demo"}, requireKey: true,
			wantError: "requires --idempotency-key",
		},
		{
			name: "submit refuses an empty key", command: "submit", args: []string{"./demo", "--idempotency-key", ""}, requireKey: true,
			wantError: "one nonempty value",
		},
		{
			name: "submit refuses a dangling key", command: "submit", args: []string{"./demo", "--idempotency-key"}, requireKey: true,
			wantError: "one nonempty value",
		},
		{
			name: "submit refuses two keys", command: "submit", requireKey: true,
			args:      []string{"./demo", "--idempotency-key", "one", "--idempotency-key", "two"},
			wantError: "one nonempty value",
		},
		{
			name: "submit refuses dot", command: "submit", args: []string{"./demo", "--dot", "--idempotency-key", "k"}, requireKey: true,
			wantError: "does not accept --dot",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parseCampaignArgs(test.command, test.args, test.allowDOT, test.requireKey)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want it to mention %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parsed != test.want {
				t.Fatalf("parsed = %+v, want %+v", parsed, test.want)
			}
		})
	}
}

func TestCampaignValidateReportsTheBundle(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	cli := campaignTestCLI(t, &out)
	if err := cli.run(context.Background(), []string{"validate", root, "--json"}); err != nil {
		t.Fatal(err)
	}
	var summary campaignValidation
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.SchemaVersion != campaignValidationSchemaVersion || !summary.Valid {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.Name != "example-campaign" || summary.Tasks != 2 || summary.Edges != 1 ||
		summary.InputFiles != 1 || summary.Files != 4 {
		t.Fatalf("summary = %+v", summary)
	}
	if !reflect.DeepEqual(summary.Roots, []string{"review"}) ||
		!reflect.DeepEqual(summary.Leaves, []string{"implement"}) {
		t.Fatalf("roots = %v, leaves = %v", summary.Roots, summary.Leaves)
	}
	if len(summary.Digest) != 64 || summary.Source != root || summary.Bytes <= 0 {
		t.Fatalf("summary = %+v", summary)
	}

	// The digest reported is the digest the packer produces for those bytes.
	bundle, err := campaign.Prepare(root, campaignTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Digest != bundle.ContentDigest {
		t.Fatalf("validate digest %s, bundle digest %s", summary.Digest, bundle.ContentDigest)
	}

	out.Reset()
	if err := cli.run(context.Background(), []string{"validate", filepath.Join(root, "workflow.yaml")}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"campaign example-campaign is valid", "tasks   2", "edges   1",
		"roots   review", "leaves  implement", bundle.ContentDigest,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not contain %q", out.String(), want)
		}
	}
}

func TestCampaignValidateNamesTheOffendingFileAndField(t *testing.T) {
	root := campaignFixture(t)
	broken := strings.Replace(campaignFixtureManifest, "prompts/review.md", "prompts/absent.md", 1)
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	cli := campaignTestCLI(t, io.Discard)
	err := cli.run(context.Background(), []string{"validate", root})
	if err == nil {
		t.Fatal("a campaign with a missing prompt file was accepted")
	}
	for _, want := range []string{root, "review", "prompt_file", "prompts/absent.md"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

func TestCampaignPlanRendersEveryFormat(t *testing.T) {
	root := campaignFixture(t)
	bundle, err := campaign.Prepare(root, campaignTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cli := campaignTestCLI(t, &out)

	if err := cli.run(context.Background(), []string{"plan", root, "--json"}); err != nil {
		t.Fatal(err)
	}
	var document struct {
		SchemaVersion int    `json:"schemaVersion"`
		Name          string `json:"name"`
		Source        string `json:"source"`
		Digest        string `json:"digest"`
		Tasks         []struct {
			Name string `json:"name"`
		} `json:"tasks"`
		Waves []struct {
			Index int      `json:"index"`
			Tasks []string `json:"tasks"`
		} `json:"waves"`
	}
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.SchemaVersion != campaign.PlanSchemaVersion || document.Name != "example-campaign" {
		t.Fatalf("document = %+v", document)
	}
	if document.Source != root || document.Digest != bundle.ContentDigest {
		t.Fatalf("source = %q digest = %q", document.Source, document.Digest)
	}
	if len(document.Tasks) != 2 || len(document.Waves) != 2 {
		t.Fatalf("tasks = %+v waves = %+v", document.Tasks, document.Waves)
	}

	out.Reset()
	if err := cli.run(context.Background(), []string{"plan", root, "--dot"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "digraph \"example-campaign\" {") ||
		!strings.Contains(out.String(), "\"review\" -> \"implement\"") {
		t.Fatalf("dot output = %q", out.String())
	}

	out.Reset()
	if err := cli.run(context.Background(), []string{"plan", root}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "example-campaign") || !strings.Contains(out.String(), bundle.ContentDigest) {
		t.Fatalf("text output = %q", out.String())
	}
}

func TestCampaignSubmitSendsThePackedArchive(t *testing.T) {
	root := campaignFixture(t)
	bundle, err := campaign.Prepare(root, campaignTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeSubmissionService{}
	var out bytes.Buffer
	var checked int
	cli := campaignCLI{
		limits:      campaignTestLimits,
		stdout:      &out,
		submissions: func() (adminSubmissionService, error) { return fake, nil },
		admin: func(args []string) error {
			t.Fatalf("submit called the admin path with %v", args)
			return nil
		},
		viability: func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
			checked++
			return campaignReadyMatrix(request), nil
		},
	}
	if err := cli.run(context.Background(), []string{"submit", root, "--idempotency-key", "campaign-1", "--no-notify"}); err != nil {
		t.Fatal(err)
	}
	if checked != 1 {
		t.Fatalf("submit ran the readiness check %d times, want 1", checked)
	}
	if fake.request.IdempotencyKey != "campaign-1" {
		t.Fatalf("request = %+v", fake.request)
	}
	if !bytes.Equal(fake.raw, bundle.Archive) || fake.size != int64(len(bundle.Archive)) {
		t.Fatalf("sent %d bytes with size %d, packed %d bytes", len(fake.raw), fake.size, len(bundle.Archive))
	}
	for _, want := range []string{
		"submission campaign-1: workflow=workflow-1 run=run-1 state=accepted",
		"t3-steward campaign show run-1",
		"t3-steward campaign graph run-1",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not contain %q", out.String(), want)
		}
	}

	out.Reset()
	if err := cli.run(context.Background(), []string{"submit", root, "--idempotency-key", "campaign-1", "--json", "--no-notify"}); err != nil {
		t.Fatal(err)
	}
	var response backlogadmin.LocalSubmissionResponse
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Key != "campaign-1" || response.RunID != "run-1" || response.State != "accepted" {
		t.Fatalf("response = %+v", response)
	}
}

func TestCampaignSubmitRefusesAnInvalidCampaignBeforeSending(t *testing.T) {
	root := campaignFixture(t)
	if err := os.Remove(filepath.Join(root, "inputs", "plan.md")); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSubmissionService{}
	cli := campaignCLI{
		limits:      campaignTestLimits,
		stdout:      io.Discard,
		submissions: func() (adminSubmissionService, error) { return fake, nil },
	}
	if err := cli.run(context.Background(), []string{"submit", root, "--idempotency-key", "campaign-1"}); err == nil {
		t.Fatal("an invalid campaign was submitted")
	}
	if fake.size != 0 || fake.raw != nil {
		t.Fatalf("the transport was called with %d bytes", len(fake.raw))
	}
}

// The aliases exist to spell an existing command differently, so the arguments
// have to arrive at the shared admin path exactly as they were typed.
func TestCampaignLifecycleAliasesForwardArgumentsUnchanged(t *testing.T) {
	commands := [][]string{
		{"list"},
		{"list", "--project", "t3-steward", "--progress", "active,blocked", "--json"},
		{"show", "run-1", "--json"},
		{"graph", "run-1", "--dot"},
		{"explain", "run-1/implement", "--json"},
		{"cancel", "run-1/implement", "--reason", "superseded", "--command-id", "cmd-1", "--json"},
	}
	for _, command := range commands {
		t.Run(command[0], func(t *testing.T) {
			var forwarded []string
			var out bytes.Buffer
			cli := campaignCLI{
				limits: campaignTestLimits,
				stdout: &out,
				submissions: func() (adminSubmissionService, error) {
					t.Fatal("a lifecycle alias resolved the submission transport")
					return nil, nil
				},
				admin: func(args []string) error {
					forwarded = args
					return nil
				},
			}
			if err := cli.run(context.Background(), command); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(forwarded, command) {
				t.Fatalf("forwarded %v, want %v", forwarded, command)
			}
			if out.Len() != 0 {
				t.Fatalf("an alias rendered %q of its own", out.String())
			}
		})
	}
}

func TestCampaignRefusesCommandsThatStayUnderBacklog(t *testing.T) {
	cli := campaignTestCLI(t, io.Discard)
	for _, command := range []string{"recover", "task", "edge", "artifact", "status", "retry"} {
		t.Run(command, func(t *testing.T) {
			err := cli.run(context.Background(), []string{command, "run-1"})
			if err == nil || !strings.Contains(err.Error(), "backlog") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCampaignHelpTopicsComeFromTheProjection(t *testing.T) {
	var out bytes.Buffer
	cli := campaignTestCLI(t, &out)
	for _, topic := range campaign.HelpTopics() {
		out.Reset()
		if err := cli.run(context.Background(), []string{"help", topic.Name}); err != nil {
			t.Fatal(err)
		}
		if out.String() != topic.Body {
			t.Fatalf("help %s rewrote its topic", topic.Name)
		}
	}
	out.Reset()
	if err := cli.run(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
	if out.String() != campaignUsage {
		t.Fatal("campaign help printed something other than the usage")
	}
	if err := cli.run(context.Background(), []string{"help", "nonexistent"}); err == nil ||
		!strings.Contains(err.Error(), "unknown campaign help topic") {
		t.Fatalf("unknown topic error = %v", err)
	}
}

// The authoring topic carries the discipline the usage block only names: a
// campaign's declared tasks are its only fan-out, each runs as its own
// Steward-scheduled T3 session, and one task is a choice rather than a
// fallback. Losing any of those makes the help agree with the behaviour it is
// meant to prevent.
func TestCampaignAuthoringTopicStatesTheDiscipline(t *testing.T) {
	var out bytes.Buffer
	cli := campaignTestCLI(t, &out)
	if err := cli.run(context.Background(), []string{"help", "authoring"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Multi-task work is authored as a static version 2 DAG",
		"run in its own T3 session",
		"must not use native subagents as a substitute for declared",
		"Hidden native delegation\nis not separately scheduled work",
		"Declare the work a task would fan out as tasks, joined with needs and\ninputs_from",
		"One task is a legitimate authoring choice, not a fallback",
		"docs/examples/campaign/three-node   the recommended multi-task template",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("the authoring topic no longer covers %q", want)
		}
	}
}

// The usage text is part of the product and enters an agent's context, so it is
// pinned. Changing it is fine; changing it without reading the help contract in
// docs/plans/campaign-manager.md is what this test is here to prevent. Update
// the digest together with the text.
func TestCampaignUsageIsPinnedAndComplete(t *testing.T) {
	// Updated when the campaign help gained the short transport note that says
	// which coordinator a submission reaches and what its exit codes mean, and
	// again when it gained the check verb, the three readiness outcomes, the
	// permanent and temporary failure lists, the recovery commands, the
	// required configuration and a complete copyable example, and again when it
	// gained rerun and submit --notify-thread. The 100-line cap left no room
	// for their detail, so the block names them and the "rerun" and "notify"
	// help topics carry the contract.
	//
	// Updated again when the manifest's commits field became visible: the graph
	// field list names it in the line it shares with outputs and verify, and the
	// topic list gains "commits". The usage was already 99 of its 100 permitted
	// lines, so the field's contract is in the "commits" help topic and not one
	// word of it is here.
	//
	// Updated again when the authoring discipline became part of the product: a
	// campaign's declared tasks are its only fan-out, each one is a separate
	// Steward-scheduled T3 session, and a task prompt may not use native
	// subagents in their place. The block states the rule and names the new
	// "authoring" topic, which carries the rest. The three lines it costs were
	// paid for by tightening the --allow-unverified and class paragraphs and by
	// putting both worked examples on one line, so the cap is unchanged.
	//
	// Updated again when campaign supervision arrived: the block names the verb
	// family, the run positional and the three fields every mutating verb
	// needs, and points at "t3-steward campaign supervision --help" for the
	// per-verb flags and the refusal classes. Four lines is what the cap can
	// pay for, so the contract itself is in that help and not here. The four
	// lines were paid for by putting show beside graph and the two optional
	// submit flag groups on one line, by reflowing the --allow-unverified
	// paragraph, and by dropping "Lifecycle JSON is unchanged" from the exit-code
	// paragraph, which the lifecycle heading three lines up already says.
	//
	// Updated again when a task with no route at all became a permanent
	// refusal: the permanent-reason paragraph names it, because a campaign
	// whose routes are missing is refused at submit and the reason has to be
	// readable where the exit code is explained. It was paid for by reflowing
	// the paragraph to the full width and by shortening "a missing credential
	// reference" and "Codes and recovery commands", so the cap is unchanged.
	//
	// Updated again when cancel gained its run form: the lifecycle line now
	// reads "cancel <run>[/<task>]" and says that naming no task cancels the
	// whole run in one command. It cost no line at all.
	//
	// Updated again to say what the run form's --json document means: its
	// willCancel key is the tasks the one command covers, computed from a read,
	// and not the outcome the coordinator has applied. An automated caller that
	// reads it as the outcome is the mistake the line exists to prevent. The
	// line was paid for by putting explain beside show and graph, so the cap is
	// unchanged.
	//
	// Updated again when help became one contract: the two prose lines under
	// the supervision heading are indented four columns rather than two, so
	// that the router scan reads them as the continuation they are and not as
	// two command forms. No line was added and no wording changed.
	//
	// Updated again for the third line of the same kind, the note under the
	// cancel entry about what its --json document means. The scan read it as a
	// command form too and it passed only by accident, because its first word
	// happens to be the name of a real verb; at four columns it is the
	// continuation of the cancel entry that it always was. No line was added
	// and no wording changed, and the line is 85 columns, inside the cap below.
	const wantDigest = "ba84fe4156b9fde54c67dacdd3b9c33ddd27fc6e2cbde8d43631d7b5b72cbe2d"
	digest := sha256.Sum256([]byte(campaignUsage))
	if got := hex.EncodeToString(digest[:]); got != wantDigest {
		t.Fatalf("usage digest = %s, want %s: re-read the help contract, then update this digest", got, wantDigest)
	}
	for _, want := range []string{
		"Usage: t3-steward campaign <command> [args]",
		"facade",
		"validate <directory|workflow.yaml> [--json]",
		"plan     <directory|workflow.yaml> [--json|--dot]",
		"submit   <directory|workflow.yaml> --idempotency-key KEY [--json]",
		"cancel <run>[/<task>] --reason TEXT",
		"no task = whole run",
		"willCancel, the tasks it covers, not the outcome it applied",
		"explain <run>/<task> [--json]",
		"workflow.yaml",
		"version: 2",
		"needs",
		"inputs_from",
		"outputs",
		"verify",
		"plan is static and explain is dynamic",
		"surplus",
		"required",
		"placement.hosts",
		"same --idempotency-key with the same directory returns the",
		"t3-steward campaign show <run>",
		"Exit codes",
		"schemaVersion",
		"docs/examples/campaign/single-lead",
		"docs/examples/campaign/three-node",
		"rerun    <run> --from TASK --idempotency-key KEY [--reason TEXT] [--json]",
		"creates a second run and never changes the first",
		"[--notify-thread <current|id>]",
		"static-versus-dynamic, plan, graph, commits, rerun, notify.",
		"commits (a Git commit a successor needs)",
		"Multi-task work is a static DAG",
		"is its own Steward-scheduled T3 session",
		"must not use native",
		"subagents in place of declared tasks",
		"Help topics: authoring, readiness,",
	} {
		if !strings.Contains(campaignUsage, want) {
			t.Fatalf("usage no longer covers %q", want)
		}
	}
	for index, line := range strings.Split(strings.TrimSuffix(campaignUsage, "\n"), "\n") {
		if len(line) > 88 {
			t.Fatalf("usage line %d is %d columns wide: %q", index+1, len(line), line)
		}
	}
	if lines := strings.Count(campaignUsage, "\n"); lines > 100 {
		t.Fatalf("usage is %d lines; it has to stay short enough to enter agent context", lines)
	}
}

// cmdCampaign is the entry point the command tree calls. Help must work before
// any configuration is loaded, so that a machine without a coordinator can
// still read it.
func TestCmdCampaignPrintsUsageWithoutConfiguration(t *testing.T) {
	output := captureStdout(t, func() {
		if err := cmdCampaign(globalFlags{configPath: filepath.Join(t.TempDir(), "absent.yaml")}, []string{"--help"}); err != nil {
			t.Fatal(err)
		}
	})
	if output != campaignUsage {
		t.Fatalf("cmdCampaign printed %q", output)
	}
}

func captureStdout(t *testing.T, body func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = write
	defer func() { os.Stdout = stdout }()
	body()
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	captured, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	return string(captured)
}
