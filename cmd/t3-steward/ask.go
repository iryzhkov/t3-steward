package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const askUsage = `Usage:
  t3-steward ask "QUESTION" --option TEXT --option TEXT [--option TEXT]... [flags]
  t3-steward ask answer ASK-ID (--option TEXT... | --text TEXT)

Ask the owner a question from inside a steward task, and park the task until
the answer arrives. It is the one way a task asks for a decision. Outside a
task it is refused: an interactive session asks with its own question tool.

The question appears in the owner's T3 thread list as a short relay thread,
"Awaiting Input", on the worker running the task. The owner answers there,
or from any host with "t3-steward ask answer". The task resumes in the same
thread with the answer as its next message and as ask-answer.json
(ask-answer/v1) at the root of its workspace. After "t3-steward ask" prints
that the task is parked, end the turn.

Flags:
  --option TEXT        a choice, 2 to 4 of them, one line of at most 120
                       bytes each; the owner can always type free text instead
  --multi              allow more than one option to be chosen
  --context FILE       supporting text shown with the question (at most 16 KiB)
  --deadline DURATION  how long to wait for an answer; then --default applies
                       or, with --on-deadline fail, the task is told to fail.
                       Without --deadline the ask waits 168h and then fails.
  --default OPTION     the option (repeatable with --multi) used at the deadline
  --on-deadline fail   at the deadline, resume the task with "no answer"; it
                       must then end failed. Needs --deadline.
  --requires approver  only a signed approver answer is accepted, from the
                       CLI; an answer given in T3 is refused. It takes no
                       --default: unanswered, it times out and fails
  --request-id ID      stable registration id (default derived from the
                       attempt and the question, so a retry is safe)
  --json               print the registered ask as JSON
  --config PATH        configuration file; a separate word

"ask answer" flags:
  --option TEXT        the chosen option (repeatable for a --multi ask)
  --text TEXT          free text, with or without options
  --json               print the answered ask as JSON

The owner is notified (needs-input) when the ask is registered and again at
half its deadline; "t3-steward triage" lists every open ask with its
ready-to-run answer command.

On a worker that cannot reach the coordinator (no coordinator client) the
ask is refused with ask-relay-unavailable: the task is not parked and no
question was sent. Decide with the brief's safe default or end the task
failed naming that reason. A campaign whose tasks may ask declares
placement.requires: [ask-relay-v1] so they run only on workers that relay
asks; "campaign check" names a fleet without one as capability-missing.

Exit codes:
  0  registered (the task is parked) or answered
  1  refused: outside a task, a bad option, an ask that already settled or
     needs the approver, or the coordinator refused it
  3  ask-relay-unavailable: this worker has no route to the coordinator
`

// askSpec is a parsed `t3-steward ask`.
type askSpec struct {
	Request   domain.AskRequest
	Deadline  time.Duration
	RequestID string
	JSON      bool
}

type repeatedText []string

func (r *repeatedText) String() string        { return strings.Join(*r, ",") }
func (r *repeatedText) Set(text string) error { *r = append(*r, text); return nil }

// parseAskArgs reads the question and its flags. The question is the one
// positional argument and may stand before, between or after the flags.
func parseAskArgs(args []string) (askSpec, error) {
	var spec askSpec
	var options, defaults repeatedText
	var onDeadline, requires, contextFile string
	fs := flag.NewFlagSet("ask", flag.ContinueOnError)
	fs.SetOutput(discardWriter{})
	fs.Var(&options, "option", "a choice")
	fs.BoolVar(&spec.Request.Multi, "multi", false, "allow several options")
	fs.StringVar(&contextFile, "context", "", "supporting text file")
	fs.DurationVar(&spec.Deadline, "deadline", 0, "how long to wait")
	fs.Var(&defaults, "default", "option used at the deadline")
	fs.StringVar(&onDeadline, "on-deadline", "", "fail")
	fs.StringVar(&requires, "requires", "", "approver")
	fs.StringVar(&spec.RequestID, "request-id", "", "stable registration id")
	fs.BoolVar(&spec.JSON, "json", false, "print JSON")
	var positional []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return spec, err
		}
		if fs.NArg() == 0 {
			break
		}
		positional = append(positional, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if len(positional) != 1 {
		return spec, fmt.Errorf("ask takes exactly one question, quoted as one argument; got %d arguments", len(positional))
	}
	spec.Request.Question = strings.TrimSpace(positional[0])
	spec.Request.Options = options
	spec.Request.Default = defaults
	spec.Request.Requires = domain.AskRequires(requires)
	if onDeadline != "" && onDeadline != string(domain.AskDeadlineFail) {
		return spec, fmt.Errorf("--on-deadline %q: the only value is fail; to resume with an answer, give --default", onDeadline)
	}
	switch {
	case spec.Deadline < 0 || spec.Deadline > domain.MaxTaskWaitDuration:
		return spec, fmt.Errorf("--deadline must be above zero and at most %s", domain.MaxTaskWaitDuration)
	case spec.Deadline == 0 && (len(defaults) != 0 || onDeadline != ""):
		return spec, errors.New("--default and --on-deadline need --deadline")
	case spec.Deadline == 0:
		spec.Deadline = domain.AskDefaultMaxWaiting
		spec.Request.OnDeadline = domain.AskDeadlineFail
	case len(defaults) != 0 && onDeadline != "":
		return spec, errors.New("--default and --on-deadline fail exclude each other")
	case len(defaults) != 0:
		spec.Request.OnDeadline = domain.AskDeadlineDefault
	case onDeadline != "":
		spec.Request.OnDeadline = domain.AskDeadlineFail
	default:
		return spec, errors.New("--deadline needs --default OPTION or --on-deadline fail, so the deadline has an outcome")
	}
	if contextFile != "" {
		content, err := readAskContext(contextFile)
		if err != nil {
			return spec, err
		}
		spec.Request.Context, spec.Request.ContextName = content, filepath.Base(contextFile)
	}
	if err := spec.Request.Validate(); err != nil {
		return spec, err
	}
	return spec, nil
}

func readAskContext(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("--context: %w", err)
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, domain.AskMaxContextBytes+1))
	if err != nil {
		return "", fmt.Errorf("--context: %w", err)
	}
	if len(content) > domain.AskMaxContextBytes {
		return "", fmt.Errorf("--context %s is larger than %d bytes; attach a summary instead", path, domain.AskMaxContextBytes)
	}
	return strings.TrimSpace(string(content)), nil
}

// askRequestID derives the registration id of an ask: stable for a retry of
// the same question by the same attempt, and different for a second question
// asked in the same turn, which a park id alone would refuse as a changed
// replay.
func askRequestID(explicit string, identity taskIdentity, request domain.AskRequest) string {
	if explicit != "" {
		return explicit
	}
	sum := sha256.Sum256([]byte(request.Question + "\x00" + strings.Join(request.Options, "\x00")))
	return fmt.Sprintf("ask-%s-%d-%s", identity.AttemptID, identity.AttemptRevision, hex.EncodeToString(sum[:])[:12])
}

// errAskNotInsideTask names the alternative: the question tool of the session
// itself, which reaches its own user directly.
var errAskNotInsideTask = errors.New("t3-steward ask is only valid inside a t3-steward task: no injected execution identity " +
	"(.t3-steward/task.env or T3_STEWARD_ATTEMPT_ID and friends). An interactive session asks its user with its own " +
	"question tool instead (AskUserQuestion in Claude Code)")

func cmdAsk(g globalFlags, args []string) error {
	if len(args) > 0 && args[0] == "answer" {
		spec, err := parseAskAnswerArgs(args[1:])
		if err != nil {
			return err
		}
		cfg, err := loadConfig(g)
		if err != nil {
			return err
		}
		return runAskAnswer(context.Background(), cfg, spec, os.Stdout)
	}
	spec, err := parseAskArgs(args)
	if err != nil {
		return err
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		if errors.Is(err, errNotInsideTask) {
			return errAskNotInsideTask
		}
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runAsk(context.Background(), cfg, spec, identity, os.Stdout)
}

// askRelayUnavailableCode is the typed reason an ask is refused on a worker
// that cannot reach the coordinator. It is the stable first word of the error,
// so the agent, its handoff and the parent all name the same thing.
const askRelayUnavailableCode = "ask-relay-unavailable"

// askCoordinatorReach answers whether this host can carry an ask to the
// coordinator. Tests replace it.
var askCoordinatorReach = hostCoordinatorReach

// errAskRelayUnavailable is the typed block for an ask on a worker without a
// coordinator client. Without it the ask failed with a bare transport error
// that read like a broken installation, and an agent could carry on as though
// it had asked. The block says the task is not parked, what to do instead, and
// how a campaign keeps such tasks off such workers.
func errAskRelayUnavailable(reach error) error {
	return &backlogadmin.TransportError{
		Class: backlogadmin.ClassClientConfiguration,
		Err: fmt.Errorf("%s: this task is NOT parked and no question was sent, because the worker running it "+
			"cannot reach the coordinator (%v). Do not wait for an answer: decide with the safe default your brief "+
			"gives, or end the task failed naming %s, and record which in your handoff. A campaign whose tasks may ask "+
			"declares placement.requires: [%s], so they run only on workers that relay asks",
			askRelayUnavailableCode, reach, askRelayUnavailableCode, workerproto.CapabilityAskRelay),
	}
}

// runAsk registers the ask on the coordinator, which parks the attempt in the
// same transaction, and tells the agent to end its turn.
func runAsk(ctx context.Context, cfg config.Config, spec askSpec, identity taskIdentity, out io.Writer) error {
	if reach := askCoordinatorReach(cfg); reach != nil {
		return errAskRelayUnavailable(reach)
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	request := spec.Request
	response, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{
		Action: "register-task",
		Task: &domain.TaskWaitRegistration{
			RequestID:     askRequestID(spec.RequestID, identity, request),
			WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID,
			AttemptID: identity.AttemptID, IssuedRevision: identity.AttemptRevision,
			ThreadID: identity.ThreadID, Wake: domain.WakeEach, MaxDuration: spec.Deadline,
			Kind: domain.WaitKindAsk, Ask: &request,
		},
	})
	if err != nil {
		return err
	}
	if len(response.TaskWaits) != 1 {
		return errors.New("the coordinator did not return exactly one ask")
	}
	registered := response.TaskWaits[0]
	if !registered.Live() {
		// Never tell the agent to end its turn for an ask that is not
		// holding the attempt.
		return fmt.Errorf("ask %s has already settled, so this task is not parked; ask again with a different --request-id", registered.ID)
	}
	if spec.JSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(registered); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "This task is now parked. End this turn now; the answer arrives as the next message and in ask-answer.json.")
		return nil
	}
	outcome := "then this task resumes told there was no answer, and must end failed"
	if request.OnDeadline == domain.AskDeadlineDefault {
		outcome = "then the default (" + strings.Join(request.Default, ", ") + ") applies"
	}
	fmt.Fprintf(out, "ask %s registered for attempt %s: %q, options %s; it waits up to %s, %s.\n",
		registered.ID, registered.AttemptID, request.Question, strings.Join(request.Options, " | "), spec.Deadline, outcome)
	fmt.Fprintln(out, "This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Fprintln(out, "until the steward resumes this thread with the answer, as the next message and in ask-answer.json.")
	return nil
}

// askAnswerSpec is a parsed `t3-steward ask answer`.
type askAnswerSpec struct {
	ID       string
	Options  []string
	FreeText string
	JSON     bool
}

func parseAskAnswerArgs(args []string) (askAnswerSpec, error) {
	var spec askAnswerSpec
	var options repeatedText
	fs := flag.NewFlagSet("ask answer", flag.ContinueOnError)
	fs.SetOutput(discardWriter{})
	fs.Var(&options, "option", "the chosen option")
	fs.StringVar(&spec.FreeText, "text", "", "free text")
	fs.BoolVar(&spec.JSON, "json", false, "print JSON")
	var positional []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return spec, err
		}
		if fs.NArg() == 0 {
			break
		}
		positional = append(positional, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if len(positional) != 1 {
		return spec, errors.New("ask answer takes exactly one ask id (tw-ask-...), as \"t3-steward triage\" prints it")
	}
	spec.ID, spec.Options = positional[0], options
	if len(spec.Options) == 0 && strings.TrimSpace(spec.FreeText) == "" {
		return spec, errors.New("ask answer needs --option TEXT or --text TEXT")
	}
	return spec, nil
}

func runAskAnswer(ctx context.Context, cfg config.Config, spec askAnswerSpec, out io.Writer) error {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	response, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{
		Action: backlogadmin.AskAnswerAction,
		Answer: &domain.AskAnswer{AskID: spec.ID, Options: spec.Options, FreeText: spec.FreeText, Source: domain.AskSourceCLI},
	})
	if err != nil {
		return err
	}
	if len(response.TaskWaits) != 1 || response.TaskWaits[0].AskAnswer == nil {
		return errors.New("the coordinator returned no answered ask")
	}
	answered := response.TaskWaits[0]
	if spec.JSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(answered)
	}
	fmt.Fprintf(out, "ask %s answered: %s. Task %s/%s resumes with it.\n",
		answered.ID, answered.AskAnswer.Summary(), answered.WorkflowRunID, answered.TaskID)
	return nil
}
