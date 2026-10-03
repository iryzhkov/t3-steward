package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/blockingwait"
	"github.com/iryzhkov/t3-steward/internal/review"
	"io"
	"os"
	"strings"
)

const reviewResultUsage = `Usage: t3-steward review result <round> [--json] [--wait [--timeout D]]

Read a durable review round and collect its evidence under
<state>/results/reviews/<round>/. Prints the combined verdict, reviewer states,
routes, finding counts, blocking titles and paths; full reviews remain in files.
summary.json contains all validated findings sorted by severity. Failed or
invalid reviewers never count as acceptance.

Exit 0 means every review was collected and valid, including a reject verdict.
Exit 2 means a reviewer failed, timed out or produced invalid evidence.
Exit 1 means the round is pending or the wait timed out; SIGINT exits 130.
Transport errors keep their existing codes. --wait uses the shared blocking
wait with immediate queries, bounded backoff and optional positive --timeout.
Interrupting or disconnecting never cancels work. Reattach with the same round.
--config PATH selects client configuration (the dispatcher consumes it).
The review submit command is not available in this foundation release.
`

func cmdReview(g globalFlags, args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		fmt.Print(reviewResultUsage)
		return nil
	}
	if args[0] != "result" {
		return errors.New("review currently supports result only; try t3-steward review result --help")
	}
	for _, arg := range args[1:] {
		if arg == "--help" || arg == "-h" {
			fmt.Print(reviewResultUsage)
			return nil
		}
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	results, err := cfg.ResolveResultsDir()
	if err != nil {
		return err
	}
	cli := reviewResultCLI{results: results, stdout: os.Stdout}
	cli.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		transport, err := newCoordinatorTransport(cfg)
		if err != nil {
			return backlogadmin.Response{}, err
		}
		q.Version = backlogadmin.Version
		q.Principal = transport.principal
		return transport.client.Query(ctx, q)
	}
	return cli.run(context.Background(), args[1:])
}

func (c reviewResultCLI) document(ctx context.Context, round, reviewer, name string) ([]byte, error) {
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryReviewDocument, RoundID: round, ReviewerID: reviewer, ReviewDocument: name})
	if err != nil {
		return nil, err
	}
	d := response.ReviewDocument
	if d == nil || d.RoundID != round || d.ReviewerID != reviewer || d.Name != name || len(d.Content) > review.MaxDocumentBytes {
		return nil, errors.New("coordinator returned no matching bounded review document")
	}
	return d.Content, nil
}

// Keep each reviewer-controlled value on its own terminal line.
func reviewTerminalLine(value string) string {
	value = strings.NewReplacer("\n", "\\n", "\t", "\\t").Replace(value)
	return string(safeTerminalText([]byte(value)))
}
func parseReviewResultArgs(args []string) (string, bool, error) {
	id := ""
	asJSON := false
	for _, arg := range args {
		switch arg {
		case "--json":
			asJSON = true
		default:
			if strings.HasPrefix(arg, "-") || id != "" {
				return "", false, errors.New("expected review result <round> [--json] [--wait]")
			}
			id = arg
		}
	}
	if !review.IDPattern.MatchString(id) {
		return "", false, errors.New("review result requires a safe round id")
	}
	return id, asJSON, nil
}

type reviewResultCLI struct {
	results string
	stdout  io.Writer
	query   func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
}

func (c reviewResultCLI) run(ctx context.Context, args []string) error {
	clean, wait, err := blockingwait.Parse(args)
	if err != nil {
		return err
	}
	id, asJSON, err := parseReviewResultArgs(clean)
	if err != nil {
		return err
	}
	if c.query == nil {
		return errors.New("coordinator query transport is unavailable")
	}
	var round *review.Round
	probe := func(ctx context.Context) (bool, error) {
		response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryReviewRound, RoundID: id})
		if err != nil {
			return false, err
		}
		if response.ReviewRound == nil || response.ReviewRound.ID != id {
			return false, errors.New("coordinator returned no matching review round")
		}
		round = response.ReviewRound
		return round.Terminal(), nil
	}
	var waitErr error
	if wait.Enabled {
		waitErr = blockingwait.Run(ctx, wait.Timeout, probe)
		if waitErr != nil && (!errors.Is(waitErr, context.DeadlineExceeded) || round == nil) {
			return blockingWaitError(waitErr, "t3-steward review result "+id+" --wait")
		}
	} else if _, err := probe(ctx); err != nil {
		return err
	}
	for i := range round.Reviewers {
		v := &round.Reviewers[i]
		if v.ReviewAvailable && v.ReviewMD == "" {
			raw, err := c.document(ctx, id, v.ID, "review.md")
			if err != nil {
				return err
			}
			v.ReviewMD = string(raw)
		}
		if v.VerdictAvailable && len(v.VerdictJSON) == 0 {
			raw, err := c.document(ctx, id, v.ID, "verdict.json")
			if err != nil {
				return err
			}
			v.VerdictJSON = raw
		}
		if v.State == "succeeded" {
			verdict, err := review.ValidateVerdict(v.VerdictJSON, round.InputManifestDigest, v.Route)
			if err != nil {
				return fmt.Errorf("coordinator returned invalid validated verdict for %s: %w", v.ID, err)
			}
			v.Verdict = &verdict
		}
	}
	reply, err := review.WriteOutput(c.results, *round)
	if err != nil {
		return err
	}
	if asJSON {
		if err := encodeCampaignJSON(c.stdout, reply); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(c.stdout, "round %s: %s\nblocking: %d; non-blocking: %d\n", reply.Round, reply.CombinedVerdict, reply.Blocking, reply.NonBlocking)
		for _, v := range reply.Reviewers {
			verdict := v.Verdict
			if verdict == "" {
				verdict = v.State
			}
			fmt.Fprintf(c.stdout, "%s: %s (%s); blocking: %d; non-blocking: %d\n", v.ID, verdict, v.Route, v.Blocking, v.NonBlocking)
			for _, title := range v.BlockingTitles {
				fmt.Fprintln(c.stdout, "  "+reviewTerminalLine(title))
			}
			if v.Failure != "" {
				fmt.Fprintln(c.stdout, "  "+reviewTerminalLine(v.Failure))
			}
			if v.ReviewPath != "" {
				fmt.Fprintln(c.stdout, "  "+v.ReviewPath)
			}
			if v.VerdictPath != "" {
				fmt.Fprintln(c.stdout, "  "+v.VerdictPath)
			}
		}
		fmt.Fprintln(c.stdout, reply.SummaryPath)
	}
	if waitErr != nil {
		return blockingWaitError(waitErr, "t3-steward review result "+id+" --wait")
	}
	if !round.Terminal() {
		return exitCodeError{code: 1, error: errors.New("review round is pending; reattach with t3-steward review result " + id + " --wait")}
	}
	for _, v := range round.Reviewers {
		if v.State != "succeeded" || v.Verdict == nil {
			return exitCodeError{code: 2, error: errors.New("review collection failed; inspect the reviewer states and paths")}
		}
	}
	return nil
}
