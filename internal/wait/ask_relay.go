package wait

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// AskRelayStore is the coordinator surface the ask relay needs: the asks this
// steward has relay work for, the relay record, and the answer. The local
// store implements it directly; a worker reaches it over the admin transport.
type AskRelayStore interface {
	AskRelayWork(ctx context.Context, workerID string) ([]domain.TaskWait, error)
	RecordAskRelay(ctx context.Context, waitID string, relay domain.AskRelay, now time.Time) (domain.TaskWait, error)
	AnswerAsk(ctx context.Context, answer domain.AskAnswer, principal string, approver bool, now time.Time) (domain.TaskWait, error)
}

// AskRelayControl is the T3 surface of the relay: read a thread, create the
// relay thread and start its one turn as two separately checked steps, read
// its question card, and archive it.
type AskRelayControl interface {
	GetThread(ctx context.Context, threadID string) (*domain.Thread, error)
	CreateAskRelayThread(ctx context.Context, relay domain.AskRelayStart) error
	StartAskRelayTurn(ctx context.Context, relay domain.AskRelayStart) error
	UserInputEvents(ctx context.Context, threadID string) ([]domain.UserInputEvent, error)
	ArchiveThread(ctx context.Context, threadID string) error
}

// AskRelayRoute is the provider route the relay turn runs on: a Claude
// instance, because the relay works through Claude's AskUserQuestion, and an
// economy model, because the turn only asks one question.
type AskRelayRoute struct {
	Instance string
	Model    string
}

// askRelayPrincipal names the relay steward when it answers through the local
// store. Over the transport the coordinator uses the authenticated principal.
const askRelayPrincipal = "ask-relay"

// A relay that cannot be opened is given up after this many attempts by one
// steward process, or once it has been opening this long since the opening
// was recorded, whichever comes first. The elapsed bound is durable: it is
// measured from the coordinator's record, so a restarting steward does not
// retry forever.
const (
	maxAskRelayStarts  = 10
	maxAskRelayOpening = 15 * time.Minute
)

// tickAskRelays drives the relay thread of every ask whose task this steward's
// worker runs: it opens a relay for a new ask, reads the question card of an
// open one and records the answer, and archives the relay once the ask has
// settled, whoever settled it.
//
// A relay is a convenience in front of the ask, never the ask itself: a relay
// that cannot be opened, asks a different question or is dismissed is archived
// and then recorded as failed with its reason, and the ask stays answerable
// from the CLI until its deadline.
func (r *Runner) tickAskRelays(ctx context.Context) {
	if r.AskRelay == nil || r.DisableTaskWaitRuntime || r.DryRun {
		return
	}
	store := r.AskStore
	if store == nil {
		store, _ = r.TaskStore.(AskRelayStore)
	}
	if store == nil {
		store, _ = r.store.(AskRelayStore)
	}
	control, ok := r.control.(AskRelayControl)
	if store == nil || !ok {
		return
	}
	worker := ""
	if r.AssignedTaskWakesOnly {
		if r.TaskWorkerID == "" {
			return
		}
		worker = r.TaskWorkerID
	}
	work, err := store.AskRelayWork(ctx, worker)
	if err != nil {
		logFailure(ctx, r.log, "list ask relay work", err, "err", err)
		return
	}
	if r.askRelayStarts == nil {
		r.askRelayStarts = map[string]int{}
	}
	for _, ask := range work {
		if ask.Ask == nil || ask.Ask.Requires == domain.AskRequiresApprover {
			continue
		}
		(&askRelay{runner: r, store: store, control: control, ask: ask,
			log: r.log.With("ask", ask.ID, "run", ask.WorkflowRunID, "task", ask.TaskID)}).advance(ctx)
	}
}

// askRelay is one pass over one ask's relay.
type askRelay struct {
	runner  *Runner
	store   AskRelayStore
	control AskRelayControl
	ask     domain.TaskWait
	log     *slog.Logger
}

func (a *askRelay) record(state domain.AskRelayState, threadID, reason string) error {
	worker := a.runner.TaskWorkerID
	if relay := a.ask.Ask.Relay; relay != nil && relay.WorkerID != "" {
		worker = relay.WorkerID
	}
	_, err := a.store.RecordAskRelay(context.Background(), a.ask.ID, domain.AskRelay{
		WorkerID: worker, ThreadID: threadID, State: state, Reason: reason,
	}, a.runner.now())
	if err != nil {
		a.log.Error("record ask relay", "state", state, "thread", threadID, "err", err)
	}
	return err
}

// fail archives the relay thread first and records the failure only once it
// is archived, so a card that is still visible is never recorded as gone. A
// failed archive is retried on the next tick, from the same state.
func (a *askRelay) fail(ctx context.Context, threadID, reason string) {
	if !a.runner.archiveRelay(ctx, a.control, threadID, a.log) {
		a.log.Warn("ask relay failing; its thread is not archived yet, retrying next tick", "thread", threadID, "reason", reason)
		return
	}
	if a.record(domain.AskRelayFailed, threadID, reason) == nil {
		a.log.Warn("ask relay failed; the ask is still answerable from the CLI", "thread", threadID, "reason", reason)
		delete(a.runner.askRelayStarts, a.ask.ID)
	}
}

func (a *askRelay) advance(ctx context.Context) {
	relay := a.ask.Ask.Relay
	switch {
	case a.ask.Settled():
		if relay == nil {
			return
		}
		if !a.runner.archiveRelay(ctx, a.control, relay.ThreadID, a.log) {
			return
		}
		if a.record(domain.AskRelayArchived, relay.ThreadID, "") == nil {
			a.log.Info("ask relay thread archived", "thread", relay.ThreadID)
			delete(a.runner.askRelayStarts, a.ask.ID)
		}
	case relay == nil:
		task, err := a.control.GetThread(ctx, a.ask.ThreadID)
		if err != nil || task == nil {
			// The task's thread is not on this host's T3, so this steward is
			// not the one to open its relay.
			return
		}
		// A single-host steward is not a named worker and records an empty
		// worker, which the coordinator's own store reads as the assignment's.
		threadID := domain.AskRelayThreadID(a.ask.ID)
		if a.record(domain.AskRelayOpening, threadID, "") != nil {
			return
		}
		a.open(ctx, domain.AskRelay{ThreadID: threadID, State: domain.AskRelayOpening, UpdatedAt: a.runner.now()})
	case relay.State == domain.AskRelayOpening:
		a.open(ctx, *relay)
	case relay.State == domain.AskRelayOpen:
		a.observe(ctx, *relay)
	}
}

// open brings an opening relay to open. Creating the thread and starting its
// turn are checked separately: a thread that exists without a turn gets its
// turn started and is never recorded open on the strength of existing alone.
// Both commands are derived from the ask, so a retry repeats them.
func (a *askRelay) open(ctx context.Context, relay domain.AskRelay) {
	starts := a.runner.askRelayStarts[a.ask.ID]
	if starts >= maxAskRelayStarts || (!relay.UpdatedAt.IsZero() && a.runner.now().Sub(relay.UpdatedAt) > maxAskRelayOpening) {
		a.fail(ctx, relay.ThreadID, fmt.Sprintf("the relay thread could not be opened after %d attempts over %s",
			starts, a.runner.now().Sub(relay.UpdatedAt).Round(time.Second)))
		return
	}
	thread, err := a.control.GetThread(ctx, relay.ThreadID)
	if err != nil {
		return
	}
	start := domain.AskRelayStart{
		AskID: a.ask.ID, ThreadID: relay.ThreadID,
		Title: domain.AskRelayTitle(a.ask), Instance: a.runner.AskRelay.Instance, Model: a.runner.AskRelay.Model,
		Prompt: domain.AskRelayPrompt(a.ask),
	}
	if thread == nil {
		task, err := a.control.GetThread(ctx, a.ask.ThreadID)
		if err != nil || task == nil {
			return
		}
		start.ProjectID = task.ProjectID
		a.runner.askRelayStarts[a.ask.ID]++
		if err := a.control.CreateAskRelayThread(ctx, start); err != nil {
			a.log.Error("create ask relay thread; retrying next tick", "thread", relay.ThreadID, "err", err)
			return
		}
		thread = &domain.Thread{ID: relay.ThreadID, ProjectID: task.ProjectID}
	}
	if thread.TurnID == "" && !thread.Running {
		a.runner.askRelayStarts[a.ask.ID]++
		if err := a.control.StartAskRelayTurn(ctx, start); err != nil {
			a.log.Error("start the ask relay turn; retrying next tick", "thread", relay.ThreadID, "err", err)
			return
		}
	}
	if a.record(domain.AskRelayOpen, relay.ThreadID, "") == nil {
		delete(a.runner.askRelayStarts, a.ask.ID)
	}
}

// observe reads the relay thread's question card. The card is checked as soon
// as it is requested: one that is not the ask's own question is withdrawn by
// archiving before anyone can answer it. An answer to the right card is
// recorded as a T3 answer; a refusal the coordinator will repeat fails the
// relay, while any other error is retried on the next tick from the card,
// which T3 keeps.
func (a *askRelay) observe(ctx context.Context, relay domain.AskRelay) {
	thread, err := a.control.GetThread(ctx, relay.ThreadID)
	if err != nil {
		return
	}
	if thread == nil || thread.ArchivedAt != nil {
		a.fail(ctx, relay.ThreadID, "the relay thread is gone")
		return
	}
	events, err := a.control.UserInputEvents(ctx, relay.ThreadID)
	if err != nil {
		a.log.Error("read ask relay question card", "thread", relay.ThreadID, "err", err)
		return
	}
	requests := map[string]domain.UserInputEvent{}
	asked := false
	for _, event := range events {
		if event.Kind == domain.UserInputRequested {
			if len(event.Questions) != 1 || !event.Questions[0].MatchesAsk(*a.ask.Ask) {
				a.fail(ctx, relay.ThreadID, "the relay asked a question other than the ask's")
				return
			}
			requests[event.RequestID] = event
			asked = true
			continue
		}
		request, found := requests[event.RequestID]
		if !found {
			continue
		}
		values := event.AnswerValues(request.Questions[0])
		if len(values) == 0 {
			a.fail(ctx, relay.ThreadID, "the question card was dismissed without an answer")
			return
		}
		options, freeText := a.ask.Ask.SplitAnswer(values)
		_, err := a.store.AnswerAsk(ctx, domain.AskAnswer{
			AskID: a.ask.ID, Options: options, FreeText: freeText,
			Source: domain.AskSourceT3, ThreadID: relay.ThreadID,
		}, askRelayPrincipal, false, a.runner.now())
		switch {
		case err == nil:
			a.log.Info("ask answered in T3", "thread", relay.ThreadID)
		case errors.Is(err, domain.ErrAskAlreadyAnswered), errors.Is(err, domain.ErrAskSettled):
			// Answered from the CLI or settled by its deadline first; the
			// relay is archived on the next tick like any settled ask's.
		case domain.IsAskAnswerRefusal(err):
			a.fail(ctx, relay.ThreadID, fmt.Sprintf("the T3 answer was refused: %v", err))
		default:
			a.log.Warn("record the T3 answer; retrying next tick", "thread", relay.ThreadID, "err", err)
		}
		return
	}
	if !asked && !thread.Running && !thread.HasPendingUserInput && thread.TurnID != "" &&
		(thread.TurnState == "completed" || thread.TurnState == "error" || thread.TurnState == "interrupted") {
		a.fail(ctx, relay.ThreadID, "the relay turn ended without asking the question")
	}
}

// archiveRelay archives a relay thread that still exists and reports whether
// it is now archived or gone.
func (r *Runner) archiveRelay(ctx context.Context, control AskRelayControl, threadID string, log *slog.Logger) bool {
	thread, err := control.GetThread(ctx, threadID)
	if err != nil {
		return false
	}
	if thread == nil || thread.ArchivedAt != nil {
		return true
	}
	if err := control.ArchiveThread(ctx, threadID); err != nil {
		log.Error("archive ask relay thread", "thread", threadID, "err", err)
		return false
	}
	return true
}
