package wait

import (
	"context"
	"errors"
	"fmt"
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

// AskRelayControl is the T3 surface of the relay: read a thread, open the
// relay thread with its one turn, read its question card, and archive it.
type AskRelayControl interface {
	GetThread(ctx context.Context, threadID string) (*domain.Thread, error)
	StartAskRelay(ctx context.Context, relay domain.AskRelayStart) error
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

// tickAskRelays drives the relay thread of every ask whose task this steward's
// worker runs: it opens a relay for a new ask, reads the question card of an
// open one and records the answer, and archives the relay once the ask has
// settled, whoever settled it.
//
// A relay is a convenience in front of the ask, never the ask itself: a relay
// that cannot be opened, asks a different question or is dismissed is recorded
// as failed with its reason, and the ask stays answerable from the CLI until
// its deadline.
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
	for _, ask := range work {
		if ask.Ask == nil || ask.Ask.Requires == domain.AskRequiresApprover {
			continue
		}
		r.relayAsk(ctx, store, control, ask)
	}
}

func (r *Runner) relayAsk(ctx context.Context, store AskRelayStore, control AskRelayControl, ask domain.TaskWait) {
	log := r.log.With("ask", ask.ID, "run", ask.WorkflowRunID, "task", ask.TaskID)
	worker := r.TaskWorkerID
	relay := ask.Ask.Relay
	record := func(state domain.AskRelayState, threadID, reason string) {
		relayWorker := worker
		if relay != nil && relay.WorkerID != "" {
			relayWorker = relay.WorkerID
		}
		if _, err := store.RecordAskRelay(ctx, ask.ID, domain.AskRelay{
			WorkerID: relayWorker, ThreadID: threadID, State: state, Reason: reason,
		}, r.now()); err != nil {
			log.Error("record ask relay", "state", state, "thread", threadID, "err", err)
		}
	}
	switch {
	case ask.Settled():
		if relay == nil {
			return
		}
		if !r.archiveRelay(ctx, control, relay.ThreadID, log) {
			return
		}
		record(domain.AskRelayArchived, relay.ThreadID, "")
		log.Info("ask relay thread archived", "thread", relay.ThreadID)
	case relay == nil:
		task, err := control.GetThread(ctx, ask.ThreadID)
		if err != nil || task == nil {
			// The task's thread is not on this host's T3, so this steward is
			// not the one to open its relay.
			return
		}
		// A single-host steward is not a named worker and records an empty
		// worker, which the coordinator's own store reads as the assignment's.
		threadID := domain.AskRelayThreadID(ask.ID)
		if _, err := store.RecordAskRelay(ctx, ask.ID, domain.AskRelay{
			WorkerID: worker, ThreadID: threadID, State: domain.AskRelayOpening,
		}, r.now()); err != nil {
			log.Error("record ask relay opening", "err", err)
			return
		}
		relay = &domain.AskRelay{WorkerID: worker, ThreadID: threadID, State: domain.AskRelayOpening}
		r.startRelay(ctx, control, ask, *task, threadID, log, record)
	case relay.State == domain.AskRelayOpening:
		thread, err := control.GetThread(ctx, relay.ThreadID)
		if err != nil {
			return
		}
		if thread != nil {
			record(domain.AskRelayOpen, relay.ThreadID, "")
			return
		}
		task, err := control.GetThread(ctx, ask.ThreadID)
		if err != nil || task == nil {
			return
		}
		r.startRelay(ctx, control, ask, *task, relay.ThreadID, log, record)
	case relay.State == domain.AskRelayOpen:
		r.observeRelay(ctx, store, control, ask, *relay, log, record)
	}
}

func (r *Runner) startRelay(ctx context.Context, control AskRelayControl, ask domain.TaskWait, task domain.Thread, threadID string,
	log interface{ Error(string, ...any) }, record func(domain.AskRelayState, string, string)) {
	err := control.StartAskRelay(ctx, domain.AskRelayStart{
		AskID: ask.ID, ThreadID: threadID, ProjectID: task.ProjectID,
		Title: domain.AskRelayTitle(ask), Instance: r.AskRelay.Instance, Model: r.AskRelay.Model,
		Prompt: domain.AskRelayPrompt(ask),
	})
	if err != nil {
		// The commands are derived from the ask, so the next tick repeats
		// them rather than opening a second thread. The ask stays
		// answerable from the CLI meanwhile.
		log.Error("open ask relay thread; retrying next tick", "thread", threadID, "err", err)
		return
	}
	record(domain.AskRelayOpen, threadID, "")
}

// observeRelay reads the relay thread's question card. An answer to the ask's
// own question is recorded as a T3 answer; a dismissed card, a different
// question, a turn that ended without asking or a thread that is gone fails
// the relay and archives what is left of it.
func (r *Runner) observeRelay(ctx context.Context, store AskRelayStore, control AskRelayControl, ask domain.TaskWait, relay domain.AskRelay,
	log interface {
		Error(string, ...any)
		Info(string, ...any)
		Warn(string, ...any)
	}, record func(domain.AskRelayState, string, string)) {
	fail := func(reason string) {
		log.Warn("ask relay failed; the ask is still answerable from the CLI", "thread", relay.ThreadID, "reason", reason)
		r.archiveRelay(ctx, control, relay.ThreadID, nil)
		record(domain.AskRelayFailed, relay.ThreadID, reason)
	}
	thread, err := control.GetThread(ctx, relay.ThreadID)
	if err != nil {
		return
	}
	if thread == nil || thread.ArchivedAt != nil {
		fail("the relay thread is gone")
		return
	}
	events, err := control.UserInputEvents(ctx, relay.ThreadID)
	if err != nil {
		log.Error("read ask relay question card", "thread", relay.ThreadID, "err", err)
		return
	}
	requests := map[string]domain.UserInputEvent{}
	asked := false
	for _, event := range events {
		if event.Kind == domain.UserInputRequested {
			requests[event.RequestID] = event
			asked = true
			continue
		}
		request, found := requests[event.RequestID]
		if !found {
			continue
		}
		if len(request.Questions) != 1 || !request.Questions[0].MatchesAsk(*ask.Ask) {
			fail("the relay asked a question other than the ask's")
			return
		}
		values := event.AnswerValues(request.Questions[0])
		if len(values) == 0 {
			fail("the question card was dismissed without an answer")
			return
		}
		options, freeText := ask.Ask.SplitAnswer(values)
		_, err := store.AnswerAsk(ctx, domain.AskAnswer{
			AskID: ask.ID, Options: options, FreeText: freeText,
			Source: domain.AskSourceT3, ThreadID: relay.ThreadID,
		}, askRelayPrincipal, false, r.now())
		switch {
		case err == nil:
			log.Info("ask answered in T3", "thread", relay.ThreadID)
		case errors.Is(err, domain.ErrAskAlreadyAnswered), errors.Is(err, domain.ErrAskSettled):
			// Answered from the CLI or settled by its deadline first; the
			// relay is archived on the next tick like any settled ask's.
		default:
			fail(fmt.Sprintf("the T3 answer was refused: %v", err))
		}
		return
	}
	if !asked && !thread.Running && !thread.HasPendingUserInput && thread.TurnID != "" &&
		(thread.TurnState == "completed" || thread.TurnState == "error" || thread.TurnState == "interrupted") {
		fail("the relay turn ended without asking the question")
	}
}

// archiveRelay archives a relay thread that still exists and reports whether
// it is now archived or gone.
func (r *Runner) archiveRelay(ctx context.Context, control AskRelayControl, threadID string, log interface{ Error(string, ...any) }) bool {
	thread, err := control.GetThread(ctx, threadID)
	if err != nil {
		return false
	}
	if thread == nil || thread.ArchivedAt != nil {
		return true
	}
	if err := control.ArchiveThread(ctx, threadID); err != nil {
		if log != nil {
			log.Error("archive ask relay thread", "thread", threadID, "err", err)
		}
		return false
	}
	return true
}
