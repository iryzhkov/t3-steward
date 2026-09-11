package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var (
	ErrManualScheduleRunOpen = errors.New("manual schedule run refused while an open run exists")
	ErrScheduleFailureHeld   = errors.New("schedule is held after a failed run")
)

// CommitScheduleTrigger transactionally deduplicates and records a schedule firing.
// It obtains a write reservation before inspecting active_run_id so concurrent
// firings cannot create more than one open workflow run.
func (s *Store) CommitScheduleTrigger(ctx context.Context, request domain.ScheduleTriggerRequest) (domain.ScheduleTriggerResult, error) {
	if err := validateScheduleTriggerRequest(request); err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("begin schedule trigger: %w", err)
	}
	defer tx.Rollback()
	result, err := commitScheduleTriggerTx(ctx, tx, request)
	if err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("commit schedule trigger: %w", err)
	}
	return result, nil
}

func commitScheduleTriggerTx(ctx context.Context, tx *sql.Tx, request domain.ScheduleTriggerRequest) (domain.ScheduleTriggerResult, error) {
	result, err := tx.ExecContext(ctx, "UPDATE coordinator_schedules SET revision = revision WHERE id = ?", request.ScheduleID)
	if err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("lock schedule %q: %w", request.ScheduleID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("inspect schedule %q lock: %w", request.ScheduleID, err)
	}
	if affected == 0 {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("schedule %q not found", request.ScheduleID)
	}

	occurrenceKey := request.OccurrenceKey()
	if existing, ok, err := loadTriggerByOccurrence(ctx, tx, occurrenceKey); err != nil {
		return domain.ScheduleTriggerResult{}, err
	} else if ok {
		replayed, err := replayScheduleTrigger(ctx, tx, existing)
		if err != nil {
			return domain.ScheduleTriggerResult{}, err
		}
		replayed.Replay = true
		if _, err := insertScheduleTriggerAuditEvent(ctx, tx, replayed.Trigger); err != nil {
			return domain.ScheduleTriggerResult{}, err
		}
		return replayed, nil
	}

	schedule, template, activeRun, err := loadScheduleDecisionState(ctx, tx, request.ScheduleID)
	if err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	reason := ""
	if request.Source == domain.ScheduleTriggerScheduled {
		switch {
		case !schedule.Enabled:
			reason = "schedule-disabled"
		case schedule.NextNotBefore != nil && request.NominalAt.Before(*schedule.NextNotBefore):
			reason = "admin-delayed"
		case request.Misfired && template.Misfire == domain.ScheduleMisfireSkip:
			reason = "misfire-skipped"
		case activeRun != nil && !activeRun.Progress.Terminal():
			reason = "overlap-forbidden"
		case activeRun != nil && activeRun.Progress == domain.ProgressFailed && template.AfterFailure == domain.ScheduleFailureHold:
			reason = "failure-hold"
		}
	} else if activeRun != nil && !activeRun.Progress.Terminal() {
		return domain.ScheduleTriggerResult{}, ErrManualScheduleRunOpen
	} else if activeRun != nil && activeRun.Progress == domain.ProgressFailed && template.AfterFailure == domain.ScheduleFailureHold {
		return domain.ScheduleTriggerResult{}, ErrScheduleFailureHeld
	}

	trigger := domain.Trigger{
		ID: request.TriggerID, ScheduleID: schedule.ID, ScheduleVersion: template.Version,
		NominalAt: request.NominalAt.UTC(), OccurrenceKey: occurrenceKey,
		State: domain.TriggerSuppressed, Reason: reason, ObservedAt: request.ObservedAt.UTC(),
	}
	var workflowRun *domain.WorkflowRun
	if reason == "" {
		trigger.State, trigger.WorkflowRunID = domain.TriggerAccepted, request.WorkflowRunID
		run := domain.WorkflowRun{
			ID: request.WorkflowRunID, WorkflowID: template.WorkflowID, ScheduleID: schedule.ID,
			TriggerID: trigger.ID, Progress: domain.ProgressQueued, Revision: 1,
			CreatedAt: request.ObservedAt.UTC(), UpdatedAt: request.ObservedAt.UTC(),
		}
		workflowRun = &run
	}
	if err := insertScheduleTrigger(ctx, tx, trigger); err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	if workflowRun != nil {
		if err := upsertJSON(ctx, tx, "workflow run", workflowRun.ID,
			"INSERT INTO coordinator_workflow_runs(id, workflow_id, schedule_id, progress, revision, record) VALUES (?, ?, ?, ?, ?, ?)",
			[]any{workflowRun.ID, workflowRun.WorkflowID, workflowRun.ScheduleID, workflowRun.Progress, workflowRun.Revision},
			*workflowRun,
		); err != nil {
			return domain.ScheduleTriggerResult{}, err
		}
		schedule.ActiveRunID = workflowRun.ID
		if schedule.NextNotBefore != nil && !request.NominalAt.Before(*schedule.NextNotBefore) {
			schedule.NextNotBefore = nil
		}
		schedule.Revision++
		schedule.UpdatedAt = request.ObservedAt.UTC()
		if err := upsertJSON(ctx, tx, "schedule", schedule.ID,
			"INSERT INTO coordinator_schedules(id, active_run_id, revision, current_version, record) VALUES (?, ?, ?, ?, ?) "+
				"ON CONFLICT(id) DO UPDATE SET active_run_id = excluded.active_run_id, revision = excluded.revision, "+
				"current_version = excluded.current_version, record = excluded.record",
			[]any{schedule.ID, schedule.ActiveRunID, schedule.Revision, schedule.Version},
			schedule,
		); err != nil {
			return domain.ScheduleTriggerResult{}, err
		}
	}
	if _, err := insertScheduleTriggerAuditEvent(ctx, tx, trigger); err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	return domain.ScheduleTriggerResult{Trigger: trigger, WorkflowRun: workflowRun}, nil
}

func insertScheduleTriggerAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	trigger domain.Trigger,
) (domain.AuditEvent, error) {
	detail, err := json.Marshal(trigger)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("encode schedule trigger %q audit detail: %w", trigger.ID, err)
	}
	event := domain.AuditEvent{
		ID: "schedule-trigger:" + trigger.ID, Kind: "schedule-trigger-" + string(trigger.State),
		WorkflowRunID: trigger.WorkflowRunID,
		TargetType:    domain.AuditTargetTrigger, TargetID: trigger.ID,
		Actor: "coordinator", Reason: trigger.Reason, Detail: detail, CreatedAt: trigger.ObservedAt,
	}
	return insertAuditEventTx(ctx, tx, event)
}

func validateScheduleTriggerRequest(request domain.ScheduleTriggerRequest) error {
	switch {
	case request.ScheduleID == "":
		return errors.New("schedule ID is required")
	case request.TriggerID == "":
		return errors.New("trigger ID is required")
	case request.WorkflowRunID == "":
		return errors.New("workflow run ID is required")
	case request.NominalAt.IsZero():
		return errors.New("nominal fire time is required")
	case request.ObservedAt.IsZero():
		return errors.New("observed time is required")
	case request.Source != domain.ScheduleTriggerScheduled && request.Source != domain.ScheduleTriggerManual:
		return fmt.Errorf("unsupported schedule trigger source %q", request.Source)
	case request.Source == domain.ScheduleTriggerManual && request.Misfired:
		return errors.New("manual schedule run cannot be a misfire")
	default:
		return nil
	}
}

func loadScheduleDecisionState(ctx context.Context, tx *sql.Tx, scheduleID string) (
	domain.Schedule, domain.ScheduleTemplate, *domain.WorkflowRun, error,
) {
	var scheduleRaw []byte
	var version int
	if err := tx.QueryRowContext(ctx,
		`SELECT current_version, record FROM coordinator_schedules WHERE id = ?`,
		scheduleID,
	).Scan(&version, &scheduleRaw); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil, fmt.Errorf("load schedule %q: %w", scheduleID, err)
	}
	var schedule domain.Schedule
	if err := json.Unmarshal(scheduleRaw, &schedule); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil, fmt.Errorf("decode schedule %q: %w", scheduleID, err)
	}

	var templateRaw []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_schedule_templates WHERE schedule_id = ? AND version = ?`,
		scheduleID, version,
	).Scan(&templateRaw); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil,
			fmt.Errorf("load schedule template %q/%d: %w", scheduleID, version, err)
	}
	var template domain.ScheduleTemplate
	if err := json.Unmarshal(templateRaw, &template); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil,
			fmt.Errorf("decode schedule template %q/%d: %w", scheduleID, version, err)
	}

	if schedule.ActiveRunID == "" {
		return schedule, template, nil, nil
	}
	var runRaw []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_workflow_runs WHERE id = ?`,
		schedule.ActiveRunID,
	).Scan(&runRaw); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil,
			fmt.Errorf("load active workflow run %q: %w", schedule.ActiveRunID, err)
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(runRaw, &run); err != nil {
		return domain.Schedule{}, domain.ScheduleTemplate{}, nil,
			fmt.Errorf("decode active workflow run %q: %w", schedule.ActiveRunID, err)
	}
	return schedule, template, &run, nil
}

func loadTriggerByOccurrence(ctx context.Context, tx *sql.Tx, occurrenceKey string) (domain.Trigger, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_triggers WHERE occurrence_key = ?`,
		occurrenceKey,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Trigger{}, false, nil
	}
	if err != nil {
		return domain.Trigger{}, false, fmt.Errorf("load trigger occurrence %q: %w", occurrenceKey, err)
	}
	var trigger domain.Trigger
	if err := json.Unmarshal(raw, &trigger); err != nil {
		return domain.Trigger{}, false, fmt.Errorf("decode trigger occurrence %q: %w", occurrenceKey, err)
	}
	return trigger, true, nil
}

func replayScheduleTrigger(ctx context.Context, tx *sql.Tx, trigger domain.Trigger) (domain.ScheduleTriggerResult, error) {
	result := domain.ScheduleTriggerResult{Trigger: trigger}
	if trigger.WorkflowRunID == "" {
		return result, nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_workflow_runs WHERE id = ?`,
		trigger.WorkflowRunID,
	).Scan(&raw); err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("load replayed workflow run %q: %w", trigger.WorkflowRunID, err)
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return domain.ScheduleTriggerResult{}, fmt.Errorf("decode replayed workflow run %q: %w", trigger.WorkflowRunID, err)
	}
	result.WorkflowRun = &run
	return result, nil
}

func insertScheduleTrigger(ctx context.Context, tx *sql.Tx, trigger domain.Trigger) error {
	raw, err := json.Marshal(trigger)
	if err != nil {
		return fmt.Errorf("marshal trigger %q: %w", trigger.ID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_triggers(id, schedule_id, schedule_version, occurrence_key, record)
		 VALUES (?, ?, ?, ?, ?)`,
		trigger.ID, trigger.ScheduleID, trigger.ScheduleVersion, trigger.OccurrenceKey, string(raw),
	); err != nil {
		return fmt.Errorf("save trigger %q: %w", trigger.ID, err)
	}
	return nil
}
