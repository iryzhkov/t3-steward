package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ScheduleTimerStore is the durable boundary used by the recurring timer.
// CommitScheduleTrigger remains the authority for occurrence replay and overlap.
type ScheduleTimerStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	CommitScheduleTrigger(context.Context, domain.ScheduleTriggerRequest) (domain.ScheduleTriggerResult, error)
}

// ScheduleTimer observes due nominal occurrences. It does not keep an in-memory
// cursor: restart recovery derives the cursor from durable trigger records.
type ScheduleTimer struct {
	Store      ScheduleTimerStore
	CatchUpMax int
	Now        func() time.Time
}

// ScheduleTickReport contains every durable trigger decision made by one tick.
type ScheduleTickReport struct {
	ObservedAt time.Time
	Results    []domain.ScheduleTriggerResult
}

func (t ScheduleTimer) Tick(ctx context.Context) (ScheduleTickReport, error) {
	if t.Store == nil {
		return ScheduleTickReport{}, fmt.Errorf("schedule timer store is required")
	}
	if t.CatchUpMax < 1 {
		return ScheduleTickReport{}, fmt.Errorf("schedule catch-up maximum must be positive")
	}
	now := time.Now().UTC()
	if t.Now != nil {
		now = t.Now().UTC()
	}
	records, err := t.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return ScheduleTickReport{}, fmt.Errorf("load schedule timer state: %w", err)
	}
	report := ScheduleTickReport{ObservedAt: now}
	schedules := append([]domain.Schedule(nil), records.Schedules...)
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].ID < schedules[j].ID })
	for _, schedule := range schedules {
		template, ok := currentScheduleTemplate(records.ScheduleTemplates, schedule)
		if !ok {
			return ScheduleTickReport{}, fmt.Errorf("schedule %q current template %d is missing", schedule.ID, schedule.Version)
		}
		expression, err := ParseScheduleExpression(template.Expression)
		if err != nil {
			return ScheduleTickReport{}, fmt.Errorf("schedule %q expression: %w", schedule.ID, err)
		}
		location, err := time.LoadLocation(template.Timezone)
		if err != nil {
			return ScheduleTickReport{}, fmt.Errorf("schedule %q timezone %q: %w", schedule.ID, template.Timezone, err)
		}
		anchor := schedule.CreatedAt.UTC()
		for _, trigger := range records.Triggers {
			if trigger.ScheduleID == schedule.ID && trigger.NominalAt.After(anchor) {
				anchor = trigger.NominalAt.UTC()
			}
		}
		due := expression.occurrences(anchor, now, location)
		if len(due) > t.CatchUpMax {
			due = due[len(due)-t.CatchUpMax:]
		}
		currentMinute := now.Truncate(time.Minute)
		for _, nominal := range due {
			request := deterministicScheduleTrigger(schedule.ID, nominal, now)
			request.Misfired = nominal.Before(currentMinute)
			result, err := t.Store.CommitScheduleTrigger(ctx, request)
			if err != nil {
				return report, fmt.Errorf("commit schedule %q occurrence %s: %w", schedule.ID, nominal.Format(time.RFC3339), err)
			}
			report.Results = append(report.Results, result)
		}
	}
	return report, nil
}

func currentScheduleTemplate(templates []domain.ScheduleTemplate, schedule domain.Schedule) (domain.ScheduleTemplate, bool) {
	for _, template := range templates {
		if template.ScheduleID == schedule.ID && template.Version == schedule.Version {
			return template, true
		}
	}
	return domain.ScheduleTemplate{}, false
}

func deterministicScheduleTrigger(scheduleID string, nominal, observed time.Time) domain.ScheduleTriggerRequest {
	identity := scheduleID + "\x00" + nominal.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(identity))
	suffix := fmt.Sprintf("%x", sum[:16])
	return domain.ScheduleTriggerRequest{
		ScheduleID: scheduleID, TriggerID: "scheduled-trigger-" + suffix,
		WorkflowRunID: "scheduled-run-" + suffix, NominalAt: nominal.UTC(),
		ObservedAt: observed.UTC(), Source: domain.ScheduleTriggerScheduled,
	}
}

// ScheduleExpression is a parsed five-field cron expression. Numeric lists,
// ranges, steps, and wildcards are accepted. Sunday is either 0 or 7.
type ScheduleExpression struct {
	minute, hour, dayOfMonth, month, dayOfWeek cronField
}

type cronField struct {
	allowed  map[int]struct{}
	wildcard bool
}

func ParseScheduleExpression(raw string) (ScheduleExpression, error) {
	fields := strings.Fields(raw)
	if len(fields) != 5 {
		return ScheduleExpression{}, fmt.Errorf("five cron fields are required")
	}
	specs := []struct {
		name     string
		min, max int
	}{
		{"minute", 0, 59}, {"hour", 0, 23}, {"day-of-month", 1, 31},
		{"month", 1, 12}, {"day-of-week", 0, 7},
	}
	parsed := make([]cronField, len(fields))
	for index, rawField := range fields {
		field, err := parseCronField(rawField, specs[index].min, specs[index].max)
		if err != nil {
			return ScheduleExpression{}, fmt.Errorf("%s field: %w", specs[index].name, err)
		}
		if index == 4 {
			if _, ok := field.allowed[7]; ok {
				field.allowed[0] = struct{}{}
				delete(field.allowed, 7)
			}
		}
		parsed[index] = field
	}
	return ScheduleExpression{
		minute: parsed[0], hour: parsed[1], dayOfMonth: parsed[2],
		month: parsed[3], dayOfWeek: parsed[4],
	}, nil
}

func parseCronField(raw string, min, max int) (cronField, error) {
	field := cronField{allowed: make(map[int]struct{}), wildcard: raw == "*"}
	if raw == "" {
		return cronField{}, fmt.Errorf("field is empty")
	}
	for _, part := range strings.Split(raw, ",") {
		step := 1
		base := part
		if strings.Count(part, "/") > 1 {
			return cronField{}, fmt.Errorf("invalid step %q", part)
		}
		if slash := strings.IndexByte(part, '/'); slash >= 0 {
			base = part[:slash]
			value, err := strconv.Atoi(part[slash+1:])
			if err != nil || value < 1 {
				return cronField{}, fmt.Errorf("step must be a positive integer")
			}
			step = value
		}
		start, end := min, max
		switch {
		case base == "*":
		case strings.Count(base, "-") == 1:
			bounds := strings.SplitN(base, "-", 2)
			var err error
			start, err = parseCronNumber(bounds[0], min, max)
			if err != nil {
				return cronField{}, err
			}
			end, err = parseCronNumber(bounds[1], min, max)
			if err != nil {
				return cronField{}, err
			}
			if start > end {
				return cronField{}, fmt.Errorf("range start exceeds end")
			}
		case strings.Contains(base, "-"):
			return cronField{}, fmt.Errorf("invalid range %q", base)
		default:
			value, err := parseCronNumber(base, min, max)
			if err != nil {
				return cronField{}, err
			}
			start, end = value, value
			if strings.Contains(part, "/") {
				end = max
			}
		}
		for value := start; value <= end; value += step {
			field.allowed[value] = struct{}{}
		}
	}
	if len(field.allowed) == 0 {
		return cronField{}, fmt.Errorf("field selects no values")
	}
	return field, nil
}

func parseCronNumber(raw string, min, max int) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("value %q must be between %d and %d", raw, min, max)
	}
	return value, nil
}

func (e ScheduleExpression) occurrences(after, through time.Time, location *time.Location) []time.Time {
	if location == nil || !after.Before(through) {
		return nil
	}
	cursor := after.UTC().Truncate(time.Minute).Add(time.Minute)
	end := through.UTC().Truncate(time.Minute)
	var result []time.Time
	for !cursor.After(end) {
		if e.matches(cursor.In(location)) {
			result = append(result, cursor)
		}
		cursor = cursor.Add(time.Minute)
	}
	return result
}

func (e ScheduleExpression) matches(local time.Time) bool {
	if !cronContains(e.minute, local.Minute()) || !cronContains(e.hour, local.Hour()) ||
		!cronContains(e.month, int(local.Month())) {
		return false
	}
	dom := cronContains(e.dayOfMonth, local.Day())
	dow := cronContains(e.dayOfWeek, int(local.Weekday()))
	switch {
	case e.dayOfMonth.wildcard && e.dayOfWeek.wildcard:
		return true
	case e.dayOfMonth.wildcard:
		return dow
	case e.dayOfWeek.wildcard:
		return dom
	default:
		return dom || dow
	}
}

func cronContains(field cronField, value int) bool {
	_, ok := field.allowed[value]
	return ok
}
