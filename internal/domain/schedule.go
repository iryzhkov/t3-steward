package domain

import (
	"fmt"
	"strings"
	"time"
)

// PrepareScheduleTemplate applies documented policy defaults and validates a
// proposed immutable template version. The returned value is safe to persist;
// the input is never mutated.
func PrepareScheduleTemplate(previous *ScheduleTemplate, next ScheduleTemplate) (ScheduleTemplate, error) {
	if next.Overlap == "" {
		next.Overlap = ScheduleOverlapForbid
	}
	if next.Misfire == "" {
		next.Misfire = ScheduleMisfireSkip
	}
	if next.AfterFailure == "" {
		next.AfterFailure = ScheduleFailureNextCycle
	}
	if err := ValidateScheduleTemplate(next); err != nil {
		return ScheduleTemplate{}, err
	}
	if previous == nil {
		if next.Version != 1 {
			return ScheduleTemplate{}, fmt.Errorf("first schedule template version must be 1, got %d", next.Version)
		}
		return next, nil
	}
	if previous.ScheduleID != next.ScheduleID {
		return ScheduleTemplate{}, fmt.Errorf("schedule template cannot move from schedule %q to %q", previous.ScheduleID, next.ScheduleID)
	}
	if next.Version != previous.Version+1 {
		return ScheduleTemplate{}, fmt.Errorf("schedule template version must advance from %d to %d, got %d", previous.Version, previous.Version+1, next.Version)
	}
	return next, nil
}

// ValidateScheduleTemplate validates one normalized immutable template.
func ValidateScheduleTemplate(template ScheduleTemplate) error {
	if strings.TrimSpace(template.ScheduleID) == "" {
		return fmt.Errorf("schedule ID is required")
	}
	if template.Version < 1 {
		return fmt.Errorf("schedule template version must be positive")
	}
	if strings.TrimSpace(template.WorkflowID) == "" {
		return fmt.Errorf("schedule workflow ID is required")
	}
	if strings.TrimSpace(template.Expression) == "" {
		return fmt.Errorf("schedule expression is required")
	}
	if strings.TrimSpace(template.Timezone) == "" {
		return fmt.Errorf("schedule timezone is required")
	}
	if _, err := time.LoadLocation(template.Timezone); err != nil {
		return fmt.Errorf("load schedule timezone %q: %w", template.Timezone, err)
	}
	if template.Overlap != ScheduleOverlapForbid {
		return fmt.Errorf("unsupported schedule overlap policy %q", template.Overlap)
	}
	if template.Misfire != ScheduleMisfireSkip {
		return fmt.Errorf("unsupported schedule misfire policy %q", template.Misfire)
	}
	switch template.AfterFailure {
	case ScheduleFailureNextCycle, ScheduleFailureHold:
	default:
		return fmt.Errorf("unsupported schedule after-failure policy %q", template.AfterFailure)
	}
	if template.CreatedAt.IsZero() {
		return fmt.Errorf("schedule template creation time is required")
	}
	return nil
}
