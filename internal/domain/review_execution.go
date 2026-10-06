package domain

import (
	"fmt"
	"math"
	"regexp"
)

// ReviewExecutionProfile is normalized immutable execution authority.
// Optional demand scalars remain zero when absent; no reservation is inferred.
type ReviewExecutionProfile struct {
	Effort      string         `json:"effort"`
	QuotaPoolID string         `json:"quotaPoolId"`
	MaxTurns    int            `json:"maxTurns"`
	Resources   ResourceDemand `json:"resources"`
	// ResourcePreset is the declared preset name, carried to the member task
	// so its expected live needs survive a CPU-class override.
	ResourcePreset string `json:"resourcePreset,omitempty"`
}

var reviewPoolID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func (p ReviewExecutionProfile) Validate() error {
	if p.Effort != "medium" && p.Effort != "high" {
		return fmt.Errorf("review effort must be medium or high")
	}
	if !reviewPoolID.MatchString(p.QuotaPoolID) {
		return fmt.Errorf("review quota_pool must be a canonical bounded identifier")
	}
	if p.MaxTurns < 1 || p.MaxTurns > 32 {
		return fmt.Errorf("review max_turns must be 1..32")
	}
	if !p.Resources.MinCPUClass.Valid() || math.IsNaN(p.Resources.CPUUnits) || math.IsInf(p.Resources.CPUUnits, 0) {
		return fmt.Errorf("review resources require valid CPU floor and finite demands")
	}
	return p.Resources.Validate()
}

func CloneReviewExecution(p *ReviewExecutionProfile) *ReviewExecutionProfile {
	if p == nil {
		return nil
	}
	copied := *p
	return &copied
}

// ValidateTaskReviewExecution distinguishes historical declarations from explicit profiles.
func ValidateTaskReviewExecution(r *TaskReviewRequirements) error {
	if r == nil || (r.Version != 1 && r.Version != 2) {
		return fmt.Errorf("review_requirements version must be 1 or 2")
	}
	for _, m := range r.Members {
		if r.Version == 1 && m.Execution != nil {
			return fmt.Errorf("version 1 refuses execution")
		}
		if r.Version == 2 {
			if m.Execution == nil {
				return fmt.Errorf("version 2 requires every execution profile")
			}
			if err := m.Execution.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}
