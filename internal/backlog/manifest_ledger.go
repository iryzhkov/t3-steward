package backlog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ManifestLedger opts a campaign into the milestone ledger the coordinator
// writes into Jocasta: a document at <jocasta_project>/handoffs/<run id>.md,
// created at submission and appended at every task terminal boundary and when
// the run ends. Only jocasta_project is required.
type ManifestLedger struct {
	JocastaProject string   `yaml:"jocasta_project"`
	Plan           string   `yaml:"plan"`
	Risk           string   `yaml:"risk"`
	Acceptance     []string `yaml:"acceptance"`
}

const (
	maxLedgerPlanBytes      = 512
	maxLedgerCriteria       = 64
	maxLedgerCriterionBytes = 1024
)

// ledgerProjectPattern is a single Jocasta path segment: the ledger path is
// built from it and must not be able to name another project's directory.
var ledgerProjectPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func validateManifestLedger(ledger *ManifestLedger) error {
	if ledger == nil {
		return nil
	}
	if ledger.JocastaProject == "" {
		return errors.New("ledger.jocasta_project is required when ledger is declared")
	}
	if !ledgerProjectPattern.MatchString(ledger.JocastaProject) || strings.Contains(ledger.JocastaProject, "..") {
		return fmt.Errorf("ledger.jocasta_project %q must be one lowercase Jocasta project name (letters, digits, '.', '_' or '-', at most 64 bytes)", ledger.JocastaProject)
	}
	if err := validateLedgerLine("ledger.plan", ledger.Plan, maxLedgerPlanBytes); err != nil {
		return err
	}
	switch ledger.Risk {
	case "", "low", "medium", "high":
	default:
		return fmt.Errorf("ledger.risk %q must be low, medium or high", ledger.Risk)
	}
	if len(ledger.Acceptance) > maxLedgerCriteria {
		return fmt.Errorf("ledger.acceptance lists %d criteria, above the limit of %d", len(ledger.Acceptance), maxLedgerCriteria)
	}
	for n, criterion := range ledger.Acceptance {
		name := fmt.Sprintf("ledger.acceptance[%d]", n)
		if strings.TrimSpace(criterion) == "" {
			return fmt.Errorf("%s is empty", name)
		}
		if err := validateLedgerLine(name, criterion, maxLedgerCriterionBytes); err != nil {
			return err
		}
	}
	return nil
}

// validateLedgerLine refuses text that could not be rendered as one line of
// the ledger header.
func validateLedgerLine(name, value string, limit int) error {
	if len(value) > limit {
		return fmt.Errorf("%s is %d bytes, above the limit of %d", name, len(value), limit)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s must be a single line without control characters", name)
	}
	return nil
}

// workflowLedger is the opt-in as the coordinator stores it on the workflow.
func (l *ManifestLedger) workflowLedger() *domain.WorkflowLedger {
	if l == nil {
		return nil
	}
	return &domain.WorkflowLedger{
		JocastaProject: l.JocastaProject,
		Plan:           strings.TrimSpace(l.Plan),
		Risk:           l.Risk,
		Acceptance:     append([]string(nil), l.Acceptance...),
	}
}
