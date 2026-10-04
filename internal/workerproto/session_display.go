package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const PackageCapabilitySessionDisplay = "session-display-v1"
const DisplayNameMaxRunes = 64
const DisplayNameMaxBytes = 256
const InitialSessionTitleMaxBytes = 512

// SessionDisplay is frozen descriptive coordinator metadata, never authority.
type SessionDisplay struct {
	WorkflowName string `json:"workflowName"`
	TaskName     string `json:"taskName"`
	ReviewJudge  bool   `json:"reviewJudge,omitempty"`
}

// SanitizeDisplayName removes controls/formatting and collapses whitespace,
// truncating at UTF-8 boundaries. Empty names deliberately use identity fallback.
func SanitizeDisplayName(name string) string {
	return displayText(name, DisplayNameMaxRunes, DisplayNameMaxBytes)
}

func displayText(value string, maxRunes, maxBytes int) string {
	var out strings.Builder
	count := 0
	space := false
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			space = out.Len() > 0
			continue
		}
		if r == utf8.RuneError {
			continue
		}
		extra := utf8.RuneLen(r)
		if space {
			extra++
		}
		runes := 1
		if space {
			runes++
		}
		if count+runes > maxRunes || out.Len()+extra > maxBytes {
			break
		}
		if space {
			out.WriteByte(' ')
			space = false
		}
		out.WriteRune(r)
		count += runes
	}
	return out.String()
}

func validateSessionDisplay(pkg ExecutionPackage) error {
	if pkg.Display == nil {
		return nil
	}
	for _, name := range []string{pkg.Display.WorkflowName, pkg.Display.TaskName} {
		if !utf8.ValidString(name) || len(name) > DisplayNameMaxBytes ||
			utf8.RuneCountInString(name) > DisplayNameMaxRunes || SanitizeDisplayName(name) != name {
			return errors.New("execution package: malformed or excessive session display name")
		}
	}
	if pkg.Supervision != nil && (pkg.Display.TaskName != "" || pkg.Display.ReviewJudge) {
		return errors.New("execution package: activation display cannot claim a task review role")
	}
	return nil
}

// InitialSessionTitle is deterministic across assignment/attempt retries and
// does not alter execution identity or make claims about progress.
func InitialSessionTitle(pkg ExecutionPackage) string {
	project := displayText(pkg.Environment.Project, 32, 128)
	workflow, task, role := "", "", "task"
	if pkg.Display != nil {
		workflow, task = pkg.Display.WorkflowName, pkg.Display.TaskName
		if pkg.Display.ReviewJudge {
			role = "review judge"
		}
	}
	if workflow == "" {
		workflow = pkg.Identity.WorkflowID
	}
	if task == "" {
		task = pkg.Identity.TaskID
	}
	if pkg.Supervision != nil {
		role = "supervision"
		task = pkg.Supervision.ActivationID
	}
	workflow = displayText(workflow, 32, 128)
	task = displayText(task, 32, 128)
	run := sha256.Sum256([]byte(pkg.Identity.WorkflowRunID))
	return fmt.Sprintf("[Steward] %s / %s / %s: %s / run %s / starting",
		project, workflow, role, task, hex.EncodeToString(run[:6]))
}
