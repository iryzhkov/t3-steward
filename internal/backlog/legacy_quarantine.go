package backlog

import (
	"crypto/sha256"
	"fmt"
)

// LegacySubmissionKey identifies retained historical quarantine records.
// It neither reads Markdown nor submits work; operator diagnostics use this key.
func LegacySubmissionKey(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return fmt.Sprintf("legacy-%x", sum[:])
}
