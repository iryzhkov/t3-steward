package t3api

import (
	"errors"
	"testing"
)

// Absence of the CLI is a distinct, matchable failure, so a caller can say
// which operation needed it and what else would do instead of repeating the
// lookup's own wording.
func TestFindT3BinaryReportsAbsenceAsASentinel(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	_, err := FindT3Binary("")
	if err == nil {
		t.Skip("a system-wide t3 CLI is installed; absence cannot be simulated here")
	}
	if !errors.Is(err, ErrT3CLINotFound) {
		t.Fatalf("err = %v, want ErrT3CLINotFound", err)
	}
	if _, err := FindT3Binary("/nonexistent/t3"); !errors.Is(err, ErrT3CLINotFound) {
		t.Fatalf("an explicit missing path is not reported as absence: %v", err)
	}
}
