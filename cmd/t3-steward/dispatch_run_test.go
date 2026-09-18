package main

import (
	"errors"
	"testing"
)

// withRecordedWatchdog replaces the dispatcher's route to the watchdog with a
// recorder for one test. Nothing starts: the point is to see where the
// dispatcher goes, not what the watchdog does.
func withRecordedWatchdog(t *testing.T) *[]globalFlags {
	t.Helper()
	previous := cmdRunWatchdog
	var reached []globalFlags
	cmdRunWatchdog = func(g globalFlags) error {
		reached = append(reached, g)
		return nil
	}
	t.Cleanup(func() { cmdRunWatchdog = previous })
	return &reached
}

// The naming decision this stage rests on is that "run" keeps meaning the
// watchdog: the packaged unit invokes "t3-steward run --config <path>", and
// the single-task start is the two-word "task run". Nothing in the suite
// asserted it, and the only proof offered was executing the built binary,
// which this test replaces.
func TestBareRunStillReachesTheWatchdog(t *testing.T) {
	reached := withRecordedWatchdog(t)
	if err := dispatch([]string{"run"}); err != nil {
		t.Fatalf("dispatching \"run\": %v", err)
	}
	if len(*reached) != 1 {
		t.Fatalf("the watchdog was reached %d time(s), want once", len(*reached))
	}
}

// The unit's own command line, with the flag it passes, reaches the same place
// and carries the configuration path it named.
func TestTheUnitsRunCommandReachesTheWatchdogWithItsConfiguration(t *testing.T) {
	reached := withRecordedWatchdog(t)
	if err := dispatch([]string{"run", "--config", "/etc/t3-steward/config.yaml"}); err != nil {
		t.Fatalf("dispatching the unit's command line: %v", err)
	}
	if len(*reached) != 1 {
		t.Fatalf("the watchdog was reached %d time(s), want once", len(*reached))
	}
	if got := (*reached)[0]; got.configPath != "/etc/t3-steward/config.yaml" || !got.configExplicit {
		t.Fatalf("the watchdog was reached with %+v", got)
	}
}

// "run" is no member of the task family, so the single-task start cannot take
// the watchdog's route and the watchdog cannot be started by a task command.
// task run is reached as two words or not at all.
func TestRunIsNotAMemberOfTheTaskFamily(t *testing.T) {
	reached := withRecordedWatchdog(t)
	// "task run --help" prints the start's contract and returns, without a
	// coordinator, a configuration or a watchdog.
	if err := dispatch([]string{"task", "run", "--help"}); err != nil {
		t.Fatalf("dispatching \"task run --help\": %v", err)
	}
	if len(*reached) != 0 {
		t.Fatalf("a task command started the watchdog: %+v", *reached)
	}
	// And the family refuses a word it does not have, rather than falling
	// through to a top-level command of the same name.
	if err := dispatch([]string{"task", "status"}); !errors.Is(err, errUnknownTaskCommand) {
		t.Fatalf("error = %v, want errUnknownTaskCommand", err)
	}
}
