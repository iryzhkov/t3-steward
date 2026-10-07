package providercontainment

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Limits is the share of the host a contained run may use. It is derived from
// the reservation the coordinator accounted for the attempt, so the numbers the
// scheduler counts are the numbers the kernel enforces. It is part of the
// launch spec, and therefore of the launch digest: a relaunch with different
// limits is refused rather than silently run with them.
//
// A zero field leaves that resource unlimited, as an unsized demand does.
type Limits struct {
	// CPUs is the whole number of CPUs the run may keep busy, which is also
	// the parallelism its build tools are told to use.
	CPUs     int `json:"cpus,omitempty"`
	MemoryMB int `json:"memoryMb,omitempty"`
}

// The bounds keep every derived value representable and refuse a spec that
// names an absurd limit. LimitsFor never produces anything outside them.
const (
	maxLimitCPUs     = 1024
	maxLimitMemoryMB = 1 << 24
	// limitTasksMax bounds the number of processes and threads, so a fork
	// loop is stopped by the run's own unit rather than by the host.
	limitTasksMax = 4096
)

// Observation values for a sized launch.
const (
	LimitsApplied    = "applied"
	LimitsNotApplied = "limits not applied"
)

// LimitsFor is the only place the enforced numbers are derived from a demand.
// CPU units are rounded up to whole CPUs, with at least one; memory is taken
// as accounted. A demand that sizes neither CPU nor memory has no limits, and
// the launch is exactly the unsized launch of earlier releases.
func LimitsFor(demand domain.ResourceDemand) *Limits {
	var limits Limits
	// The comparison is false for NaN, so a value that is not a number is unsized.
	if demand.CPUUnits > 0 {
		limits.CPUs = maxLimitCPUs
		if demand.CPUUnits < maxLimitCPUs {
			limits.CPUs = max(1, int(math.Ceil(demand.CPUUnits)))
		}
	}
	if demand.MemoryMB > 0 {
		limits.MemoryMB = min(demand.MemoryMB, maxLimitMemoryMB)
	}
	if limits == (Limits{}) {
		return nil
	}
	return &limits
}

// Validate refuses limits LimitsFor could not have produced. No limits at all
// is valid; an empty, non-nil value is not, because it would digest differently
// from the unsized launch it behaves like.
func (l *Limits) Validate() error {
	if l == nil {
		return nil
	}
	if *l == (Limits{}) {
		return errors.New("containment limits must size CPU or memory")
	}
	if l.CPUs < 0 || l.CPUs > maxLimitCPUs || l.MemoryMB < 0 || l.MemoryMB > maxLimitMemoryMB {
		return fmt.Errorf("containment limits out of range: %d CPUs, %d MB", l.CPUs, l.MemoryMB)
	}
	return nil
}

// Properties are the systemd unit properties that enforce the limits.
func (l *Limits) Properties() []string {
	if l == nil {
		return nil
	}
	var properties []string
	if l.CPUs > 0 {
		properties = append(properties, fmt.Sprintf("CPUQuota=%d%%", l.CPUs*100))
	}
	if l.MemoryMB > 0 {
		// Swap would let a run exceed its reservation by paging out the
		// desktop and services beside it instead of being stopped.
		properties = append(properties, fmt.Sprintf("MemoryMax=%dM", l.MemoryMB), "MemorySwapMax=0")
	}
	return append(properties, fmt.Sprintf("TasksMax=%d", limitTasksMax))
}

// Environment tells the build tools inside the sandbox how many CPUs they
// have, so they do not start one job per host CPU and thrash in the quota.
func (l *Limits) Environment() []string {
	if l == nil || l.CPUs <= 0 {
		return nil
	}
	n := strconv.Itoa(l.CPUs)
	return []string{"GOMAXPROCS=" + n, "GOFLAGS=-p=" + n, "MAKEFLAGS=-j" + n, "CARGO_BUILD_JOBS=" + n}
}

// appliedBy reports whether the unit properties systemd reports back carry the
// limits. A user manager without cgroup controller delegation accepts the
// properties and reports them as unlimited.
func (l *Limits) appliedBy(properties map[string]string) bool {
	if l.CPUs > 0 {
		if quota := properties["CPUQuotaPerSecUSec"]; quota == "" || quota == "infinity" {
			return false
		}
	}
	if l.MemoryMB > 0 && properties["MemoryMax"] != strconv.FormatInt(int64(l.MemoryMB)<<20, 10) {
		return false
	}
	return true
}

// oomFailure names why a run the kernel killed for memory ended.
func (l *Limits) oomFailure() string {
	if l == nil || l.MemoryMB <= 0 {
		return "contained run was killed for exceeding available memory"
	}
	return fmt.Sprintf("contained run exceeded its %d MB memory reservation", l.MemoryMB)
}

// maxFailureBytes bounds the recorded failure, which only this package writes.
const maxFailureBytes = 512

// recordedFailure reads the failure an earlier observation recorded in the
// supervisor journal, or nothing when none was recorded.
func recordedFailure(dir string) (string, error) {
	file, err := os.Open(filepath.Join(dir, "failure"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxFailureBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxFailureBytes {
		return "", errors.New("invalid supervisor failure record")
	}
	return string(data), nil
}

// noteOnce logs a warning the first time any supervisor, in this or a later
// worker process, observes the condition for this launch. The marker in the
// supervisor journal is what makes it once; losing it only repeats the log.
func (s Supervisor) noteOnce(dir, marker, message string, args ...any) {
	if err := writeExclusive(filepath.Join(dir, marker), nil); err != nil {
		if !errors.Is(err, os.ErrExist) {
			s.logWarning(message, append(args, "marker_error", err)...)
		}
		return
	}
	s.logWarning(message, args...)
}

func (s Supervisor) logWarning(message string, args ...any) {
	if s.warn != nil {
		s.warn(message, args...)
		return
	}
	slog.Warn(message, args...)
}
