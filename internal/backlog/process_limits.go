package backlog

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ProcessLimits is the reservation a task's setup and verification processes
// run within: the cpu units and memory its effective demand declares. The zero
// value is an unsized task, whose processes run as they always have.
type ProcessLimits struct {
	CPUUnits float64
	MemoryMB int
}

// ProcessLimitsFor takes the limits from a task's effective demand, the demand
// committed with its assignment. A negative or non-finite size is no limit.
func ProcessLimitsFor(demand domain.ResourceDemand) ProcessLimits {
	limits := ProcessLimits{}
	if demand.CPUUnits > 0 && !math.IsInf(demand.CPUUnits, 0) {
		limits.CPUUnits = demand.CPUUnits
	}
	if demand.MemoryMB > 0 {
		limits.MemoryMB = demand.MemoryMB
	}
	return limits
}

// Jobs is the parallelism a sized task uses: its cpu units rounded up, and at
// least one. It is zero for a task without a cpu size.
func (l ProcessLimits) Jobs() int {
	if l.CPUUnits <= 0 || math.IsNaN(l.CPUUnits) {
		return 0
	}
	if l.CPUUnits >= math.MaxInt32 {
		return math.MaxInt32
	}
	return max(1, int(math.Ceil(l.CPUUnits)))
}

// environment is the parallelism a sized process is told to use. GOFLAGS is
// appended to rather than replaced, so a worker's own Go flags survive.
func (l ProcessLimits) environment(lookup func(string) (string, bool)) []string {
	jobs := l.Jobs()
	if jobs == 0 {
		return nil
	}
	n := strconv.Itoa(jobs)
	goflags := "-p=" + n
	if existing, ok := lookup("GOFLAGS"); ok && strings.TrimSpace(existing) != "" {
		goflags = strings.TrimSpace(existing) + " " + goflags
	}
	return []string{"GOMAXPROCS=" + n, "GOFLAGS=" + goflags, "MAKEFLAGS=-j" + n, "CARGO_BUILD_JOBS=" + n}
}

// properties are the systemd resource limits of a sized scope. A controller
// the user manager has not been delegated cannot enforce its property, so the
// property is left out rather than failing the command.
func (l ProcessLimits) properties(cpu, memory bool) []string {
	var properties []string
	if jobs := l.Jobs(); jobs > 0 && cpu {
		properties = append(properties, fmt.Sprintf("CPUQuota=%d%%", jobs*100))
	}
	if l.MemoryMB > 0 && memory {
		properties = append(properties, fmt.Sprintf("MemoryMax=%dM", l.MemoryMB), "MemorySwapMax=0")
	}
	return properties
}

// scopeLimitArguments are the systemd-run arguments that apply the limits.
func (r SystemdScopeRunner) scopeLimitArguments(limits ProcessLimits) []string {
	if limits == (ProcessLimits{}) {
		return nil
	}
	lookup := r.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	controllers := r.Controllers
	if controllers == nil {
		controllers = delegatedControllers
	}
	var arguments []string
	for _, value := range limits.environment(lookup) {
		arguments = append(arguments, "--setenv="+value)
	}
	cpu, memory := controllers()
	for _, property := range limits.properties(cpu, memory) {
		arguments = append(arguments, "--property="+property)
	}
	return arguments
}

var (
	delegationOnce   sync.Once
	delegationCPU    bool
	delegationMemory bool
)

// delegatedControllers reports whether this user's systemd manager may set
// cpu and memory limits on the scopes it creates, which needs those cgroup v2
// controllers delegated to it. It is read once per process, and a host
// without them is warned about once, not on every command.
func delegatedControllers() (bool, bool) {
	delegationOnce.Do(func() {
		uid := os.Getuid()
		path := filepath.Join("/sys/fs/cgroup/user.slice", fmt.Sprintf("user-%d.slice", uid), fmt.Sprintf("user@%d.service", uid), "cgroup.controllers")
		data, err := os.ReadFile(path)
		if err == nil {
			for _, controller := range strings.Fields(string(data)) {
				switch controller {
				case "cpu":
					delegationCPU = true
				case "memory":
					delegationMemory = true
				}
			}
		}
		if !delegationCPU || !delegationMemory {
			slog.Warn("systemd user manager lacks delegated cgroup controllers; sized setup and verification run without the missing limits",
				"cpu", delegationCPU, "memory", delegationMemory, "controllers", path, "error", err)
		}
	})
	return delegationCPU, delegationMemory
}

type processLimitsKey struct{}

// WithProcessLimits carries a task's limits to the setup and verification
// processes started under ctx.
func WithProcessLimits(ctx context.Context, limits ProcessLimits) context.Context {
	return context.WithValue(ctx, processLimitsKey{}, limits)
}

// ProcessLimitsFromContext returns the limits WithProcessLimits attached, or
// none.
func ProcessLimitsFromContext(ctx context.Context) ProcessLimits {
	limits, _ := ctx.Value(processLimitsKey{}).(ProcessLimits)
	return limits
}

// ParallelismInstruction is the one line of the first-turn instructions that
// tells the agent the parallelism its own builds and tests should use. It is
// empty for a task without a cpu size.
func ParallelismInstruction(limits ProcessLimits) string {
	jobs := limits.Jobs()
	if jobs == 0 {
		return ""
	}
	return fmt.Sprintf("This task reserves %[1]d CPUs: run builds and tests with GOMAXPROCS=%[1]d, GOFLAGS=-p=%[1]d, MAKEFLAGS=-j%[1]d and CARGO_BUILD_JOBS=%[1]d (or the equivalent flags), never with more parallelism.", jobs)
}

// FirstTurnPromptWithLimits is FirstTurnPrompt with the parallelism line after
// the Steward contract for a sized task, and exactly FirstTurnPrompt otherwise.
func FirstTurnPromptWithLimits(prompt string, outputs []domain.ArtifactDeclaration, limits ProcessLimits) string {
	line := ParallelismInstruction(limits)
	if line == "" {
		return FirstTurnPrompt(prompt, outputs)
	}
	return TaskCompletionSupplement(outputs) + "\n" + line + "\n\n" + prompt
}
