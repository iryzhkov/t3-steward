package backlog

import (
	"log/slog"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func gateCarriedDependencyPath(c domain.CarriedInput) string {
	if c.SourceKind == domain.ArtifactGate && c.Name == "gate" {
		return "gate/report.json"
	}
	return filepath.ToSlash(c.Name)
}

func gateDependencyPath(a domain.Artifact) string {
	if a.Kind == domain.ArtifactGate && a.Name == "gate" {
		return "gate/report.json"
	}
	return filepath.ToSlash(a.Name)
}

// dispatchGate is the gate a package carries: the stored gate with each
// command's timeout bounded by the coordinator's verification.command_timeout.
//
// Submission refuses a longer timeout, but a stored task can still exceed the
// current maximum: a rerun or amendment of a task accepted under a larger
// setting, or a coordinator whose setting was lowered later. Building no
// package for it withheld the assignment on every cycle, with the reason only
// in a coordinator log. Bounded, it dispatches, and a gate that needs longer
// fails with a structured timeout naming the command.
func dispatchGate(g *domain.TaskGate, maximum time.Duration) *domain.TaskGate {
	gate := cloneTaskGate(g)
	if gate != nil && maximum > 0 && gate.Timeout > maximum {
		slog.Warn("gate timeout bounded by verification.command_timeout", "declared", gate.Timeout, "maximum", maximum)
		gate.Timeout = maximum
	}
	return gate
}

func cloneTaskGate(g *domain.TaskGate) *domain.TaskGate {
	if g == nil {
		return nil
	}
	return &domain.TaskGate{Commands: append([]string(nil), g.Commands...), Timeout: g.Timeout}
}
