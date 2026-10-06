package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
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

func cloneTaskGate(g *domain.TaskGate) *domain.TaskGate {
	if g == nil {
		return nil
	}
	return &domain.TaskGate{Commands: append([]string(nil), g.Commands...), Timeout: g.Timeout}
}
