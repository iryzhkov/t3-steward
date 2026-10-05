package main

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// coordinatorDeclaredReviewAdmission is a callable foundation for the later lifecycle.
// This entry accepts identity only and is not registered as a public task CLI.
type coordinatorDeclaredReviewAdmission struct {
	service backlog.ReviewAdmissionService
}

func newCoordinatorDeclaredReviewAdmission(settings config.BacklogV2, store *sqlite.Store) (coordinatorDeclaredReviewAdmission, error) {
	catalog, err := workerruntime.NewConfiguredAdmissionCatalog(settings)
	if err != nil {
		return coordinatorDeclaredReviewAdmission{}, err
	}
	projects, profiles := workerruntime.BuildFleetDefinitions(settings)
	projects, profiles, _ = backlog.PartitionCatalog(projects, profiles)
	projectCatalog, err := backlog.NewProjectCatalog(projects, profiles)
	if err != nil {
		return coordinatorDeclaredReviewAdmission{}, fmt.Errorf("configured review projects: %w", err)
	}
	return coordinatorDeclaredReviewAdmission{backlog.ReviewAdmissionService{Store: store, Projects: projectCatalog, Catalog: catalog, Artifacts: backlog.CoordinatorArtifactStore{Root: settings.Storage.Artifacts, SubmissionRoot: settings.Storage.Bundles, Catalog: store}}}, nil
}
func (c coordinatorDeclaredReviewAdmission) FreezeDeclared(ctx context.Context, r backlog.DeclaredAdmissionRequest) (backlog.AdmissionSnapshot, error) {
	return c.service.FreezeDeclared(ctx, r)
}
