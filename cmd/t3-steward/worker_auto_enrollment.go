package main

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const automaticEnrollmentBackoff = time.Minute

func reconcileWorkerEnrollments(ctx context.Context, settings config.BacklogV2, store *sqlite.Store, enroll backlogadmin.WorkerEnrollmentHandler, now time.Time) error {
	requirements, err := store.LoadWorkerRequirements(ctx)
	if err != nil {
		return err
	}
	enrollments, err := store.LoadWorkerEnrollments(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, requirement := range requirements {
		worker, configured := settings.Workers[requirement.WorkerID]
		if !configured || worker.Connection == "" || !worker.AcceptBacklog || requirement.Draining || requirement.Connection == "removed" {
			continue
		}
		view := backlogadmin.Worker{Requirement: &requirement}
		for _, enrollment := range enrollments {
			if enrollment.Request.WorkerID == requirement.WorkerID {
				current := enrollment
				view.Enrollment = &current
				view.Enrolled = enrollment.Request.CatalogRevision == requirement.CatalogRevision && enrollment.WorkerEpoch == requirement.WorkerEpoch && enrollment.CredentialRef == requirement.CredentialRef && enrollment.Connection == requirement.Connection
				break
			}
		}
		if view.Enrolled {
			continue
		}
		if prior := requirement.AutomaticEnrollment; prior != nil && now.Before(prior.RetryAfter) {
			continue
		}
		request := currentCatalogRequest(domain.WorkerEnrollmentRequest{Reason: "automatic enrollment during configured worker reconciliation"}, view)
		request.ID = "auto-" + request.ID
		attempt := domain.AutomaticWorkerEnrollment{AttemptedAt: now, RetryAfter: now.Add(automaticEnrollmentBackoff), Reason: request.Reason}
		// Persist the retry bound before attempting network I/O, including across reloads.
		if err := store.SaveAutomaticWorkerEnrollment(ctx, requirement.WorkerID, attempt); err != nil {
			failures = append(failures, err)
			continue
		}
		_, enrollErr := enroll(ctx, backlogadmin.Principal{ID: "automatic-worker-enrollment"}, request)
		if enrollErr != nil {
			attempt.Reason = request.Reason + ": " + enrollErr.Error()
			failures = append(failures, enrollErr)
		}
		if err := store.SaveAutomaticWorkerEnrollment(ctx, requirement.WorkerID, attempt); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
