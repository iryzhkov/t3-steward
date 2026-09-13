package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"os"
	"slices"
	"time"
)

func coordinatorEnrollmentHandler(settings config.BacklogV2, store *sqlite.Store, epoch int64, artifacts backlog.CoordinatorArtifactStore) backlogadmin.WorkerEnrollmentHandler {
	return func(ctx context.Context, p backlogadmin.Principal, r domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
		if prior, found, err := store.WorkerEnrollmentReplay(ctx, r, p.ID); err != nil || found {
			return prior, err
		}
		worker, ok := settings.Workers[r.WorkerID]
		if !ok || worker.Connection == "" || !worker.AcceptBacklog {
			return domain.WorkerEnrollment{}, errors.New("worker needs a configured persistent connection")
		}
		binding, err := workerruntime.BuildWorkerBinding(settings, r.WorkerID, time.Now())
		if err != nil {
			return domain.WorkerEnrollment{}, err
		}
		if r.CatalogRevision != binding.CatalogRevision {
			return domain.WorkerEnrollment{}, errors.New("expected catalog digest differs from effective configuration")
		}
		id, err := newCoordinatorWorkerSessionID(settings.Coordinator.ID, r.WorkerID)
		if err != nil {
			return domain.WorkerEnrollment{}, err
		}
		session, err := newCoordinatorWorkerSession(ctx, settings, store, r.WorkerID, epoch, id, workerruntime.ProtocolResolver{}, time.Now(), nil, artifacts)
		if err != nil {
			return domain.WorkerEnrollment{}, err
		}
		if session.Close != nil {
			defer session.Close()
		}
		snapshot, err := session.Client.Snapshot(ctx)
		if err != nil {
			return domain.WorkerEnrollment{}, err
		}
		if snapshot.Inventory.CatalogRevision != r.CatalogRevision || !snapshot.Inventory.AcceptBacklog || snapshot.Inventory.Health != domain.WorkerHealthReady {
			return domain.WorkerEnrollment{}, errors.New("worker is not ready for effective catalog")
		}
		for _, capability := range []string{"git", "huyang"} {
			if !slices.Contains(snapshot.Inventory.Capabilities, capability) {
				return domain.WorkerEnrollment{}, errors.New("worker lacks required observed capabilities")
			}
		}
		for instance, route := range worker.Providers {
			found := false
			for _, available := range snapshot.Inventory.Providers {
				if available.InstanceID != instance || !available.Available {
					continue
				}
				found = true
				for _, model := range route.Models {
					if !slices.Contains(available.Models, model) {
						found = false
					}
				}
			}
			if !found {
				return domain.WorkerEnrollment{}, errors.New("configured provider route is unavailable on worker")
			}
		}
		if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
			return domain.WorkerEnrollment{}, err
		}
		return store.CommitWorkerEnrollment(ctx, domain.WorkerEnrollment{Request: r, WorkerEpoch: worker.Epoch, CoordinatorID: settings.Coordinator.ID, CredentialRef: worker.Credential, Principal: "ssh:" + r.WorkerID, Connection: worker.Connection, Actor: p.ID, EnrolledAt: time.Now().UTC()}, snapshot)
	}
}
func cmdWorkerEnroll(g globalFlags, args []string) error {
	if len(args) < 1 {
		return errors.New("worker enroll requires worker ID")
	}
	request := domain.WorkerEnrollmentRequest{WorkerID: args[0], ExpectedRevision: -1}
	fs := flag.NewFlagSet("worker enroll", flag.ContinueOnError)
	fs.StringVar(&request.ID, "request-id", "", "stable request ID")
	fs.StringVar(&request.CatalogRevision, "catalog-revision", "", "expected catalog digest")
	fs.StringVar(&request.Reason, "reason", "", "operator reason")
	fs.Int64Var(&request.ExpectedRevision, "expected-revision", -1, "current enrollment revision, zero for first enrollment")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || request.ID == "" || request.CatalogRevision == "" || request.Reason == "" || request.ExpectedRevision < 0 {
		return errors.New("worker enroll requires request-id, catalog-revision, reason and expected-revision")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	path, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	client := backlogadmin.LocalClient{Path: path, MaxResponseBytes: cfg.BacklogV2.MessageLimits.MaxBytes, RequestTimeout: cfg.BacklogV2.Transport.RequestTimeout.D()}
	result, err := client.EnrollWorker(context.Background(), request)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
