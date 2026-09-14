package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// failingAdminReader fails the store read every query begins with, which is the
// ordinary way the coordinator loses the ability to answer its own question.
type failingAdminReader struct{ err error }

func (r failingAdminReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return sqlite.CoordinatorRecords{}, r.err
}

func (r failingAdminReader) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return nil, r.err
}

func (r failingAdminReader) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, r.err
}

// probeViabilitySettings is the catalog probeReadinessService configures, so a
// coordinator built by hand answers about the same project.
func probeViabilitySettings(observer backlogadmin.RepositoryObserver) backlogadmin.ViabilitySettings {
	return backlogadmin.ViabilitySettings{
		Projects: []backlog.ProjectDefinition{{
			Name: "dev-fleet", Repository: probeRepository, DefaultRef: "main",
			SetupProfile: "go", RequiredCredentials: []string{probeCredentialRef},
		}},
		SetupProfiles: []backlog.SetupProfile{{
			Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute,
		}},
		Repository: observer,
	}
}

// TestAcceptanceGateRefusesWhenItCannotValidate covers every branch that cannot
// produce a verdict. Each one must refuse, create nothing, say what failed, and
// be distinguishable from "this campaign can never run".
//
// The whole validator could previously be deleted and only one test would have
// noticed, because every internal failure accepted silently.
func TestAcceptanceGateRefusesWhenItCannotValidate(t *testing.T) {
	tests := []struct {
		name      string
		validator func(*testing.T) coordinatorPermanentValidator
		manifest  string
		want      string
	}{
		{
			name: "no readiness service",
			validator: func(*testing.T) coordinatorPermanentValidator {
				return coordinatorPermanentValidator{}
			},
			want: "no readiness service",
		},
		{
			name: "the coordinator has no configured project",
			validator: func(t *testing.T) coordinatorPermanentValidator {
				store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				service, err := backlogadmin.New(store, localAdminAuthorizer{})
				if err != nil {
					t.Fatal(err)
				}
				service.SetClock(func() time.Time { return probeNow })
				// SetViability is deliberately not called: this is a coordinator
				// that cannot answer a viability query at all.
				return coordinatorPermanentValidator{admin: service}
			},
			want: "readiness query failed",
		},
		{
			name: "the store cannot be read",
			validator: func(t *testing.T) coordinatorPermanentValidator {
				service, err := backlogadmin.New(
					failingAdminReader{err: errors.New("disk I/O error")}, localAdminAuthorizer{})
				if err != nil {
					t.Fatal(err)
				}
				service.SetClock(func() time.Time { return probeNow })
				service.SetViability(probeViabilitySettings(nil))
				return coordinatorPermanentValidator{admin: service}
			},
			want: "readiness query failed",
		},
		{
			name: "the manifest cannot be projected",
			validator: func(t *testing.T) coordinatorPermanentValidator {
				service, _ := probeReadinessService(t, nil, "homelab")
				return coordinatorPermanentValidator{admin: service}
			},
			// A manifest with no task at all projects to a plan with no task,
			// and the readiness request cannot be built from it.
			manifest: "version: 2\nname: empty\nenvironment:\n  project: dev-fleet\ntasks: {}\n",
			want:     "readiness request could not be built",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := test.manifest
			if manifest == "" {
				manifest = probeCampaignManifest
			}
			parsed, err := backlog.ParseManifest([]byte(manifest))
			if err != nil {
				t.Fatalf("the fixture manifest does not parse: %v", err)
			}
			err = test.validator(t).ValidatePermanent(context.Background(), parsed)
			if err == nil {
				t.Fatal("the acceptance gate accepted a submission it could not validate")
			}
			if !errors.Is(err, backlog.ErrValidationUnavailable) {
				t.Fatalf("error is not classified as validation unavailable: %v", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error %q does not say %q", err.Error(), test.want)
			}
			if strings.Contains(err.Error(), "can never run as written") {
				t.Fatalf("an internal fault was reported as a verdict about the campaign: %v", err)
			}
		})
	}
}

// TestSubmissionCreatesNothingWhenValidationIsUnavailable states the property
// the classification exists for: the run is not created, and the same
// idempotency key makes the retry safe once the coordinator is repaired.
func TestSubmissionCreatesNothingWhenValidationIsUnavailable(t *testing.T) {
	service, store := probeReadinessService(t, probeObserver(map[string]repositoryProbeClient{
		"homelab": reachableWorker(t, "homelab"),
	}), "homelab")
	submissions := probeSubmissions(t, service, store)
	// A validator with no readiness service stands in for every way the
	// coordinator can lose the ability to judge.
	submissions.Permanent = coordinatorPermanentValidator{}

	_, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "campaign-unavailable",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	})
	if err == nil {
		t.Fatal("a submission was accepted while validation was unavailable")
	}
	if !errors.Is(err, backlog.ErrValidationUnavailable) {
		t.Fatalf("error is not classified as validation unavailable: %v", err)
	}
	if count := probeWorkflowCount(t, store); count != 0 {
		t.Fatalf("an unvalidated submission created %d workflow(s)", count)
	}

	// Repairing the coordinator and retrying with the same key succeeds, which
	// is what makes refusing the safe choice.
	submissions.Permanent = coordinatorPermanentValidator{admin: service}
	if _, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "campaign-unavailable",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	}); err != nil {
		t.Fatalf("the retry after repair failed: %v", err)
	}
	if count := probeWorkflowCount(t, store); count != 1 {
		t.Fatalf("workflows after retry = %d, want 1", count)
	}
}
