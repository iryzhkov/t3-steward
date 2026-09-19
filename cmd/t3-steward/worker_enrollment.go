package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"io"
	"os"
	"slices"
	"strings"
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
		// Enrollment inspects readiness before any assignment exists, so it
		// states that nothing is parked, which is true of a worker that has
		// not been given work yet.
		snapshot, err := session.Client.Snapshot(ctx, workerproto.SnapshotRequest{ParkedReported: true})
		if err != nil {
			return domain.WorkerEnrollment{}, err
		}
		if snapshot.Inventory.CatalogRevision != r.CatalogRevision || !snapshot.Inventory.AcceptBacklog || snapshot.Inventory.Health != domain.WorkerHealthReady {
			return domain.WorkerEnrollment{}, enrollmentReadinessRefusal(r.WorkerID, r.CatalogRevision, snapshot.Inventory)
		}
		for _, capability := range enrollmentCapabilities {
			if !slices.Contains(snapshot.Inventory.Capabilities, capability) {
				return domain.WorkerEnrollment{}, enrollmentCapabilityRefusal(r.WorkerID, capability, snapshot.Inventory.Capabilities)
			}
		}
		for instance, route := range worker.Providers {
			found := false
			for _, available := range snapshot.Inventory.Providers {
				if available.InstanceID != instance || !available.Available {
					continue
				}
				// The authored policy may be a sole "*"; the observation
				// holds concrete models only, so compare through the policy.
				found = domain.ModelsObserved(route.Models, available.Models)
			}
			if !found {
				return domain.WorkerEnrollment{}, enrollmentRouteRefusal(r.WorkerID, instance, route.Models, snapshot.Inventory.Providers)
			}
		}
		if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
			return domain.WorkerEnrollment{}, err
		}
		return store.CommitWorkerEnrollment(ctx, domain.WorkerEnrollment{Request: r, WorkerEpoch: worker.Epoch, CoordinatorID: settings.Coordinator.ID, CredentialRef: worker.Credential, Principal: "ssh:" + r.WorkerID, Connection: worker.Connection, Actor: p.ID, EnrolledAt: time.Now().UTC()}, snapshot)
	}
}

// enrollmentCapabilities are the capabilities a worker must report before this
// coordinator will admit it. They are named once, here, so that the refusal
// can list the whole required set rather than the one it stopped at.
var enrollmentCapabilities = []string{"git", "huyang"}

// The three refusals enrollment makes about what a worker reported. Each names
// the worker, the fact that failed with what was observed against what is
// required, and the command to run next. The standard is the one "task run"
// already meets in this binary: a refusal lists the acceptable values instead
// of stating that the value given was not one of them. Before this they were
// three bare sentences -- "configured provider route is unavailable on worker"
// -- which named neither the route nor the remedy, and which "worker enroll
// --all" prints once per worker with nothing to tell the workers apart.

// enrollmentRetryHint is the read and the retry every enrollment refusal ends
// with, in one wording.
func enrollmentRetryHint(workerID string) string {
	return fmt.Sprintf("read the current state with \"t3-steward backlog workers --json\", and once it is fixed re-run \"t3-steward worker enroll %s --current-catalog --reason TEXT\"", workerID)
}

// enrollmentReadinessRefusal names every readiness fact that failed, not the
// first: a worker with a stale catalog and a degraded health is two fixes, and
// reporting one of them sends the operator back for the other.
func enrollmentReadinessRefusal(workerID, required string, inventory domain.WorkerInventory) error {
	var observed []string
	if inventory.CatalogRevision != required {
		observed = append(observed, fmt.Sprintf("it accepted catalog %s and this coordinator requires %s",
			firstNonEmptyText(shortCatalogDigest(inventory.CatalogRevision), "none"), shortCatalogDigest(required)))
	}
	if !inventory.AcceptBacklog {
		observed = append(observed, "it reports accept_backlog off, and enrollment admits a worker that takes backlog work")
	}
	if inventory.Health != domain.WorkerHealthReady {
		observed = append(observed, fmt.Sprintf("it reports health %q and %q is required",
			firstNonEmptyText(string(inventory.Health), "none"), domain.WorkerHealthReady))
	}
	return fmt.Errorf("worker %q is not ready for this coordinator's effective catalog: %s. The worker's own snapshot is what reports this, so restart or reload the worker so that it picks up the catalog and reports a new one; %s",
		workerID, strings.Join(observed, "; "), enrollmentRetryHint(workerID))
}

// enrollmentCapabilityRefusal names the capability that is missing, the whole
// required set and what the worker does advertise, because "lacks required
// observed capabilities" does not say which one or where the list lives.
func enrollmentCapabilityRefusal(workerID, missing string, observed []string) error {
	advertised := "it advertises none"
	if len(observed) != 0 {
		advertised = "it advertises " + strings.Join(observed, ", ")
	}
	return fmt.Errorf("worker %q does not advertise the capability %q that enrollment requires: the required set is %s, and %s. Capabilities come from the worker's own inventory, so install or enable %s on the worker host and let it report a new snapshot; %s",
		workerID, missing, strings.Join(enrollmentCapabilities, ", "), advertised, missing, enrollmentRetryHint(workerID))
}

// enrollmentRouteRefusal explains a provider route this coordinator configures
// for the worker and the worker cannot serve. It names the configured models
// against what the worker advertises for that instance, and it distinguishes
// the three ways the instance can fail -- absent, present and unavailable,
// present with other models -- because each one is fixed somewhere else.
func enrollmentRouteRefusal(workerID, instance string, configured []string, providers []domain.WorkerProviderInventory) error {
	advertised := fmt.Sprintf("the worker advertises no provider instance %q at all", instance)
	for _, available := range providers {
		if available.InstanceID != instance {
			continue
		}
		switch {
		case !available.Available:
			advertised = fmt.Sprintf("the worker has %s installed and reports it unavailable, which is not signed in or not enabled", instance)
		case len(available.Models) == 0:
			advertised = fmt.Sprintf("the worker offers %s with no model", instance)
		default:
			advertised = fmt.Sprintf("the worker offers %s with %s", instance, strings.Join(available.Models, ", "))
		}
	}
	models := "no model"
	if len(configured) != 0 {
		models = strings.Join(configured, ", ")
	}
	return fmt.Errorf("worker %q cannot serve a provider route this coordinator configures for it: backlog_v2.workers.%s.providers.%s authorizes %s, and %s. See what it offers with \"t3-steward models --instance %s\", then either fix that instance on the worker host or bring the configured models into line with it; %s",
		workerID, workerID, instance, models, advertised, instance, enrollmentRetryHint(workerID))
}

const workerEnrollUsage = `Usage: t3-steward worker enroll <worker> --current-catalog --reason TEXT [--request-id ID]
       t3-steward worker enroll --all --current-catalog --reason TEXT
       t3-steward worker enroll <worker> --request-id ID \
    --catalog-revision DIGEST --reason TEXT --expected-revision N

Admit one configured worker to this coordinator's current catalog. Mutating, and
it prints JSON on success.

It must be run on the coordinator host. Enrollment binds a worker to this
coordinator's identity, epoch and credential reference, so the remote-admin role
is refused it even over an authenticated coordinator client.

Self-serving form. --current-catalog reads the catalog digest this coordinator
requires of the worker and the worker's current enrollment revision from the
coordinator itself (the same values "t3-steward backlog workers --json" reports)
and submits the enrollment with them, so neither has to be copied by hand. With
--all it re-enrolls every configured worker whose accepted digest differs from
the required one, printing one line per worker: enrolled, already current, or
refused with the reason; any refusal fails the command after every worker was
tried. --request-id then defaults to a stable id derived from the worker id, the
first twelve characters of the required digest and the fenced revision
(enroll-<worker>-<digest12>-rev<N>). That id replays the first answer only while
no enrollment has committed: a retry after a refusal reuses it, and a retry must
pass the same --reason, because the replay compares the whole request. Once an
enrollment succeeds the coordinator advances the worker's revision, so running
the single-worker form again derives a new id and enrolls again (harmless: the
same digest at the next revision); --all skips a worker that is already current.

Fenced form. --catalog-revision and --expected-revision pin the exact digest and
revision the enrollment is fenced against; they are for a deliberate fence read
out of band and cannot be combined with --current-catalog.

Examples:
  t3-steward worker enroll homelab --current-catalog \
    --reason "re-enroll after home-assistant-config was added"
  t3-steward worker enroll --all --current-catalog --reason "catalog changed"
  t3-steward worker enroll homelab --request-id 2026-09-14-homelab \
    --catalog-revision 9f2c1a --reason "admit homelab after the rebuild" \
    --expected-revision 0

Required configuration: backlog_v2.mode=coordinator, a backlog_v2.workers entry
for the worker with a connection, an epoch and a secretref:f02-protocol/<host>
credential. Admin references (secretref:f03-admin/...) are refused for workers.

Idempotency: --request-id. Repeating it returns the first enrollment rather than
enrolling twice. --expected-revision fences a concurrent change; 0 is a first
enrollment.

Common failures. Permanent until something changes: an unknown or unconfigured
worker, a catalog digest that differs from the effective configuration, a stale
--expected-revision, or a worker whose observed capabilities or provider routes
do not match its configuration. Usually temporary: the worker being unreachable
or not yet ready.

Recovery:
  t3-steward backlog workers --json        Read the current state and revision.
  Re-run with --current-catalog, or with the same --request-id, once the
  reported reason is resolved.

--config PATH names the configuration file this coordinator host reads (default
$XDG_CONFIG_HOME/t3-steward/config.yaml). The dispatcher takes it out of the
arguments before the worker family is entered, so every worker verb accepts it.
`

func cmdWorkerEnroll(g globalFlags, args []string) error {
	return runWorkerEnroll(g, args, os.Stdout)
}

// runWorkerEnroll is cmdWorkerEnroll with its output injected, so a test can
// hold the per-worker report of --all to its wording.
func runWorkerEnroll(g globalFlags, args []string, out io.Writer) error {
	if len(args) < 1 {
		return errors.New("worker enroll requires a worker ID or --all")
	}
	if isHelp(args[0]) {
		fmt.Fprint(out, workerEnrollUsage)
		return nil
	}
	request := domain.WorkerEnrollmentRequest{ExpectedRevision: -1}
	rest := args
	if !strings.HasPrefix(args[0], "-") {
		request.WorkerID = args[0]
		rest = args[1:]
	}
	var currentCatalog, all bool
	fs := flag.NewFlagSet("worker enroll", flag.ContinueOnError)
	fs.StringVar(&request.ID, "request-id", "", "stable request ID")
	fs.StringVar(&request.CatalogRevision, "catalog-revision", "", "expected catalog digest")
	fs.StringVar(&request.Reason, "reason", "", "operator reason")
	fs.Int64Var(&request.ExpectedRevision, "expected-revision", -1, "current enrollment revision, zero for first enrollment")
	fs.BoolVar(&currentCatalog, "current-catalog", false, "read the required catalog digest and the current enrollment revision from the coordinator")
	fs.BoolVar(&all, "all", false, "with --current-catalog, re-enroll every configured worker whose accepted digest is stale")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("worker enroll takes one worker ID before its flags")
	}
	if all && !currentCatalog {
		return errors.New("--all re-enrolls every stale worker and needs --current-catalog")
	}
	if all && request.WorkerID != "" {
		return errors.New("--all takes no worker ID; --current-catalog with one worker enrolls that worker alone")
	}
	if currentCatalog && (request.CatalogRevision != "" || request.ExpectedRevision >= 0) {
		return errors.New("--current-catalog reads the catalog digest and the enrollment revision from the coordinator; --catalog-revision and --expected-revision are the fenced form and cannot be combined with it")
	}
	if all && request.ID != "" {
		return errors.New("--request-id cannot be combined with --all; each worker's id is derived from its digest and revision")
	}
	if request.Reason == "" {
		return errors.New("worker enroll requires --reason")
	}
	if !currentCatalog && (request.WorkerID == "" || request.ID == "" || request.CatalogRevision == "" || request.ExpectedRevision < 0) {
		return errors.New("worker enroll requires a worker ID and either --current-catalog or request-id, catalog-revision and expected-revision")
	}
	if currentCatalog && !all && request.WorkerID == "" {
		return errors.New("worker enroll requires a worker ID or --all with --current-catalog")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if !currentCatalog {
		result, err := transport.client.EnrollWorker(ctx, request)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	}
	// The digest and revision come from the coordinator's own workers view,
	// the query behind "backlog workers --json": the requirement row carries
	// the digest the coordinator computed over its effective configuration,
	// and the enrollment row carries the revision the next enrollment must
	// fence against. Reading them here, rather than out of printed output, is
	// what makes the command self-serving without inventing a second source.
	workers, err := queryWorkerEnrollmentState(ctx, transport, request.WorkerID)
	if err != nil {
		return err
	}
	if !all {
		worker, found := workers[request.WorkerID]
		if !found || worker.Requirement == nil {
			return fmt.Errorf("worker %q has no enrollment requirement on this coordinator (only a configured worker with a persistent connection has one), so there is no current catalog digest to enroll it to; enroll it with --catalog-revision and --expected-revision", request.WorkerID)
		}
		result, err := transport.client.EnrollWorker(ctx, currentCatalogRequest(request, worker))
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	}
	ids := make([]string, 0, len(workers))
	for id, worker := range workers {
		if worker.Requirement != nil {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) == 0 {
		return errors.New("this coordinator has no configured worker with an enrollment requirement")
	}
	var refused []string
	for _, id := range ids {
		worker := workers[id]
		digest := worker.Requirement.CatalogRevision
		if worker.Enrolled {
			fmt.Fprintf(out, "%s: already current (catalog %s, revision %d)\n", id, shortCatalogDigest(digest), worker.Enrollment.Revision)
			continue
		}
		result, err := transport.client.EnrollWorker(ctx, currentCatalogRequest(request, worker))
		if err != nil {
			fmt.Fprintf(out, "%s: refused: %v\n", id, err)
			refused = append(refused, id)
			continue
		}
		fmt.Fprintf(out, "%s: enrolled (catalog %s, revision %d)\n", id, shortCatalogDigest(result.Request.CatalogRevision), result.Revision)
	}
	if len(refused) != 0 {
		return fmt.Errorf("%d of %d workers refused enrollment: %s", len(refused), len(ids), strings.Join(refused, ", "))
	}
	return nil
}

// queryWorkerEnrollmentState runs the workers query and indexes its answer by
// worker id. An empty workerID asks for every worker.
func queryWorkerEnrollmentState(ctx context.Context, transport coordinatorTransport, workerID string) (map[string]backlogadmin.Worker, error) {
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkers, Principal: transport.principal,
		Filter: backlogadmin.Filter{WorkerID: workerID},
	})
	if err != nil {
		return nil, err
	}
	workers := make(map[string]backlogadmin.Worker, len(response.Workers))
	for _, worker := range response.Workers {
		id := worker.Snapshot.WorkerID
		if worker.Requirement != nil {
			id = worker.Requirement.WorkerID
		}
		workers[id] = worker
	}
	return workers, nil
}

// currentCatalogRequest completes an enrollment request from the coordinator's
// view of one worker: the required digest, the revision the enrollment must
// fence against (0 for a worker that never enrolled), and a derived request id
// when the operator gave none.
func currentCatalogRequest(request domain.WorkerEnrollmentRequest, worker backlogadmin.Worker) domain.WorkerEnrollmentRequest {
	request.WorkerID = worker.Requirement.WorkerID
	request.CatalogRevision = worker.Requirement.CatalogRevision
	request.ExpectedRevision = 0
	if worker.Enrollment != nil {
		request.ExpectedRevision = worker.Enrollment.Revision
	}
	if request.ID == "" {
		request.ID = fmt.Sprintf("enroll-%s-%s-rev%d", request.WorkerID, shortCatalogDigest(request.CatalogRevision), request.ExpectedRevision)
	}
	return request
}

func shortCatalogDigest(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}
