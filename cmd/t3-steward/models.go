package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const modelsUsage = `Usage: t3-steward models [--project NAME] [--instance ID] [--available] [--json]

Every provider route the fleet can run right now, joined from three facts that
fail separately:

  authorized   the coordinator's fleet catalog binds the instance to a quota
               pool, so it may be routed to at all
  advertised   at least one worker reports the instance with its models, so
               there is somewhere to run it
  quota        the pool's admission state and the worst of its buckets, phase
               and used percent, from the observations workers report; USED
               and AGE come from one reading, the most used bucket's, and
               "stale" marks a pool any of whose readings is older than an
               hour (set backlog_v2.coordinator_client.defaults.quota_stale_after
               to change it) or taken before its window reset, whose phase
               and percent describe a window that may be over

Each row is one instance/model pair, in the form "t3-steward task run --model
[INSTANCE/]MODEL" takes. A bare model name is enough when exactly one instance
offers it.

STATUS says "available" only when every window the pool's provider declares
(Claude five_hour and seven_day; Codex primary, and secondary when it reports
one) has a reading no older than the stale threshold, no window is used up, the
pool admits work, and at least one ready worker both advertises the route's
model and is authorized for it by the coordinator. A worker may advertise
more models than it is authorized for, and those routes are not available.
Each model is judged on its own, so --available drops a model only unready or
unauthorized workers offer while keeping the instance's other models.
Otherwise it names the first reason: "not authorized", "quota unknown",
"missing <window>", "stale <window>", "exhausted <window> until <reset>",
"pool <admission>" or "no ready worker".
--json carries the same verdict for each route under routes[].availability,
and the instance's availability is available when any of its routes is, with
a stable code: available, missing-binding, not-advertised,
model-not-authorized, quota-unknown, missing-window, stale-window,
exhausted-window, pool-admission or no-ready-worker. Each worker's
authorizedModels is the coordinator's allowlist for it.

Narrowing the answer. Each of these says what to read, and all of them are
applied before anything decides how much of it to print, so a narrowed answer
is the whole of a smaller question rather than a window onto a larger one:

  --project NAME   count only the workers eligible for that project as
                   advertising; "t3-steward backlog projects" lists them
  --instance ID    report that one provider instance
  --available      drop the routes that cannot run now, which is the table
                   minus everything the reasons below explain

A narrowed table says how many routes and instances the unnarrowed answer
holds and which filter is in force, so it is never mistaken for the fleet.

An instance that is authorized and advertised by nobody, or advertised with no
quota binding, is listed with that as its status rather than omitted: a route
that cannot run is the thing the caller most needs to see. Under the table, one
line per instance and worker the coordinator authorizes for that worker and
whose route cannot run, with the reason. A worker that has the instance
installed and is signed in to it is listed there too when the coordinator
dropped that instance from the worker's catalog: the host offers the route and
the fleet authorizes no pool to charge it to, so it still cannot run. A pair
the coordinator authorizes for nothing has no line: these reasons are what the
coordinator reports about the instances it does authorize.

  missing binding   the coordinator dropped it at load: no quota pool of that
                    worker is authorized for the instance
  no models         the fleet authorizes the instance for no model
  not installed     the worker has no such provider instance
  unavailable       installed, and not signed in or not enabled

A coordinator older than this one reports no per-worker authorization, and the
table is then built from the quota pools and the inventories alone, with no
reasons under it.

--config PATH names the configuration file this host reads (default
$XDG_CONFIG_HOME/t3-steward/config.yaml). The dispatcher takes it out of the
arguments before this verb is entered, so the path is a separate word:
--config=PATH is not recognised there and travels on to this verb, which
refuses it.
` + coordinatorTransportHelp

// modelsSchemaVersion versions the models document. It is an agent-facing
// surface from its first release, so it is versioned from its first release.
const modelsSchemaVersion = 1

// modelsDocument is what "models --json" prints: one document, one entry per
// provider instance, sorted by instance id.
type modelsDocument struct {
	SchemaVersion int    `json:"schemaVersion"`
	Project       string `json:"project,omitempty"`
	// Instance and Available echo the scope this document answers, so that a
	// document holding two routes says which question it answers rather than
	// looking like a fleet with two routes.
	Instance  string `json:"instance,omitempty"`
	Available bool   `json:"available,omitempty"`
	// TotalInstances and TotalRoutes are the sizes before the scope narrowed
	// the document. They equal what is in Instances when nothing narrowed it.
	TotalInstances int              `json:"totalInstances"`
	TotalRoutes    int              `json:"totalRoutes"`
	Instances      []modelsInstance `json:"instances"`
}

// modelsScope is what the command line asked models to read: the project whose
// eligible workers count as advertising, the provider instance to report, and
// whether routes that cannot run now are wanted at all. Every field narrows
// what is read, and all of them are applied before the document is rendered or
// encoded, so no cap and no summary can ever precede the scope.
type modelsScope struct {
	Project   string
	Instance  string
	Available bool
}

// Narrowed reports whether this scope drops anything from the built document.
// --project is not one of those: it is answered by the coordinator, in the
// query that decides which workers count as advertising.
func (s modelsScope) Narrowed() bool {
	return s.Instance != "" || s.Available
}

// Words is the scope in the words the command line used, for a line that has
// to say which filter is in force.
func (s modelsScope) Words() string {
	var words []string
	if s.Instance != "" {
		words = append(words, "--instance "+s.Instance)
	}
	if s.Available {
		words = append(words, "--available")
	}
	return strings.Join(words, " and ")
}

// modelsInstance is one provider instance as the three views see it.
type modelsInstance struct {
	Windows  []modelsWindow `json:"windows,omitempty"`
	Instance string         `json:"instance"`
	// Authorized reports that the fleet catalog the coordinator loaded binds
	// this instance to a quota pool. The coordinator will not route to an
	// instance it has not authorized, whoever advertises it.
	Authorized bool `json:"authorized"`
	// Advertised reports that at least one eligible worker offers it.
	Advertised bool `json:"advertised"`
	// QuotaPool is the authorized pool, or the pool a worker advertises when
	// the catalog authorizes none.
	QuotaPool string `json:"quotaPool,omitempty"`
	Admission string `json:"admission,omitempty"`
	// Phase and Percent are the worst bucket of the pool as the workers'
	// merged observations report it. They are absent when no observation of
	// the pool has reached this coordinator.
	Phase   string   `json:"phase,omitempty"`
	Percent *float64 `json:"percent,omitempty"`
	// Percent, ResetsAt and ObservedAt are one observation: the most used
	// bucket's percent, its reset and when it was read, so the three always
	// describe a reading some worker actually took. OldestObservedAt is when
	// the oldest reading of any of the pool's buckets was taken, which is what
	// the pool's freshness rests on. Stale marks a pool one of whose buckets
	// was read longer ago than the stale threshold, or before its own reset, which
	// has since passed: its phase and percent may describe a window that is
	// over (S2: a pool read 98% draining for hours after its seven-day window
	// had reset).
	ObservedAt       *time.Time `json:"observedAt,omitempty"`
	ResetsAt         *time.Time `json:"resetsAt,omitempty"`
	OldestObservedAt *time.Time `json:"oldestObservedAt,omitempty"`
	Stale            bool       `json:"stale,omitempty"`
	// QuotaUnknown marks a pool whose governing buckets the coordinator could
	// not resolve: its state is unknown, not read from every window.
	QuotaUnknown bool `json:"quotaUnknown,omitempty"`
	// Models is every model the eligible workers advertise for the instance,
	// deduplicated and sorted.
	Models  []string       `json:"models,omitempty"`
	Workers []modelsWorker `json:"workers,omitempty"`
	// MissingBinding reports an instance no authorized quota pool holds: one a
	// worker advertises that the fleet catalog binds nowhere, or one the
	// coordinator dropped at load for want of a binding.
	MissingBinding bool `json:"missingBinding"`
	// Reason is why no eligible worker advertises this instance, in the
	// vocabulary the worker rows use, and is empty when one does or when no
	// worker reported an authorization that could explain it.
	Reason string `json:"reason,omitempty"`
	// Routes is the verdict for each model of the instance, in the order of
	// Models, or one route with no model when the instance advertises none. A
	// route is judged against the ready workers that advertise its model, so
	// two models of one instance can differ.
	Routes []modelsRoute `json:"routes,omitempty"`
	// Availability is "available" when at least one of Routes is, and
	// otherwise the first route's reason, as a stable code and the status
	// column's words.
	Availability modelsAvailability `json:"availability"`
}

// modelsRoute is one instance/model pair and whether it can run now. Model is
// empty for an instance that advertises no model.
type modelsRoute struct {
	Model        string             `json:"model"`
	Availability modelsAvailability `json:"availability"`
}

// modelsAvailability is the verdict the status column prints. Code is
// "available" or one of the reason codes: missing-binding, not-advertised,
// quota-unknown, missing-window, stale-window, exhausted-window,
// pool-admission, model-not-authorized or no-ready-worker. Window and
// ResetsAt name the window a
// window code is about.
type modelsAvailability struct {
	Code     string     `json:"code"`
	Window   string     `json:"window,omitempty"`
	ResetsAt *time.Time `json:"resetsAt,omitempty"`
	Detail   string     `json:"detail"`
}

// The availability codes that are not window codes; those are domain's.
const (
	modelsAvailabilityMissingBinding = "missing-binding"
	modelsAvailabilityNotAdvertised  = "not-advertised"
	modelsAvailabilityPoolAdmission  = "pool-admission"
	modelsAvailabilityNoReadyWorker  = "no-ready-worker"
	// modelsAvailabilityModelNotAuthorized is a route whose model no worker
	// that advertises it is authorized for.
	modelsAvailabilityModelNotAuthorized = "model-not-authorized"
)

// modelsWorker is one worker the instance is authorized for, advertised by, or
// both. The two halves fail separately, so each is reported.
type modelsWorker struct {
	Worker string `json:"worker"`
	Ready  bool   `json:"ready"`
	// Authorized reports that the coordinator's configuration names this
	// instance for this worker, whatever became of it at load. It is absent,
	// not false, when the coordinator reported no per-worker authorization at
	// all: a release older than this one answers the same query without it, and
	// false would assert as fact what that answer does not carry.
	Authorized *bool `json:"authorized,omitempty"`
	// AuthorizedModels is the coordinator's model allowlist for this instance
	// on this worker, as domain.ModelAuthorized reads it (a sole "*" is every
	// model). A worker may advertise more models than it is authorized for,
	// and a route runs only where both name its model.
	AuthorizedModels []string `json:"authorizedModels,omitempty"`
	// grants keeps each authorization entry's allowlist apart, because a
	// sole "*" in one entry stops being one once merged with another; a model
	// is authorized when any entry authorizes it, as validatePolicyCatalog
	// reads them.
	grants [][]string
	// Advertised reports that this worker's inventory offers it now.
	Advertised bool `json:"advertised"`
	// Reason is why this instance and worker pair cannot run a route:
	// "missing binding", "no models", "not installed" or "unavailable". It is
	// set even when Advertised is true, because an instance the coordinator
	// dropped cannot be routed to whatever the host offers.
	Reason string   `json:"reason,omitempty"`
	Models []string `json:"models,omitempty"`
}

// The two reasons a route fails on the worker rather than in the catalog. The
// other two are the coordinator's own drop reasons, so they are named once in
// the configuration package and used here.
const (
	modelsReasonNotInstalled = "not installed"
	modelsReasonUnavailable  = "unavailable"
)

// modelsReasonOrder is the precedence an instance's own reason follows when
// its workers disagree: a fault in the fleet's authorization first, because it
// is one edit away from fixed and it explains every worker at once; then the
// host-side facts, the actionable one first.
var modelsReasonOrder = []string{
	config.DroppedProviderMissingBinding, config.DroppedProviderNoModels,
	modelsReasonUnavailable, modelsReasonNotInstalled,
}

// modelsCLI is the seam: everything models needs is one query service, so the
// whole verb is testable against fixture responses.
type modelsCLI struct {
	service   adminQueryService
	principal backlogadmin.Principal
	stdout    io.Writer
	// staleAfter is the stale threshold; zero means defaultModelsStaleAfter.
	staleAfter time.Duration
}

func cmdModels(g globalFlags, args []string) error {
	if answered, err := admitHelp(os.Stdout, []string{"models"}, args); answered || err != nil {
		return err
	}
	scope, asJSON, err := parseModelsArgs(args)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	cli := modelsCLI{service: transport.client, principal: transport.principal, stdout: os.Stdout, staleAfter: modelsStaleAfter(cfg)}
	return cli.run(context.Background(), scope, asJSON)
}

// parseModelsArgs takes the flags models has, each of which narrows what is
// read. It refuses a positional argument rather than ignoring it, because
// "t3-steward models steward" is a plausible mistake and a silently unfiltered
// table is a worse answer than a refusal that names the flag.
func parseModelsArgs(args []string) (modelsScope, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return modelsScope{}, false, err
	}
	var scope modelsScope
	for i := 0; i < len(clean); i++ {
		switch clean[i] {
		case "--project":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return modelsScope{}, false, errors.New("--project needs a project name; t3-steward backlog projects lists them")
			}
			scope.Project = clean[i+1]
			i++
		case "--instance":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return modelsScope{}, false, errors.New("--instance needs a provider instance id; t3-steward models with no filter lists them")
			}
			scope.Instance = clean[i+1]
			i++
		case "--available":
			scope.Available = true
		default:
			return modelsScope{}, false, fmt.Errorf("models usage: t3-steward models [--project NAME] [--instance ID] [--available] [--json] (got %q)", clean[i])
		}
	}
	return scope, asJSON, nil
}

// run reads the three views, joins them, and narrows the result to the scope
// the command line asked for. The scope is applied to the document before
// either the encoder or the renderer sees it, so the text form and the JSON
// form answer the same question and neither can narrow something the other
// already capped.
func (c modelsCLI) run(ctx context.Context, scope modelsScope, asJSON bool) error {
	workers, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
	if err != nil {
		return err
	}
	quotas, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuota})
	if err != nil {
		return err
	}
	var eligible map[string]bool
	if scope.Project != "" {
		projects, err := c.query(ctx, backlogadmin.Query{
			Kind: backlogadmin.QueryProjects, Filter: backlogadmin.Filter{Project: scope.Project},
		})
		if err != nil {
			// Only --project needs the catalog: the table itself is built from
			// the workers and quota queries, which every release answers.
			return explainRefusedProjectsQuery(ctx, c.query, err, modelsWithoutTheCatalog)
		}
		if len(projects.Projects) == 0 {
			return fmt.Errorf("project %q is not in this coordinator's catalog; t3-steward backlog projects lists the projects it has", scope.Project)
		}
		eligible = make(map[string]bool)
		for _, worker := range projects.Projects[0].Workers {
			eligible[worker.Worker] = true
		}
	}
	document := scopeModelsDocument(buildModelsDocument(scope.Project, workers.Workers, quotas.Quotas, eligible, c.staleAfter), scope)
	if asJSON {
		return c.encode(document)
	}
	return renderModels(c.stdout, document)
}

// encode prints the document as models --json does.
func (c modelsCLI) encode(document modelsDocument) error {
	encoder := json.NewEncoder(c.stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(document)
}

func (c modelsCLI) query(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	query.Version = backlogadmin.Version
	query.Principal = c.principal
	return c.service.Query(ctx, query)
}

// buildModelsDocument joins the catalog, the inventories and the observations.
// eligible, when not nil, is the set of workers whose advertisement counts;
// the quota observations are always read from every worker, because a pool's
// state is a fleet fact and does not depend on which project is asking.
// modelsNow is the clock the quota age is read against; a seam for tests.
var modelsNow = time.Now

// defaultModelsStaleAfter is the age past which a pool reading is marked
// stale when backlog_v2.coordinator_client.defaults.quota_stale_after is not
// set.
const defaultModelsStaleAfter = time.Hour

// modelsStaleAfter is the configured stale threshold.
func modelsStaleAfter(cfg config.Config) time.Duration {
	if configured := cfg.BacklogV2.CoordinatorClient.Defaults.QuotaStaleAfter.D(); configured > 0 {
		return configured
	}
	return defaultModelsStaleAfter
}

func buildModelsDocument(project string, workers []backlogadmin.Worker, quotas []backlogadmin.Quota, eligible map[string]bool, staleAfter time.Duration) modelsDocument {
	snapshots := make([]domain.WorkerSnapshot, 0, len(workers))
	for _, worker := range workers {
		snapshots = append(snapshots, worker.Snapshot)
	}
	states := domain.MergeQuotaObservations(nil, snapshots)
	now := modelsNow()
	windowSets := make(map[string]domain.QuotaWindowSet, len(quotas))

	instances := make(map[string]*modelsInstance)
	entry := func(id string) *modelsInstance {
		if existing, ok := instances[id]; ok {
			return existing
		}
		created := &modelsInstance{Instance: id}
		instances[id] = created
		return created
	}
	for _, quota := range quotas {
		observation := domain.ObserveQuotaPool(quota.Pool, states)
		windowSets[quota.Pool.ID] = domain.ReadQuotaWindows(quota.Pool, states, now, staleAfter)
		for _, id := range quota.Pool.ProviderInstanceIDs {
			item := entry(id)
			item.Authorized = true
			item.QuotaPool = quota.Pool.ID
			item.Windows = modelsPoolWindows(quota.Pool, states, now, staleAfter)
			item.Admission = string(quota.Pool.Admission)
			if quota.Admission != nil {
				item.Admission = string(quota.Admission.Admission)
			}
			item.QuotaUnknown = quota.Pool.BucketSelection == domain.BucketSelectionUnknown
			if observation.Buckets > 0 {
				percent := observation.Percent
				item.Phase = string(observation.Phase)
				item.Percent = &percent
				observed := observation.ObservedAt
				item.ResetsAt, item.ObservedAt = observation.ResetsAt, &observed
				item.OldestObservedAt, item.Stale = modelsPoolFreshness(quota.Pool, states, now, staleAfter)
			}
		}
	}
	// worker.Providers is the coordinator's own statement of what each worker
	// is authorized for. When it carries something for any worker, the
	// coordinator reports the per-worker catalog, and an instance absent from a
	// worker's list (or a worker with no list at all) is a fact the row says;
	// when it is empty for every worker the coordinator reported nothing, and
	// the rows say nothing either. This is the rule validatePolicyCatalog
	// applies, decided over every worker before the project filter.
	reported := false
	for _, worker := range workers {
		if len(worker.Providers) > 0 {
			reported = true
			break
		}
	}
	for _, worker := range workers {
		id := worker.Snapshot.WorkerID
		if eligible != nil && !eligible[id] {
			continue
		}
		ready := worker.Enrolled && !worker.Stale && worker.Snapshot.Connected &&
			worker.State == "observed" && worker.Health == string(domain.WorkerHealthReady)
		rows := map[string]*modelsWorker{}
		row := func(instance string) *modelsWorker {
			if existing, ok := rows[instance]; ok {
				return existing
			}
			created := &modelsWorker{Worker: id, Ready: ready}
			if reported {
				created.Authorized = new(bool)
			}
			rows[instance] = created
			return created
		}
		installed := make(map[string]domain.WorkerProviderInventory, len(worker.Snapshot.Inventory.Providers))
		for _, provider := range worker.Snapshot.Inventory.Providers {
			installed[provider.InstanceID] = provider
		}
		// The authorization half first, so that an instance the coordinator
		// dropped is in the document at all: it is in no quota pool and no
		// inventory, and it is exactly the state that was invisible before.
		for _, granted := range worker.Providers {
			entry(granted.Instance)
			current := row(granted.Instance)
			yes := true
			current.Authorized = &yes
			if granted.Dropped == "" {
				current.AuthorizedModels = mergeSorted(current.AuthorizedModels, granted.Models)
				current.grants = append(current.grants, granted.Models)
			}
			current.Reason = workerRouteReason(granted, installed)
		}
		for _, provider := range worker.Snapshot.Inventory.Providers {
			if !provider.Available {
				continue
			}
			item := entry(provider.InstanceID)
			item.Advertised = true
			if item.QuotaPool == "" {
				item.QuotaPool = provider.QuotaPoolID
			}
			item.Models = mergeSorted(item.Models, provider.Models)
			advertised := row(provider.InstanceID)
			advertised.Advertised = true
			advertised.Models = mergeSorted(nil, provider.Models)
		}
		for instance, entered := range rows {
			item := entry(instance)
			item.Workers = append(item.Workers, *entered)
		}
	}
	document := modelsDocument{
		SchemaVersion: modelsSchemaVersion, Project: project,
		Instances: make([]modelsInstance, 0, len(instances)),
	}
	for _, item := range instances {
		item.MissingBinding = item.Advertised && !item.Authorized
		reasons := map[string]bool{}
		for _, worker := range item.Workers {
			if worker.Reason != "" {
				reasons[worker.Reason] = true
			}
		}
		if reasons[config.DroppedProviderMissingBinding] {
			// The coordinator dropped it for at least one worker, so no pool
			// holds it there whatever the rest of the fleet reports.
			item.MissingBinding = true
		}
		if !item.Advertised {
			for _, reason := range modelsReasonOrder {
				if reasons[reason] {
					item.Reason = reason
					break
				}
			}
		}
		sort.Slice(item.Workers, func(i, j int) bool { return item.Workers[i].Worker < item.Workers[j].Worker })
		var windows *domain.QuotaWindowSet
		if set, ok := windowSets[item.QuotaPool]; ok && item.Authorized {
			windows = &set
		}
		item.Routes = modelsRoutesFor(*item, windows)
		item.Availability = modelsInstanceAvailability(item.Routes)
		document.Instances = append(document.Instances, *item)
	}
	sort.Slice(document.Instances, func(i, j int) bool {
		return document.Instances[i].Instance < document.Instances[j].Instance
	})
	return document
}

// scopeModelsDocument narrows a built document to the scope the command line
// asked for and records what it narrowed. It runs before anything renders or
// encodes the document, which is the whole of the ordering rule: what to read
// is decided first, how much of it to show second, so the answer to a narrowed
// question is complete rather than truncated.
//
// --project is absent here on purpose: it is answered by the coordinator, in
// the projects query that decides which workers count as advertising, and
// applying it a second time here would report a different fleet than the one
// the document was built from.
func scopeModelsDocument(document modelsDocument, scope modelsScope) modelsDocument {
	document.Instance, document.Available = scope.Instance, scope.Available
	document.TotalInstances = len(document.Instances)
	document.TotalRoutes = countModelsRoutes(document.Instances)
	if !scope.Narrowed() {
		return document
	}
	kept := make([]modelsInstance, 0, len(document.Instances))
	for _, instance := range document.Instances {
		if scope.Instance != "" && instance.Instance != scope.Instance {
			continue
		}
		if scope.Available {
			if modelsStatus(instance) != modelsStatusAvailable {
				continue
			}
			instance = keepAvailableRoutes(instance)
		}
		kept = append(kept, instance)
	}
	document.Instances = kept
	return document
}

// keepAvailableRoutes narrows an instance to the routes that can run now, so
// that --available drops an unavailable model of an instance whose other
// model is available, rather than keeping the whole instance for it.
func keepAvailableRoutes(instance modelsInstance) modelsInstance {
	if len(instance.Routes) == 0 {
		return instance
	}
	routes := make([]modelsRoute, 0, len(instance.Routes))
	var models []string
	for _, route := range instance.Routes {
		if route.Availability.Code != modelsStatusAvailable {
			continue
		}
		routes = append(routes, route)
		if route.Model != "" {
			models = append(models, route.Model)
		}
	}
	instance.Routes, instance.Models = routes, models
	return instance
}

// countModelsRoutes counts the rows the table would print: one per model of an
// instance, and one for an instance that advertises none, because a route that
// cannot run is still a line the caller reads.
func countModelsRoutes(instances []modelsInstance) int {
	rows := 0
	for _, instance := range instances {
		if len(instance.Models) == 0 {
			rows++
			continue
		}
		rows += len(instance.Models)
	}
	return rows
}

// modelsScopeWords is the scope a document answers, in the words of the
// command line that asked for it.
func modelsScopeWords(document modelsDocument) string {
	return modelsScope{Instance: document.Instance, Available: document.Available}.Words()
}

// workerRouteReason says why one authorized instance and worker pair cannot
// run a route, and is empty when it can. The catalog's own reasons come first,
// and they hold however available the instance is on the host: an instance the
// coordinator dropped, or authorized for no model, cannot be routed to
// whatever the worker has installed, and reporting the host-side fact instead
// would send an operator to the wrong machine.
func workerRouteReason(authorized backlogadmin.WorkerProviderAuthorization, installed map[string]domain.WorkerProviderInventory) string {
	switch provider, exists := installed[authorized.Instance]; {
	case authorized.Dropped != "":
		return authorized.Dropped
	case len(authorized.Models) == 0:
		return config.DroppedProviderNoModels
	case !exists:
		return modelsReasonNotInstalled
	case !provider.Available:
		return modelsReasonUnavailable
	default:
		return ""
	}
}

// mergeSorted returns the union of two string lists, deduplicated and sorted.
func mergeSorted(into []string, add []string) []string {
	seen := make(map[string]bool, len(into)+len(add))
	result := make([]string, 0, len(into)+len(add))
	for _, list := range [][]string{into, add} {
		for _, value := range list {
			if value == "" || seen[value] {
				continue
			}
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

// renderModels prints one table whose rows are the routes themselves, so that
// choosing one is copying one field rather than joining two columns by eye.
func renderModels(out io.Writer, document modelsDocument) error {
	if len(document.Instances) == 0 {
		if document.Instance != "" || document.Available {
			// An empty narrowed answer is not an empty fleet, and saying so is
			// the difference between a typo and an outage.
			_, err := fmt.Fprintf(out, "no provider route matches %s; this coordinator answers with %d instances and %d routes, which \"t3-steward models\" with no filter lists\n",
				modelsScopeWords(document), document.TotalInstances, document.TotalRoutes)
			return err
		}
		_, err := fmt.Fprintln(out, "no provider instance is authorized or advertised; "+
			"the fleet catalog is what authorizes one and a worker inventory is what advertises it")
		return err
	}
	if document.Project != "" {
		fmt.Fprintf(out, "project %s\n\n", document.Project)
	}
	instances := append([]modelsInstance(nil), document.Instances...)
	sort.SliceStable(instances, func(i, j int) bool { return instances[i].QuotaPool < instances[j].QuotaPool })
	currentPool := "\x00"
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ROUTE\tPOOL\tPHASE\tUSED\tAGE\tADMISSION\tWORKERS\tSTATUS")
	for _, instance := range instances {
		if instance.QuotaPool != currentPool {
			if err := table.Flush(); err != nil {
				return err
			}
			currentPool = instance.QuotaPool
			fmt.Fprintf(out, "\npool %s\n", firstNonEmptyText(currentPool, "(none)"))
			if len(instance.Windows) == 0 {
				fmt.Fprintln(out, "  governing windows: unknown")
			} else {
				for _, w := range instance.Windows {
					if w.Unknown && w.Key.LimitID == "" {
						fmt.Fprintf(out, "  %s: missing (the provider declares it and no reading has arrived)\n", w.Key.Window)
						continue
					}
					if w.Unknown {
						fmt.Fprintf(out, "  %s (%s): unknown (no governing observation)\n", w.Key.Window, w.Key.LimitID)
						continue
					}
					reset := "unknown"
					if w.ResetsAt != nil {
						reset = w.ResetsAt.UTC().Format(time.RFC3339)
					}
					freshness := "fresh"
					if w.Stale {
						freshness = "stale"
					}
					fmt.Fprintf(out, "  %s (%s): used %.1f%%; headroom %.1f%%; reset %s; %s\n", w.Key.Window, w.Key.LimitID, w.UsedPercent, w.Headroom, reset, freshness)
				}
			}
		}
		used := "unknown"
		if instance.Percent != nil {
			used = fmt.Sprintf("%.0f%%", *instance.Percent)
		}
		age := "-"
		if instance.QuotaUnknown {
			age = "quota unknown"
		}
		if instance.ObservedAt != nil {
			age = modelsAge(modelsNow().Sub(*instance.ObservedAt))
			if instance.Stale {
				age += " stale"
			}
		}
		routes := instance.Models
		if len(routes) == 0 {
			routes = []string{""}
		}
		for _, model := range routes {
			route := instance.Instance
			if model != "" {
				route += "/" + model
			}
			// The column counts the workers that are offering this route now:
			// a worker that does not advertise the model, or is not authorized
			// for it, cannot run it however ready it is, and the reason is in
			// the status column or below the table.
			ready, advertising := 0, 0
			for _, worker := range instance.Workers {
				if !modelsWorkerServes(worker, model) {
					continue
				}
				advertising++
				if worker.Ready {
					ready++
				}
			}
			workers := fmt.Sprintf("%d/%d ready", ready, advertising)
			if advertising == 0 {
				workers = "none"
			}
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", route,
				firstNonEmptyText(instance.QuotaPool, "(none)"),
				firstNonEmptyText(instance.Phase, "unknown"), used, age,
				firstNonEmptyText(instance.Admission, "unknown"), workers,
				modelsRouteAvailability(instance, model).Detail)
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if document.Instance != "" || document.Available {
		fmt.Fprintf(out, "\nshowing %d of %d routes on %d of %d provider instances: %s is in force, and dropping it prints the rest.\n",
			countModelsRoutes(document.Instances), document.TotalRoutes,
			len(document.Instances), document.TotalInstances, modelsScopeWords(document))
	}
	return renderModelsReasons(out, document)
}

// renderModelsReasons lists the instance and worker pairs the coordinator
// authorizes and whose route cannot run, with the reason. That scope is the
// coordinator's: a reason is recorded only for a pair it reported an
// authorization for, so a pair it authorizes for nothing gets no row here
// however little it can run. A pair is listed even when the worker advertises
// the instance: the coordinator drops an instance it cannot bind to a quota
// pool, and no amount of advertising makes that route runnable. It is a second
// table rather than a column of the first because the reason belongs to the
// pair, not to the route: one instance can be missing on one worker and signed
// out on another.
func renderModelsReasons(out io.Writer, document modelsDocument) error {
	type pair struct{ instance, worker, reason string }
	var pairs []pair
	for _, instance := range document.Instances {
		for _, worker := range instance.Workers {
			if worker.Reason == "" {
				continue
			}
			pairs = append(pairs, pair{instance.Instance, worker.Worker, worker.Reason})
		}
	}
	if len(pairs) == 0 {
		return nil
	}
	fmt.Fprintln(out, "\nroutes that cannot run:")
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "INSTANCE\tWORKER\tREASON\tWHAT IT MEANS")
	for _, item := range pairs {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", item.instance, item.worker, item.reason, modelsReasonText(item.reason))
	}
	return table.Flush()
}

// modelsReasonText is the one sentence each reason is explained with, in the
// same words in the table and in the status column.
func modelsReasonText(reason string) string {
	switch reason {
	case config.DroppedProviderMissingBinding:
		return "the coordinator dropped it: no quota pool is authorized for it"
	case config.DroppedProviderNoModels:
		return "the fleet authorizes the instance for no model"
	case modelsReasonNotInstalled:
		return "the worker has no such provider instance"
	case modelsReasonUnavailable:
		return "installed, and not signed in or not enabled"
	default:
		return reason
	}
}

// modelsPoolFreshness judges each bucket of the pool on its own: a bucket is
// stale when its reading is older than staleAfter (zero means
// defaultModelsStaleAfter) or was taken before
// its own reset, which has since passed. The pool is stale when any of its
// buckets is, and its age is its oldest bucket's, because that is the reading
// its phase and percent may still rest on. The buckets are the ones
// ObserveQuotaPool counts: the pool's named buckets, or its instances' buckets
// when it names none.
func modelsPoolFreshness(pool domain.QuotaPool, states []domain.BucketState, now time.Time, staleAfter time.Duration) (*time.Time, bool) {
	if staleAfter <= 0 {
		staleAfter = defaultModelsStaleAfter
	}
	belongs := domain.PoolBucketMatcher(pool)
	var oldest *time.Time
	stale := false
	for _, state := range states {
		if !belongs(state) {
			continue
		}
		observed := state.ObservedAt
		if oldest == nil || observed.Before(*oldest) {
			oldest = &observed
		}
		if modelsBucketStale(state, now, staleAfter) {
			stale = true
		}
	}
	return oldest, stale
}

// modelsAge prints a reading's age at the precision a person reads it at.
func modelsAge(age time.Duration) string {
	switch {
	case age < 0:
		return "0s"
	case age < time.Minute:
		return fmt.Sprintf("%ds", int(age.Seconds()))
	case age < time.Hour:
		return fmt.Sprintf("%dm", int(age.Minutes()))
	case age < 48*time.Hour:
		return fmt.Sprintf("%dh", int(age.Hours()))
	}
	return fmt.Sprintf("%dd", int(age.Hours()/24))
}

// modelsStatusAvailable is the status of a route that can run now. --available
// keeps exactly the instances whose status is this one, so the flag and the
// column can never disagree about what "available" means.
const modelsStatusAvailable = "available"

// modelsStatus is the one phrase that says whether this route can run, and
// what is missing when it cannot. It is the availability built with the
// document; an instance built without one is judged with no quota reading,
// so it is never called available on telemetry nobody read.
func modelsStatus(instance modelsInstance) string {
	if instance.Availability.Code != "" {
		return instance.Availability.Detail
	}
	return modelsAvailabilityFor(instance, nil, "").Detail
}

// modelsRoutesFor judges every route of an instance: one per model it
// advertises, or one with no model when it advertises none.
func modelsRoutesFor(instance modelsInstance, windows *domain.QuotaWindowSet) []modelsRoute {
	models := instance.Models
	if len(models) == 0 {
		models = []string{""}
	}
	routes := make([]modelsRoute, 0, len(models))
	for _, model := range models {
		routes = append(routes, modelsRoute{Model: model, Availability: modelsAvailabilityFor(instance, windows, model)})
	}
	return routes
}

// modelsInstanceAvailability is the instance's verdict from its routes':
// available when any route is, otherwise the first route's reason.
func modelsInstanceAvailability(routes []modelsRoute) modelsAvailability {
	for _, route := range routes {
		if route.Availability.Code == modelsStatusAvailable {
			return route.Availability
		}
	}
	if len(routes) == 0 {
		return modelsAvailability{}
	}
	return routes[0].Availability
}

// modelsRouteAvailability is the verdict for one route of an instance: the
// one built with the document, or, for an instance built without one, the
// verdict with no quota reading.
func modelsRouteAvailability(instance modelsInstance, model string) modelsAvailability {
	for _, route := range instance.Routes {
		if route.Model == model {
			return route.Availability
		}
	}
	return modelsAvailabilityFor(instance, nil, model)
}

// modelsWorkerOffers reports whether a worker advertises a route: the
// instance, and the model when the route names one.
func modelsWorkerOffers(worker modelsWorker, model string) bool {
	if !worker.Advertised {
		return false
	}
	if model == "" {
		return true
	}
	for _, offered := range worker.Models {
		if offered == model {
			return true
		}
	}
	return false
}

// modelsWorkerAuthorizes reports whether the coordinator authorizes a worker
// for a route: the instance, and the model when the route names one. A worker
// the coordinator reported no authorization for at all (a release older than
// the per-worker catalog) is not refused here, because that answer does not
// say; a worker it reported authorizations for, none of which names the
// instance, is refused.
func modelsWorkerAuthorizes(worker modelsWorker, model string) bool {
	if worker.Authorized == nil {
		return true
	}
	if !*worker.Authorized {
		return false
	}
	if model == "" {
		return len(worker.AuthorizedModels) > 0
	}
	if worker.grants == nil {
		return domain.ModelAuthorized(worker.AuthorizedModels, model)
	}
	return slices.ContainsFunc(worker.grants, func(allowed []string) bool { return domain.ModelAuthorized(allowed, model) })
}

// modelsWorkerServes reports whether a worker both advertises a route and is
// authorized for it, which is what dispatch needs from one worker.
func modelsWorkerServes(worker modelsWorker, model string) bool {
	return modelsWorkerOffers(worker, model) && modelsWorkerAuthorizes(worker, model)
}

// modelsAvailabilityFor decides whether a route can run now (F2: models
// called a route available on stale, incomplete or exhausted telemetry, and
// with no ready worker). In order: the catalog has to bind it, a worker has
// to advertise it, some worker that advertises the model has to be authorized
// for that model, its pool's window set has to be complete, fresh and not
// exhausted (windows is nil when the instance has no authorized pool), the
// pool has to admit work, and at least one ready worker that both advertises
// the model and is authorized for it has to be able to dispatch it (an empty
// model asks for any model of the instance). The first failure is the reason.
func modelsAvailabilityFor(instance modelsInstance, windows *domain.QuotaWindowSet, model string) modelsAvailability {
	switch {
	case instance.MissingBinding:
		return modelsAvailability{Code: modelsAvailabilityMissingBinding, Detail: "no quota binding: the fleet catalog authorizes no pool for it"}
	case !instance.Advertised && instance.Reason != "":
		return modelsAvailability{Code: modelsAvailabilityNotAdvertised, Detail: "not advertised: " + modelsReasonText(instance.Reason)}
	case !instance.Advertised:
		return modelsAvailability{Code: modelsAvailabilityNotAdvertised, Detail: "authorized, not advertised: no worker offers it"}
	case !slices.ContainsFunc(instance.Workers, func(worker modelsWorker) bool { return modelsWorkerServes(worker, model) }):
		return modelsAvailability{Code: modelsAvailabilityModelNotAuthorized, Detail: "not authorized: no worker that offers it is authorized for this model"}
	case windows == nil:
		return modelsAvailability{Code: domain.QuotaWindowUnknown, Detail: domain.QuotaWindowProblem{Code: domain.QuotaWindowUnknown}.String()}
	}
	if problem, found := windows.Problem(); found {
		return modelsAvailability{Code: problem.Code, Window: problem.Window, ResetsAt: problem.ResetsAt, Detail: problem.String()}
	}
	if instance.Admission != "" && instance.Admission != string(domain.AdmissionOpen) {
		return modelsAvailability{Code: modelsAvailabilityPoolAdmission, Detail: "pool " + instance.Admission}
	}
	for _, worker := range instance.Workers {
		if modelsWorkerServes(worker, model) && worker.Ready && worker.Reason == "" {
			return modelsAvailability{Code: modelsStatusAvailable, Detail: modelsStatusAvailable}
		}
	}
	return modelsAvailability{Code: modelsAvailabilityNoReadyWorker, Detail: "no ready worker"}
}
