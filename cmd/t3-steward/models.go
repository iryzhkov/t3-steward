package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
               and used percent, from the observations workers report; AGE
               is how old the newest reading is, and "stale" marks one older
               than an hour or taken before its window reset, whose phase and
               percent describe a window that may be over

Each row is one instance/model pair, in the form "t3-steward task run --model
[INSTANCE/]MODEL" takes. A bare model name is enough when exactly one instance
offers it.

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
	Instance string `json:"instance"`
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
	// ObservedAt is when the newest of those readings was taken and ResetsAt
	// the earliest reset they report. Stale marks a reading older than
	// modelsStaleAfter, or one taken before a reset that has since passed: its
	// phase and percent describe a window that may be over (S2: a pool read
	// 98% draining for hours after its seven-day window had reset).
	ObservedAt *time.Time `json:"observedAt,omitempty"`
	ResetsAt   *time.Time `json:"resetsAt,omitempty"`
	Stale      bool       `json:"stale,omitempty"`
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
}

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
	cli := modelsCLI{service: transport.client, principal: transport.principal, stdout: os.Stdout}
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
	document := scopeModelsDocument(buildModelsDocument(scope.Project, workers.Workers, quotas.Quotas, eligible), scope)
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(document)
	}
	return renderModels(c.stdout, document)
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

// modelsStaleAfter is the age past which a pool reading is marked stale. It
// matches the coordinator's default freshness.quota_max_age.
const modelsStaleAfter = time.Hour

func buildModelsDocument(project string, workers []backlogadmin.Worker, quotas []backlogadmin.Quota, eligible map[string]bool) modelsDocument {
	snapshots := make([]domain.WorkerSnapshot, 0, len(workers))
	for _, worker := range workers {
		snapshots = append(snapshots, worker.Snapshot)
	}
	states := domain.MergeQuotaObservations(nil, snapshots)

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
		for _, id := range quota.Pool.ProviderInstanceIDs {
			item := entry(id)
			item.Authorized = true
			item.QuotaPool = quota.Pool.ID
			item.Admission = string(quota.Pool.Admission)
			if quota.Admission != nil {
				item.Admission = string(quota.Admission.Admission)
			}
			if observation.Buckets > 0 {
				percent := observation.Percent
				item.Phase = string(observation.Phase)
				item.Percent = &percent
				observedAt := observation.ObservedAt
				item.ObservedAt = &observedAt
				item.ResetsAt = observation.ResetsAt
				now := modelsNow()
				item.Stale = now.Sub(observedAt) > modelsStaleAfter ||
					observation.ResetsAt != nil && observedAt.Before(*observation.ResetsAt) && !now.Before(*observation.ResetsAt)
			}
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
		// worker.Providers is the coordinator's own statement of what this
		// worker is authorized for. When it carries something, an instance
		// absent from it is a fact and the row says so; when it is empty the
		// coordinator reported nothing, and the row says nothing either.
		reported := len(worker.Providers) > 0
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
		if scope.Available && modelsStatus(instance) != modelsStatusAvailable {
			continue
		}
		kept = append(kept, instance)
	}
	document.Instances = kept
	return document
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
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ROUTE\tPOOL\tPHASE\tUSED\tAGE\tADMISSION\tWORKERS\tSTATUS")
	for _, instance := range document.Instances {
		// The column counts the workers that are offering the route now, not
		// the ones it is authorized for: a worker that does not advertise it
		// cannot run it however ready it is, and the reason is below the table.
		ready, advertising := 0, 0
		for _, worker := range instance.Workers {
			if !worker.Advertised {
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
		used := "unknown"
		if instance.Percent != nil {
			used = fmt.Sprintf("%.0f%%", *instance.Percent)
		}
		age := "-"
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
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", route,
				firstNonEmptyText(instance.QuotaPool, "(none)"),
				firstNonEmptyText(instance.Phase, "unknown"), used, age,
				firstNonEmptyText(instance.Admission, "unknown"), workers,
				modelsStatus(instance))
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
// what is missing when it cannot.
func modelsStatus(instance modelsInstance) string {
	switch {
	case instance.MissingBinding:
		return "no quota binding: the fleet catalog authorizes no pool for it"
	case !instance.Advertised && instance.Reason != "":
		return "not advertised: " + modelsReasonText(instance.Reason)
	case !instance.Advertised:
		return "authorized, not advertised: no worker offers it"
	case instance.Admission != "" && instance.Admission != string(domain.AdmissionOpen):
		return "pool " + instance.Admission
	default:
		return modelsStatusAvailable
	}
}
