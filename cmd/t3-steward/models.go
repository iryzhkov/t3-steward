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

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const modelsUsage = `Usage: t3-steward models [--project NAME] [--json]

Every provider route the fleet can run right now, joined from three facts that
fail separately:

  authorized   the coordinator's fleet catalog binds the instance to a quota
               pool, so it may be routed to at all
  advertised   at least one worker reports the instance with its models, so
               there is somewhere to run it
  quota        the pool's admission state and the worst of its buckets, phase
               and used percent, from the observations workers report

Each row is one instance/model pair, in the form "t3-steward task run --model
[INSTANCE/]MODEL" takes. A bare model name is enough when exactly one instance
offers it. --project narrows the advertised half to the workers eligible for
that project; "t3-steward backlog projects" lists the projects.

An instance that is authorized and advertised by nobody, or advertised with no
quota binding, is listed with that as its status rather than omitted: a route
that cannot run is the thing the caller most needs to see.
` + coordinatorTransportHelp

// modelsSchemaVersion versions the models document. It is an agent-facing
// surface from its first release, so it is versioned from its first release.
const modelsSchemaVersion = 1

// modelsDocument is what "models --json" prints: one document, one entry per
// provider instance, sorted by instance id.
type modelsDocument struct {
	SchemaVersion int              `json:"schemaVersion"`
	Project       string           `json:"project,omitempty"`
	Instances     []modelsInstance `json:"instances"`
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
	// Models is every model the eligible workers advertise for the instance,
	// deduplicated and sorted.
	Models  []string       `json:"models,omitempty"`
	Workers []modelsWorker `json:"workers,omitempty"`
	// MissingBinding reports an instance a worker advertises that the loaded
	// fleet catalog authorizes in no quota pool. Reason is empty at this
	// release: the coordinator does not yet report why it dropped an instance,
	// and inventing a cause here would be a guess. The field exists so that the
	// document does not change shape when it can be filled.
	MissingBinding bool   `json:"missingBinding"`
	Reason         string `json:"reason,omitempty"`
}

// modelsWorker is one worker that advertises an instance.
type modelsWorker struct {
	Worker string   `json:"worker"`
	Ready  bool     `json:"ready"`
	Models []string `json:"models,omitempty"`
}

// modelsCLI is the seam: everything models needs is one query service, so the
// whole verb is testable against fixture responses.
type modelsCLI struct {
	service   adminQueryService
	principal backlogadmin.Principal
	stdout    io.Writer
}

func cmdModels(g globalFlags, args []string) error {
	if len(args) > 0 && isHelp(args[0]) {
		fmt.Print(modelsUsage)
		return nil
	}
	project, asJSON, err := parseModelsArgs(args)
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
	return cli.run(context.Background(), project, asJSON)
}

// parseModelsArgs takes the two flags models has. It refuses a positional
// argument rather than ignoring it, because "t3-steward models steward" is a
// plausible mistake and a silently unfiltered table is a worse answer than a
// refusal that names the flag.
func parseModelsArgs(args []string) (string, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return "", false, err
	}
	project := ""
	for i := 0; i < len(clean); i++ {
		switch clean[i] {
		case "--project":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return "", false, errors.New("--project needs a project name; t3-steward backlog projects lists them")
			}
			project = clean[i+1]
			i++
		default:
			return "", false, fmt.Errorf("models usage: t3-steward models [--project NAME] [--json] (got %q)", clean[i])
		}
	}
	return project, asJSON, nil
}

func (c modelsCLI) run(ctx context.Context, project string, asJSON bool) error {
	workers, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
	if err != nil {
		return err
	}
	quotas, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuota})
	if err != nil {
		return err
	}
	var eligible map[string]bool
	if project != "" {
		projects, err := c.query(ctx, backlogadmin.Query{
			Kind: backlogadmin.QueryProjects, Filter: backlogadmin.Filter{Project: project},
		})
		if err != nil {
			return err
		}
		if len(projects.Projects) == 0 {
			return fmt.Errorf("project %q is not in this coordinator's catalog; t3-steward backlog projects lists the projects it has", project)
		}
		eligible = make(map[string]bool)
		for _, worker := range projects.Projects[0].Workers {
			eligible[worker.Worker] = true
		}
	}
	document := buildModelsDocument(project, workers.Workers, quotas.Quotas, eligible)
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
			item.Workers = append(item.Workers, modelsWorker{
				Worker: id, Ready: ready, Models: mergeSorted(nil, provider.Models),
			})
		}
	}
	document := modelsDocument{
		SchemaVersion: modelsSchemaVersion, Project: project,
		Instances: make([]modelsInstance, 0, len(instances)),
	}
	for _, item := range instances {
		item.MissingBinding = item.Advertised && !item.Authorized
		sort.Slice(item.Workers, func(i, j int) bool { return item.Workers[i].Worker < item.Workers[j].Worker })
		document.Instances = append(document.Instances, *item)
	}
	sort.Slice(document.Instances, func(i, j int) bool {
		return document.Instances[i].Instance < document.Instances[j].Instance
	})
	return document
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
		_, err := fmt.Fprintln(out, "no provider instance is authorized or advertised; "+
			"the fleet catalog is what authorizes one and a worker inventory is what advertises it")
		return err
	}
	if document.Project != "" {
		fmt.Fprintf(out, "project %s\n\n", document.Project)
	}
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ROUTE\tPOOL\tPHASE\tUSED\tADMISSION\tWORKERS\tSTATUS")
	for _, instance := range document.Instances {
		ready := 0
		for _, worker := range instance.Workers {
			if worker.Ready {
				ready++
			}
		}
		workers := fmt.Sprintf("%d/%d ready", ready, len(instance.Workers))
		if len(instance.Workers) == 0 {
			workers = "none"
		}
		used := "unknown"
		if instance.Percent != nil {
			used = fmt.Sprintf("%.0f%%", *instance.Percent)
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
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", route,
				firstNonEmptyText(instance.QuotaPool, "(none)"),
				firstNonEmptyText(instance.Phase, "unknown"), used,
				firstNonEmptyText(instance.Admission, "unknown"), workers,
				modelsStatus(instance))
		}
	}
	return table.Flush()
}

// modelsStatus is the one phrase that says whether this route can run, and
// what is missing when it cannot.
func modelsStatus(instance modelsInstance) string {
	switch {
	case instance.MissingBinding:
		return "no quota binding: the fleet catalog authorizes no pool for it"
	case !instance.Advertised:
		return "authorized, not advertised: no worker offers it"
	case instance.Admission != "" && instance.Admission != string(domain.AdmissionOpen):
		return "pool " + instance.Admission
	default:
		return "available"
	}
}
