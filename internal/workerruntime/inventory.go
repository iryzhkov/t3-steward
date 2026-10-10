package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type HostInventoryProbe struct {
	DataDir    string
	Capability func(context.Context, string) bool
	// Observed are host capabilities the host reported for itself
	// (workerproto.HostObservedCapability). They are advertised in addition to
	// the configured list, and their absence never degrades the worker: a
	// task that needs one waits for a capable worker instead.
	Observed []string
	// ProjectAvailable must observe the local T3 project catalog.
	ProjectAvailable func(context.Context, string) (bool, error)
	// Warn receives each host warning the snapshot finds, such as a TMPDIR
	// that is a symbolic link. A warning does not degrade the worker's health.
	// Nil logs each distinct warning once per process.
	Warn func(string)
}

// loggedInventoryWarnings holds the warnings already logged, so that a
// snapshot taken every few seconds does not repeat them.
var loggedInventoryWarnings sync.Map

func (p HostInventoryProbe) warn(warning string) {
	if p.Warn != nil {
		p.Warn(warning)
		return
	}
	if _, logged := loggedInventoryWarnings.LoadOrStore(warning, true); !logged {
		slog.Warn("worker host warning; verification and gate commands get the resolved temporary directory", "component", "worker-inventory", "warning", warning)
	}
}

func (p HostInventoryProbe) Observe(ctx context.Context, wanted domain.WorkerInventory) (domain.WorkerInventory, error) {
	// Copy every mutable slice before observations replace configured values.
	raw, err := json.Marshal(wanted)
	if err != nil {
		return wanted, err
	}
	var result domain.WorkerInventory
	if err = json.Unmarshal(raw, &result); err != nil {
		return wanted, err
	}
	result.Health = domain.WorkerHealthReady
	result.AcceptBacklog = wanted.AcceptBacklog
	result.ObservedAt = time.Now().UTC()
	for _, warning := range backlog.TempDirectoryWarnings(nil) {
		p.warn(warning)
	}
	var capabilities []string
	for _, name := range wanted.Capabilities {
		if p.Capability != nil && p.Capability(ctx, name) {
			capabilities = append(capabilities, name)
		} else {
			result.Health = domain.WorkerHealthDegraded
			result.AcceptBacklog = false
		}
	}
	for _, name := range p.Observed {
		if !slices.Contains(capabilities, name) {
			capabilities = append(capabilities, name)
		}
	}
	result.Capabilities = capabilities
	for i, project := range wanted.Projects {
		available := false
		if p.ProjectAvailable != nil {
			available, err = p.ProjectAvailable(ctx, project.Name)
			if err != nil {
				available = false
			}
		}
		result.Projects[i].Available = available
		result.Projects[i].UpdatedAt = result.ObservedAt
	}
	for i, provider := range wanted.Providers {
		models, available := p.provider(provider.InstanceID)
		result.Providers[i].Available = available
		result.Providers[i].Models = nil
		for _, model := range models {
			if available && domain.ModelAuthorized(provider.Models, model) {
				result.Providers[i].Models = append(result.Providers[i].Models, model)
			}
		}
		if len(result.Providers[i].Models) == 0 {
			result.Providers[i].Available = false
		}
	}
	return result, nil
}
func (p HostInventoryProbe) provider(instance string) ([]string, bool) {
	if !bootstrapItem.MatchString(instance) || filepath.Base(instance) != instance {
		return nil, false
	}
	file, err := os.Open(filepath.Join(p.DataDir, "caches", instance+".json"))
	if err != nil {
		return nil, false
	}
	defer file.Close()
	var cache struct {
		InstanceID string `json:"instanceId"`
		Enabled    bool   `json:"enabled"`
		Installed  bool   `json:"installed"`
		Status     string `json:"status"`
		Auth       struct {
			Status string `json:"status"`
		} `json:"auth"`
		Models []struct {
			Slug string `json:"slug"`
		} `json:"models"`
	}
	raw, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(raw) > 4<<20 {
		return nil, false
	}
	if err = json.Unmarshal(raw, &cache); err != nil {
		return nil, false
	}
	if cache.InstanceID != instance || !cache.Enabled || !cache.Installed || cache.Status == "error" || cache.Status == "disabled" || cache.Auth.Status != "authenticated" {
		return nil, false
	}
	var models []string
	for _, m := range cache.Models {
		models = append(models, m.Slug)
	}
	return models, true
}

var ErrWorkerNotEnrolled = errors.New("worker is configured but not enrolled for this catalog")
