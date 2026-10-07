package workerruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// maxPreviousCatalogRevisions bounds how many capacity-only predecessors a
// worker keeps executing packages of. Each is added only while work from it
// may still be live, so a handful covers any realistic run of resizes.
const maxPreviousCatalogRevisions = 32

// capacityOnlyChange reports whether next differs from current only in the
// worker's cpu class and executor capacity. Those are coordinator admission
// data: the worker executes nothing differently because of them, so a busy
// worker can adopt them without draining.
func capacityOnlyChange(current, next CatalogProjection) bool {
	if current.Revision == next.Revision {
		return false
	}
	neutral := func(p CatalogProjection) CatalogProjection {
		p.Revision = ""
		p.Worker.CPUClass = ""
		p.Worker.Executors = config.V2Executors{}
		return p
	}
	a, errA := json.Marshal(neutral(current))
	b, errB := json.Marshal(neutral(next))
	return errA == nil && errB == nil && string(a) == string(b)
}

// catalogChangeAreas names the parts of the catalog that differ, for a
// refusal an operator can act on.
func catalogChangeAreas(current, next CatalogProjection) string {
	var areas []string
	differ := func(name string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			areas = append(areas, name)
		}
	}
	capacity := func(w config.V2Worker) config.V2Worker {
		w.CPUClass = ""
		w.Executors = config.V2Executors{}
		return w
	}
	differ("worker settings", capacity(current.Worker), capacity(next.Worker))
	differ("executor capacity", [2]any{current.Worker.CPUClass, current.Worker.Executors}, [2]any{next.Worker.CPUClass, next.Worker.Executors})
	differ("projects", current.Projects, next.Projects)
	differ("setup profiles", current.SetupProfiles, next.SetupProfiles)
	differ("transport", current.Transport, next.Transport)
	differ("message limits", current.MessageLimits, next.MessageLimits)
	differ("freshness", current.Freshness, next.Freshness)
	differ("leases", current.Leases, next.Leases)
	if len(areas) == 0 {
		return "the catalog identity"
	}
	return strings.Join(areas, ", ")
}

// liveAttemptCount counts the attempts a catalog replacement would have to
// drain: every one that is not terminal, and every terminal one whose
// settlement is still owed.
func liveAttemptCount(attempts map[string]AttemptRecord) int {
	live := 0
	for _, record := range attempts {
		terminal := record.Phase == PhaseCompleted || record.Phase == PhaseFailed ||
			(record.Phase == PhaseStopped && record.StopConfirmed && hasCommandRequest(record, domain.WorkerCommandStop))
		if record.SettlePending || !terminal {
			live++
		}
	}
	return live
}

// busyCatalogRefusal is the reason a busy worker gives for a catalog change it
// cannot take without draining.
func busyCatalogRefusal(workerID string, live int, areas string) error {
	noun := "assignments"
	if live == 1 {
		noun = "assignment"
	}
	return fmt.Errorf("catalog change requires draining retained execution: worker %q has %d active or parked %s; catalog change touches %s; retry when idle",
		workerID, live, noun, areas)
}

// appendPreviousRevision records that packages of previous stay executable
// after a capacity-only change to current.
func appendPreviousRevision(revisions []string, previous, current string) []string {
	result := slices.DeleteFunc(slices.Clone(revisions), func(r string) bool { return r == previous || r == current })
	result = append(result, previous)
	if len(result) > maxPreviousCatalogRevisions {
		result = result[len(result)-maxPreviousCatalogRevisions:]
	}
	return result
}

// catalogRevisionSet is the catalog revisions a driver executes packages of:
// the current one and the capacity-only predecessors whose packages may still
// be live. It changes in place when a busy worker adopts a capacity change.
type catalogRevisionSet struct {
	mu       sync.RWMutex
	current  string
	previous []string
}

func (d *LocalDriver) acceptsCatalogRevision(revision string) bool {
	if d.revisions == nil {
		return revision == d.Config.CatalogRevision || slices.Contains(d.Config.CompatibleCatalogRevisions, revision)
	}
	d.revisions.mu.RLock()
	defer d.revisions.mu.RUnlock()
	return revision == d.revisions.current || slices.Contains(d.revisions.previous, revision)
}

func (d *LocalDriver) adoptCatalogRevision(current string, previous []string) {
	if d.revisions == nil {
		d.revisions = &catalogRevisionSet{}
	}
	d.revisions.mu.Lock()
	defer d.revisions.mu.Unlock()
	d.revisions.current = current
	d.revisions.previous = slices.Clone(previous)
}

// adoptCapacity changes what this runtime reports about its configured
// capacity, and the catalog revision that names it, without touching any
// attempt.
func (r *Runtime) adoptCapacity(revision string, class domain.CPUClass, allocatable domain.AllocatableCapacity) {
	for _, inventory := range []*domain.WorkerInventory{&r.desiredInventory, &r.config.Inventory} {
		inventory.CatalogRevision = revision
		inventory.CPUClass = class
		inventory.Allocatable = allocatable
	}
}

// canAdoptCatalogInPlace reports whether adoptCapacityCatalog can change this
// service; a runtime over another driver is replaced instead.
func (s *WorkerService) canAdoptCatalogInPlace() bool {
	if s == nil || s.Exchange.Runtime == nil {
		return false
	}
	_, ok := s.Exchange.Runtime.driver.(*LocalDriver)
	return ok
}

// adoptCapacityCatalog applies a capacity-only catalog change to the running
// service in place: execution packages, the journal and every assignment stay
// as they are, and the new capacity is what the next snapshot reports.
func (s *WorkerService) adoptCapacityCatalog(projection CatalogProjection, previous []string) error {
	runtime := s.Exchange.Runtime
	if runtime == nil {
		return errors.New("worker service has no runtime")
	}
	driver, ok := runtime.driver.(*LocalDriver)
	if !ok {
		return errors.New("worker runtime cannot adopt a catalog in place")
	}
	driver.adoptCatalogRevision(projection.Revision, previous)
	runtime.adoptCapacity(projection.Revision, domain.CPUClass(projection.Worker.CPUClass), domain.AllocatableCapacity{
		ExecutorSlots: projection.Worker.Executors.Slots,
		CPUUnits:      projection.Worker.Executors.CPUUnits,
		MemoryMB:      projection.Worker.Executors.MemoryMB,
		ScratchMB:     projection.Worker.Executors.ScratchMB,
	})
	return nil
}
