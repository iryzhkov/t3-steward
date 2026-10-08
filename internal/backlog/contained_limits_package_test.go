package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type flakyLimitsInventory struct {
	packageRecordStore
	fail *bool
}

func (s flakyLimitsInventory) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	if *s.fail {
		return nil, errors.New("database is locked")
	}
	return []domain.WorkerSnapshot{{WorkerID: "normandy", Inventory: domain.WorkerInventory{Capabilities: []string{workerproto.PackageCapabilityContainedLimits}}}}, nil
}

// An unreadable inventory withholds a sized offer instead of building a
// package without the demand, which a later replay would then contradict.
func TestUnreadableInventoryWithholdsASizedOfferInsteadOfDroppingItsLimits(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	sized := domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}
	assignment.ExecutorDemand = &sized
	for index := range records.Assignments {
		if records.Assignments[index].ID == assignment.ID {
			records.Assignments[index] = assignment
		}
	}
	builder := packageBuilder(t, records)
	fail := true
	builder.Store = flakyLimitsInventory{packageRecordStore: builder.Store.(packageRecordStore), fail: &fail}
	if offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute)); err == nil {
		t.Fatalf("offer built without reading the inventory: demand %+v", offer.Package.Package.ResourceDemand)
	}
	fail = false
	first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil || first.Package.Package.ResourceDemand == nil {
		t.Fatalf("recovered offer: %+v %v", first.Package.Package.ResourceDemand, err)
	}
	replay, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
	if err != nil || !reflect.DeepEqual(first.Package, replay.Package) {
		t.Fatalf("replay changed the package: %v", err)
	}
}

func TestOfferCarriesTheAccountedDemandToAWorkerThatEnforcesIt(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	sized := domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, CPUUnits: 4, MemoryMB: 6000, ScratchMB: 100}
	build := func(t *testing.T, executor *domain.ResourceDemand, placement *domain.ResourceDemand, capabilities map[string][]string) workerproto.AssignmentOffer {
		t.Helper()
		records, assignment := packageBuilderFixture(now)
		assignment.ExecutorDemand = executor
		if placement != nil {
			assignment.Placement = &domain.PlacementDecision{Demand: *placement, DecidedAt: now}
		}
		for index := range records.Assignments {
			if records.Assignments[index].ID == assignment.ID {
				records.Assignments[index] = assignment
			}
		}
		builder := packageBuilder(t, records)
		builder.WorkerCapabilities = capabilities
		first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		replay, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
		if err != nil || !reflect.DeepEqual(first.Package, replay.Package) {
			t.Fatalf("replay changed the package: %v", err)
		}
		if err := workerproto.ValidateExecutionPackageManifest(first.Package, 4<<20); err != nil {
			t.Fatal(err)
		}
		return first
	}
	enforcing := map[string][]string{"normandy": {workerproto.PackageCapabilityContainedLimits}}
	baseline := build(t, nil, nil, nil)

	t.Run("enforcing worker gets the executor demand", func(t *testing.T) {
		pkg := build(t, &sized, nil, enforcing).Package.Package
		if pkg.ResourceDemand == nil || !reflect.DeepEqual(*pkg.ResourceDemand, sized) {
			t.Fatalf("demand %+v", pkg.ResourceDemand)
		}
		if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityContainedLimits) {
			t.Fatalf("capabilities %q", pkg.RequiredCapabilities)
		}
	})
	t.Run("placement demand of an assignment without executor demand", func(t *testing.T) {
		pkg := build(t, nil, &sized, enforcing).Package.Package
		if pkg.ResourceDemand == nil || !reflect.DeepEqual(*pkg.ResourceDemand, sized) {
			t.Fatalf("demand %+v", pkg.ResourceDemand)
		}
	})
	t.Run("executor demand wins over placement evidence", func(t *testing.T) {
		larger := sized
		larger.MemoryMB = 9000
		pkg := build(t, &sized, &larger, enforcing).Package.Package
		if pkg.ResourceDemand == nil || pkg.ResourceDemand.MemoryMB != 6000 {
			t.Fatalf("demand %+v", pkg.ResourceDemand)
		}
	})
	for name, test := range map[string]struct {
		demand       *domain.ResourceDemand
		capabilities map[string][]string
	}{
		"older worker":            {&sized, map[string][]string{"normandy": {"internet"}}},
		"unknown worker":          {&sized, nil},
		"another worker enforces": {&sized, map[string][]string{"other": {workerproto.PackageCapabilityContainedLimits}}},
		"unsized demand":          {&domain.ResourceDemand{}, enforcing},
		"class only demand":       {&domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, ScratchMB: 10}, enforcing},
		"no demand evidence":      {nil, enforcing},
	} {
		t.Run(name, func(t *testing.T) {
			offer := build(t, test.demand, nil, test.capabilities)
			if !reflect.DeepEqual(offer.Package, baseline.Package) {
				t.Fatal("a package without enforced limits changed its content address")
			}
			data, _ := json.Marshal(offer.Package.Package)
			if strings.Contains(string(data), "resourceDemand") || strings.Contains(string(data), workerproto.PackageCapabilityContainedLimits) {
				t.Fatalf("legacy package gained limits wire fields: %s", data)
			}
		})
	}
}
