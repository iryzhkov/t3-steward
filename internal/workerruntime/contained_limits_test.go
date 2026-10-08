package workerruntime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

func TestContainedLaunchLimitsComeFromThePackageDemand(t *testing.T) {
	pkg := testPackage()
	if got := containedLimits(pkg); got != nil {
		t.Fatalf("package without demand got limits %+v", got)
	}
	pkg.ResourceDemand = &domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}
	if got, want := containedLimits(pkg), providercontainment.LimitsFor(*pkg.ResourceDemand); !reflect.DeepEqual(got, want) || got == nil {
		t.Fatalf("limits %+v, want %+v", got, want)
	}
}

func TestContainedPreparationRefusesChangedLimits(t *testing.T) {
	verifier, _ := containedVerificationFixture(t)
	if _, err := verifier.manager.preparation(verifier.pkg); err != nil {
		t.Fatalf("unsized preparation refused: %v", err)
	}
	// The recorded preparation was unsized; a package that now carries a
	// demand must not adopt it and run without the limits it promises.
	sized := verifier.pkg
	sized.ResourceDemand = &domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}
	if _, err := verifier.manager.preparation(sized); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("preparation with different limits accepted: %v", err)
	}
}

func TestContainedObservationNamesTheMemoryReservation(t *testing.T) {
	failure := "contained run exceeded its 6000 MB memory reservation"
	manager := ContainedT3{observe: func(context.Context, providercontainment.Launch) (providercontainment.SupervisorObservation, error) {
		return providercontainment.SupervisorObservation{State: "failed/failed", Failure: failure}, nil
	}}
	err := manager.observation(context.Background(), ContainedAttachment{InvocationID: "invocation"})
	if err == nil || !strings.Contains(err.Error(), failure) {
		t.Fatalf("observation error %v does not name the reservation", err)
	}
	if got := containedFailure(providercontainment.SupervisorObservation{State: "failed/failed", Failure: failure}); got == nil || errors.Is(got, ErrContainedCustody) || !strings.Contains(got.Error(), failure) {
		t.Fatalf("custody error %v", got)
	}
	if got := containedFailure(providercontainment.SupervisorObservation{State: "inactive/dead"}); !errors.Is(got, ErrContainedCustody) || got.Error() != ErrContainedCustody.Error()+": supervisor is inactive/dead" {
		t.Fatalf("plain custody error changed: %v", got)
	}
}
