package workerproto

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestWorkerOwnedGatePackageContract(t *testing.T) {
	p := validPackage()
	p.Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}
	if err := ValidateExecutionPackage(p); err == nil {
		t.Fatal("gate without required capability accepted")
	}
	p.RequiredCapabilities = []string{PackageCapabilityWorkerOwnedGate}
	if err := ValidateExecutionPackage(p); err != nil {
		t.Fatal(err)
	}
	p.Gate.Timeout = 11 * time.Minute
	if err := ValidateExecutionPackage(p); err == nil {
		t.Fatal("gate exceeding configured maximum accepted")
	}
}

func TestGateCacheOriginsPackageContract(t *testing.T) {
	p := validPackage()
	p.GateCacheOrigins = []string{"attempt-1"}
	if err := ValidateExecutionPackage(p); err == nil {
		t.Fatal("gate cache origins without a gate accepted")
	}
	p.Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}
	p.RequiredCapabilities = []string{PackageCapabilityWorkerOwnedGate}
	if err := ValidateExecutionPackage(p); err != nil {
		t.Fatal(err)
	}
	for name, origins := range map[string][]string{
		"duplicate": {"attempt-1", "attempt-1"},
		"empty":     {""},
		"padded":    {" attempt-1"},
		"too many":  make([]string, MaxGateCacheOrigins+1),
	} {
		if name == "too many" {
			for index := range origins {
				origins[index] = fmt.Sprintf("attempt-%d", index)
			}
		}
		p.GateCacheOrigins = origins
		if err := ValidateExecutionPackage(p); err == nil {
			t.Fatalf("%s gate cache origins accepted", name)
		}
	}
}
