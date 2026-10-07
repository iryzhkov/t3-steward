package workerproto

import (
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
