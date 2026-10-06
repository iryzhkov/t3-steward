package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
	"time"
)

func TestGateCollectionBudgetIncludesVerifyAndGate(t *testing.T) {
	r := &Runtime{}
	r.config.FinalizationTimeout = time.Minute
	p := workerproto.ExecutionPackage{Verification: []string{"one", "two"}, Gate: &domain.TaskGate{Commands: []string{"gate", "gate2"}, Timeout: 45 * time.Minute}, Limits: workerproto.ExecutionLimits{VerificationTimeout: 30 * time.Minute}}
	got := r.finalizationTimeout(AttemptRecord{Package: workerproto.ExecutionPackageManifest{Package: p}})
	if got != 150*time.Minute {
		t.Fatalf("budget %s", got)
	}
	task, _ := packageRecords(p, time.Now())
	if task.Gate == nil || task.Gate.Timeout != 45*time.Minute {
		t.Fatalf("lost gate %+v", task)
	}
}
