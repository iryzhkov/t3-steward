package main

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// The upgrade fixture must satisfy the same Driver contract as production:
// local quota drains send first and permit a later stop without checkpoint data.
func TestM5RuntimeDriverSupportsLocalQuotaPause(t *testing.T) {
	var driver workerruntime.Driver = m5RuntimeDriver{}
	ctx := context.Background()
	pkg := workerproto.ExecutionPackage{}
	if err := driver.RequestQuotaDrain(ctx, pkg, domain.ThrottleCommand{}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := driver.ReadQuotaCheckpoint(ctx, pkg)
	if err != nil || checkpoint != nil {
		t.Fatalf("optional checkpoint: got %v, err %v", checkpoint, err)
	}
}
