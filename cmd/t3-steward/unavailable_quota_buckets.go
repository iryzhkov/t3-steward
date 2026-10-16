package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type unavailableQuotaBuckets struct{}

func (unavailableQuotaBuckets) ListBuckets(context.Context) ([]domain.BucketState, error) {
	return nil, errors.New("local watchdog state is unavailable")
}
