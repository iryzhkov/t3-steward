package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

const nodeSummaryRecordLimit = 256 * 1024

type nodeSummaryKV interface {
	GetKV(context.Context, string) (string, bool, error)
	SetKV(context.Context, string, string) error
}

// recordNodeSummary uses the daemon's local store; the runner logs a failure
// once for the claimed wake and continues sending it.
func recordNodeSummary(store nodeSummaryKV) func(context.Context, string, wait.WakeSummary) error {
	return func(ctx context.Context, id string, summary wait.WakeSummary) error {
		data, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		if len(data) > nodeSummaryRecordLimit {
			return fmt.Errorf("node wake summary exceeds %d bytes", nodeSummaryRecordLimit)
		}
		return store.SetKV(ctx, "wake-summary/"+id, string(data))
	}
}

func recordedNodeSummary(store nodeSummaryKV) func(context.Context, string) (*wait.WakeSummary, error) {
	return func(ctx context.Context, id string) (*wait.WakeSummary, error) {
		data, ok, err := store.GetKV(ctx, "wake-summary/"+id)
		if err != nil || !ok {
			return nil, err
		}
		if len(data) > nodeSummaryRecordLimit {
			return nil, fmt.Errorf("recorded node summary exceeds %d bytes", nodeSummaryRecordLimit)
		}
		var summary wait.WakeSummary
		if err := json.Unmarshal([]byte(data), &summary); err != nil {
			return nil, fmt.Errorf("decode recorded node summary: %w", err)
		}
		return &summary, nil
	}
}
