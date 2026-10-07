package main

import (
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func resourceEvaluationText(evaluation domain.ResourceEvaluation) string {
	line := fmt.Sprintf("%s telemetry: %s; rank %d; score %.3f; cpu headroom %.3f; memory headroom %.3f", evaluation.WorkerID, evaluation.State, evaluation.Rank, evaluation.Score, evaluation.CPUHeadroom, evaluation.MemoryHeadroom)
	if evaluation.Telemetry != nil {
		if raw, err := json.Marshal(evaluation.Telemetry); err == nil {
			line += "; observed " + string(raw)
		}
	}
	return line
}
