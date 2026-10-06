package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestResourceCheckRenderingReportsTelemetryAndSelection(t *testing.T) {
	var document campaignCheck
	if err := json.Unmarshal([]byte(`{"name":"resources","matrix":{"outcome":"ready","tasks":[{"task":"build","outcome":"ready","selectedWorker":"worker-a","candidates":[{"worker":"worker-a","outcome":"ready","resourceEvaluation":{"workerId":"worker-a","state":"known","rank":1,"score":1.5,"cpuHeadroom":0.75,"memoryHeadroom":0.75,"telemetry":{"cpu_count":8,"load_1":2,"load_5":1,"memory_available_mb":8192,"swap_used_mb":0,"workspace_free_mb":20000,"temp_free_mb":20000,"running_attempts":1}}}]}]}}`), &document); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := renderCampaignCheck(&out, document); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"selected worker: worker-a", "telemetry: known", "rank 1", "cpu headroom 0.750", "memory headroom 0.750", "memory_available_mb"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("check lacks %q: %s", want, out.String())
		}
	}
}

func TestResourceExplainRenderingReportsPlacement(t *testing.T) {
	var explanation backlogadmin.Explanation
	if err := json.Unmarshal([]byte(`{"workflowRunId":"run","taskId":"build","placement":{"selectedWorkerId":"worker-a","resourceEvaluations":[{"workerId":"worker-a","state":"unknown","rank":1}],"rejections":[{"workerId":"worker-b","code":"resource-memory","detail":"memory below reserve"}]}}`), &explanation); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	renderExplanation(&out, &explanation)
	for _, want := range []string{"selected worker: worker-a", "worker-a telemetry: unknown", "worker-b resource-memory: memory below reserve"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("explain lacks %q: %s", want, out.String())
		}
	}
}
