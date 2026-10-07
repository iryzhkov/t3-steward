package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWorkflowRunWithoutQuotaAdmissionRetainsJSON(t *testing.T) {
	got, err := json.Marshal(WorkflowRun{})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"","workflowId":"","progress":"","revision":0,"createdAt":"0001-01-01T00:00:00Z","updatedAt":"0001-01-01T00:00:00Z"}`
	if string(got) != want {
		t.Fatalf("legacy run JSON changed: %s", got)
	}
}

func TestWorkflowRunQuotaAdmissionReceiptRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC)
	run := WorkflowRun{ID: "run", QuotaAdmission: &QuotaAdmissionReceipt{AdmittedAt: at, Pools: []QuotaAdmissionPoolReceipt{{PoolID: "pool", RootCost: 3, AlreadyAdmittedDemand: 2, CampaignBudgetCeiling: 9, Decision: "admitted"}}, Roots: []QuotaAdmissionRootReceipt{{TaskID: "root", TaskName: "root task", Cost: 3, Route: ProviderRoute{QuotaPoolID: "pool"}, Decision: "admitted"}}}}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	var decoded WorkflowRun
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.QuotaAdmission == nil || !decoded.QuotaAdmission.AdmittedAt.Equal(at) || decoded.QuotaAdmission.Pools[0].CampaignBudgetCeiling != 9 || decoded.QuotaAdmission.Roots[0].Route.QuotaPoolID != "pool" {
		t.Fatalf("receipt lost: %s", raw)
	}
}
