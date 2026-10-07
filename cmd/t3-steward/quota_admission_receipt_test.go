package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Campaign show --json publishes this existing WorkflowDetail projection.
// Keeping the receipt on WorkflowRun makes it available without CLI plumbing.
func TestCampaignShowJSONContainsQuotaAdmissionReceipt(t *testing.T) {
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	detail := backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run", QuotaAdmission: &domain.QuotaAdmissionReceipt{AdmittedAt: at, Pools: []domain.QuotaAdmissionPoolReceipt{{PoolID: "pool", RootCost: 3, CampaignBudgetCeiling: 12, Decision: "admitted"}}, Roots: []domain.QuotaAdmissionRootReceipt{{TaskID: "root", Route: domain.ProviderRoute{QuotaPoolID: "pool"}, Cost: 3, Decision: "admitted"}}}}}}
	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Summary struct {
			Run struct {
				QuotaAdmission *domain.QuotaAdmissionReceipt `json:"quotaAdmission"`
			} `json:"run"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	receipt := document.Summary.Run.QuotaAdmission
	if receipt == nil || !receipt.AdmittedAt.Equal(at) || len(receipt.Pools) != 1 || receipt.Pools[0].CampaignBudgetCeiling != 12 || len(receipt.Roots) != 1 || receipt.Roots[0].Route.QuotaPoolID != "pool" {
		t.Fatalf("campaign JSON lost receipt: %s", raw)
	}
}
