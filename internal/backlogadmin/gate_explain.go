package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"io"
)

// Gate evidence is separately authorized artifact content, just like retrieval
// through backlog artifact get. Unavailable content never hides task status.
func (s *Service) addGateExplanation(ctx context.Context, principal Principal, explanation *Explanation) {
	if explanation.GateArtifactID == "" {
		return
	}
	content, err := s.OpenArtifact(ctx, principal, explanation.GateArtifactID)
	if err != nil {
		explanation.Details = append(explanation.Details, "worker gate evidence content unavailable")
		return
	}
	raw, readErr := io.ReadAll(io.LimitReader(content.Content, backlog.GateEvidenceMaxBytes+1))
	closeErr := content.Content.Close()
	if readErr != nil || closeErr != nil || len(raw) > backlog.GateEvidenceMaxBytes {
		explanation.Details = append(explanation.Details, "worker gate evidence content unavailable")
		return
	}
	var report backlog.GateReport
	if err := json.Unmarshal(raw, &report); err != nil {
		explanation.Details = append(explanation.Details, "worker gate evidence content invalid")
		return
	}
	explanation.Gate = &report
	explanation.Details = append(explanation.Details, fmt.Sprintf("worker gate: passed=%t attempt=%s tree=%s log=%s", report.Passed, report.Attempt, report.TreeHash, report.LogArtifact))
}
