package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"testing"
	"time"
)

func TestQuotaSubmissionRejectsFutureObservation(t *testing.T) {
	a, s, r := quotaSubmissionFixture()
	s.States[0].ObservedAt = s.States[0].ObservedAt.Add(time.Minute)
	if _, err := a.evaluate(r, s); err == nil {
		t.Fatal("future observation accepted despite planner freshness rule")
	}
}

func TestQuotaSubmissionRejectsAggregateEstimateOverflow(t *testing.T) {
	a, s, r := quotaSubmissionFixture()
	r.Attempts = nil
	cost := math.MaxFloat64
	r.Tasks = []domain.Task{{ID: "one", EstimatedCost: &cost, Routes: r.Tasks[0].Routes}, {ID: "two", EstimatedCost: &cost, Routes: r.Tasks[0].Routes}}
	if _, err := a.evaluate(r, s); err == nil {
		t.Fatal("overflowed aggregate ceiling accepted")
	}
}
func TestQuotaSubmissionRejectsAmbiguousImplicitPool(t *testing.T) {
	a, s, r := quotaSubmissionFixture()
	duplicate := s.Records.QuotaPools[0]
	duplicate.ID = "duplicate"
	s.Records.QuotaPools = append(s.Records.QuotaPools, duplicate)
	r.Tasks[0].Routes[0].QuotaPoolID = ""
	if _, err := a.evaluate(r, s); err == nil {
		t.Fatal("ambiguous implicit quota pool accepted")
	}
}
