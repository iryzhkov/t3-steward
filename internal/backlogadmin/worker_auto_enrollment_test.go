package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestViabilityIncludesAutomaticEnrollmentRefusal(t *testing.T) {
	for _, enrolled := range []bool{false, true} {
		v := viabilityView(t, func(v *view) {
			v.requirements[0].AutomaticEnrollment = &domain.AutomaticWorkerEnrollment{Reason: "automatic enrollment: missing huyang"}
			if enrolled {
				v.enrollments[0].Request.CatalogRevision = viabilityAcceptedDigest
			} else {
				v.enrollments = nil
			}
		})
		matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
		code := ReasonWorkerNotEligible
		if enrolled {
			code = ReasonCatalogDigestMismatch
		}
		reason, found := candidateReason(t, matrix, code)
		if !found || !strings.Contains(reason.Detail, "automatic enrollment: missing huyang") {
			t.Fatalf("refusal missing: %+v", matrix)
		}
	}
}
