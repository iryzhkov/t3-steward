package backlog

import (
	"fmt"
)

// Legacy route grants are unchanged. Explicit profiles freeze only grants for
// their exact quota pool, project, instance and authorized model; no live state.
func admissionMemberMetadata(c AdmissionCatalog, project string, m AdmissionMember) (AdmissionClassification, []admissionGrant, error) {
	metadata, grants, err := admissionRouteMetadata(c, project, m.Route)
	if err != nil || m.Execution == nil {
		return metadata, grants, err
	}
	if err := m.Execution.Validate(); err != nil {
		return AdmissionClassification{}, nil, err
	}
	selected := make([]admissionGrant, 0, len(grants))
	for _, g := range grants {
		if g.QuotaPoolID == m.Execution.QuotaPoolID {
			selected = append(selected, g)
		}
	}
	if len(selected) == 0 {
		return AdmissionClassification{}, nil, fmt.Errorf("review route %q: quota pool %q has no exact authored eligible grant", m.Route, m.Execution.QuotaPoolID)
	}
	return metadata, selected, nil
}

func cloneManifestReview(r *ManifestReviewRequirements) *ManifestReviewRequirements {
	if r == nil {
		return nil
	}
	copied := *r
	copied.Members = append([]ManifestReviewMember(nil), r.Members...)
	for i, m := range r.Members {
		if m.Execution == nil {
			continue
		}
		e := *m.Execution
		if e.Resources != nil {
			resources := effectiveResources(ManifestResources{}, *e.Resources)
			e.Resources = &resources
		}
		copied.Members[i].Execution = &e
	}
	return &copied
}
