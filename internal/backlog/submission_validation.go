package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type validatedManifestAdmission struct {
	digest     string
	selections map[string]domain.RoleSelection
}

func (v validatedManifestAdmission) ResolveManifestRoles(ctx context.Context, m Manifest) (map[string]domain.RoleSelection, error) {
	if err := v.ValidatePermanent(ctx, m); err != nil {
		return nil, err
	}
	return v.selections, nil
}

func (v validatedManifestAdmission) ValidatePermanent(_ context.Context, m Manifest) error {
	if v.digest != admissionDigest(m) {
		return fmt.Errorf("submission manifest changed after permanent configured validation; resubmit immutable bundle")
	}
	return nil
}
