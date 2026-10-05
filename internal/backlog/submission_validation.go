package backlog

import (
	"context"
	"fmt"
)

type validatedManifestAdmission struct{ digest string }

func (v validatedManifestAdmission) ValidatePermanent(_ context.Context, m Manifest) error {
	if v.digest != admissionDigest(m) {
		return fmt.Errorf("submission manifest changed after permanent configured validation; resubmit immutable bundle")
	}
	return nil
}
