package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestM8ReviewSubmissionAuditRecordsRegisterOnly(t *testing.T) {
	for _, register := range []bool{false, true} {
		t.Run(fmt.Sprint(register), func(t *testing.T) {
			service := gateService(t)
			var audits []SubmissionAudit
			service.Audit = func(_ context.Context, a SubmissionAudit) { audits = append(audits, a) }
			request := DirectorySubmission{IdempotencyKey: "audit-mode", BundleDir: validBundle(t)}
			if register {
				if err := json.Unmarshal([]byte(`{"RegisterOnly":true}`), &request); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.SubmitDirectory(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if len(audits) != 1 {
				t.Fatalf("got %d audits", len(audits))
			}
			raw, err := json.Marshal(audits[0])
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["RegisterOnly"] != register {
				t.Fatalf("audit does not record mode %t: %s", register, raw)
			}
			if _, err := service.SubmitDirectory(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if len(audits) != 1 {
				t.Fatalf("replay produced an extra audit: %d", len(audits))
			}
		})
	}
}
