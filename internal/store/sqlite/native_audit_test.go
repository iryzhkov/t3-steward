package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func assertNativeAuditEvent(t *testing.T, store *Store, id, actor, outcome string) {
	t.Helper()
	events, err := store.LoadAuditEvents(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.ID != id {
			continue
		}
		if event.Sequence < 1 || event.TargetType == "" || event.TargetID == "" ||
			event.Actor != actor || strings.TrimSpace(event.Reason) == "" || event.CreatedAt.IsZero() {
			t.Fatalf("incomplete native audit event %q: %+v", id, event)
		}
		var detail nativeAuditDetail
		if err := json.Unmarshal(event.Detail, &detail); err != nil {
			t.Fatalf("decode native audit event %q: %v", id, err)
		}
		if detail.IdempotencyIdentity == "" || detail.Outcome != outcome {
			t.Fatalf("incomplete native audit detail %q: %+v", id, detail)
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{
			"lease-token-1", "dispatch-token-1", "lease-1", "dispatch-1",
			"/runs/", "resolved-secret", "credential-secret",
		} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("native audit event %q exposed %q: %s", id, secret, raw)
			}
		}
		return
	}
	t.Fatalf("native audit event %q not found in %+v", id, events)
}
