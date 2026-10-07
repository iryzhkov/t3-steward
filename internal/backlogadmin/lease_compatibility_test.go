package backlogadmin

import (
	"errors"
	"strings"
	"testing"
)

func TestLeaseCompatibilityStrictDecodeIsSpecific(t *testing.T) {
	for _, message := range []string{
		"decode local admin frame: json: unknown field \"lease\"",
		"decode local admin frame: json: unknown field \"leases\"",
		"decode local admin frame: json: unknown field \"reason\"",
		"decode response: json: unknown field \"lease\"",
		"unreachable",
	} {
		original := &TransportError{Class: ClassProtocol, Operation: "lease", Coordinator: "old", Err: errors.New(message)}
		got := leaseCompatibilityError(original)
		wantUpgrade := message == "decode local admin frame: json: unknown field \"lease\""
		if wantUpgrade {
			var transport *TransportError
			if !errors.As(got, &transport) || transport.Class != original.Class || transport.Operation != original.Operation || transport.Coordinator != original.Coordinator || !strings.Contains(got.Error(), "the coordinator does not support leases; upgrade it") {
				t.Fatalf("lost upgrade guidance or transport metadata: %v", got)
			}
			if original.Err.Error() != message {
				t.Fatal("mutated original error")
			}
		} else if got != original {
			t.Fatalf("rewrote unrelated error %q: %v", message, got)
		}
	}
	if leaseCompatibilityError(nil) != nil {
		t.Fatal("rewrote success")
	}
}
