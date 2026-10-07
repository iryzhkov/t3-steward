package domain

import (
	"strings"
	"testing"
)

// Self-review of fix round 1: a padded or whitespace-only plan rendered as
// "(plan    , ...)", and line or paragraph separators and bidi overrides passed
// the control-character check into the refusal text other threads read.
func TestLeaseTextRejectsPaddingAndInvisibleSeparators(t *testing.T) {
	valid := LeaseRequest{Action: "acquire", Name: "repo:Steward/main", OwnerThread: "thread", Principal: "admin", Reason: "integrate", RequestID: "one"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*LeaseRequest){
		"blank plan":           func(r *LeaseRequest) { r.Plan = "   " },
		"padded plan":          func(r *LeaseRequest) { r.Plan = " jocasta:x@1 " },
		"line separator plan":  func(r *LeaseRequest) { r.Plan = "x refused: y" },
		"bidi override plan":   func(r *LeaseRequest) { r.Plan = "x‮y" },
		"paragraph sep reason": func(r *LeaseRequest) { r.Reason = "a b" },
		"zero width reason":    func(r *LeaseRequest) { r.Reason = "a​b" },
	} {
		r := valid
		mutate(&r)
		if err := r.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	r := valid
	r.Plan, r.Reason = "jocasta:abc@3", `rc.116 "integrate", día 2`
	if err := r.Validate(); err != nil {
		t.Fatalf("ordinary plan and reason refused: %v", err)
	}
}

func TestLeaseNameRefusalHasNoTrailingPunctuation(t *testing.T) {
	err := ValidateLeaseName("branch:x")
	if err == nil {
		t.Fatal("unknown prefix accepted")
	}
	if message := err.Error(); strings.HasSuffix(message, ":") || strings.HasSuffix(message, ".") || !strings.Contains(message, "repo:") || !strings.Contains(message, "release:") {
		t.Fatalf("unclear or punctuated refusal: %q", message)
	}
}
