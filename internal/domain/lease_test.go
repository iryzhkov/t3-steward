package domain

import (
	"strings"
	"testing"
	"time"
)

func TestLeaseValidation(t *testing.T) {
	valid := LeaseRequest{Action: "acquire", Name: "repo:Steward/main", OwnerThread: "thread", Principal: "admin", Reason: "integrate", RequestID: "one"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []LeaseRequest{}
	for _, name := range []string{"", "deploy:fleet/window", "other:name", "repo:", "repo:main", "release:", "repo:s/main x", "repo:" + strings.Repeat("a", 124) + "/main"} {
		r := valid
		r.Name = name
		cases = append(cases, r)
	}
	for _, ttl := range []time.Duration{-1, time.Minute, 12*time.Hour + 1} {
		r := valid
		r.TTL = ttl
		cases = append(cases, r)
	}
	r := valid
	r.Reason = ""
	cases = append(cases, r)
	r = valid
	r.Plan = strings.Repeat("界", 257)
	cases = append(cases, r)
	r = valid
	r.OwnerThread = " "
	cases = append(cases, r)
	r = valid
	r.Action = "renew"
	r.Token = 0
	cases = append(cases, r)
	r = valid
	r.Action = "release"
	r.Force = true
	r.Reason = ""
	cases = append(cases, r)
	r = valid
	r.Action = "bad"
	cases = append(cases, r)
	for i, r := range cases {
		if err := r.Validate(); err == nil {
			t.Fatalf("case %d accepted %+v", i, r)
		}
	}
	for _, action := range []string{"check", "show", "list"} {
		r := valid
		r.Action = action
		if action == "list" {
			r.Name = ""
		}
		if r.Mutating() {
			t.Fatal(action + " is mutation")
		}
	}
}
