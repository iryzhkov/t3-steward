package compat

import "testing"

func TestCheck(t *testing.T) {
	cases := map[string]Status{
		MinServerVersion:       Supported,
		"v" + MaxServerVersion: Supported,
		"0.0.1":                TooOld,
		"9.9.9":                Untested,
		"garbage":              Unknown,
		"0.0.38-beta.1":        Supported,
	}
	for v, want := range cases {
		if got := Check(v); got != want {
			t.Errorf("%s: got %s want %s", v, got, want)
		}
	}
	if ok, _ := ControlAllowed("9.9.9", false); ok {
		t.Fatal("untested version allowed without override")
	}
	if ok, _ := ControlAllowed("9.9.9", true); !ok {
		t.Fatal("override ignored")
	}
}
