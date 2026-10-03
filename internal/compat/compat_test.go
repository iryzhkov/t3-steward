package compat

import "testing"

// T3 counts a turn input as a JavaScript string: UTF-16 code units, not bytes
// and not runes.
func TestTurnInputLengthCountsUTF16CodeUnits(t *testing.T) {
	for text, want := range map[string]int{
		"":     0,
		"abc":  3,
		"é":    1,
		"日本":   2,
		"a😀b":  4,
		"\x00": 1,
	} {
		if got := TurnInputLength(text); got != want {
			t.Errorf("TurnInputLength(%q) = %d, want %d", text, got, want)
		}
	}
}

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
