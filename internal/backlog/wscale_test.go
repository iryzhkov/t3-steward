package backlog

import "testing"

func TestWindowScale(t *testing.T) {
	cases := map[string]float64{
		"five_hour":                  1,
		"primary":                    1,
		"seven_day":                  5.0 / 168.0,
		"seven_day_opus":             5.0 / 168.0,
		"seven_day_overage_included": 5.0 / 168.0,
		"secondary":                  5.0 / 168.0,
		"spending":                   1,
		"":                           1,
	}
	for w, want := range cases {
		if got := WindowScale(w); got != want {
			t.Errorf("WindowScale(%q) = %v, want %v", w, got, want)
		}
	}
}

func TestIgnoredWindow(t *testing.T) {
	r := &Runner{opts: Options{IgnoreWindows: []string{"*overage*", ""}}}
	for _, w := range []string{"seven_day_overage_included", "overage", "OVERAGE"} {
		if !r.ignoredWindow(w) {
			t.Errorf("ignoredWindow(%q) = false, want true", w)
		}
	}
	for _, w := range []string{"seven_day", "five_hour", "primary"} {
		if r.ignoredWindow(w) {
			t.Errorf("ignoredWindow(%q) = true, want false", w)
		}
	}
	none := &Runner{opts: Options{}}
	if none.ignoredWindow("seven_day_overage_included") {
		t.Error("empty IgnoreWindows must ignore nothing")
	}
}
