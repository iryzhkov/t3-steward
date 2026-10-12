package campaign

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func sizeUnit(code, tests string) []byte {
	return []byte("## Unit\n### Code map pinned ref\n" + code + "\n### Tests to add\n" + tests + "\n### End\n- ignored\n")
}
func TestCompileSizeBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, code, tests string
		raw, estimate     int64
		invalid           bool
	}{
		{"sum", "- small [changed-lines: 1]\n- large [changed-lines: 5]\n- tie [changed-lines: 5]", "- tests [changed-lines: 2]", 13, 17, false},
		{"missing", "- code", "- test [changed-lines: 1]", 0, 0, true},
		{"malformed", "- code [changed-lines: one]", "- test [changed-lines: 1]", 0, 0, true},
		{"zero", "- code [changed-lines: 0]", "- test [changed-lines: 1]", 0, 0, true},
		{"negative", "- code [changed-lines: -1]", "- test [changed-lines: 1]", 0, 0, true},
		{"fraction", "- code [changed-lines: 1.5]", "- test [changed-lines: 1]", 0, 0, true},
		{"value overflow", "- code [changed-lines: 9223372036854775808]", "- test [changed-lines: 1]", 0, 0, true},
		{"sum overflow", "- code [changed-lines: 9223372036854775807]", "- test [changed-lines: 1]", 0, 0, true},
		{"ceil overflow", "- code [changed-lines: 9223372036854775806]", "- test [changed-lines: 1]", 0, 0, true},
		{"duplicate", "- same [changed-lines: 1]", "-   same [changed-lines: 2]", 0, 0, true},
		{"suffix", "- code [changed-lines: 1] extra", "- test [changed-lines: 1]", 0, 0, true},
		{"fences", "- code [changed-lines: 1]\n~~~md\n### Tests to add\n- bad [changed-lines: nope]\n~~~", "- test [changed-lines: 1]", 2, 3, false},
		{"wrapped", "- code spans\n  two lines [changed-lines: 1]", "- test [changed-lines: 1]", 2, 3, false},
		{"subsection", "#### Detail\n* code [changed-lines: 1]", "1. test [changed-lines: 3]", 4, 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := sizeUnit(tc.code, tc.tests)
			before := bytes.Clone(input)
			got := EstimateCompileSize(input, SizeCalibration{})
			if !got.Available || (len(got.Diagnostics) > 0) != tc.invalid || !bytes.Equal(input, before) {
				t.Fatalf("%+v", got)
			}
			if !tc.invalid && (got.Raw != tc.raw || got.Estimate != tc.estimate || got.Multiplier != 1.25 || got.Label != "uncalibrated" || got.SampleCount != 0) {
				t.Fatalf("%+v", got)
			}
			if tc.name == "sum" && (got.Contributors[0].Text != "large" || got.Contributors[1].Text != "tie" || got.Contributors[0].Line != 4) {
				t.Fatalf("%+v", got.Contributors)
			}
		})
	}
	if got := EstimateCompileSize([]byte("### Tests to add\n- first [changed-lines: 1]\n### Code map\n- second [changed-lines: 1]"), SizeCalibration{}); len(got.Diagnostics) > 0 || got.Contributors[0].Text != "first" {
		t.Fatalf("%+v", got)
	}
	for _, input := range []string{"legacy", "~~~\n[changed-lines: nope]\n~~~", "```\n[changed-lines: 1]\n```"} {
		if got := EstimateCompileSize([]byte(input), SizeCalibration{}); got.Available || len(got.Diagnostics) > 0 {
			t.Fatalf("%+v", got)
		}
	}
	for _, input := range []string{"[changed-lines: nope]", "### Code map\n- x [changed-lines: 1]", "### Tests to add\n- x [changed-lines: 1]"} {
		if got := EstimateCompileSize([]byte(input), SizeCalibration{}); !got.Available || len(got.Diagnostics) == 0 || !strings.Contains(got.Diagnostics[0], "line") {
			t.Fatalf("%+v", got)
		}
	}
	raw := bytes.ReplaceAll(readCompileFixture(t), []byte("List the runs a thread owns."), bytes.TrimPrefix(sizeUnit("- other [changed-lines: 5]", "- test [changed-lines: 1]"), []byte("## Unit\n")))
	plan, err := ParseCompilePlan("plan.md", raw)
	if err != nil {
		t.Fatal(err)
	}
	if EstimateCompileSize([]byte(compiledFile(t, plan.Units[0], "inputs/unit.md")), SizeCalibration{}).Available || !EstimateCompileSize([]byte(compiledFile(t, plan.Units[1], "inputs/unit.md")), SizeCalibration{}).Available {
		t.Fatal("unit annotations leaked")
	}
}
func TestSizeCalibration(t *testing.T) {
	sample := sizeSample{Raw: 1, Observed: 1, Base: strings.Repeat("a", 40), Head: strings.Repeat("b", 40), AcceptRef: "retained ACCEPT", GateRef: "passing gate"}
	encode := func(samples []sizeSample) []byte {
		raw, err := json.Marshal(map[string]any{"samples": samples})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, tc := range []struct {
		raw      int64
		ratios   []int64
		want     int64
		overflow bool
	}{
		{1, []int64{}, 3, false}, {1, []int64{5, 1, 4, 2, 3}, 10, false},
		{1, []int64{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}, 18, false}, {2, []int64{1}, 2, false},
		{1, []int64{1000}, 2000, false}, {1, []int64{math.MaxInt64}, 0, true},
	} {
		samples := make([]sizeSample, len(tc.ratios))
		for i, n := range tc.ratios {
			samples[i] = sample
			samples[i].Raw = tc.raw
			samples[i].Observed = n
		}
		raw := encode(samples)
		before := bytes.Clone(raw)
		calibration, err := ParseSizeCalibration(raw)
		if err != nil {
			t.Fatal(err)
		}
		got := EstimateCompileSize(sizeUnit("- a [changed-lines: 1]", "- b [changed-lines: 1]"), calibration)
		label := "calibrated"
		if len(samples) == 0 {
			label = "uncalibrated"
		}
		if got.Estimate != tc.want || (len(got.Diagnostics) > 0) != tc.overflow || got.SampleCount != len(samples) || got.Label != label || !bytes.Equal(raw, before) {
			t.Fatalf("%+v", got)
		}
	}
	for _, field := range []string{"Raw", "Observed", "Base", "Head", "AcceptRef", "GateRef"} {
		bad := sample
		value := reflect.ValueOf(&bad).Elem().FieldByName(field)
		value.Set(reflect.Zero(value.Type()))
		if _, err := ParseSizeCalibration(encode([]sizeSample{bad})); err == nil {
			t.Fatal(field)
		}
	}
	valid := string(encode([]sizeSample{sample}))
	for _, raw := range []string{`{}`, `null`, `{"samples":null}`, `{"samples":[],"unknown":1}`, valid + " {}", strings.Replace(valid, `"raw":1`, `"raw":-1`, 1), strings.Replace(valid, `"raw":1`, `"raw":1.5`, 1), strings.Replace(valid, `"raw":1`, `"raw":9223372036854775808`, 1), strings.Replace(valid, strings.Repeat("a", 40), strings.Repeat("g", 40), 1), strings.Replace(valid, "retained ACCEPT", "  ", 1), strings.Replace(valid, `"raw":1`, `"extra":1,"raw":1`, 1), string(encode(make([]sizeSample, 1001))), strings.Repeat(" ", 1<<20) + valid} {
		if _, err := ParseSizeCalibration([]byte(raw)); err == nil {
			t.Fatalf("accepted %.80s", raw)
		}
	}
}
