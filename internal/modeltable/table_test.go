package modeltable

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestShippedTable(t *testing.T) {
	table, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	wantEntries := []struct {
		route, effort string
		quality, cost float64
	}{
		{"codex/gpt-6.1-sol", "high", 50.2, 0.32},
		{"claudeAgent/claude-opus-5-5", "high", 53.6, 1.82},
		{"claudeAgent/claude-sonnet-5-5", "medium", 40.8, 0.48},
	}
	if len(table.Entries) != len(wantEntries) {
		t.Fatalf("entries = %d", len(table.Entries))
	}
	for i, want := range wantEntries {
		got := table.Entries[i]
		if got.Route != want.route || got.Effort != want.effort || *got.Quality != want.quality || *got.CostUSD != want.cost ||
			got.Source != "Artificial Analysis via Slopalytics" || got.Recorded != "2026-10-09" {
			t.Fatalf("entry %d = %+v", i, got)
		}
	}
	wantRoles := []Role{
		{Name: "plan", Preference: []Candidate{{"claudeAgent/claude-opus-5-5", "high"}, {"codex/gpt-6.1-sol", "high"}}},
		{Name: "execute", Preference: []Candidate{{"claudeAgent/claude-opus-5-5", "medium"}, {"codex/gpt-6.1-sol", "medium"}}},
		{Name: "review", Preference: []Candidate{{"claudeAgent/claude-opus-5-5", "medium"}, {"codex/gpt-6.1-sol", "medium"}}},
		{Name: "critical-review", Preference: []Candidate{{"claudeAgent/claude-fable-5-1", "medium"}, {"codex/gpt-6-astra", "medium"}}},
		{Name: "read", Preference: []Candidate{{"codex/gpt-6-luna", "high"}, {"claudeAgent/claude-haiku-5-5", "high"}}},
		{Name: "3d", Preference: []Candidate{{"codex/gpt-6-astra", "medium"}}},
	}
	if !reflect.DeepEqual(table.Roles, wantRoles) {
		t.Fatalf("roles = %+v", table.Roles)
	}
	// Callers receive independent data, including pointed-to metrics.
	*table.Entries[0].Quality = 0
	again, err := Load()
	if err != nil || *again.Entries[0].Quality != 50.2 {
		t.Fatalf("Load leaked mutation: %v", err)
	}
}

const valid = `schema: model-table/v1
entries:
  - route: codex/gpt-6.1-sol
    effort: high
    quality: 50.2
    cost_usd: 0.32
    source: Artificial Analysis via Slopalytics
    recorded: "2026-10-09"
roles:
  - name: plan
    preference:
      - {route: codex/gpt-6.1-sol, effort: high}
`

func TestParseValidation(t *testing.T) {
	for _, tc := range []struct{ name, raw, want string }{
		{"unknown top field", valid + "unknown: true\n", "field unknown"},
		{"unknown entry field", strings.Replace(valid, "quality: 50.2", "quality: 50.2\n    unknown: true", 1), "field unknown"},
		{"unknown role field", strings.Replace(valid, "name: plan", "name: plan\n    unknown: true", 1), "field unknown"},
		{"unknown preference field", strings.Replace(valid, "effort: high}", "effort: high, unknown: true}", 1), "field unknown"},
		{"duplicate yaml field", valid + "schema: model-table/v1\n", "already defined"},
		{"schema", strings.Replace(valid, "model-table/v1", "model-table/v2", 1), "unsupported"},
		{"multiple documents", valid + "---\n{}", "exactly one"},
		{"malformed yaml", "schema: [", "invalid model table"},
		{"empty", "", "invalid model table"},
		{"duplicate entries", strings.Replace(valid, "roles:", strings.Split(strings.Split(valid, "entries:\n")[1], "roles:")[0]+"roles:", 1), "duplicate entry"},
		{"missing quality", strings.Replace(valid, "    quality: 50.2\n", "", 1), "missing quality"},
		{"null quality", strings.Replace(valid, "quality: 50.2", "quality: null", 1), "missing quality"},
		{"negative quality", strings.Replace(valid, "quality: 50.2", "quality: -1", 1), "quality must be"},
		{"missing cost", strings.Replace(valid, "    cost_usd: 0.32\n", "", 1), "missing cost_usd"},
		{"null cost", strings.Replace(valid, "cost_usd: 0.32", "cost_usd: null", 1), "missing cost_usd"},
		{"negative cost", strings.Replace(valid, "cost_usd: 0.32", "cost_usd: -1", 1), "cost_usd must be"},
		{"nan quality", strings.Replace(valid, "quality: 50.2", "quality: .nan", 1), "quality must be"},
		{"infinite cost", strings.Replace(valid, "cost_usd: 0.32", "cost_usd: .inf", 1), "cost_usd must be"},
		{"missing source", strings.Replace(valid, "    source: Artificial Analysis via Slopalytics\n", "", 1), "source is required"},
		{"missing recorded", strings.Replace(valid, "    recorded: \"2026-10-09\"\n", "", 1), "recorded must be"},
		{"bad date", strings.Replace(valid, "2026-10-09", "2026-02-30", 1), "recorded must be"},
		{"no entries", "schema: model-table/v1\n", "at least one entry"},
		{"no roles", strings.Split(valid, "roles:")[0], "at least one role"},
		{"duplicate roles", valid + strings.Split(valid, "roles:\n")[1], "duplicate role"},
		{"bad role", strings.Replace(valid, "name: plan", "name: bad/name", 1), "invalid role"},
		{"floor low", strings.Replace(valid, "name: plan", "name: plan\n    floor: -1", 1), "floor must be"},
		{"floor high", strings.Replace(valid, "name: plan", "name: plan\n    floor: 101", 1), "floor must be"},
		{"floor nan", strings.Replace(valid, "name: plan", "name: plan\n    floor: .nan", 1), "floor must be"},
		{"no preference", strings.Split(valid, "    preference:")[0], "at least one preference"},
		{"duplicate preference", valid + "      - {route: codex/gpt-6.1-sol, effort: high}\n", "duplicate preference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCandidateValidation(t *testing.T) {
	for _, route := range []string{"model", "/model", "instance/", "instance/model name", strings.Repeat("a", 257) + "/model"} {
		for _, where := range []string{"entry", "preference"} {
			raw := strings.Replace(valid, "codex/gpt-6.1-sol", route, 1)
			if where == "preference" {
				raw = strings.Replace(valid, "{route: codex/gpt-6.1-sol", "{route: "+route, 1)
			}
			if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), "malformed route") {
				t.Fatalf("route %q: %v", route, err)
			}
		}
	}
	for _, effort := range []string{"", "xhigh", "max", "ultra"} {
		for _, where := range []string{"entry", "preference"} {
			raw := strings.Replace(valid, "effort: high\n", "effort: \""+effort+"\"\n", 1)
			if where == "preference" {
				raw = strings.Replace(valid, "effort: high}", "effort: \""+effort+"\"}", 1)
			}
			if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), "effort") {
				t.Fatalf("%s %q: %v", where, effort, err)
			}
		}
	}
	// This table intentionally rejects the installed policy's haiku exception.
	raw := strings.ReplaceAll(valid, "codex/gpt-6.1-sol", "claudeAgent/claude-haiku-5-5")
	raw = strings.ReplaceAll(raw, "effort: high", "effort: xhigh")
	if _, err := Parse([]byte(raw)); err == nil {
		t.Fatal("accepted haiku xhigh")
	}
	for _, effort := range []string{"low", "medium", "high"} {
		if _, err := Parse([]byte(strings.ReplaceAll(valid, "effort: high", "effort: "+effort))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOptionalFloorAndZeroMetrics(t *testing.T) {
	for _, floor := range []string{"0", "50.2", "100"} {
		raw := strings.Replace(valid, "name: plan", "name: plan\n    floor: "+floor, 1)
		table, err := Parse([]byte(raw))
		if err != nil || table.Roles[0].Floor == nil {
			t.Fatalf("floor %s: %v", floor, err)
		}
	}
	raw := strings.Replace(valid, "quality: 50.2", "quality: 0", 1)
	raw = strings.Replace(raw, "cost_usd: 0.32", "cost_usd: 0", 1)
	if _, err := Parse([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	table, _ := Load()
	inf := math.Inf(1)
	table.Roles[0].Floor = &inf
	if err := table.Validate(); err == nil {
		t.Fatal("accepted infinite floor")
	}
}
