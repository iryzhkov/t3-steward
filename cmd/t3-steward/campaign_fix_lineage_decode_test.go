package main

import (
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"strings"
	"testing"
)

func TestCampaignFixLineageRequiresExactFields(t *testing.T) {
	value := campaign.FixLineage{Schema: campaign.FixLineageSchema, RootRun: "root", RootProducingTask: "implement", RootReviewTask: "review", RoundLimit: 4, RoundsDeclared: 2}
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := decodeFixLineage(raw); e != nil {
		t.Fatal(e)
	}
	var fields map[string]json.RawMessage
	if e = json.Unmarshal(raw, &fields); e != nil {
		t.Fatal(e)
	}
	for key, body := range fields {
		delete(fields, key)
		missing, _ := json.Marshal(fields)
		fields[key] = body
		if _, e := decodeFixLineage(missing); e == nil {
			t.Fatalf("missing %s accepted", key)
		}
		if key != "gate" && key != "history" {
			fields[key] = json.RawMessage("null")
			null, _ := json.Marshal(fields)
			fields[key] = body
			if _, e := decodeFixLineage(null); e == nil {
				t.Fatalf("null %s accepted", key)
			}
		}
	}
	duplicate := strings.Replace(string(raw), `"roundsUsedBefore":0`, `"roundsUsedBefore":0,"roundsUsedBefore":1`, 1)
	if _, e := decodeFixLineage([]byte(duplicate)); e == nil {
		t.Fatal("duplicate accepted")
	}
	if _, e := decodeFixLineage(append(raw, []byte("{}")...)); e == nil {
		t.Fatal("trailing accepted")
	}
}
