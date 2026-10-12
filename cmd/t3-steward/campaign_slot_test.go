package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestCampaignCheckCapacityJSONAndHuman(t *testing.T) {
	root := campaignFixture(t)
	for _, state := range []string{"current", "stale", "unknown"} {
		for _, free := range []int{1, 0, 1} {
			answer := func(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
				matrix := campaignReadyMatrix(request)
				for i := range matrix.Tasks {
					c := &matrix.Tasks[i].Candidates[0]
					c.Capacity = &backlogadmin.ViabilityCapacityObservation{State: state}
					if state != "unknown" {
						c.Capacity.Slots = &backlogadmin.ViabilitySlotCapacity{Allocatable: 2, Reserved: 2 - free, Free: free, Required: 1}
						c.Capacity.CPUUnits = &backlogadmin.ViabilityCPUCapacity{DeclaredDemand: 3}
					}
					if free == 0 || state != "current" {
						c.Outcome = backlogadmin.ViabilityAcceptedWaiting
						matrix.Tasks[i].Outcome = c.Outcome
						matrix.Outcome = c.Outcome
					}
				}
				return matrix
			}
			var human, raw bytes.Buffer
			cli, _ := campaignCheckCLI(t, &human, answer)
			if err := cli.run(context.Background(), []string{"check", root, "--verbose"}); err != nil {
				t.Fatal(err)
			}
			cli, _ = campaignCheckCLI(t, &raw, answer)
			if err := cli.run(context.Background(), []string{"check", root, "--json"}); err != nil {
				t.Fatal(err)
			}
			var document campaignCheck
			if err := json.Unmarshal(raw.Bytes(), &document); err != nil {
				t.Fatal(err)
			}
			c := document.Matrix.Tasks[0].Candidates[0]
			if c.Capacity.State != state {
				t.Fatalf("JSON lost capacity state: %s", raw.String())
			}
			for _, note := range candidateNotes(c) {
				if !strings.Contains(human.String(), note) {
					t.Fatalf("human/JSON evidence differs: %q absent from %s", note, human.String())
				}
			}
			if state != "unknown" && (!strings.Contains(human.String(), "cpu units   ungoverned") ||
				!strings.Contains(human.String(), "requires 1") || c.Capacity.Slots.Free != free || c.Capacity.CPUUnits.DeclaredDemand != 3) {
				t.Fatalf("capacity dimensions lost: %s", human.String())
			}
		}
	}
}
