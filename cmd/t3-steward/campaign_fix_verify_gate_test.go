package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignFixProducerVerifyGate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		producerGate *domain.TaskGate
		verify       []string
		args         campaignFixArgs
		source       string
		commands     []string
		timeout      time.Duration
		refused      bool
	}{
		{name: "verify only", verify: []string{"make test", "make lint"}, source: "producer-verify", commands: []string{"make test", "make lint"}, timeout: 30 * time.Minute},
		{name: "producer gate wins", producerGate: &domain.TaskGate{Commands: []string{"producer gate"}, Timeout: time.Minute}, verify: []string{"make test", "make lint"}, source: "producer", commands: []string{"producer gate"}, timeout: time.Minute},
		{name: "flag wins", verify: []string{"make test", "make lint"}, args: campaignFixArgs{gate: []string{"custom"}, gateTimeout: 2 * time.Minute}, source: "flag", commands: []string{"custom"}, timeout: 2 * time.Minute},
		{name: "no gate wins", verify: []string{"make test", "make lint"}, args: campaignFixArgs{noGate: true}, source: "no-gate", commands: []string{}},
		{name: "neither gate nor verify", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, _ := fixCommandFixture(t)
			old := cli.detail
			cli.detail = func(ctx context.Context, run string) (backlogadmin.WorkflowDetail, error) {
				d, err := old(ctx, run)
				d.Tasks[0].Task.Gate = tc.producerGate
				d.Tasks[0].Task.Verification = tc.verify
				return d, err
			}
			tc.args.node = domain.NodeRef{RunID: "run", TaskID: "review"}
			r, err := cli.resolve(context.Background(), tc.args)
			if tc.refused {
				if err == nil || !strings.Contains(err.Error(), "no gate is resolvable") {
					t.Fatalf("expected unresolved gate refusal, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			timeout := ""
			if tc.timeout != 0 {
				timeout = tc.timeout.String()
			}
			if r.Gate.Source != tc.source || !reflect.DeepEqual(r.Gate.Commands, tc.commands) || r.Gate.Timeout != timeout {
				t.Fatalf("gate receipt = %+v, want source %s commands %v timeout %s", r.Gate, tc.source, tc.commands, timeout)
			}
			unit, err := campaign.GenerateFixChain(r.Source, r.Options)
			if err != nil {
				t.Fatal(err)
			}
			var manifest *backlog.Manifest
			for _, file := range unit.Files {
				if file.Path == "workflow.yaml" {
					parsed, err := backlog.ParseManifest(file.Content)
					if err != nil {
						t.Fatal(err)
					}
					manifest = &parsed
				}
			}
			if manifest == nil {
				t.Fatal("missing generated workflow")
			}
			gate := manifest.Tasks["fix2"].Gate
			if tc.source == "no-gate" {
				if gate != nil {
					t.Fatalf("unexpected generated gate: %+v", gate)
				}
			} else if gate == nil || !reflect.DeepEqual(gate.Commands, tc.commands) || gate.Timeout != tc.timeout {
				t.Fatalf("generated final gate = %+v, want commands %v timeout %s", gate, tc.commands, tc.timeout)
			}
			if tc.source == "producer-verify" {
				r.Options.Gate.Commands[0] = "mutated"
				if tc.verify[0] != "make test" {
					t.Fatal("inherited gate aliases producer verification")
				}
			}
		})
	}
}
