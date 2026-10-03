package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"io"
	"strings"
	"testing"
)

func TestM8RegisterOnlyDoesNotResolveWake(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	c := campaignTestCLI(t, &out)
	c.viability = func(_ context.Context, r backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
		return campaignReadyMatrix(r), nil
	}
	c.resolveThread = func(string) (string, error) { t.Fatal("registration resolved a wake thread"); return "", nil }
	c.submissions = func() (adminSubmissionService, error) {
		return submissionFunc(func(_ context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, _ int64) (backlogadmin.LocalSubmissionResponse, error) {
			raw, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["registerOnly"] != true {
				t.Fatal("registration sent an ordinary submission")
			}
			return backlogadmin.LocalSubmissionResponse{Key: request.IdempotencyKey, WorkflowID: "workflow-1", State: "accepted"}, nil
		}), nil
	}
	if err := c.run(context.Background(), []string{"submit", root, "--register-only", "--idempotency-key", "registration"}); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(out.String(), "run ") || strings.Contains(out.String(), "\nrun ") || strings.Contains(out.String(), "End this turn") {
		t.Fatalf("registration promises a run or wake: %s", out.String())
	}
	if !strings.Contains(out.String(), "workflow-1") || !strings.Contains(out.String(), "t3-steward schedules put") {
		t.Fatalf("registration does not name schedule command: %s", out.String())
	}
}
