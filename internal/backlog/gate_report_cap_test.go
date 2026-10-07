package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// failLastGateRunner succeeds every gate command except the last, which fails
// with an error message of the configured size.
type failLastGateRunner struct {
	inner  directRunner
	last   string
	reason string
}

func (r *failLastGateRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	if len(request.Args) > 0 && request.Args[len(request.Args)-1] == r.last {
		return ProcessResult{ExitCode: 3}, &ProcessExitError{ExitCode: 3, Err: errors.New(r.reason)}
	}
	return r.inner.Run(ctx, request)
}

// The worker capped the compact JSON of its gate report but uploads it
// indented, and the coordinator caps the uploaded bytes. A report between the
// two sizes passed the worker's check and was then rejected as malformed,
// losing the gate evidence instead of recording an importable failure.
func TestGateReportCapUsesUploadedEncoding(t *testing.T) {
	commands := make([]string, 0, domain.MaxGateCommands)
	for i := range domain.MaxGateCommands {
		c := fmt.Sprintf("true %02d #", i)
		c += strings.Repeat("<", (domain.MaxGateCommandsBytes/domain.MaxGateCommands)-len(c))
		commands = append(commands, c)
	}
	run := func(n int) (domain.Task, GateReport) {
		req := h2GateRequest(h2GateRepository(t), "cap")
		req.Task.Gate = &domain.TaskGate{Commands: commands, Timeout: 5 * time.Second}
		runner := &failLastGateRunner{last: commands[len(commands)-1], reason: strings.Repeat("<", n)}
		report, _, err := (AttemptFinalizer{StorageRoot: t.TempDir(), Processes: runner}).runGate(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		return req.Task, report
	}
	_, probe := run(1000)
	compact, err := json.Marshal(probe)
	if err != nil {
		t.Fatal(err)
	}
	// Each '<' adds six escaped bytes to both Error and Failure.Reason; aim
	// the compact encoding just under the limit.
	n := 1000 + (GateEvidenceMaxBytes-200-len(compact))/12
	if n > 15900 {
		t.Fatalf("fixture cannot reach the limit: compact probe is %d bytes", len(compact))
	}
	task, report := run(n)
	// Finalize stores exactly this encoding.
	uploaded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	uploaded = append(uploaded, '\n')
	decoded, err := decodeGateEvidence(uploaded)
	if err != nil {
		t.Fatalf("uploaded=%d limit=%d: worker accepted a report the coordinator refuses: %v", len(uploaded), GateEvidenceMaxBytes, err)
	}
	if err := validateGateReport(task, decoded); err != nil || decoded.Passed {
		t.Fatalf("gate report is not an importable failure: passed=%v err=%v", decoded.Passed, err)
	}
}
