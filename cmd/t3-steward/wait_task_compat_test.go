package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// rc69TaskWaitRegistration is the task-wait registration exactly as rc.69
// (76d3c5d, internal/domain/task_wait.go) declares it. The coordinator decodes
// the request envelope strictly, so a registration from this build must carry
// no field outside this set unless it needs a newer coordinator anyway.
type rc69TaskWaitRegistration struct {
	RequestID      string          `json:"requestId"`
	WorkflowRunID  string          `json:"workflowRunId"`
	TaskID         string          `json:"taskId"`
	AttemptID      string          `json:"attemptId"`
	IssuedRevision int64           `json:"issuedRevision"`
	ThreadID       string          `json:"threadId"`
	Wake           domain.WakeMode `json:"wake"`
	MaxDuration    time.Duration   `json:"maxDuration"`
	Name           string          `json:"name,omitempty"`
	Condition      string          `json:"condition,omitempty"`
}

func decodeRC69(t *testing.T, registration domain.TaskWaitRegistration) error {
	t.Helper()
	raw, err := json.Marshal(registration)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var decoded rc69TaskWaitRegistration
	return decoder.Decode(&decoded)
}

// A plain shell `--task current` registration marshals to exactly the rc.69
// field set, so an updated host keeps parking tasks against an rc.69
// coordinator. The new kinds and --or-timeout carry their fields and are
// refused by that coordinator's strict decode, which is the honest answer.
func TestShellTaskWaitRegistrationIsByteCompatibleWithRC69(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	identity := taskIdentity{WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", AttemptRevision: 7, ThreadID: "thread-1"}
	spec, err := parseLocalWaitSpec([]string{"--task", "current", "--request-id", "ci", "--name", "ci", "--", "gh", "run", "view"}, now)
	if err != nil {
		t.Fatal(err)
	}
	registration := localTaskWaitRegistration(spec, identity)
	if err := decodeRC69(t, registration); err != nil {
		t.Fatalf("a shell registration carries a field rc.69 refuses: %v", err)
	}
	if registration.RequestID != "ci" || registration.AttemptID != "attempt-1" || registration.IssuedRevision != 7 || registration.Wake != domain.WakeEach || registration.Condition == "" {
		t.Fatalf("registration = %+v", registration)
	}

	spec, err = parseLocalWaitSpec([]string{"--task", "current", "--for", "1h"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeRC69(t, localTaskWaitRegistration(spec, identity)); err == nil {
		t.Fatal("a time registration hid its kind from an rc.69 coordinator")
	}

	spec, err = parseLocalWaitSpec([]string{"--task", "current", "--or-timeout", "--", "true"}, now)
	if err != nil {
		t.Fatal(err)
	}
	registration = localTaskWaitRegistration(spec, identity)
	if registration.Kind != "" || !registration.OrTimeout {
		t.Fatalf("shell --or-timeout registration = %+v", registration)
	}
	if err := decodeRC69(t, registration); err == nil {
		t.Fatal("--or-timeout on a shell wait hid itself from an rc.69 coordinator")
	}
}
