package backlogadmin

import (
	"encoding/json"
	"testing"
	"time"
)

// rc68RuntimeStatus is a verbatim copy of RuntimeStatus at v0.11.0-rc.68
// (git show 5cc1994:internal/backlogadmin/types.go). Every deployed admin
// client (rc.56, rc.67, rc.68) decodes the status document into this shape,
// and time.Time is an Unmarshaler, so a lastReload that is not a time fails the
// whole response on those hosts. The type is kept here, not shared, so that a
// change to the live struct cannot silently change what this test pins.
type rc68RuntimeStatus struct {
	Release              string    `json:"release,omitempty"`
	ConfigurationDigest  string    `json:"configurationDigest,omitempty"`
	LastReload           time.Time `json:"lastReload,omitzero"`
	Mode                 string    `json:"mode"`
	Owner                string    `json:"owner"`
	Epoch                int64     `json:"epoch"`
	Health               string    `json:"health"`
	Transport            string    `json:"transport"`
	FreshWorkers         int       `json:"freshWorkers"`
	StaleWorkers         int       `json:"staleWorkers"`
	FreshQuotaPools      int       `json:"freshQuotaPools"`
	StaleQuotaPools      int       `json:"staleQuotaPools"`
	ReconciliationIssues []string  `json:"reconciliationIssues,omitempty"`
	UnknownExecutionIDs  []string  `json:"unknownExecutionIds,omitempty"`
	CustodyIncidentIDs   []string  `json:"custodyIncidentIds,omitempty"`
}

// rc68Response mirrors how an rc.68 client receives the runtime block: nested
// under status.runtime of a whole Response, which is what fails as a unit.
type rc68Response struct {
	Status *struct {
		Runtime rc68RuntimeStatus `json:"runtime"`
	} `json:"status"`
}

// The status document produced after the coordinator's first SIGHUP, with a
// reload receipt present, still decodes on an rc.68 admin host: lastReload
// stays the activation time and the receipt travels under its own key.
func TestRuntimeStatusWithAReceiptDecodesOnAnRC68Client(t *testing.T) {
	activated := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	receipt := &ReloadReceipt{
		RequestedAt: activated.Add(time.Minute), CompletedAt: activated.Add(time.Minute + time.Second),
		Outcome: ReloadRejected, Error: "worker normandy has retained assignment assignment-1",
		ConfigurationDigest: "digest-1", PreviousDigest: "digest-1", Release: "rc.69",
		Blockers: []ReloadBlocker{{WorkerID: "normandy", AssignmentID: "assignment-1", AttemptID: "attempt-1", Unblock: "t3-steward backlog cancel run-1/implement --reason TEXT"}},
	}
	produced := Response{Version: Version, Kind: QueryStatus, Status: &Status{Runtime: RuntimeStatus{
		Release: "rc.69", ConfigurationDigest: "digest-1", LastReload: activated, LastReloadReceipt: receipt,
		Mode: "coordinator", Owner: "coordinator-1", Epoch: 7, Health: "healthy", Transport: "ssh",
	}}}
	raw, err := json.Marshal(produced)
	if err != nil {
		t.Fatal(err)
	}

	var old rc68Response
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("an rc.68 client cannot decode the status document: %v\n%s", err, raw)
	}
	if old.Status == nil {
		t.Fatalf("the rc.68 decode lost the status block: %s", raw)
	}
	if !old.Status.Runtime.LastReload.Equal(activated) {
		t.Fatalf("rc.68 lastReload = %v, want the activation time %v", old.Status.Runtime.LastReload, activated)
	}
	if old.Status.Runtime.ConfigurationDigest != "digest-1" || old.Status.Runtime.Owner != "coordinator-1" {
		t.Fatalf("rc.68 runtime block = %+v", old.Status.Runtime)
	}

	// The receipt is carried under its own key, and the current type reads
	// both back.
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	runtime, _ := fields["status"].(map[string]any)["runtime"].(map[string]any)
	if _, present := runtime["lastReloadReceipt"].(map[string]any); !present {
		t.Fatalf("the receipt is not carried as lastReloadReceipt: %s", raw)
	}
	var current Response
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Runtime.LastReload.Equal(activated) || current.Status.Runtime.LastReloadReceipt == nil || current.Status.Runtime.LastReloadReceipt.Outcome != ReloadRejected {
		t.Fatalf("the current type does not round-trip: %+v", current.Status.Runtime)
	}
}
