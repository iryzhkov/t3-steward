package backlogadmin

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// F-3: the status query's runtime block carries the coordinator's last reload
// receipt as lastReloadReceipt, read at query time so it follows every SIGHUP,
// and omits the field before the first one. lastReload stays the activation
// time it has always been.
func TestAdminStatusCarriesTheLastReloadReceipt(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	var current *ReloadReceipt
	service.SetRuntimeInfo(RuntimeInfo{
		Mode: "coordinator", Owner: "coordinator-1", Epoch: 7, Transport: "ssh",
		Release: "rc.69", ConfigurationDigest: "digest-1", LastReload: adminTestNow.Add(-time.Hour),
		LastReloadReceipt: func() *ReloadReceipt { return current },
	})
	query := Query{Version: Version, Kind: QueryStatus, Principal: Principal{ID: "operator-1", Roles: []string{"backlog-reader"}}}

	response, err := service.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status.Runtime.LastReloadReceipt != nil {
		t.Fatalf("a coordinator that was never signalled reports a reload: %+v", response.Status.Runtime.LastReloadReceipt)
	}
	if response.Status.Runtime.LastReload != adminTestNow.Add(-time.Hour) {
		t.Fatalf("lastReload = %v", response.Status.Runtime.LastReload)
	}
	raw, err := json.Marshal(response.Status.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["lastReloadReceipt"]; present {
		t.Fatalf("lastReloadReceipt is present before the first reload: %s", raw)
	}
	if _, isTime := fields["lastReload"].(string); !isTime {
		t.Fatalf("lastReload is not the activation time: %s", raw)
	}

	current = &ReloadReceipt{
		RequestedAt: adminTestNow.Add(-time.Minute), CompletedAt: adminTestNow.Add(-time.Minute + time.Second),
		Outcome: ReloadRejected, Error: "worker normandy has retained assignment assignment-1",
		ConfigurationDigest: "digest-1", PreviousDigest: "digest-1", Release: "rc.69",
		Blockers: []ReloadBlocker{{WorkerID: "normandy", AssignmentID: "assignment-1", AttemptID: "attempt-1", Unblock: "t3-steward backlog cancel run-1/implement --reason TEXT"}},
	}
	response, err = service.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	got := response.Status.Runtime.LastReloadReceipt
	if got == nil || got.Outcome != ReloadRejected || got.Error != current.Error || len(got.Blockers) != 1 || got.Blockers[0].AttemptID != "attempt-1" {
		t.Fatalf("lastReloadReceipt = %+v", got)
	}
	raw, err = json.Marshal(response.Status.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, isTime := fields["lastReload"].(string); !isTime {
		t.Fatalf("lastReload stopped being the activation time after a reload: %s", raw)
	}
	last, _ := fields["lastReloadReceipt"].(map[string]any)
	for _, key := range []string{"requestedAt", "completedAt", "outcome", "error", "configurationDigest", "previousDigest", "release", "blockers"} {
		if _, present := last[key]; !present {
			t.Fatalf("lastReloadReceipt lacks %q: %s", key, raw)
		}
	}
}
