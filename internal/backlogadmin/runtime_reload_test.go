package backlogadmin

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// F-3: the status query's runtime block carries the coordinator's last reload
// receipt, read at query time so it follows every SIGHUP, and omits the field
// before the first one.
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
		Release: "rc.69", ConfigurationDigest: "digest-1", ActivatedAt: adminTestNow.Add(-time.Hour),
		LastReload: func() *ReloadReceipt { return current },
	})
	query := Query{Version: Version, Kind: QueryStatus, Principal: Principal{ID: "operator-1", Roles: []string{"backlog-reader"}}}

	response, err := service.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status.Runtime.LastReload != nil {
		t.Fatalf("a coordinator that was never signalled reports a reload: %+v", response.Status.Runtime.LastReload)
	}
	if response.Status.Runtime.ActivatedAt != adminTestNow.Add(-time.Hour) {
		t.Fatalf("activatedAt = %v", response.Status.Runtime.ActivatedAt)
	}
	raw, err := json.Marshal(response.Status.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["lastReload"]; present {
		t.Fatalf("lastReload is present before the first reload: %s", raw)
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
	got := response.Status.Runtime.LastReload
	if got == nil || got.Outcome != ReloadRejected || got.Error != current.Error || len(got.Blockers) != 1 || got.Blockers[0].AttemptID != "attempt-1" {
		t.Fatalf("lastReload = %+v", got)
	}
	raw, err = json.Marshal(response.Status.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	last, _ := fields["lastReload"].(map[string]any)
	for _, key := range []string{"requestedAt", "completedAt", "outcome", "error", "configurationDigest", "previousDigest", "release", "blockers"} {
		if _, present := last[key]; !present {
			t.Fatalf("lastReload lacks %q: %s", key, raw)
		}
	}
}
