package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestWaitListShowsHeldLocalDelivery(t *testing.T) {
	w := localCheck("held-local", "thread-1")
	w.Status = wait.StatusMet
	w.SettledAt = &waitListNow
	// Decode additive fields so this regression compiles before the repair.
	raw, _ := json.Marshal(w)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	fields["delivery"] = "held"
	fields["deliveryReason"] = "claudeAgent/claude/five_hour is stopped at 99%"
	raw, _ = json.Marshal(fields)
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	sources := waitListSources{host: "worker", local: func(context.Context, string) ([]wait.Wait, error) { return []wait.Wait{w}, nil }}
	text, err := listWaits(t, sources, waitListOptions{thread: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"held-local", "state=met", "delivery=held", "stopped at 99%"} {
		if !strings.Contains(text, want) {
			t.Errorf("list missing %q: %s", want, text)
		}
	}
	machine, err := listWaits(t, sources, waitListOptions{thread: "thread-1", asJSON: true})
	if err != nil {
		t.Fatal(err)
	}
	var answer struct {
		Waits []map[string]any `json:"waits"`
	}
	if err := json.Unmarshal([]byte(machine), &answer); err != nil {
		t.Fatal(err)
	}
	if len(answer.Waits) != 1 {
		t.Fatalf("held wait hidden: %s", machine)
	}
	if answer.Waits[0]["delivery"] != "held" || answer.Waits[0]["deliveryReason"] != fields["deliveryReason"] || answer.Waits[0]["settled"] != true {
		t.Errorf("JSON lost hold or settlement: %s", machine)
	}
	// Delivered waits remain hidden by default, visible with --all.
	w.Status = wait.StatusWoken
	raw, _ = json.Marshal(w)
	_ = json.Unmarshal(raw, &fields)
	fields["delivery"] = "delivered"
	delete(fields, "deliveryReason")
	raw, _ = json.Marshal(fields)
	w = wait.Wait{}
	_ = json.Unmarshal(raw, &w)
	answerDefault := collectWaitList(context.Background(), sources, waitListOptions{thread: "thread-1"})
	if len(answerDefault.Rows) != 0 || answerDefault.Hidden != 1 {
		t.Errorf("delivered wait should be hidden: %+v", answerDefault)
	}
	machine, err = listWaits(t, sources, waitListOptions{thread: "thread-1", all: true, asJSON: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(machine, `"delivery": "delivered"`) || strings.Contains(machine, "deliveryReason") {
		t.Errorf("delivered JSON: %s", machine)
	}
}
