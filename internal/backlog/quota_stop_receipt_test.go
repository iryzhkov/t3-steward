package backlog

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestQuotaStopReceiptSurvivesRejectedResumeAndClearsOnSuccess(t *testing.T) {
	now := throttleDeliveryTime
	binding := throttleBinding("attempt", "worker", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleStop)
	initial, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive}, []ThrottleAttemptBinding{binding}, now)
	if err != nil || len(initial) != 1 {
		t.Fatalf("stop intent: %+v %v", initial, err)
	}
	stop := initial[0].Record
	ack := domain.ThrottleAcknowledgement{CommandID: stop.Command.ID, AttemptID: stop.AttemptID, Accepted: true, Result: domain.ThrottleResultStopped, AcknowledgedAt: now.Add(time.Second)}
	accepted, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{stop}, []domain.ThrottleAcknowledgement{ack}, now.Add(time.Second))
	if err != nil || len(accepted) != 1 {
		t.Fatalf("stop ACK: %+v %v", accepted, err)
	}
	stop = accepted[0].Record
	if stop.StopReceipt == nil || !stop.StopReceipt.AcknowledgedAt.Equal(ack.AcknowledgedAt) {
		t.Fatal("accepted actual stop was not retained")
	}
	// Older persisted stop records have an accepted ACK but no receipt field.
	originalReceipt := stop.StopReceipt
	stop.StopReceipt = nil
	pool := domain.QuotaPool{ID: stop.Command.QuotaPoolID, MaxConcurrent: 2}
	admissions := []domain.QuotaAdmissionRecord{{QuotaPoolID: pool.ID, Admission: domain.AdmissionOpen}}
	resumes, _, err := PlanThrottleResumes([]domain.ThrottleAttemptRecord{stop}, admissions, []domain.QuotaPool{pool}, now.Add(time.Minute))
	if err != nil || len(resumes) != 1 {
		t.Fatalf("resume intent: %+v %v", resumes, err)
	}
	resume := resumes[0].Record
	rejected, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{resume}, []domain.ThrottleAcknowledgement{{CommandID: resume.Command.ID, AttemptID: resume.AttemptID, Error: "grant expired", AcknowledgedAt: now.Add(2 * time.Minute)}}, now.Add(2*time.Minute))
	if err != nil || len(rejected) != 1 {
		t.Fatalf("resume rejection: %+v %v", rejected, err)
	}
	raw, err := json.Marshal(rejected[0].Record)
	if err != nil {
		t.Fatal(err)
	}
	var restored domain.ThrottleAttemptRecord
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	receipt, ok := restored.AcceptedQuotaStop()
	if !ok || !reflect.DeepEqual(receipt, originalReceipt) || receipt.AcknowledgedAt.Equal(*restored.AcknowledgedAt) {
		t.Fatalf("rejection ACK replaced stop authority: %+v", restored)
	}
	for _, tc := range []string{"lineage", "custody", "bucket epoch", "stop id", "stop ACK", "resume result", "cancelled", "new stop"} {
		t.Run(tc, func(t *testing.T) {
			r := cloneThrottleAttemptRecord(restored)
			switch tc {
			case "lineage":
				r.PriorCommandIDs = nil
			case "custody":
				r.Command.AssignmentEpoch++
			case "bucket epoch":
				r.StopReceipt.Command.BucketEpochs = []domain.QuotaBucketEpoch{{Epoch: "changed"}}
			case "stop id":
				r.StopReceipt.Command.ID = "forged"
			case "stop ACK":
				r.StopReceipt.AcknowledgedAt = time.Time{}
			case "resume result":
				r.StopReceipt.Result = domain.ThrottleResultResumed
			case "cancelled":
				r.Delivery = domain.ThrottleDeliveryCancelled
			case "new stop":
				r.Command.Kind = domain.ThrottleCommandHardStop
				r.Delivery = domain.ThrottleDeliveryPending
			}
			if _, ok := r.AcceptedQuotaStop(); ok {
				t.Fatal("invalid retained stop accepted")
			}
		})
	}
	retry, _, err := PlanThrottleResumes([]domain.ThrottleAttemptRecord{restored}, admissions, []domain.QuotaPool{pool}, now.Add(3*time.Minute))
	if err != nil || len(retry) != 1 {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	r := retry[0].Record
	completed, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{r}, []domain.ThrottleAcknowledgement{{CommandID: r.Command.ID, AttemptID: r.AttemptID, Accepted: true, Result: domain.ThrottleResultResumed, AcknowledgedAt: now.Add(4 * time.Minute)}}, now.Add(4*time.Minute))
	if err != nil || len(completed) != 1 || completed[0].Record.StopReceipt != nil {
		t.Fatalf("successful resume retained stop: %+v %v", completed, err)
	}
}
