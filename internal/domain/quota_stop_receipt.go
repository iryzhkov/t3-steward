package domain

import (
	"reflect"
	"slices"
	"time"
)

// QuotaStopReceipt retains an actual accepted quota stop across rejected resume
// revisions. It is coordinator state, never authority supplied by a resume grant.
type QuotaStopReceipt struct {
	Command        ThrottleCommand               `json:"command"`
	Result         ThrottleAcknowledgementResult `json:"result"`
	AcknowledgedAt time.Time                     `json:"acknowledgedAt"`
}

// AcceptedQuotaStop returns the current accepted stop or its retained receipt.
// A new stop command, changed custody, or successful resume cannot reuse it.
func (record ThrottleAttemptRecord) AcceptedQuotaStop() (*QuotaStopReceipt, bool) {
	var receipt *QuotaStopReceipt
	if record.Delivery == ThrottleDeliveryAcknowledged && record.AcknowledgedAt != nil &&
		(record.Command.Kind == ThrottleCommandDrain || record.Command.Kind == ThrottleCommandHardStop) {
		receipt = &QuotaStopReceipt{Command: record.Command, Result: record.Result, AcknowledgedAt: *record.AcknowledgedAt}
	} else if record.Command.Kind == ThrottleCommandResume && record.Control != ControlRunning && record.Delivery != ThrottleDeliveryCancelled &&
		record.StopReceipt != nil && slices.Contains(record.PriorCommandIDs, record.StopReceipt.Command.ID) {
		receipt = record.StopReceipt
	}
	if receipt == nil || receipt.Command.ID == "" || receipt.Command.AttentionStop != nil ||
		receipt.AcknowledgedAt.IsZero() || receipt.AcknowledgedAt.Before(receipt.Command.CreatedAt) ||
		!((receipt.Command.Kind == ThrottleCommandDrain && receipt.Result == ThrottleResultCheckpointed) ||
			(receipt.Command.Kind == ThrottleCommandHardStop && receipt.Result == ThrottleResultStopped)) {
		return nil, false
	}
	stop, current := receipt.Command, record.Command
	if stop.DirectiveID != record.DirectiveID || stop.AttemptID != record.AttemptID ||
		stop.DirectiveID != current.DirectiveID || stop.AttemptID != current.AttemptID ||
		stop.AssignmentID != current.AssignmentID || stop.AssignmentEpoch != current.AssignmentEpoch ||
		stop.WorkerID != current.WorkerID || stop.ThreadID != current.ThreadID ||
		stop.WorkspacePath != current.WorkspacePath || stop.QuotaPoolID != current.QuotaPoolID ||
		!reflect.DeepEqual(stop.Route, current.Route) || !reflect.DeepEqual(stop.BucketEpochs, current.BucketEpochs) {
		return nil, false
	}
	return receipt, true
}

// QuotaStopSuperseded prevents an older directive from resuming through a
// subsequent stop for the same attempt, including a stop still awaiting ACK.
func QuotaStopSuperseded(record ThrottleAttemptRecord, records []ThrottleAttemptRecord) bool {
	stop, accepted := record.AcceptedQuotaStop()
	if !accepted {
		return true
	}
	for _, other := range records {
		if other.AttemptID != record.AttemptID {
			continue
		}
		if other.DirectiveID == record.DirectiveID {
			if other.Command.ID != record.Command.ID || other.Revision != record.Revision ||
				!reflect.DeepEqual(other.StopReceipt, record.StopReceipt) {
				return true
			}
			continue
		}
		if (other.Command.Kind == ThrottleCommandDrain || other.Command.Kind == ThrottleCommandHardStop ||
			other.StopReceipt != nil) && !other.Command.CreatedAt.Before(stop.Command.CreatedAt) {
			return true
		}
	}
	return false
}
