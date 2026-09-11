package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuotaAdmissionTransitionStore is the durable boundary between deterministic
// admission planning and outward worker communication.
type QuotaAdmissionTransitionStore interface {
	LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error)
	CommitQuotaAdmissionTransitions(context.Context, []domain.QuotaAdmissionTransition) error
}

// ReconcileQuotaAdmissionTransitions persists the complete admission batch
// before returning any directive that an outward dispatcher may send.
func ReconcileQuotaAdmissionTransitions(
	ctx context.Context,
	store QuotaAdmissionTransitionStore,
	derived []QuotaPoolAdmissionSnapshot,
	appliedAt time.Time,
) ([]domain.ThrottleDirective, error) {
	if store == nil {
		return nil, fmt.Errorf("quota admission transition store is required")
	}
	previous, err := store.LoadQuotaAdmissions(ctx)
	if err != nil {
		return nil, fmt.Errorf("load quota admissions: %w", err)
	}
	transitions, err := PlanQuotaAdmissionTransitions(previous, derived, appliedAt)
	if err != nil {
		return nil, err
	}
	if len(transitions) == 0 {
		return nil, nil
	}
	if err := store.CommitQuotaAdmissionTransitions(ctx, transitions); err != nil {
		return nil, fmt.Errorf("commit quota admission transitions: %w", err)
	}

	directives := make([]domain.ThrottleDirective, 0, len(transitions))
	for _, transition := range transitions {
		if transition.Directive != nil {
			directives = append(directives, cloneThrottleDirective(*transition.Directive))
		}
	}
	return directives, nil
}

// PlanQuotaAdmissionTransitions compares durable admission records with one
// derived fleet snapshot. Results are canonical and detached from all inputs.
func PlanQuotaAdmissionTransitions(
	previous []domain.QuotaAdmissionRecord,
	derived []QuotaPoolAdmissionSnapshot,
	appliedAt time.Time,
) ([]domain.QuotaAdmissionTransition, error) {
	if appliedAt.IsZero() {
		return nil, fmt.Errorf("quota admission transition time must be set")
	}

	previousByPool := make(map[string]domain.QuotaAdmissionRecord, len(previous))
	for _, record := range previous {
		if err := validateQuotaAdmissionRecord(record); err != nil {
			return nil, err
		}
		if _, exists := previousByPool[record.QuotaPoolID]; exists {
			return nil, fmt.Errorf("quota admission records repeat pool %q", record.QuotaPoolID)
		}
		record.BucketEpochs = cloneDomainBucketEpochs(record.BucketEpochs)
		sortDomainBucketEpochs(record.BucketEpochs)
		previousByPool[record.QuotaPoolID] = record
	}

	snapshots := append([]QuotaPoolAdmissionSnapshot(nil), derived...)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].QuotaPoolID < snapshots[j].QuotaPoolID })
	seen := make(map[string]struct{}, len(snapshots))
	transitions := make([]domain.QuotaAdmissionTransition, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if err := validateDerivedAdmissionSnapshot(snapshot); err != nil {
			return nil, err
		}
		if _, exists := seen[snapshot.QuotaPoolID]; exists {
			return nil, fmt.Errorf("derived admissions repeat pool %q", snapshot.QuotaPoolID)
		}
		seen[snapshot.QuotaPoolID] = struct{}{}

		var epochs []domain.QuotaBucketEpoch
		if len(snapshot.BucketEpochs) != 0 {
			epochs = make([]domain.QuotaBucketEpoch, len(snapshot.BucketEpochs))
		}
		for index, epoch := range snapshot.BucketEpochs {
			epochs[index] = domain.QuotaBucketEpoch{Bucket: epoch.Bucket, Epoch: epoch.Epoch}
		}
		sortDomainBucketEpochs(epochs)
		prior, exists := previousByPool[snapshot.QuotaPoolID]
		if exists && sameAdmissionProjection(prior, snapshot, epochs) {
			continue
		}

		expectedRevision := int64(0)
		if exists {
			expectedRevision = prior.Revision
		}
		record := domain.QuotaAdmissionRecord{
			QuotaPoolID:  snapshot.QuotaPoolID,
			Revision:     expectedRevision + 1,
			Admission:    snapshot.Admission,
			ObservedAt:   snapshot.ObservedAt,
			AppliedAt:    appliedAt,
			BucketEpochs: epochs,
			Reason:       snapshot.Reason,
		}
		transition := domain.QuotaAdmissionTransition{
			ExpectedRevision: expectedRevision,
			Record:           record,
		}
		severity := throttleSeverity(snapshot.Admission)
		if severity != "" && (!exists || severity != throttleSeverity(prior.Admission) || !sameBucketEpochs(prior.BucketEpochs, epochs)) {
			directive := domain.ThrottleDirective{
				ID:                throttleDirectiveID(record, severity),
				QuotaPoolID:       record.QuotaPoolID,
				AdmissionRevision: record.Revision,
				Severity:          severity,
				Reason:            record.Reason,
				Deadline:          cloneTime(snapshot.DirectiveDeadline),
				BucketEpochs:      cloneDomainBucketEpochs(epochs),
				CreatedAt:         appliedAt,
			}
			transition.Directive = &directive
		}
		transitions = append(transitions, transition)
	}
	return transitions, nil
}

func validateQuotaAdmissionRecord(record domain.QuotaAdmissionRecord) error {
	if strings.TrimSpace(record.QuotaPoolID) != record.QuotaPoolID || record.QuotaPoolID == "" {
		return fmt.Errorf("quota admission record pool ID must be nonempty and trimmed")
	}
	if record.Revision <= 0 {
		return fmt.Errorf("quota admission record %q revision must be positive", record.QuotaPoolID)
	}
	if !validAdmissionState(record.Admission) {
		return fmt.Errorf("quota admission record %q has invalid state %q", record.QuotaPoolID, record.Admission)
	}
	return validateDomainBucketEpochs(record.QuotaPoolID, record.BucketEpochs)
}

func validateDerivedAdmissionSnapshot(snapshot QuotaPoolAdmissionSnapshot) error {
	if strings.TrimSpace(snapshot.QuotaPoolID) != snapshot.QuotaPoolID || snapshot.QuotaPoolID == "" {
		return fmt.Errorf("derived quota admission pool ID must be nonempty and trimmed")
	}
	if !validAdmissionState(snapshot.Admission) {
		return fmt.Errorf("derived quota admission %q has invalid state %q", snapshot.QuotaPoolID, snapshot.Admission)
	}
	epochs := make([]domain.QuotaBucketEpoch, len(snapshot.BucketEpochs))
	for index, epoch := range snapshot.BucketEpochs {
		epochs[index] = domain.QuotaBucketEpoch{Bucket: epoch.Bucket, Epoch: epoch.Epoch}
	}
	return validateDomainBucketEpochs(snapshot.QuotaPoolID, epochs)
}

func validateDomainBucketEpochs(poolID string, epochs []domain.QuotaBucketEpoch) error {
	seen := make(map[domain.BucketKey]struct{}, len(epochs))
	for _, epoch := range epochs {
		if err := validateAdmissionBucketKey(epoch.Bucket); err != nil {
			return fmt.Errorf("quota admission %q: %w", poolID, err)
		}
		if strings.TrimSpace(epoch.Epoch) != epoch.Epoch {
			return fmt.Errorf("quota admission %q bucket %q epoch must be trimmed", poolID, epoch.Bucket.String())
		}
		if _, exists := seen[epoch.Bucket]; exists {
			return fmt.Errorf("quota admission %q repeats bucket %q", poolID, epoch.Bucket.String())
		}
		seen[epoch.Bucket] = struct{}{}
	}
	return nil
}

func validAdmissionState(state domain.AdmissionState) bool {
	switch state {
	case domain.AdmissionOpen, domain.AdmissionConstrained, domain.AdmissionDraining,
		domain.AdmissionClosed, domain.AdmissionRecovering:
		return true
	default:
		return false
	}
}

func sameAdmissionProjection(record domain.QuotaAdmissionRecord, snapshot QuotaPoolAdmissionSnapshot, epochs []domain.QuotaBucketEpoch) bool {
	return record.Admission == snapshot.Admission &&
		record.ObservedAt.Equal(snapshot.ObservedAt) &&
		record.Reason == snapshot.Reason &&
		sameBucketEpochs(record.BucketEpochs, epochs)
}

func sameBucketEpochs(left, right []domain.QuotaBucketEpoch) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func throttleSeverity(admission domain.AdmissionState) domain.ThrottleSeverity {
	switch admission {
	case domain.AdmissionConstrained:
		return domain.ThrottleWarn
	case domain.AdmissionDraining:
		return domain.ThrottleDrain
	case domain.AdmissionClosed:
		return domain.ThrottleStop
	default:
		return ""
	}
}

func throttleDirectiveID(record domain.QuotaAdmissionRecord, severity domain.ThrottleSeverity) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\x00%d\x00%s", record.QuotaPoolID, record.Revision, severity)
	for _, epoch := range record.BucketEpochs {
		fmt.Fprintf(hash, "\x00%s\x00%s", epoch.Bucket.String(), epoch.Epoch)
	}
	return "throttle-" + hex.EncodeToString(hash.Sum(nil)[:16])
}

func sortDomainBucketEpochs(epochs []domain.QuotaBucketEpoch) {
	sort.Slice(epochs, func(i, j int) bool {
		if epochs[i].Bucket.String() != epochs[j].Bucket.String() {
			return epochs[i].Bucket.String() < epochs[j].Bucket.String()
		}
		return epochs[i].Epoch < epochs[j].Epoch
	})
}

func cloneDomainBucketEpochs(epochs []domain.QuotaBucketEpoch) []domain.QuotaBucketEpoch {
	return append([]domain.QuotaBucketEpoch(nil), epochs...)
}

func cloneThrottleDirective(directive domain.ThrottleDirective) domain.ThrottleDirective {
	directive.BucketEpochs = cloneDomainBucketEpochs(directive.BucketEpochs)
	directive.Deadline = cloneTime(directive.Deadline)
	return directive
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
