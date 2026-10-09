package backlog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ProviderResumeLimits is the coordinator's maximum for a worker's in-session
// resumes after provider-side errors. The zero value is the default:
// domain.DefaultProviderResumeMax resumes, each delayed by at most
// domain.MaxProviderResumeDelay.
type ProviderResumeLimits struct {
	// Configured marks limits read from configuration; MaxResumes zero then
	// disables resuming.
	Configured bool
	MaxResumes int
	MaxDelay   time.Duration
}

func (l ProviderResumeLimits) apply(policy *workerproto.ProviderResumePolicy) {
	resumes, delay := domain.DefaultProviderResumeMax, domain.MaxProviderResumeDelay
	if l.Configured {
		resumes = l.MaxResumes
		if l.MaxDelay > 0 {
			delay = l.MaxDelay
		}
	}
	policy.MaxResumes = min(max(resumes, 0), domain.MaxProviderResumes)
	policy.MaxDelaySeconds = int64(min(delay, domain.MaxProviderResumeDelay) / time.Second)
}

// providerResumePolicy is the resume policy one worker is sent: the default
// limits, which FleetCoordinator replaces with its configured ones, and the
// complete list of quota pools whose admission is closed or draining, so a
// resume due on one of them waits as a new dispatch would.
func providerResumePolicy(ctx context.Context, source any) (*workerproto.ProviderResumePolicy, error) {
	policy := &workerproto.ProviderResumePolicy{}
	ProviderResumeLimits{}.apply(policy)
	admissions, ok := source.(interface {
		LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error)
	})
	if !ok {
		return policy, nil
	}
	records, err := admissions.LoadQuotaAdmissions(ctx)
	if err != nil {
		return nil, fmt.Errorf("load quota admissions for provider resume: %w", err)
	}
	for _, record := range records {
		if record.Admission != domain.AdmissionClosed && record.Admission != domain.AdmissionDraining {
			continue
		}
		if len(policy.ClosedPools) == workerproto.MaxClosedQuotaPools {
			break
		}
		reason := record.Reason
		if len(reason) > 1024 {
			reason = strings.ToValidUTF8(reason[:1021], "") + "..."
		}
		policy.ClosedPools = append(policy.ClosedPools, workerproto.ClosedQuotaPool{
			PoolID: record.QuotaPoolID, Admission: record.Admission, Reason: reason,
		})
	}
	sort.Slice(policy.ClosedPools, func(i, j int) bool { return policy.ClosedPools[i].PoolID < policy.ClosedPools[j].PoolID })
	return policy, nil
}
