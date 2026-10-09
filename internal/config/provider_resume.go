package config

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// V2ProviderResume configures the in-session resume of a task whose provider
// turn ended on a provider-side error (capacity, overload, rate limit,
// transient server error, session not ready).
//
// Backoff is worker-local: the delay before each resume on this host, its
// length the resume budget; empty uses domain.DefaultProviderResumeBackoff
// (1m, 5m, 15m). MaxResumes and MaxDelay are the coordinator's maximum,
// sent to every worker that resumes: the most resumes, and the longest
// single delay, any worker's schedule may use. An absent max_resumes is
// domain.DefaultProviderResumeMax and zero disables resuming; a zero
// max_delay is the ceiling, domain.MaxProviderResumeDelay.
type V2ProviderResume struct {
	Backoff    []Duration `yaml:"backoff"`
	MaxResumes *int       `yaml:"max_resumes"`
	MaxDelay   Duration   `yaml:"max_delay"`
}

// Schedule is the worker's backoff schedule, nil for the default.
func (p V2ProviderResume) Schedule() []time.Duration {
	if len(p.Backoff) == 0 {
		return nil
	}
	out := make([]time.Duration, len(p.Backoff))
	for index, delay := range p.Backoff {
		out[index] = delay.D()
	}
	return out
}

// Maximum is the coordinator's maximum resume count and delay, as
// FleetCoordinator sends it to workers.
func (p V2ProviderResume) Maximum() (int, time.Duration) {
	resumes := domain.DefaultProviderResumeMax
	if p.MaxResumes != nil {
		resumes = *p.MaxResumes
	}
	delay := p.MaxDelay.D()
	if delay <= 0 {
		delay = domain.MaxProviderResumeDelay
	}
	return resumes, delay
}

func (p V2ProviderResume) validate() error {
	if len(p.Backoff) > domain.MaxProviderResumes {
		return fmt.Errorf("backlog_v2.provider_resume.backoff lists %d delays; at most %d resumes are allowed", len(p.Backoff), domain.MaxProviderResumes)
	}
	for index, delay := range p.Backoff {
		if delay.D() <= 0 || delay.D() > domain.MaxProviderResumeDelay {
			return fmt.Errorf("backlog_v2.provider_resume.backoff[%d] must be a positive duration of at most %s, for example 5m", index, domain.MaxProviderResumeDelay)
		}
	}
	if p.MaxResumes != nil && (*p.MaxResumes < 0 || *p.MaxResumes > domain.MaxProviderResumes) {
		return fmt.Errorf("backlog_v2.provider_resume.max_resumes must be between 0 and %d", domain.MaxProviderResumes)
	}
	if p.MaxDelay.D() < 0 || p.MaxDelay.D() > domain.MaxProviderResumeDelay {
		return fmt.Errorf("backlog_v2.provider_resume.max_delay must be at most %s", domain.MaxProviderResumeDelay)
	}
	return nil
}
