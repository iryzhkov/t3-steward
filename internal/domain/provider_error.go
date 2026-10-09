package domain

import (
	"fmt"
	"strings"
	"time"
)

// FailureClassInfrastructure is the failure class of an attempt that failed
// because the infrastructure under it failed, not because of the task: here,
// a provider that kept ending the attempt's turns with a provider-side error
// after every in-session resume was spent. The name is the one the
// failure-class taxonomy uses; a failure reason of this class starts with it
// and a colon.
const FailureClassInfrastructure = "infrastructure"

// DefaultProviderResumeBackoff is the in-session resume schedule a worker
// uses when its configuration names none: the first resume a minute after
// the provider error, the second after five, the third after fifteen. Its
// length is the resume budget.
var DefaultProviderResumeBackoff = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// DefaultProviderResumeMax is the coordinator's maximum number of resumes
// per attempt when its configuration names none.
const DefaultProviderResumeMax = 3

// MaxProviderResumes and MaxProviderResumeDelay are the ceilings neither a
// worker's schedule nor a coordinator's maximum may exceed: past them an
// attempt holds a slot for hours on a provider that is not coming back, and
// the whole-attempt retry is the better answer.
const (
	MaxProviderResumes     = 10
	MaxProviderResumeDelay = 6 * time.Hour
)

// ProviderErrorKind is the kind of provider-side error that ended a task's
// provider turn. Only these kinds are resumed in the same session; anything
// else that ends a turn is the task's own ending.
type ProviderErrorKind string

const (
	// ProviderErrorCapacity is a model at capacity.
	ProviderErrorCapacity ProviderErrorKind = "capacity"
	// ProviderErrorOverload is a provider reporting itself overloaded.
	ProviderErrorOverload ProviderErrorKind = "overload"
	// ProviderErrorRateLimit is a provider rate limit (too many requests).
	ProviderErrorRateLimit ProviderErrorKind = "rate-limit"
	// ProviderErrorServer is a transient provider server or gateway error.
	ProviderErrorServer ProviderErrorKind = "server-error"
	// ProviderErrorSessionNotReady is a provider session T3 left in error,
	// or not ready, at the end of a turn.
	ProviderErrorSessionNotReady ProviderErrorKind = "session-not-ready"
)

// ProviderResumeState is where a worker is with an attempt whose turn ended
// on a provider-side error.
type ProviderResumeState string

const (
	// ProviderResumeScheduled: the error is recorded and the resume of the
	// same session waits for its backoff.
	ProviderResumeScheduled ProviderResumeState = "resume-scheduled"
	// ProviderResumeQuotaWait: the backoff has passed, but the route's quota
	// does not admit a turn; WaitReason says which.
	ProviderResumeQuotaWait ProviderResumeState = "waiting-for-quota"
	// ProviderResumeSent: the continue message is in the session and its
	// turn is awaited.
	ProviderResumeSent ProviderResumeState = "resumed"
	// ProviderResumeExhausted: the resume budget was spent and the attempt
	// failed with FailureClassInfrastructure.
	ProviderResumeExhausted ProviderResumeState = "exhausted"
	// ProviderResumeRecovered: a resumed turn ended without a provider
	// error; the attempt went on to be collected as usual.
	ProviderResumeRecovered ProviderResumeState = "recovered"
)

// WorkerProviderError is a worker's report of the provider-side error that
// ended an attempt's latest provider turn, and of the in-session resume it
// answers with. It travels in the journal excerpt only to a coordinator that
// asked for it (workerproto.CapabilityProviderResume).
type WorkerProviderError struct {
	Kind ProviderErrorKind `json:"kind"`
	// Detail is the provider's message as T3 recorded it, redacted and
	// bounded by the worker.
	Detail string `json:"detail,omitempty"`
	// TurnID is the ended provider turn the error belongs to.
	TurnID     string              `json:"turnId,omitempty"`
	ObservedAt time.Time           `json:"observedAt"`
	State      ProviderResumeState `json:"state"`
	// Resumes is how many resumes have been claimed, Budget how many the
	// worker's schedule allows under the coordinator's maximum.
	Resumes int `json:"resumes"`
	Budget  int `json:"budget"`
	// Errors counts every provider error this attempt's turns ended with.
	Errors int `json:"errors"`
	// ResumeAt is when the scheduled resume is due.
	ResumeAt *time.Time `json:"resumeAt,omitempty"`
	// WaitReason is the typed reason a due resume waits, such as
	// "quota-closed: pool claude-main is closed".
	WaitReason string `json:"waitReason,omitempty"`
}

// Active reports whether the provider error still holds the attempt: it is
// neither recovered nor failed.
func (e WorkerProviderError) Active() bool {
	switch e.State {
	case ProviderResumeScheduled, ProviderResumeQuotaWait, ProviderResumeSent:
		return true
	}
	return false
}

// Summary is the one-line account explain, task show, campaign show, task
// result and triage print.
func (e WorkerProviderError) Summary() string {
	var text strings.Builder
	fmt.Fprintf(&text, "%s (%s)", e.Kind, FailureClassInfrastructure)
	if e.Detail != "" {
		fmt.Fprintf(&text, ": %s", e.Detail)
	}
	switch e.State {
	case ProviderResumeScheduled:
		fmt.Fprintf(&text, "; resume %d of %d of the same session", e.Resumes, e.Budget)
		if e.ResumeAt != nil {
			fmt.Fprintf(&text, " at %s", e.ResumeAt.UTC().Format(time.RFC3339))
		}
	case ProviderResumeQuotaWait:
		fmt.Fprintf(&text, "; resume %d of %d waits: %s", e.Resumes, e.Budget, e.WaitReason)
	case ProviderResumeSent:
		fmt.Fprintf(&text, "; resumed the same session (%d of %d)", e.Resumes, e.Budget)
	case ProviderResumeExhausted:
		fmt.Fprintf(&text, "; every resume was spent (%d of %d), the attempt failed", e.Resumes, e.Budget)
	case ProviderResumeRecovered:
		fmt.Fprintf(&text, "; recovered after %d resume(s)", e.Resumes)
	}
	return text.String()
}
