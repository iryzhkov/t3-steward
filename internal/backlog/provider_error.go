package backlog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ProviderTurnError is a provider-side error that ended a thread's latest
// provider turn, or kept its current turn start request from becoming one.
type ProviderTurnError struct {
	Kind domain.ProviderErrorKind
	// Detail is the provider's message as T3 recorded it, whitespace folded
	// and bounded; it is not yet checked for credentials.
	Detail string
}

// providerErrorRefusals are messages of a request the provider refused for
// what it asked, not for the state the provider was in: a prompt over the
// input limit, a credential or permission problem, a policy refusal, or an
// exhausted plan. Resuming the same session would make the same request
// again, so they are never provider errors, whatever else the text says.
var providerErrorRefusals = regexp.MustCompile(`(?i)(too long|context (length|window)|maximum context|input limit|invalid[ _]request|permission|policy|refus|unauthori[sz]ed|authenticat|forbidden|\b40[13]\b|usage limit|insufficient[ _]quota|billing|credit balance)`)

// providerErrorKinds are matched in order against a provider message; the
// first kind that matches is the error's kind.
var providerErrorKinds = []struct {
	kind    domain.ProviderErrorKind
	pattern *regexp.Regexp
}{
	{domain.ProviderErrorRateLimit, regexp.MustCompile(`(?i)(rate[ _-]?limit|too many requests|\b429\b)`)},
	{domain.ProviderErrorOverload, regexp.MustCompile(`(?i)(overload|\b529\b)`)},
	{domain.ProviderErrorCapacity, regexp.MustCompile(`(?i)capacity`)},
	{domain.ProviderErrorServer, regexp.MustCompile(`(?i)(server[ _]error|bad gateway|service[ _]unavailable|gateway time-?out|upstream|connection reset|econnreset|socket hang up|stream (disconnected|error)|\b50[0234]\b)`)},
	{domain.ProviderErrorSessionNotReady, regexp.MustCompile(`(?i)not ready`)},
}

// classifyProviderText is the kind of provider error a message names, and
// false for a message that names none or names a refused request.
func classifyProviderText(text string) (domain.ProviderErrorKind, bool) {
	if providerErrorRefusals.MatchString(text) {
		return "", false
	}
	for _, candidate := range providerErrorKinds {
		if candidate.pattern.MatchString(text) {
			return candidate.kind, true
		}
	}
	return "", false
}

// sessionFailureKind is the kind of a provider session T3 left in error or
// not ready: the kind its last error names, or session-not-ready when it
// names none. A last error that names a refused request is not a provider
// error.
func sessionFailureKind(detail string) (domain.ProviderErrorKind, bool) {
	if detail == "" {
		return domain.ProviderErrorSessionNotReady, true
	}
	if providerErrorRefusals.MatchString(detail) {
		return "", false
	}
	if kind, ok := classifyProviderText(detail); ok {
		return kind, true
	}
	return domain.ProviderErrorSessionNotReady, true
}

// ClassifyProviderTurnEnd reports whether the latest turn of the thread in
// archive ended on a provider-side error that a resume of the same session
// can recover from: capacity, overload, a rate limit, a transient server
// error, or a session left not ready.
//
// Everything else is the task's own ending and is reported false: a turn the
// agent completed in a ready session, a turn interrupted by a stop, a thread
// waiting for input or approval, a turn that failed with a message naming no
// provider condition, and a request the provider refused for what it asked
// (see providerErrorRefusals).
func ClassifyProviderTurnEnd(archive []byte, threadID string) (ProviderTurnError, bool, error) {
	var snapshot threadArchive
	if err := json.Unmarshal(archive, &snapshot); err != nil {
		return ProviderTurnError{}, false, fmt.Errorf("thread archive is invalid: %w", err)
	}
	thread := snapshot.Thread
	if threadID == "" || thread.ID != threadID {
		return ProviderTurnError{}, false, nil
	}
	if thread.HasPendingApprovals || thread.HasPendingUserInput {
		return ProviderTurnError{}, false, nil
	}
	sessionDetail := ""
	sessionFailed := false
	if session := thread.Session; session != nil {
		if session.LastError != nil {
			sessionDetail = providerDetail(*session.LastError)
		}
		sessionFailed = session.Status == "error" || sessionDetail != ""
	}
	// A refused start is read before the latest turn, as completion does:
	// after a refused resume the latest turn is an earlier one.
	if refused, ok := snapshot.latestTurnStartFailure(); ok {
		kind, provider := classifyProviderText(refused.Detail)
		return ProviderTurnError{Kind: kind, Detail: refused.Detail}, provider, nil
	}
	turn := thread.LatestTurn
	if turn == nil || turn.TurnID == "" {
		if !sessionFailed {
			return ProviderTurnError{}, false, nil
		}
		kind, provider := sessionFailureKind(sessionDetail)
		return ProviderTurnError{Kind: kind, Detail: sessionDetail}, provider, nil
	}
	switch turn.State {
	case "completed":
		session := thread.Session
		if session != nil && session.Status == "ready" && session.ActiveTurnID == nil && sessionDetail == "" {
			return ProviderTurnError{}, false, nil
		}
		kind, provider := sessionFailureKind(sessionDetail)
		return ProviderTurnError{Kind: kind, Detail: sessionDetail}, provider, nil
	case "error":
		detail := snapshot.failedTurnDetail()
		kind, provider := classifyProviderText(detail)
		return ProviderTurnError{Kind: kind, Detail: detail}, provider, nil
	case "running":
		// T3 can leave the turn running after the provider session failed
		// under it; nothing will end that turn on its own.
		if thread.Session == nil || thread.Session.Status != "error" {
			return ProviderTurnError{}, false, nil
		}
		detail := strings.TrimSpace(snapshot.failedTurnDetail())
		kind, provider := sessionFailureKind(detail)
		return ProviderTurnError{Kind: kind, Detail: detail}, provider, nil
	}
	return ProviderTurnError{}, false, nil
}
