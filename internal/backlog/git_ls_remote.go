package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// RepositoryReachability classifies one repository-reachability observation. It
// is a closed set so that a caller branches on the code rather than on the
// wording of a Git message, which changes between releases.
type RepositoryReachability string

const (
	// RepositoryAuthenticatedOK means the repository answered and the ref exists.
	RepositoryAuthenticatedOK RepositoryReachability = "authenticated-ok"
	// RepositoryAuthenticationFailed means the remote rejected the identity the
	// worker presented.
	RepositoryAuthenticationFailed RepositoryReachability = "authentication-failed"
	// RepositoryNotFound means the remote answered and has no such repository.
	RepositoryNotFound RepositoryReachability = "repository-not-found"
	// RepositoryRefNotFound means the repository exists and the ref does not.
	RepositoryRefNotFound RepositoryReachability = "ref-not-found"
	// RepositoryProbeTimeout means the probe exceeded its runtime bound.
	RepositoryProbeTimeout RepositoryReachability = "timeout"
	// RepositoryDNSFailure means the host name did not resolve.
	RepositoryDNSFailure RepositoryReachability = "dns-failure"
	// RepositoryNetworkUnavailable means the connection did not complete. It is
	// also what an unrecognised failure is classified as, because a Git message
	// this table does not know must never manufacture a permanent refusal.
	RepositoryNetworkUnavailable RepositoryReachability = "network-unavailable"
)

// Permanent reports whether waiting could change this observation. The
// distinction is about the request rather than about the moment: a repository
// that does not exist stays absent however long the caller waits, while a host
// that did not answer may answer in a minute.
func (r RepositoryReachability) Permanent() bool {
	switch r {
	case RepositoryAuthenticationFailed, RepositoryNotFound, RepositoryRefNotFound:
		return true
	default:
		return false
	}
}

// RepositoryProbeEvidenceTTL is how long one reachability observation may be
// reused. It is deliberately short. Reachability and authentication change
// exactly when a credential is rotated or a repository is renamed, which is
// when a stale positive is most expensive.
const RepositoryProbeEvidenceTTL = 10 * time.Minute

// defaultGitLsRemoteTimeout bounds one probe run when the caller declared no
// step timeout of its own.
const defaultGitLsRemoteTimeout = 30 * time.Second

// defaultGitLsRemoteOutputBytes bounds what one probe run may accumulate when
// the caller declared no bound. A repository with very many refs would
// otherwise be buffered in full before anything truncated it.
const defaultGitLsRemoteOutputBytes = 64 << 10

// gitLsRemoteProbe observes whether the project's repository and ref can be
// read under the execution identity and credential references the real task
// would use.
//
// The argument vector is fixed: git ls-remote --exit-code -- <repository>
// <ref>. There is no shell, and a manifest cannot supply the argv. Both values
// are validated by the same validators the project catalog applies before any
// argument vector is built, so a repository or ref that begins with a dash is
// refused as a value rather than passed where Git would read it as an option.
func gitLsRemoteProbe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	if err := ValidateRepositoryProbeArguments(request.Repository, request.Ref); err != nil {
		return ProbeResult{}, err
	}
	bound := request.MaxOutputBytes
	if bound <= 0 {
		bound = defaultGitLsRemoteOutputBytes
	}
	bounded := request
	bounded.MaxOutputBytes = bound
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultGitLsRemoteTimeout)
		defer cancel()
	}
	return runProbeCommand(ctx, bounded, "git", "ls-remote", "--exit-code", "--", request.Repository, request.Ref)
}

// ValidateRepositoryProbeArguments refuses anything the probe may not turn into
// an argument vector. It reuses the catalog's repository and ref validators
// rather than restating them, so a value the catalog accepts and the probe
// refuses cannot exist.
func ValidateRepositoryProbeArguments(repository, ref string) error {
	if err := validateGitRepository(repository); err != nil {
		return fmt.Errorf("repository probe repository: %w", err)
	}
	if err := validateGitRef(ref); err != nil {
		return fmt.Errorf("repository probe ref: %w", err)
	}
	// The validators above already reject a leading dash, but the refusal is
	// restated here because it is the property that keeps a value from being
	// read as an option, and it must not depend on a detail of another package's
	// syntax rules staying as it is.
	if strings.HasPrefix(repository, "-") {
		return errors.New("repository probe repository: a value beginning with \"-\" is refused, never passed as a flag")
	}
	if strings.HasPrefix(ref, "-") {
		return errors.New("repository probe ref: a value beginning with \"-\" is refused, never passed as a flag")
	}
	return nil
}

// ClassifyRepositoryProbe maps one probe run onto the frozen classification.
//
// The table below was measured against real Git output rather than assumed.
// git ls-remote --exit-code reports 0 when at least one ref matched, 2 when the
// repository answered and no ref matched, and 128 for every transport,
// resolution, authentication and lookup failure alike, so everything except a
// missing ref has to be read out of the message.
//
// The order of the checks matters. "Could not read from remote repository"
// accompanies both an authentication failure and a missing repository, so the
// specific evidence is tested before the generic wording.
func ClassifyRepositoryProbe(exitCode int, output string, err error) RepositoryReachability {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return RepositoryProbeTimeout
	}
	if err == nil && exitCode == 0 {
		return RepositoryAuthenticatedOK
	}
	if exitCode == 2 {
		return RepositoryRefNotFound
	}
	message := strings.ToLower(output)
	if err != nil {
		message += "\n" + strings.ToLower(err.Error())
	}
	switch {
	case containsAny(message, "could not resolve host", "name or service not known", "no address associated with hostname", "temporary failure in name resolution"):
		return RepositoryDNSFailure
	case containsAny(message, "authentication failed", "access denied", "permission denied", "invalid username or password", "terminal prompts disabled", "could not read username", "403 forbidden", "401 unauthorized"):
		return RepositoryAuthenticationFailed
	case containsAny(message, "repository not found", "does not appear to be a git repository", "not found", "404"):
		return RepositoryNotFound
	case containsAny(message, "failed to connect", "could not connect to server", "connection refused", "connection timed out", "network is unreachable", "connection reset", "operation timed out", "ssl", "tls"):
		return RepositoryNetworkUnavailable
	default:
		// An unrecognised failure is temporary on purpose. Classifying it as
		// permanent would refuse a submission on evidence nobody measured.
		return RepositoryNetworkUnavailable
	}
}

func containsAny(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

// RepositoryProbeKey identifies one reachability observation. A change to any
// component invalidates the evidence, which is what makes a cached positive
// safe: rotating a credential or renaming a repository produces a different
// key rather than a stale answer.
type RepositoryProbeKey struct {
	WorkerID       string
	CatalogDigest  string
	Repository     string
	Ref            string
	CredentialRefs []string
}

// Digest is the stable identity of the key. Credential references take part in
// it, and no credential value ever does.
func (k RepositoryProbeKey) Digest() string {
	refs := append([]string(nil), k.CredentialRefs...)
	sort.Strings(refs)
	var builder strings.Builder
	for _, part := range append([]string{k.WorkerID, k.CatalogDigest, k.Repository, k.Ref}, refs...) {
		builder.WriteString(part)
		builder.WriteByte(0)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// RepositoryProbeObservation is one classified, retained result. It carries no
// credential value, and its detail has been redacted before it was stored.
type RepositoryProbeObservation struct {
	Key        RepositoryProbeKey
	Class      RepositoryReachability
	ExitCode   int
	Detail     string
	ObservedAt time.Time
}

// RepositoryProbeCache retains observations for RepositoryProbeEvidenceTTL.
type RepositoryProbeCache struct {
	TTL     time.Duration
	Now     func() time.Time
	mu      sync.Mutex
	entries map[string]RepositoryProbeObservation
}

func (c *RepositoryProbeCache) ttl() time.Duration {
	if c != nil && c.TTL > 0 {
		return c.TTL
	}
	return RepositoryProbeEvidenceTTL
}

func (c *RepositoryProbeCache) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

// Lookup returns a retained observation for key that is still inside its TTL.
func (c *RepositoryProbeCache) Lookup(key RepositoryProbeKey) (RepositoryProbeObservation, bool) {
	if c == nil {
		return RepositoryProbeObservation{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	observation, found := c.entries[key.Digest()]
	if !found {
		return RepositoryProbeObservation{}, false
	}
	if c.now().Sub(observation.ObservedAt) >= c.ttl() {
		delete(c.entries, key.Digest())
		return RepositoryProbeObservation{}, false
	}
	return observation, true
}

// Store retains one observation under its key.
func (c *RepositoryProbeCache) Store(observation RepositoryProbeObservation) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]RepositoryProbeObservation)
	}
	if observation.ObservedAt.IsZero() {
		observation.ObservedAt = c.now()
	}
	c.entries[observation.Key.Digest()] = observation
}

// ObserveRepository runs the built-in probe for key through runner and retains
// the classified result. A retained observation inside its TTL is returned
// without running anything.
//
// The detail is the first line of the redacted output. Credentials never reach
// it: the output is redacted before it is stored, and the probe reports that a
// credential reference resolved rather than what it resolved to.
func ObserveRepository(ctx context.Context, cache *RepositoryProbeCache, runner PreflightRunner, workspaceDir string, key RepositoryProbeKey) (RepositoryProbeObservation, error) {
	if observation, found := cache.Lookup(key); found {
		return observation, nil
	}
	if err := ValidateRepositoryProbeArguments(key.Repository, key.Ref); err != nil {
		return RepositoryProbeObservation{}, err
	}
	result, err := gitLsRemoteProbe(ctx, ProbeRequest{
		Runner:       runner,
		WorkspaceDir: workspaceDir,
		WorkerID:     key.WorkerID,
		StepID:       "repository-reachability",
		Repository:   key.Repository,
		Ref:          key.Ref,
	})
	var exitError *ProcessExitError
	if err != nil && !errors.As(err, &exitError) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return RepositoryProbeObservation{}, err
	}
	detail, _ := DefaultRedactor().Redact(result.Output)
	observation := RepositoryProbeObservation{
		Key:        key,
		Class:      ClassifyRepositoryProbe(result.ExitCode, result.Output, err),
		ExitCode:   result.ExitCode,
		Detail:     firstLine(strings.TrimSpace(detail)),
		ObservedAt: cache.now(),
	}
	cache.Store(observation)
	return observation, nil
}
