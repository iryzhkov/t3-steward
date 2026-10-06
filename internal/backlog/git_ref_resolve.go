package backlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidExactRef marks a ref name the exact-ref resolver refuses before it
// runs anything.
var ErrInvalidExactRef = errors.New("invalid exact ref")

// ValidateExactRef accepts only a full, well-formed ref name.
//
// It applies the catalog's ref rules, which follow git check-ref-format, and
// then the two rules that make a name exact: it must be a full name under refs/
// (so no short name Git would expand, and no HEAD), and it must not contain any
// pattern character. The catalog already refuses *, ? and [; the refusal of a
// component ending in .lock and of a lone @ is added here because the catalog
// checks only the whole name.
func ValidateExactRef(ref string) error {
	if err := validateGitRef(ref); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExactRef, err)
	}
	components := strings.Split(ref, "/")
	if len(components) < 3 || components[0] != "refs" {
		return fmt.Errorf("%w: %q is not a full ref name under refs/", ErrInvalidExactRef, ref)
	}
	for _, component := range components {
		if strings.HasSuffix(component, ".lock") || component == "@" || strings.ContainsRune(component, '\x7f') {
			return fmt.Errorf("%w: %q is not a safe Git ref", ErrInvalidExactRef, ref)
		}
	}
	return nil
}

// ExactRefRequest names one exact ref on one repository. CredentialRefs are
// references only; the caller has already established that they resolve here.
type ExactRefRequest struct {
	Repository     string
	Ref            string
	CredentialRefs []string
	MaxOutputBytes int
	Timeout        time.Duration
}

// ExactRefResolution is one observation of one ref. ObjectID is set only when
// Found. Class is the reachability classification: authenticated-ok when the
// remote answered, ref-not-found when it answered without the ref, and the
// probe's own classes when it did not answer.
type ExactRefResolution struct {
	Ref      string
	Found    bool
	ObjectID string
	Class    RepositoryReachability
	ExitCode int
	Detail   string
}

// ResolveExactRef asks the remote which object exactly one ref names. It runs
// the probe's fixed argument vector, git ls-remote --exit-code -- <repository>
// <ref>, through runner, and retains nothing: every call observes the remote.
//
// git ls-remote matches its pattern against the tail of each ref name, so the
// listing for refs/heads/a can also contain refs/heads/x/refs/heads/a. Only a
// record whose name is byte-for-byte the requested ref counts. A listing that
// was truncated, malformed, or names the ref twice with different objects
// yields an error and never an object ID.
func ResolveExactRef(ctx context.Context, runner PreflightRunner, workspaceDir string, request ExactRefRequest) (ExactRefResolution, error) {
	if err := ValidateExactRef(request.Ref); err != nil {
		return ExactRefResolution{}, err
	}
	if err := ValidateRepositoryProbeArguments(request.Repository, request.Ref); err != nil {
		return ExactRefResolution{}, err
	}
	if runner == nil {
		return ExactRefResolution{}, errors.New("resolve exact ref: no process runner")
	}
	bound := request.MaxOutputBytes
	if bound <= 0 {
		bound = defaultGitLsRemoteOutputBytes
	}
	timeout := request.Timeout
	if timeout <= 0 {
		timeout = defaultGitLsRemoteTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := runner.Run(ctx, ProcessRequest{
		ID:             "probe-repository-ref-git",
		Dir:            workspaceDir,
		Program:        "git",
		Args:           []string{"ls-remote", "--exit-code", "--", request.Repository, request.Ref},
		MaxOutputBytes: bound,
	})
	var exitError *ProcessExitError
	if err != nil && !errors.As(err, &exitError) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return ExactRefResolution{}, err
	}
	detail, _ := DefaultRedactor().Redact(result.Output)
	resolution := ExactRefResolution{
		Ref:      request.Ref,
		Class:    ClassifyRepositoryProbe(result.ExitCode, result.Output, err),
		ExitCode: result.ExitCode,
		Detail:   firstLine(strings.TrimSpace(detail)),
	}
	if resolution.Class != RepositoryAuthenticatedOK {
		return resolution, nil
	}
	objectID, err := exactRefObject(result, request.Ref)
	if err != nil {
		return ExactRefResolution{Ref: request.Ref, Class: RepositoryAuthenticatedOK, ExitCode: result.ExitCode}, err
	}
	if objectID == "" {
		// The remote answered and listed only refs whose names end with the
		// requested one. That is the requested ref's absence, not its presence.
		resolution.Class = RepositoryRefNotFound
		return resolution, nil
	}
	resolution.Found, resolution.ObjectID, resolution.Detail = true, objectID, ""
	return resolution, nil
}

// exactRefObject reads the one object ID listed for ref. It returns "" when no
// record names ref exactly.
func exactRefObject(result ProcessResult, ref string) (string, error) {
	if result.Truncated {
		return "", errors.New("resolve exact ref: the ref listing exceeded its output bound")
	}
	if result.Output != "" && !strings.HasSuffix(result.Output, "\n") {
		return "", errors.New("resolve exact ref: the ref listing ends with an incomplete record")
	}
	found := ""
	for _, line := range strings.Split(strings.TrimSuffix(result.Output, "\n"), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			return "", errors.New("resolve exact ref: the ref listing has a malformed record")
		}
		if fields[1] != ref {
			continue
		}
		if !isLowerFullObjectID(fields[0]) {
			return "", errors.New("resolve exact ref: the ref listing names no full object ID")
		}
		if found != "" && found != fields[0] {
			return "", errors.New("resolve exact ref: the ref listing names the ref twice with different objects")
		}
		found = fields[0]
	}
	return found, nil
}

func isLowerFullObjectID(value string) bool {
	return isFullGitObjectID(value) && strings.ToLower(value) == value
}
