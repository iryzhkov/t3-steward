package backlog

import (
	"testing"
	"time"
)

// TestClassifyForgeWordings pins the forge-specific messages that fell through
// to the temporary default.
//
// Provenance matters here, so it is recorded per row. The GitHub, Azure
// DevOps organisation, Codeberg and sourcehut rows were measured directly with
// an anonymous read-only git ls-remote. The GitLab "could not be found",
// Azure DevOps TF401019 and Gitea "Unauthorized" rows come from the review that
// found them: all three are emitted only on an authenticated request, and
// reproducing them would have required live credentials against a private
// repository, which this work may not use. Their bytes are pinned here exactly
// as reported, so a future measurement either confirms them or fails this test.
func TestClassifyForgeWordings(t *testing.T) {
	tests := []struct {
		name     string
		output   string
		measured bool
		want     RepositoryReachability
	}{
		{
			name:   "gitlab hides a private or absent project behind could not be found",
			output: "remote: \nremote: ========================================================================\nremote: \nremote: The project you were looking for could not be found or you don't have permission to view it.\nremote: \nremote: ========================================================================\nremote: \nfatal: Could not read from remote repository.\n",
			want:   RepositoryNotFound,
		},
		{
			name:   "azure devops reports TF401019",
			output: "remote: TF401019: The Git repository with name or identifier fleet does not exist or you do not have permissions for the operation you are attempting.\nfatal: repository 'https://dev.azure.com/org/project/_git/fleet/' not found\n",
			want:   RepositoryNotFound,
		},
		{
			name:   "gitea refuses with a bare Unauthorized",
			output: "remote: Unauthorized\nfatal: Authentication failed for 'https://gitea.example/owner/project.git/'\n",
			want:   RepositoryAuthenticationFailed,
		},
		{
			name:     "azure devops reports an absent organisation as not found",
			output:   "fatal: repository 'https://dev.azure.com/no-such-org-xyz-123/_git/x/' not found\n",
			measured: true,
			want:     RepositoryNotFound,
		},
		{
			name:     "codeberg reports an absent repository",
			output:   "remote: Not found.\nfatal: repository 'https://codeberg.org/no-such-user-xyz-123/x.git/' not found\n",
			measured: true,
			want:     RepositoryNotFound,
		},
		{
			name:     "sourcehut reports an absent repository",
			output:   "fatal: repository 'https://git.sr.ht/~nosuchuser-xyz-123/x/' not found\n",
			measured: true,
			want:     RepositoryNotFound,
		},
		{
			name:     "gitlab refuses an anonymous request with basic auth",
			output:   "remote: HTTP Basic: Access denied. If a password was provided for Git authentication, the password was incorrect or you're required to use a token instead of a password.\nfatal: Authentication failed for 'https://gitlab.com/gitlab-org/no-such-project-xyz-123.git/'\n",
			measured: true,
			want:     RepositoryAuthenticationFailed,
		},
		{
			name:     "azure devops refuses an anonymous request",
			output:   "fatal: Authentication failed for 'https://dev.azure.com/microsoft/_git/no-such-repo-xyz/'\n",
			measured: true,
			want:     RepositoryAuthenticationFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyRepositoryProbe(128, test.output, &ProcessExitError{ExitCode: 128})
			if got != test.want {
				t.Fatalf("classification = %q, want %q", got, test.want)
			}
			if !got.Permanent() {
				t.Fatalf("%q must be permanent: a forge that answered is evidence", got)
			}
		})
	}
}

// TestGitLabWordingDoesNotContainNotFound is the reason the GitLab row needs a
// substring of its own: the generic "not found" test would not have matched it.
func TestGitLabWordingDoesNotContainNotFound(t *testing.T) {
	const wording = "the project you were looking for could not be found or you don't have permission to view it."
	if containsAny(wording, "not found") {
		t.Fatal("the generic substring already covers this wording; the specific one is redundant")
	}
	if containsAny(wording, "permission denied") {
		t.Fatal("the authentication list claims this wording before the not-found list can")
	}
}

// TestRepositoryProbeCacheUsesAnElapsedClock states that retention is measured
// on elapsed time. A wall clock that steps backwards must not extend a live
// window, which is what calling UTC on every reading used to allow.
func TestRepositoryProbeCacheUsesAnElapsedClock(t *testing.T) {
	reading := time.Now()
	cache := &RepositoryProbeCache{TTL: time.Minute, Now: func() time.Time { return reading }}
	key := RepositoryProbeKey{WorkerID: "homelab", Repository: "https://example.invalid/x.git", Ref: "main"}
	cache.Store(RepositoryProbeObservation{Key: key, Class: RepositoryAuthenticatedOK})

	// A backwards step larger than the window. The entry must still expire on
	// schedule, because the expiry was fixed when the entry was written.
	reading = reading.Add(-time.Hour)
	if _, found := cache.Lookup(key); !found {
		t.Fatal("a backwards clock step expired a live entry")
	}
	reading = reading.Add(time.Hour + time.Minute)
	if _, found := cache.Lookup(key); found {
		t.Fatal("a backwards clock step extended the retention window")
	}

	defaulted := &RepositoryProbeCache{}
	defaulted.Store(RepositoryProbeObservation{Key: key, Class: RepositoryAuthenticatedOK})
	if _, found := defaulted.Lookup(key); !found {
		t.Fatal("the default clock expired an entry immediately")
	}
}

// TestRepositoryProbeCacheIsBounded states that the retained set cannot grow
// without limit. A coordinator sees a new key whenever a worker re-enrols, a
// ref changes or a campaign names a different repository, and nothing ever
// looks those keys up again.
func TestRepositoryProbeCacheIsBounded(t *testing.T) {
	reading := time.Now()
	cache := &RepositoryProbeCache{TTL: time.Minute, Now: func() time.Time { return reading }}
	for index := range maxRepositoryProbeEntries * 2 {
		cache.Store(RepositoryProbeObservation{
			Key:   RepositoryProbeKey{WorkerID: "homelab", Repository: "https://example.invalid/x.git", Ref: refName(index)},
			Class: RepositoryAuthenticatedOK,
		})
	}
	if size := cache.Len(); size > maxRepositoryProbeEntries {
		t.Fatalf("retained %d entries, want at most %d", size, maxRepositoryProbeEntries)
	}

	// Expired entries are swept on write rather than waiting for a lookup that
	// may never come.
	reading = reading.Add(2 * time.Minute)
	cache.Store(RepositoryProbeObservation{
		Key:   RepositoryProbeKey{WorkerID: "homelab", Repository: "https://example.invalid/x.git", Ref: "fresh"},
		Class: RepositoryAuthenticatedOK,
	})
	if size := cache.Len(); size != 1 {
		t.Fatalf("retained %d entries after everything expired, want 1", size)
	}
}

func refName(index int) string {
	const digits = "0123456789"
	name := ""
	for value := index; ; value /= 10 {
		name = string(digits[value%10]) + name
		if value < 10 {
			break
		}
	}
	return "refs/heads/r" + name
}
