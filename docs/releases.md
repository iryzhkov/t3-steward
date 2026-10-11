# Releases

Dispatch the `release` workflow from main with a full 40-character commit SHA
and a new `v*` version tag. The commit must be reachable from main; abbreviated
SHAs and existing tags are refused. The workflow waits up to 30 minutes for the
latest matching push run of `ci.yml` on main. The exact SHA, run, current attempt
and every job must be successful; PR CI does not count. Missing CI, failure,
cancellation, skipped jobs or timeout prevents tagging and publication.

After the gate, the workflow checks out the accepted SHA, creates and pushes its
tag without force, and publishes with GoReleaser in the same run. A tag pushed
with GITHUB_TOKEN does not start a second workflow. A hand-cut `v*` tag still
triggers the fallback release path, with the same ancestry and exact-CI gate.
Release this workflow change on main before dispatching a release of an older
commit, so the dispatch checkout contains the helper.

GoReleaser continues to publish SHA-256 `checksums.txt`. After publication the
workflow downloads all assets and requires a one-to-one match between the
checksum entries and assets (excluding the checksum file itself), with every
digest verified. A missing, extra or corrupt asset fails the run. This is a
post-publication check: failure does not undo a published release or tag.
Investigate failures before retrying; a dispatch with an existing tag refuses,
so the tag-push fallback can be rerun after diagnosing its failed run.

Users can still verify downloaded archives with:

```sh
sha256sum -c --ignore-missing checksums.txt
```

The decision helper supports offline replay without credentials:

```sh
go run ./scripts/release-ci --sha FULL_SHA --runs runs.json --jobs jobs.json --timed-out
```

`runs.json` has the workflow-runs API response shape; `jobs.json` is the array
of job-response pages emitted by `gh api --paginate --slurp`. The live helper
queries the jobs of the selected run attempt to exclude earlier attempts.
Tests for the gate and asset verification run under `make test`. No signing
keys or new secrets are involved.
