# Private GitHub SSH readiness qualification

Run explicitly with paths to an already authorized key and an already verified known-hosts file:

```sh
QUAL_GITHUB_KEY_PATH=/absolute/path/to/key \
QUAL_GITHUB_KNOWN_HOSTS_PATH=/absolute/path/to/known_hosts \
bash scripts/qualification/run-private-github.sh
```

The runner creates a disposable two-worker fleet under /tmp/t3qual.*, then checks the real private repository git@github.com:iryzhkov/UpKeeper.git at HEAD. It does not authorize keys, copy or read private-key bytes, submit workflow runs, or write to GitHub. SSH uses strict host checking, an explicit identity path, IdentitiesOnly, and no agent. The key must already grant read access.

The project declares credential reference qual-github-ssh. In phase one only the one-shot worker-a resolves its reference and has its SSH identity configured. The persistent worker-b has neither. The matrix must show authenticated-ok for worker-a and an unobserved repository with an explicit unavailable-reference detail for worker-b, which must never be ready_now.

Phase two references the same key for worker-b and restarts that worker and the coordinator. This clears process-local probe caching without changing repository, ref, or catalog. Configuration file hashes are checked to preserve this distinction. Both workers must now report observed authenticated-ok. Both phases verify zero workflow runs.

A coordinator with no credential resolver still honestly reports credential availability as unchecked, because snapshots do not contain secret-store state. Worker probes resolve the reference and perform the actual authenticated SSH Git read. A missing reference currently surfaces as network-unavailable with the precise credential error; this test does not relabel it authentication-failed.

## Evidence

Successful complete run: /tmp/t3qual.tbvnKz/evidence.

- private-github-one.json: worker-a authenticated-ok; worker-b observed false, explicit qual-github-ssh unavailable.
- private-github-both.json: both observed authenticated-ok.
- private-github-config-hashes.json: catalog/config bytes preserved across credential availability change.
- private-github-result.json: PASS, zero workflow runs.
- coordinator-phase-one.log, coordinator.log and worker-b.log: process evidence.

The synthetic quota observation remains stale, so candidates are accepted_waiting even when repository readiness succeeds. No claim of overall runnable readiness or live provider execution is made.

Earlier disposable attempts: t9b7lf stopped at a fixture syntax error; 5IE7qd proved phase one but stopped because its initial oracle expected a different error code. Both were cleaned up before the successful run.
