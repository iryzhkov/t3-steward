# Remote task-wait qualification

The earlier lifecycle harness passed the coordinator's configuration directly to the task wait CLI and shared one synthetic T3 server across hosts. It therefore did not prove that a non-coordinator host could register a task wait through its configured remote endpoint or deliver the resulting wake to its own provider.

## Disposable scenario

Run `bash scripts/qualification/remote_wait.sh`. Optional `REMOTE_WAIT_SOURCE=/absolute/isolated/worktree` selects the source to build; the harness always builds its own binary and records its commit, dirty status and binary SHA256. It retains its private temporary root and stops its tracked processes on exit.

The test creates three distinct synthetic T3 servers for the coordinator, worker-a and worker-b; a real loopback OpenSSH server; restricted coordinator/worker forced commands; private per-host configuration, bootstrap identities and credentials; and disposable Git repositories. It does not call an installed coordinator or provider. The catalog's unused HTTPS route points at a refused loopback port, not a live forge.

A real workflow task on worker-a invokes `t3-steward wait add --config <worker-a-config> --task current` using its issued workspace identity. Its configuration names the restricted SSH coordinator endpoint. The worker has no coordinator admin socket. The condition asserts worker-a's private home identity and runs in the prepared worker workspace. The coordinator receives the durable task-wait registration, while the executable local polling record remains in worker-a's database.

Three samples require `waiting-external` progress/control after the provider's first turn has actually stopped, with no settlement or second turn. The harness then changes the worker-local condition. Success requires coordinator-owned met/delivered/resumption evidence, exactly two starts on the original worker thread, zero starts for that thread at the other two T3 endpoints, a successful final attempt, collected output and verification. The SSH journal must name the restricted coordinator admin command.

## Red evidence

Final harness `9a5f2d6` on product baseline `9ebde2f` failed as required:

- Root: `/tmp/t3qual.akmQMD`
- Foreground execution log: `/tmp/remote-wait-final-red.log`, exit 1.
- Actual CLI evidence: `evidence/register.txt` attempts `worker-a/state.db.admin.sock` and reports that the socket does not exist, despite the worker configuration naming remote SSH.

The earlier equivalent baseline failure at `/tmp/t3qual.g8QBFk` is diagnostic corroboration. Initial fixture-only failures (missing catalog input and a polling interval below the supported minimum) are not product regression evidence.

## Preliminary green evidence

`/tmp/t3qual.FSuonb` completed with exit 0 against the implementer's dirty transport worktree; `/tmp/remote-wait-green1.log` and `evidence/remote-wait-verdict.json` record all success assertions. That run preceded final bootstrap fencing and used a harness under development, so it is diagnostic evidence only. The following frozen-source result supersedes it.

## Final green evidence

Clean product source `d2e4bc5ff6a2cb4859e3cb2167af3ab3e1e5a60a`, harness `9a5f2d6` (only this report was uncommitted), passed every assertion with exit 0. Root `/tmp/t3qual.glkbgF`; log `/tmp/remote-wait-final-green.log`; decisive record `evidence/remote-wait-verdict.json`. Source status was empty. Binary SHA256: `a50b11dd36ff9d9a3a3217a1fd54f39a0afa8105d6141871f837b67cc3e1cb9b`.

- Run: `run-a204689027421b457701c38b05eb5c6c`
- Attempt: `attempt-18e532e2e1ece6e727d408e08cb8b108`
- Thread: `thread-39f8401eca51f6ddf6c45c51118afd84`
- Task wait: `tw-b5b58ea8`
- Delivery: `task-wake:tw-b5b58ea8:6`

The worker-local condition recorded both pending and ready observations in worker-a's private home and prepared workspace. Three durable parked samples followed the first provider turn's completion. The coordinator recorded met, resumption and delivered states. Exactly two starts reached the original worker thread; neither other T3 endpoint received a start for it. Final workflow evidence contained both collected output and verification. All tracked fixture processes were stopped before the exit-0 result.

This proves one remote task-bound wait. It does not replace separate grouped each/all, restart or race coverage. Earlier shared-provider lifecycle evidence must not be cited as proof of remote registration or worker-owned delivery.
