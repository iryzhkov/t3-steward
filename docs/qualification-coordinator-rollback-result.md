# Case 17 coordinator rollback extension, 2026-09-15

**PASS: all 15 process assertions.**

## Source and exact commands

UpKeeper binary: `0c6255bce225b28cb0ae78ad1bb9a6d2a1280f63`,
`vcs.modified=false`, based on `443adc4`.
The command was:

```sh
python3 scripts/qualification/coordinator_rollback.py /home/igor/Work/wt-qual-rollback-source
```

It exited 0. Log: `/tmp/coordinator-rollback-final.log`.
Evidence: `/tmp/t3-coordinator-rollback.cyfmm3dd/evidence`.
No Steward code or binary is involved in this additional ownership test.

The runner compiles the actual CLI, applies coordinator-only intent through a
real `__agent` process, and invokes the supported
`upkeeper rollback --to <historical-commit> --hosts normandy --components steward-fleet-configuration --json`.
The release checkout is disposable, with the baseline intent actually committed
to Git. The actual controller reads that historical release and dispatches the
actual target agent in a separate process.

Only the SSH network transport is replaced with an explicit local subprocess
adapter, and systemctl reports absent disposable legacy units. These boundaries
prevent contact with fleet hosts or the real user service manager. This proves
the process, historical-release selection and filesystem rollback path; it
does not qualify SSH transport or service convergence.

## Outcomes

| Historical release | Outcome |
| --- | --- |
| Existing coordinator intent | Exact prior coordinator bytes restored |
| Fleet component absent | Coordinator file returns to absent |
| Another host named coordinator | Coordinator file returns to absent |

In every case the forward application changed the coordinator file and set
mode 0600. Rollback returned exit 0, preserved sibling files, and durably retained
the forward coordinator bytes in its own run-private prior.json before replacement
or removal. The worker bootstrap sentinel survived all three cases. Client absence
survived the existing-coordinator rollback; client sentinel bytes survived the
absent-component and moved-coordinator rollback. The latter sentinels are seeded
after forward application because a coordinator intentionally removes a remote
client projection during forward application.

Exact pre/post digests, dispatched payloads, binary metadata and complete CLI
reports are saved under the evidence root. Existing-document digest before and
after rollback:
`49a7b15fc3a2ca1c3ee25a8af926b0b32dad2131beb0feee4a885b2b62ace159`.

## Negative control and fix

The same process oracle at clean `443adc4` exited 1:
`/tmp/coordinator-rollback-negative2.log`,
`/tmp/t3-coordinator-rollback.qtxrio22/evidence`.
Existing-document restoration passed, but absent/moved historical intent
returned success while leaving the newer authorization file active. Neither
retirement had a retained rollback record.

The fix dispatches coordinator cleanup for an absent fleet component during
explicit historical rollback when that component, or all components, is selected.
A host no longer named by present fleet intent uses the same cleanup.
The cleanup only removes an owner-only regular file with the known closed
coordinator schema/version, retaining its bytes first. Worker/client siblings
are outside that cleanup. Unknown/future formats, symlinks and public files
are refused. Ordinary convergence of releases without the component remains
unchanged.

An earlier harness attempt failed because disposable legacy service observations
were missing; `/tmp/coordinator-rollback-negative.log` is preserved but is not
the product negative control.

## Validation and boundaries

- `go test ./internal/agent ./internal/converge -count=1`: PASS.
- `make go-gate`: PASS (format, vet, package tests, race tests, linux amd64/arm64
  builds); log `/tmp/coordinator-rollback-go-gate.log`.
- Added unit coverage proves preview does not write, retirement retains bytes
  restorable through the existing internal recovery function, sibling preservation,
  and unknown/future/public/symlink refusal.
- Full gate ran on the final code diff before commit; the process rerun compiled
  the exact clean committed source afterward.
- No full all-case qualification suite was rerun. Earlier cases keep their
  separately recorded source identities.
- No live config, credential installation, enrollment, publication or deployment.
  This runner leaves evidence files, with no background services.
