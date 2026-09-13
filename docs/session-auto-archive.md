# Settlement-based UI archiving

Steward automatically moves settled sessions to T3's reversible archive:
background tasks after two hours, user-initiated sessions after 24 hours. The
clock uses T3's settlement timestamp. Unknown background provenance uses 24 hours;
unknown settlement time is not eligible. Data and provider transcripts remain.

```yaml
ui_archive:
  enabled: true
  dry_run: false
  background_after: 2h
  user_after: 24h
  max_per_pass: 10
```

These are the defaults. UI cleanup has its own enable/dry-run settings and runs
on thread polls, independently of watchdog quota enforcement. The existing
`archive:` NAS export/deletion schedule remains separate and unchanged.

`t3-steward ui-archive candidates` prints JSON candidate IDs, classification and
settlement timestamps, plus total/archived counts, without archiving anything.
It also accepts `--config PATH`. Daemon effects are bounded by max_per_pass and
recorded as `ui-archive` actions after T3 confirms its archive projection.

Running or starting sessions, native background activity, pending approvals/input,
active overrides, post-settlement user activity, resume/wait delivery and
unfinished V2 custody block archiving. Provenance comes from legacy task state,
coordinator assignments/attempts and local worker journals. The normal UpKeeper
persistent-worker.yaml bootstrap is inspected read-only alongside the watchdog
configuration so remote worker custody is included. Unreadable/corrupt state
fences the pass. Lease expiry is never a release signal.

The session and busy state are re-read before dispatch. T3 0.0.38 does not expose
an atomic conditional archive command, so this is a best-effort race check,
not an atomic lock against simultaneous UI activity. Ambiguous responses are
accepted only when the archive projection is observed. The recorded settlement
prevents re-archiving a manually unarchived session until it settles again.

Read-only candidates and unit tests do not establish an improvement in UI latency;
verify deployed behavior and counts separately. Cold-storage deletion, if enabled
under archive:, retains its original independent policy.
