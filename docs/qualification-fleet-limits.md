# Supplemental fleet and limits qualification

Run from this checkout:

```sh
UPKEEPER_SOURCE=/path/to/validated/UpKeeper bash scripts/qualification/fleet_limits.sh
```

The script builds Steward from this checkout and UpKeeper from the supplied source,
creates its own disposable fleet using the existing SSH harness, and keeps its
temporary root. It never reuses another harness's state. It takes about twelve
minutes because it waits for the product's real ten-minute repository evidence TTL.

## Cases and boundaries

- **Case 5:** compiled `upkeeper fleet-plan` reads the real shared fixture; compiled
  `upkeeper __agent` applies profiles into isolated homes named homelab and omarchy-pc.
  The plan reports Opus desired, absent observations as null, and `applied: false`.
  Case 5 remains **FAIL** until an end-to-end ownership consumer connects authored
  model allowlists to Steward's coordinator catalog and an accepted worker snapshot
  proves observed and effective Opus availability. Writing a worker bootstrap or
  displaying a review artifact does not prove that behavior.
- **Case 6:** change the desired worker model list and restart the coordinator.
  Read drift over the remote admin carrier before enrollment. Supply the additional
  synthetic provider observation independently. Reject a stale catalog digest and
  stale enrollment revision, accept the current fence, then execute exactly one
  task on the persistent worker. Assert both full digests and the expected revision
  appear in the matrix, no repeated drift log stream, and one provider turn start.
- **Case 17:** reject a worker-signed admin frame through the actual forced command;
  reject a frame over the server's 4 MiB cap; download a real declared artifact
  normally then reject it at a one-byte client cap without returning its payload.
  A separate client-configuration assertion rejects worker credential namespaces.
  Query expired worker snapshots and fresh closed quota admissions through SSH.
  Prime good repository evidence, break the actual Git forced-command endpoint,
  verify the cached answer still holds, then wait 605 seconds and observe a fresh
  temporary network failure.
- **Rollback:** the compiled UpKeeper target-side protocol applies earlier intent
  after changing both owned files. Both worker and client document bytes must
  change, and both must return byte-for-byte to their initial content on both
  disposable hosts. This qualifies target-side restore by earlier desired state.
  It does not claim to exercise the controller's Git release selection or a live
  service rollback.

The synthetic provider advertises models and executes scripts; no real provider
turn or quota is used. Stale worker observations and quota closure are explicit
fault injection into the disposable SQLite store, not tests of a provider's
quota ingestion. The fake Git failure is an actual nonzero remote process through
sshd, not an actual network outage.

`fleet_security.py <root>` can repeat the wire/closed-admission/artifact checks
while that disposable fleet is alive. Its evidence is in
`<root>/evidence/security-verdicts.txt`. The main run records its verdicts in
`supplemental-verdicts.txt`, and each process records stdout, stderr, and exit code
separately. Neither script logs credential values or attack request bodies.

The primary four-case harness and historical lifecycle scripts are unchanged.
This supplemental suite exits nonzero for every unqualified requirement,
including the still-unimplemented case 5 effective-model oracle.
