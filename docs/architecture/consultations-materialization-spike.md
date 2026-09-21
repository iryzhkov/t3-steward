# Bounded consultation materialization feasibility

This C0 test-only spike adds no production schema, scheduler or API. It uses
`CoalesceSupervisionEvents` and the real SQLite supervision inbox as existing seams.

`TestMaterializationLeavesSQLiteInboxUnreadAcrossRestart` appends 100 and 1,000
synthetic events, selects at most four under a 2,048-byte serialized event-array cap,
then closes and reopens the database. Every event remains present and unconsumed.
`TestMaterializationOversizedEntryDoesNotStarveSmallerEntries` gives an oversized
first event an explicit projection outcome while selecting later smaller events.
It also reproduces why advancing a scalar cursor to sequence 2 hides an omitted
sequence 1. Projection alone is not acknowledgement or completed answer delivery.

## Integration decision

Select consultation IDs from authoritative pending consultation-request state.
An event cursor may acknowledge a notification but must never terminalize its
question. Answer commit, cancellation, timeout and selected-but-unanswered failure
operate on explicit request IDs and fences. Unselected requests remain pending.
This follows the reviewed plan and preserves ordinary supervision cursor semantics.
Do not introduce a generic event-acknowledgement rewrite merely to add consultations.
If generic event omission is later introduced, it must separately preserve omitted
holes; this test forbids silently treating scalar high-water advancement as that proof.

Only the `JSON` projection is a candidate model-input fragment. Omitted IDs and
oversized diagnostics are internal bookkeeping and must not be appended unboundedly
to model input. The spike loads the entire existing inbox; production must use bounded
queries. It does not yet bound the final package, fixed instructions, tool schemas,
other campaign data or runtime transcript. Aggregate final serialization, durable
selection and terminalization races, restart after acknowledgement, finalization
priority and model-facing flood qualification remain implementation/release gates.
