# Normalized 0.0.45 fixtures

codex-0045.log is the supplied sanitized failure event from the approved plan.
codex-0045-live.log and claude-0045.log were captured from real disposable npm
t3@0.0.45 sessions during this implementation on 2026-10-04 UTC. Only the
canonical quota envelope and normalized windows were retained; event/thread
identifiers were replaced with disposable labels. Native lines, account/auth
metadata, credential identities and tokens were excluded.

Upstream source tag v0.0.45 resolves to
6c8fed35dded9ff71c5b46807125457acbb76be6. Contracts:
packages/contracts/src/providerUsageLimits.ts and providerRuntime.ts;
mappers: apps/server/src/provider/Layers/codexUsageLimits.ts and
claudeUsageLimits.ts. These were read through Huyang.

The live Codex model was gpt-6.1-sol; the live Claude model was
claude-sonnet-4-6. No fixture asserts availability of a model-specific Codex
limit: upstream suppresses those notifications. See docs/t3-protocol.md for
qualification reproduction and evidence limits.
