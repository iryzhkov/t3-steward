# Model discovery policy

An operator may set a provider's desired model list to ["*"] to authorize
current and future models for that provider. This policy is explicit and
provider-specific; it does not enable another provider or quota pool.
An empty list still revokes model authorization, and a list of model names
still restricts authorization to those names. A wildcard must appear alone.

The worker resolves the policy against its authenticated, enabled, installed
provider cache on every inventory observation. Its advertised inventory contains
concrete models only. A newly discovered model can therefore become schedulable
without editing the fleet manifest or restarting the worker. Missing or
unauthenticated providers advertise no models. Coordinator scheduling still
requires the accepted worker snapshot, project eligibility, quota admission
and the existing execution fences.

This does not make an upstream rollout available to an account that has not
received it, or make an old client understand a newer provider protocol.
Keep the clients updated and their normal provider-cache refresh running.

For `t3-steward models --json` consumers, use `routes[].availability` for the
specific model's admission verdict; instance availability summarizes whether
any route is available. Pair `percent` with `observedAt` and `resetsAt`, which
come from the same governing quota reading. Use `oldestObservedAt` to inspect
the oldest pool reading instead of treating `observedAt` as the pool's oldest
sample. On a coordinator reporting worker authorization, an absent entry means
`authorized: false`; a coordinator that reports no authorization data retains
unknown worker authorization. Provider-cache model lists are observations,
not this command's admission or authorization verdict.
