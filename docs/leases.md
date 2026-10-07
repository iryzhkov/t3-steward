# Ownership leases

Lease shared integration and release resources before changing them:
`repo:<project>/<branch>` for a repository main branch and
`release:<name>` for a release manifest, such as `release:upkeeper/manifest`.
Names preserve case, accept ASCII letters, digits and `._/:-`, and are limited
to 128 characters. `deploy:` is reserved for a later deployment-window feature.
Because case is preserved, `repo:Steward/main` and `repo:steward/main` are
different leases and do not exclude each other: always spell the project exactly
as the catalog does.

1. Acquire before integrating or running `upkeeper push`:
   `t3-steward lease acquire repo:t3-steward-github/main --plan jocasta:PLAN@REV --reason "integrate release" --ttl 2h --json`.
2. Save the returned fencing token. Check immediately before `git push` to main
   or `upkeeper push`: `t3-steward lease check repo:t3-steward-github/main`.
3. Renew before expiry when work runs long:
   `t3-steward lease renew repo:t3-steward-github/main --token TOKEN --ttl 1h`.
4. Release after publishing:
   `t3-steward lease release repo:t3-steward-github/main --token TOKEN`.

The owner defaults to the current canonical T3 thread. If resolution fails,
pass `--owner-thread THREAD`; provider session IDs are not canonical T3 thread IDs.
Acquire requires a reason; plan text is optional and limited to 256 characters.
TTL defaults to 2h and must be between 5m and 12h. Acquiring again as the same
holder returns its existing token without extending expiry. A new acquisition
after release or expiry increments the token; renew retains it.

Inspect with `lease show NAME --json` or `lease list --json`.
Every verb accepts `--json`. Check returns 0 when your thread holds the live
lease, 10 when another thread holds it, and 11 when free or expired.
Mutation conflicts and wrong fencing tokens return 10. Coordinator transport
failures retain exits 3–8: publish scripts must proceed only on exit 0.
An older coordinator reports that leases are unsupported and needs upgrading.

Mutation requests have generated replay IDs. Supply and retain
`--request-id KEY` to retry a lost answer; a different request under the same
ID is refused. For recovery, `lease release NAME --force --reason TEXT` records
the forcing thread and authenticated principal. Task-scoped supervisor
principals cannot mutate leases.

These leases provide a scriptable fence. Automatic Git hooks, publish-skill and
UpKeeper enforcement, resource-lock dispatch, deployment windows and expiry
notifications are follow-ups; operators must perform the checks above.
