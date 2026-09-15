# Fleet route revocation

An authored catalog change now fences three boundaries:

- Coordinator planning ignores persisted observations whose worker was removed or whose catalog digest is superseded. A fresh exchange must report the current catalog before new placement.
- Offer construction validates the committed worker/provider/model/quota route against the current authored binding before packaging or input delivery.
- The worker validates that same route before workspace preparation and again before thread creation. This second check covers a journal that already completed preparation.

Thread creation also resolves the package against the current per-worker project catalog, including after recovery finds an already prepared workspace. Removing project eligibility or changing its resolved environment refuses a new provider turn. An unrelated catalog digest change does not invalidate a still-matching prepared environment.

These authorization checks use authored inventories. Availability observations and draining are separate concerns and do not retroactively revoke authorized execution. Empty provider or model lists authorize no routes. Project authorization remains enforced by the per-worker project catalog.

The protocol is unchanged. Production constructors always supply current authorization; the lower-level builder and driver retain optional authorization for existing embedding/test callers.

## Evidence

The red reproduction in commit 8fc83e9 uses a real SQLite coordinator store, an old persisted worker snapshot, a changed authored catalog, real assignment planning and package construction, the worker offer journal and LocalDriver, and a recording T3 client. Before the fix it records one thread creation with the revoked model. No external provider is called.

The regression independently checks planning snapshot rejection, offer refusal, preparation refusal and thread-creation refusal. It also verifies that a current catalog observation remains eligible and that the previously authorized offer still packages successfully as a control.

## Operational limit

Already committed offered assignments retain their old route. They are withheld by the new offer gate rather than silently rerouted. Operators can cancel/rerun affected pending work or restore the route deliberately. This change does not terminate provider turns already running when authorization changes.
