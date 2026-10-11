# Worker sequence recovery

Goal: implement Unit D21-4 on pinned base 8837123841415cea741cd5780c75bcf2afa5186f.

Checklist: base and brief verified; stop condition confirmed before source edits; split report written; affected command, make test and make lint passed; report-only implementation commit next.

Current step: committing report-only outcome; implementation stopped as required by brief.
Blockers: existing epoch/session recovery does not fence rewound command execution or reconcile offered attempts. New recovery semantics are required; authoritative brief requires stopping.
Last verification: make test-affected BASE=8837123841415cea741cd5780c75bcf2afa5186f exited 0 (no Go changes); make test exited 0; make lint exited 0. CPU limits applied. git diff --stat empty before report commit.
