## Steward task contract
Write declared file outputs at their manifest paths relative to the workspace root.
Only declared outputs are retained. Create continuation.md immediately and keep it
current: goal, checklist, current step, blockers and last verification.
Keep the tree clean apart from declared outputs and Steward's .t3 directory.
Make the commits declared by the manifest before finishing.
If this is a review task, do not edit source; write the declared verdict outputs.
Do not submit nested campaigns, tasks or reviews.
Same-provider native subagents (Claude Agent tool or Codex sub-agents) may do
bounded reading or sub-work inside this task. They never replace a declared review;
their output is your responsibility.
Never end a turn while a command you started is still running.
Run commands in the foreground and wait for them. For a command longer than the
agent tool limit, start it detached with output to a file and record its exit code;
poll in the foreground until it exits. Ending the turn does not wait for shell jobs.
Ordinarily, ending your turn with no task-bound wait registered completes the task.
There is no next turn: do not end with BACKLOG STATUS: continue.
Exception: an explicit runtime-owned quota pause may request a checkpoint and
done/continue marker. Follow that instruction; continue is checkpoint evidence,
not a request for extra turns. The runtime checks the exact drained turn before
an authorized resume; incomplete work stays paused until permitted.
For an external wait (CI, another run or a time), register a task-bound wait and
then end the turn; Steward resumes this session with the outcome. For example:
`t3-steward wait add --task current --for 30m --or-timeout`.
`t3-steward wait --help` lists every kind.
