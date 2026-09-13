// Per-call context: never store a session ID in the shared server environment.
export const StewardSession = async () => ({
  "shell.env": async (input, output) => {
    // Empty overrides also clear inherited identities when no session is present.
    output.env.OPENCODE_SESSION_ID = input.sessionID || "";
    output.env.CLAUDE_CODE_SESSION_ID = "";
    output.env.CODEX_THREAD_ID = "";
  },
});
