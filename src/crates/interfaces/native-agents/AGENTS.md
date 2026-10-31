# Native Agents Guide

Scope: this guide applies to `src/crates/interfaces/native-agents`.

`openbitfun-native-agents` owns the native agent drivers: Claude Code
(stream-json), Codex (app-server JSON-RPC) and Pi (RPC). They run the vendor
CLI through its own protocol and are not ACP clients. Decisions and the
runtime-ownership plan are in
[`docs/architecture/native-agents.md`](../../../../docs/architecture/native-agents.md).

## Guardrails

- Do not add a new native agent unless the product owner explicitly asks
  (decision D1).
- Persisted identifiers never change: `acp:<client>` agent types,
  `acp_<client>_<uuid>` session ids, `provider: "acp"` metadata, and driver
  metadata keys such as `claudeNativeHistory`.
- This crate must not depend on `openbitfun-acp`. The ACP client depends on
  this crate for the shared external-CLI utilities.
- Keep host-specific APIs (Tauri) out; hosts receive presentation updates
  through `NativeAgentNotificationSink`.
- Approvals and questions go through `interaction.rs` to the runtime
  permission and user-question owners. Do not add a custom event or a
  driver-specific answer command; every surface answers through the mailbox.

## Drivers

The native driver registry and the object-safe `NativeAgentDriver` /
`NativeAgentSession` interfaces live in `src/lib.rs`. Manager and
desktop dispatch use that registry; adding a native provider must not extend the
ACP connection path. Capability facts live in `capabilities.rs`;
`process.rs` owns shared local/SSH stdio launch and supervision, line writes and
stderr tails, and `events.rs` preserves canonical EventQueue ingress ordering.
`preferences.rs` stores per-client defaults in `native_agent_preferences.json`,
reads the legacy Claude file only when the new file is absent, and falls back
from session metadata `nativeAgentPreferences` to `claudeNativePreferences`.

`claude/` remains the Claude Code stream-json implementation,
exposed through the existing external-client menu (`claude-native`), not an ACP
adapter. Claude identity/resume validation, control correlation, permissions,
turn state and subagent projection stay with that driver. Its process is
session-scoped; `result` ends a foreground segment, not the process. Preserve
generation fences, PID-scoped shutdown, native tool IDs in permission cards,
and the canonical EventQueue ingress. PID-tree helpers live in
`process_tree.rs`. Background tasks (`background.rs`) are tracked from `task_*` / `background_tasks_changed` frames between turns too;
an idle process (no turn, permission or running background task) is released
after `IDLE_RELEASE_AFTER` and the next turn resumes it.

Verification from the repository root (memory streams; no Claude account or
SSH host required):

```bash
CARGO_TARGET_DIR=target/check-native cargo test -p openbitfun-native-agents --lib
# One driver:
CARGO_TARGET_DIR=target/check-native cargo test -p openbitfun-native-agents --lib claude::
```

Live tests (`live_tests.rs`) are `#[ignore]` and need an installed, logged-in
CLI.

The tests cover projection, control correlation, permission wire compatibility,
turn cleanup, cancellation, stderr/EOF, retired generations, background notices,
legacy metadata and preference migration. They do not establish live SSH,
relay reconnect, Peer Device Mode or Detached Dispatch behavior.
