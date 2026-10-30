# Native Agents: Decisions and Runtime Ownership

Native agents are the OpenBitFun drivers that run an external coding CLI
through its own native protocol: Claude Code (stream-json), Codex
(app-server JSON-RPC), and Pi (RPC). They are **not** ACP clients. The generic
ACP client path (`opencode`, `dsh`, `omp`, ACP-mode `claude-code`/`codex`) is
a separate feature and is out of scope here.

This document records the decisions for the native agent line and the plan
that moves native sessions from Desktop ownership into the shared runtime.
Driver-level implementation notes stay in the owning crate `AGENTS.md`.

## Reference

[codex-host](https://github.com/BytePioneer-AI/codex-host) is the reference
design for multi-agent hosting. Its `HarnessAdapter` / `HarnessSession`
contract, capability inventory, per-harness native notes, and delegation
specification are followed as **design references**, not as code
dependencies:

- its packages are unpublished (`private`, version `0.0.0`), LGPL-3.0, and its
  core is bound to the Codex Desktop protocol;
- the parts worth tracking over time are the capability inventory, the
  per-harness native protocol notes, and the delegation/thread semantics.

## Decisions

| # | Decision | Status |
|---|---|---|
| D1 | Do not add new native agents unless the product owner explicitly asks. Work goes into depth (runtime ownership, delegation, remote) for Claude, Codex, and Pi. | Accepted |
| D2 | codex-host is a design reference only; no code dependency or vendored adapters. | Accepted |
| D3 | Move native drivers out of `openbitfun-acp` into a dedicated interface crate (`openbitfun-native-agents`). The move is mechanical and changes no behavior. | Accepted |
| D4 | Native sessions are owned by the shared runtime, not by Desktop. Desktop, CLI, mobile web, IM bots, cron, `SessionMessage`, and delegation reach them through one runtime port. | Accepted |
| D5 | Native approvals and questions use the runtime `PermissionRequestManager` and `UserInputManager`. The custom `backend-event-acppermissionrequest` path is removed, not kept in parallel. | Accepted |
| D6 | The backend persists native turns. Frontend persistence of native turns is removed. | Accepted |
| D7 | Persisted identifiers stay unchanged: `acp:<client>` agent types, `acp_<client>_<uuid>` session ids, `provider: "acp"` metadata, driver metadata keys (`claudeNativeHistory`, ...). | Accepted |
| D8 | Delegation is built on top of D4, following the codex-host semantics (non-blocking start, independent writable child session, read/wait/send/cancel/watch). | Planned |

### D3: crate split

The native drivers depend on ACP only through one type (`agent_client_protocol::schema::Cost`)
and through names (`AcpSessionOptions`, `AcpAvailableCommand`,
`SubmitAcpPermissionResponseRequest`, ...). Their real dependencies are
generic external-CLI utilities currently in the ACP client module: process
group supervision, remote shell rendering, executable probing, session
metadata persistence, and tool-card normalization.

- New crate: `src/crates/interfaces/native-agents`, same layer as `acp`
  (it depends on `assembly/core` event queue, session storage, and SSH
  manager, so it cannot be an adapter).
- The shared CLI utilities move to the new crate; `openbitfun-acp` depends on
  it, not the reverse.
- Rust types may be renamed (`AcpSessionOptions` -> `AgentSessionOptions`)
  with unchanged serde shapes.
- Register the crate in `scripts/core-boundaries/rules/crate-layout.mjs`.
  Existing boundary rules reference only `acp/src/client/manager.rs` and
  `dsh_profile.rs`, so no native rule paths move.

### D5: one interaction path

Before: each driver emits `BackendEvent::Custom("backend-event-acppermissionrequest")`
and waits on a private oneshot with a timeout. Only the Desktop WebView and
Peer Mode fan-out receive it; the permission mailbox used by mobile web
(`get_session_interaction_mailbox`, `respond_permission`) and IM bots does not.

After: drivers register approvals with `PermissionRequestManager` and
questions with `UserInputManager`, then await the returned receiver. All
surfaces read and answer through the existing mailbox. The old event and the
`submit_acp_permission_response` path are removed; Desktop and Web UI are
updated in the same change. The product is upgraded as a whole, so no
old-client fallback is kept (see Compatibility).

### D6: backend persistence

Native turns are persisted only by the Web UI today. Commit `444fb2bcf`
("keep an ACP conversation on disk") made the frontend projection the sole
writer because no backend owner existed: the runtime neither starts nor
completes these turns, so it allocates no storage slot and holds no history
branch. This was a missing-owner workaround, not a compatibility requirement.

After D4 the runtime starts every native turn, so it allocates the slot and
persists the turn from the native event stream. The frontend allocation
(`nextStorageTurnIndex`) and frontend saves for `provider: "acp"` sessions are
removed. Effects of the current gap that this closes: no history without an
open window, and a turn lost when the window closes mid-turn.

## Current state (verified in code)

- Native sessions are prepared and driven only by Desktop Tauri commands in
  `src/apps/desktop/src/api/acp_client_api.rs`. The scheduler and coordinator
  have no native route.
- Presentation updates (context usage, background tasks, retry, rate limits,
  plan, options) are emitted with Tauri `AppHandle::emit` from
  `native_notification_sink`, so only the Desktop window receives them.
- Turn content events already enter the shared `EventQueue`
  (`native/events.rs`), so live projection works for any event subscriber.
- Mobile web and IM bots submit through `submit_remote_dialog`, which restores
  the session via the session manager. For an `acp:*` session the restore path
  finds no registered agent and falls back to a local mode
  (`session_manager.rs`, "Persisted session mode is unavailable; applying
  executable fallback"). Expected effect: the message runs on a local agent
  and the persisted mode is rewritten. **Needs a live check before the fix.**
- `remote_surface/table.rs` lists `create_acp_flow_session` and
  `start_acp_dialog_turn` as `Unaudited` / `CLI_NOT_IMPLEMENTED`.
- Native CLI processes run over `open_workspace_stdio` for SSH and container
  workspaces, so remote workspaces already work for an attached Desktop.
  Claude CLI update on a remote host is refused.

## Plan: runtime ownership (D3-D6)

Each step merges on its own.

1. **Crate split (D3).** Mechanical move, no behavior change.
2. **Notifications.** Replace the Tauri sink with the shared event emitter;
   event names and payloads stay the same, so the Web UI is unchanged.
3. **Interactions (D5).** Register approvals and questions with the runtime
   managers; remove the custom event and its Web UI handling.
4. **Runtime port (D4).**
   - Add an external-session port in `runtime-ports`; the native crate
     implements it and every host registers it at startup (same pattern as
     `set_openbitfun_control_port`).
   - Session restore recognizes `provider: "acp"` sessions and keeps them as
     external instead of applying a local fallback mode.
   - The scheduler gets a native execution branch beside
     `FreshExternalSubagent`: start through the port, record the active turn,
     and report the outcome on `scheduler_notify_tx` as `TurnOutcome`
     when the driver emits its terminal event. Busy, queue, steer, and cancel
     reuse scheduler semantics.
   - Desktop Tauri commands become thin calls into the port.
5. **Persistence (D6).** Runtime-owned slot allocation and turn save for
   native sessions; remove the frontend writer.

Per-surface result after step 5:

| Surface | Result |
|---|---|
| SSH / container workspace | Unchanged; the driver already launches the CLI there |
| Mobile web, IM bots | Send, cancel, steer, approve, and answer on existing native sessions. Creating a native session, switching model/permission: explicit unsupported reply |
| Peer Device Mode | Commands keep their registry rows; interaction moves to the mailbox |
| Detached Dispatch | Out of scope; explicit unsupported |
| CLI | Registers the port, so cron and `SessionMessage` work in CLI-hosted runtimes; CLI peer handlers are follow-up work |

Not in this plan: delegation (D8), remote CLI install/update, keeping a turn
alive across a controller disconnect, new native agents (D1).

## Driver contract gaps (follow-up)

Compared with the codex-host `HarnessAdapter` contract, `NativeAgentDriver`
already covers open/resume/fork/rollback, turns, steer, model/thinking/
permission selection, interactions, subagents, autonomous turns, and
background tasks. Missing pieces, to add when a consumer needs them:

- availability inspection without creating a session (install, auth, catalog);
- read-only snapshot from native history without starting the agent;
- typed errors (`busy`, `invalidState`, `unsupported`) instead of
  `Validation` strings;
- an unattended execution policy at create time (needed by D8);
- a session fault signal when the native process dies unrecoverably.

## Compatibility

- The product is upgraded as a whole on every machine the user runs. Wire and
  event shapes between Desktop, Web UI, and the runtime may change together
  without old-client fallbacks.
- Data on disk is still read by newer code: identifiers in D7 never change,
  and new metadata fields are additive with defaults.
- Tauri command names are kept unless a step removes the command entirely;
  any added or removed command updates its Product Operation Registry row.

## Open checks before step 4

- Confirm live what mobile web and IM bots do today when sending to a native
  session (expected: silent local fallback, see Current state).
- Confirm the mobile question card renders a native question once it is
  registered with `UserInputManager` (mobile answers through
  `submit_user_answers`, which is `HANDLED` for runtime-owned questions).
