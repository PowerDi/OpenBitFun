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
| D3 | Move native drivers out of `openbitfun-acp` into a dedicated interface crate (`openbitfun-native-agents`). The move is mechanical and changes no behavior. | Done |
| D4 | Native sessions are owned by the shared runtime, not by Desktop. Desktop, CLI, mobile web, IM bots, cron, `SessionMessage`, and delegation reach them through one runtime port. | Accepted |
| D5 | Native approvals and questions use the runtime `PermissionRequestManager` and `UserInputManager`. The custom `backend-event-acppermissionrequest` path is removed, not kept in parallel. | Accepted |
| D6 | The backend persists native turns. Frontend persistence of native turns is removed. | Accepted |
| D7 | Persisted identifiers stay unchanged: `acp:<client>` agent types, `acp_<client>_<uuid>` session ids, `provider: "acp"` metadata, driver metadata keys (`claudeNativeHistory`, ...). | Accepted |
| D8 | Delegation is built on top of D4, following the codex-host semantics (non-blocking start, independent writable child session, read/wait/send/cancel/watch). See "D8: delegation". | Accepted, in progress |

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

After (step 3, done): `native-agents/src/interaction.rs` is the single bridge.
Drivers keep their wire state and publish each ask:

- Approvals go to `PermissionRequestManager` with `source.kind = provider`,
  `source.identity` = agent name, `tool_call_id` = native tool id,
  `round_id` = turn id, and the workspace permission project (remote
  workspaces use host + POSIX root). Subagent asks carry `delegation`, so the
  parent session shows them.
- Questions (Claude `AskUserQuestion`, Codex `requestUserInput`, MCP forms,
  Pi dialogs) go to the user-question mailbox under the card id; an answer the
  driver rejects as incomplete re-opens the question.
- MCP confirm elicitations are approvals with action `mcp`.
- When the driver settles an ask itself (turn ended, process exit, Codex
  `serverRequest/resolved`), it withdraws the runtime request. A runtime
  cancellation answers the agent with a rejection, so the agent never waits on
  a request nobody can see.

Reply mapping (user decision: approvals use the composer band, "allow for
this session" is dropped):

| Runtime reply | Native answer |
|---|---|
| `once` | allow once |
| `always` | the agent's own persistent rule (Claude settings rule, Codex exec-policy amendment); offered only when the agent suggests one and named in `displayMetadata.agentAlwaysScope`. `save_resources` is empty, so OpenBitFun stores no grant |
| `reject` + feedback | deny; Claude receives the feedback text |
| plan review (`displayMetadata.planReview`) | `once` approves the plan; `reject` + feedback keeps planning |

The generic ACP client uses the same bridge, so the old per-card path was
removed entirely: `backend-event-acppermissionrequest`, the
`submit_acp_permission_response` command and registry row, the Web UI
`AcpPermissionToolCardModule`, `ToolApprovalBar`, `AcpPermissionActions`, the
`acpPermission` tool-item field, and the dead tool-card confirm/reject
callbacks. Desktop shows every request in the composer approval band, which
gained the plan view. The product is upgraded as a whole, so no old-client
fallback is kept (see Compatibility).

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

### D8: delegation

What is followed from codex-host (behavior and output shapes, so later
codex-host changes can be mirrored): `delegate start` returns immediately with
the child session; the child is an ordinary writable session in the session
list; the delegator chooses `read` (result view by default, no tool traces),
bounded `wait` (a timeout is a running checkpoint, not a failure), `send`,
`cancel`, one-shot `watch`, and `list`; idempotent retry by request id; no
depth limit; the host never re-delegates from task text.

What is not followed: the transport. codex-host gives each harness a
`codexhost` CLI plus a loopback endpoint and token, which needs a second
install and a reverse connection on SSH hosts. OpenBitFun instead exposes the
delegation tools on the protocol pipe each driver already owns, so local and
SSH workspaces behave the same and the remote host needs only the agent CLI:

| Agent | Tool channel | Note |
|---|---|---|
| Claude Code | SDK MCP server declared in `initialize` (`sdkMcpServers`), calls arrive as `control_request` `mcp_message` | Official SDK channel |
| Codex | `dynamicTools` on `thread/start`, calls arrive as `item/tool/call` | Experimental API (`experimentalApi: true`); accepted risk, breakage must fail loudly |
| Pi | Tool registered by the OpenBitFun extension, round trip over the extension UI channel | Same mechanism as approvals |

Live probe (local Windows, model `deepseek-flash` for Claude and Pi): all
three channels round-trip a model-initiated tool call and the model uses the
result. Codex keeps the dynamic tools after `thread/resume` in a new
process. Pi tool results travel as an extension `input` dialog, so the
driver must tell tool requests apart from approval and user dialogs by
title prefix. SSH is not probed yet; it uses the same pipes.

Product decisions (owner answers, 2026-07):

1. Delegated child sessions run unattended (codex-host behavior): Claude
   `bypassPermissions`, Codex `approvalPolicy: never` with
   `danger-full-access`, Pi approval bridge allows every call. Only children
   created by delegation; ordinary sessions keep their selected mode. Agent
   questions (not approvals) still reach the driving surface through the D5
   path.
2. `send` to a busy child queues through the scheduler (unlike codex-host's
   `THREAD_BUSY`) and reports `queued` to the caller.
3. OpenBitFun's own agents: `SessionMessage` today exists only in Claw mode and
   can target only `Standard` / `Cowork` / `DeepResearch`. The delegation
   service is shared, so OpenBitFun agents may later target native agents
   through it; native agents can target each other from the first release.
4. The Codex experimental API is accepted.

Ownership: one delegation service under the native-agents crate owns tool
definitions, child creation, the parent link (an additive optional field in
the child metadata), and watch. Submission, queueing, and cancel go through
the scheduler (D4); child turns are persisted by D6; `watch` uses the
scheduler reply route, so a busy parent receives the notice as a queued turn.
CLI-hosted runtimes answer with an explicit unsupported error until the CLI
registers the port.

Steps: (0) live probe of the three tool channels, local and SSH; (1) shared
service and tool set; (2) wire Claude, Codex, Pi; (3) UI: delegated-by label,
tool card links to the child; (4) tests and this document.

Implemented (steps 1-2, `native-agents/src/delegation/`):

- `tools.rs`: one catalog (`agent_inspect`, `delegate_start`, `session_read`,
  `session_wait`, `session_send`, `session_cancel`, `session_watch`,
  `session_list`) and one handler; results are JSON text, errors carry a code.
- `service.rs`: children are `acp_<client>_<uuid>` records in the caller's
  workspace storage with `nativeDelegation.parentSessionId`, a fixed title
  (`titleUserEdited`), and the unattended mode as a session preference
  (Claude `bypassPermissions`, Codex/Pi `full-access`). Turns go through the
  scheduler (`AgentSession` source, queued when busy). `watch` waits for the
  child to go idle and submits one notice turn to the caller (also queued).
  A start fails before creating anything when the host has no scheduler or
  no external-session port for the target (`external_sessions_supported`);
  a failed first submit deletes the child.
- `snapshot.rs`: read view from persisted turns: final text of the last
  round with text, no tool traces, messages view, `interrupted` for a turn
  file left in progress.
- Drivers: Claude declares `sdkMcpServers: ["openbitfun"]` and answers
  `mcp_message` off the read loop, and allows `mcp__openbitfun` so the
  tools never prompt; Codex enables `experimentalApi`, sends `dynamicTools`
  on `thread/start`/`thread/fork` (not `thread/resume`, which rejects the
  field) and answers `item/tool/call`; Pi's bridge extension registers the
  tools and carries calls as `input` dialogs titled `openbitfun-tool:`.
- Live check (`delegation::live_tests`, `OBF_LIVE_DELEGATION=1`): each real
  CLI calls `session_list` and `delegate_start` through the real driver and
  reports the results (local Windows; Claude and Pi on `deepseek-flash`).

Session list (step 3): no dedicated delegation view. A delegated child nests
under its delegator in the existing session list, reusing the one-level
nesting of review children. The backend sends `delegatedBy` on
`SessionCreated`; persisted records carry `nativeDelegation.parentSessionId`.
Nesting stays one level by owner decision: a child of a child lists under
the top-level ancestor (Web UI hierarchy and backend paging agree), and its
tooltip names the direct delegator. A child whose delegator is gone lists
top level. Paging counts only top-level sessions.

Conversation cards (owner decision: reuse the subagent card, no new panel):

- Every driver reports the delegation tools as `mcp__openbitfun__<tool>`
  (`delegation::card_tool_name`; Claude already does, Codex dynamic tools and
  Pi extension tools are renamed in their projectors).
- `delegate_start` renders the subagent launch card (`AgentControlToolCard`):
  agent icon, child title, live status from the session list status source,
  hover preview with agent and task. The card shows no model. Several
  launches share a row.
- `session_send` / `read` / `wait` / `cancel` / `watch` render one relation
  row each, never a new card. `agent_inspect` and `session_list` keep the MCP
  card.
- Every tool that addresses a child returns its `sessionId`, `agent` and
  `title`; the card reads them from the result and never parses the session id.
  Drivers deliver the result text as `{ result: <json text> }`, the only shape
  the card accepts.
- Clicking a card or row opens the child in the side pane next to the
  delegator; the child keeps its own persisted record (it is loaded first,
  never created as a side chat), and the composer can target it. The pane
  header names the model of the child's latest model round.
- A watch notice is a user turn with `nativeWake.source = "delegation"`, so it
  renders as a grey system line. Each driver has one turn-opening path that
  takes the user message metadata, so no path drops it.

Model on rounds: each turn opens with the model the agent resolves for it,
the value its model selector shows (Codex: selection, reported thread model,
config default, catalog default; Claude: reported model, else the selection;
Pi: current model). A model the CLI reports during the turn replaces it.
When none is known the name is empty, never a placeholder.
- Tool descriptions steer the delegator to `watch=true` and ending its turn;
  `session_wait` is described as holding the turn and reserved for results
  needed now. The loop itself is unchanged.

Model selection (owner decision: exactly codex-host `harness inspect` and
`validateConfiguration`):

- `agent_inspect` returns the target agent's own catalog (`id`, `name`,
  `provider`, `defaultModel`) for the caller's workspace and host. It comes
  from a throwaway process of the agent CLI that sends no turn and persists
  nothing (`NativeAgentDriver::inspect_options`), so remote workspaces read
  the remote host's catalog and project settings apply. The catalog is not
  read from or merged with configuration files: Claude's `initialize` list
  already carries the user's custom models.
- `delegate_start` takes an optional `model`. Omitted: nothing is written
  and the child uses the usual default (the user's last choice for that
  agent, else the CLI default). Given: it must equal a catalog `id` exactly;
  no case folding, display-name, prefix, or partial matching. Otherwise the
  start fails with `INVALID_ARGUMENT` and `details.validModelIds`, and the
  calling agent decides (pick an id, or ask the user). The chosen id is a
  session preference of the child and is echoed in the result.
- The host check is a pre-check; the CLI launch is the final confirmation.
  A catalog entry the user's gateway does not serve fails the child's first
  turn, which the caller sees through `session_read` (accepted limit; the
  catalog lists configuration, not gateway support).
- Live check: `delegation::live_tests::live_inspect_reports_each_agent_catalog`.

Not yet: the full child round trip in a running desktop (create, run,
read, watch notice), SSH, mobile view of children, request-id idempotency across restarts (in memory only), watches
across restarts (in memory only, same as codex-host), thinking-level
selection on delegation.

## Current state (verified in code)

- Native sessions are prepared and driven only by Desktop Tauri commands in
  `src/apps/desktop/src/api/acp_client_api.rs`. The scheduler and coordinator
  have no native route.
- Presentation updates (context usage, background tasks, retry, rate limits,
  plan, options) go through the shared backend event system (step 2), so any
  host emitter and Peer Mode controllers receive them.
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
- Native approvals and questions are in the runtime mailbox (step 3), so mobile
  web and Peer controllers can see and answer them; live check still owed.
- Native CLI processes run over `open_workspace_stdio` for SSH and container
  workspaces, so remote workspaces already work for an attached Desktop.
  Claude CLI update on a remote host is refused.

## Plan: runtime ownership (D3-D6)

Each step merges on its own.

1. **Crate split (D3).** Mechanical move, no behavior change. Done:
   `src/crates/interfaces/native-agents`; `openbitfun_acp::client::native`
   re-exports it for existing hosts.
2. **Notifications.** Replace the Tauri sink with the shared event emitter;
   event names and payloads stay the same, so the Web UI is unchanged.
   Done: `notifications::global_notification_sink` publishes through
   `emit_global_event`; Desktop adds the events to the Peer Mode fan-out list.
3. **Interactions (D5).** Register approvals and questions with the runtime
   managers; remove the custom event and its Web UI handling. Done; see D5.
4. **Runtime port (D4).** Done (user decision: desktop also submits through
   the scheduler, so a message sent while the agent is busy queues).
   - `runtime-ports::ExternalSessionPort` (start / steer / cancel, one outcome
     callback per turn); `openbitfun_acp::client::NativeExternalSessionPort`
     implements it for the native drivers; Desktop registers it in
     `AppState` via `set_external_session_port`. A host without it answers
     with an explicit "cannot run <client> sessions" error.
   - The scheduler recognizes `provider: "acp"` from persisted metadata
     before any restore, never loads the session into the Session manager,
     owns the active-turn slot and queue, and dispatches the next queued turn
     on the driver outcome (reported from `native-agents/src/events.rs` when
     the turn's terminal event is queued).
   - Session restore skips mode, model and reasoning fallback for these
     sessions, so a stray restore can no longer rewrite `acp:<client>`.
   - Remote cancel, remote control state, the host dialog queue (list,
     cancel, promote-to-steer) and steering work on unloaded external
     sessions.
   - Native drivers advertise `input.queue`, so the Web UI sends through the
     host queue like local turns. The old `start_acp_dialog_turn` path stays
     for generic ACP clients.
   Original plan items:
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
5. **Persistence (D6).** Done: `native-agents/src/turn_recorder.rs` folds the
   drivers' canonical events into `DialogTurnData` and saves it through
   `PersistenceManager` when the turn starts, at each model-round end and
   when it settles, before the event is published. Sessions and subagent
   child records register their storage on open. The slot is the driver's
   own `turn_index` (from metadata `turn_count`), so the frontend no longer
   allocates it for native turns (they take the host-queue path since step
   4). The Web UI may still save the same turn file; both writers use the
   same identity and atomic writes, last write wins. The frontend writer is
   kept for generic ACP clients, which have no backend owner yet.

Per-surface result after step 5:

| Surface | Result |
|---|---|
| SSH / container workspace | Unchanged; the driver already launches the CLI there |
| Mobile web, IM bots | Send, cancel, steer, approve, and answer on existing native sessions. Creating a native session, switching model/permission: explicit unsupported reply |
| Peer Device Mode | Commands keep their registry rows; interaction moves to the mailbox |
| Detached Dispatch | Out of scope; explicit unsupported |
| CLI | Registers the port, so cron and `SessionMessage` work in CLI-hosted runtimes; CLI peer handlers are follow-up work |

Not in this plan: remote CLI install/update, keeping a turn
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

## Open checks (live, not yet run)

- Step 4: send from mobile web and an IM bot to a native session (start,
  queue a second message, cancel, steer); desktop send while busy queues;
  SSH workspace session from mobile.
- CLI does not register the port yet; CLI-hosted runtimes answer native
  sessions with the explicit unsupported error.

## Open checks before step 4 (historical)

- Confirm live what mobile web and IM bots do today when sending to a native
  session (expected: silent local fallback, see Current state).
- Confirm live that mobile web shows and answers a native approval and a
  native question (mailbox and `submit_user_answers`), and that a Peer
  controller sees the band. Step 3 was verified with unit tests only.
