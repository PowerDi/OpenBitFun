[中文](AGENTS-CN.md) | **English**

# ACP Protocol Surface Guide

Scope: this guide applies to `src/crates/interfaces/acp`.

`openbitfun-acp` owns the Agent Client Protocol surface over the assembled product
runtime. Keep ACP protocol/client details here or in app-surface adapters;
share only stable capability facts through contract crates.

The crate exposes two additive roles. `client` owns ACP process discovery,
configuration, remote probing, session transport, and tool-card projection; it
selects the Core Agent Runtime plus concrete SSH support. `server` owns the
CLI-hosted ACP server and runtime projection through `DeliveryProfile::Acp`; it
selects the exact Core tool, document, subscription, and external-source
owners used by that path, but does not select SSH. The compatibility default is
exactly `client + server`. Desktop selects only `client`; CLI selects both.
Keep these role features additive and do not replace either closure with
`product-full`.

## Guardrails

- Remote ACP workspaces reuse local ACP client configuration. Preserve the
  manager, remote shell probing, remote capability store, and workspace menu
  availability semantics when changing ACP client behavior.
- ACP config persistence, remote probing, timeout policy, and workspace surface
  selection are ACP/app-surface behavior. Do not move them into `core-types`,
  `runtime-ports`, or `agent-tools`.
- ACP external-agent tool naming, schema, validation, presentation, and result
  shape are portable contracts owned by `openbitfun-agent-tools`; ACP should call
  those helpers instead of redefining them locally.
- Keep ACP stdio/connection ownership and protocol notification projection in
  this crate. Shared runtime facts may cross the SDK boundary; ACP protocol
  requests, client choices, and lifecycle state may not.
- If a future contract is needed, make it observational: environment identity,
  capability facts, and request/response DTOs only.

## Verification

```bash
cargo check -p openbitfun-acp --no-default-features --features client
cargo check -p openbitfun-acp --no-default-features --features server
cargo test -p openbitfun-acp
cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::prompt::tests
```

The focused client prompt tests cover protocol errors, retry, cancellation,
partial output, and transport termination with in-memory agent streams. These
fixtures do not require a live provider or a device connection. The explicit
Core `git` feature satisfies the worktree tool dependency in the current client
closure without enabling the server role or `product-full`.

## Claude native stdio driver

The native driver registry and the object-safe `NativeAgentDriver` /
`NativeAgentSession` interfaces live in `client/native/mod.rs`. Manager and
desktop dispatch use that registry; adding a native provider must not extend the
ACP connection path. Capability facts live in `native/capabilities.rs`;
`process.rs` owns shared local/SSH stdio launch and supervision, line writes and
stderr tails, and `events.rs` preserves canonical EventQueue ingress ordering.
`preferences.rs` stores per-client defaults in `native_agent_preferences.json`,
reads the legacy Claude file only when the new file is absent, and falls back
from session metadata `nativeAgentPreferences` to `claudeNativePreferences`.

`client/native/claude` remains the Claude Code stream-json implementation,
exposed through the existing external-client menu (`claude-native`), not an ACP
adapter. Claude identity/resume validation, control correlation, permissions,
turn state and subagent projection stay with that driver. Its process is
session-scoped; `result` ends a foreground segment, not the process. Preserve
generation fences, PID-scoped shutdown, native tool IDs in permission cards,
and the canonical EventQueue ingress. PID-tree helpers continue to use the
existing ACP process supervisor.

Verification from `src/crates` (memory streams; no Claude account or SSH host required):

```bash
CARGO_TARGET_DIR=target/check-native cargo test -p openbitfun-acp --lib
# Focus just the relocated driver and preference coverage:
CARGO_TARGET_DIR=target/check-native cargo test -p openbitfun-acp --lib client::native
```

The tests cover projection, control correlation, permission wire compatibility,
turn cleanup, cancellation, stderr/EOF, retired generations, background notices,
legacy metadata and preference migration. They do not establish live SSH,
relay reconnect, Peer Device Mode or Detached Dispatch behavior.
