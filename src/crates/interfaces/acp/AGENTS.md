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
- Session options may include `availableCommands` from the same metadata read.
  Keep it optional: missing means an older host uses the commands endpoint;
  an empty list is a supported combined snapshot. Do not infer this capability
  from a package version. Frontends share in-flight reads, not settled snapshots.

## Verification

```bash
cargo check -p openbitfun-acp --no-default-features --features client
cargo check -p openbitfun-acp --no-default-features --features server
cargo test -p openbitfun-acp
cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::prompt::tests
cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::session_options::tests
cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::execution::tests
```

The focused client prompt tests cover protocol errors, retry, cancellation,
partial output, and transport termination with in-memory agent streams. These
fixtures do not require a live provider or a device connection. The explicit
Core `git` feature satisfies the worktree tool dependency in the current client
closure without enabling the server role or `product-full`.

The session-options tests cover legacy payload round trips and the additive
combined metadata response. They do not establish live Agent startup latency.

For metadata concurrency, run
`cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::manager::tests::metadata_`.
The protocol receiver remains serialized. A session-scoped watch snapshot serves
reads while a prompt or mutation owns it; cold readers wait for publication or
initialization-lock release. Publish after model/config changes and stream metadata
updates. Idle readers still drain pending notifications. Recreated connections
must not reuse an old snapshot. Do not solve this by adding a second stream reader.

The execution tests cover prompt admission receipts, turn-bound cancellation,
and coherent live/replay cursors. The ACP owner records execution events before
the host publishes them and reads execution snapshots without the long-held
protocol session mutex. Desktop injects its existing session event journal;
the additive `executionProjection: "acp"` restore marker identifies that authority.

## Claude native stdio driver

`client/claude_native` is the native Claude Code SDK stream-json driver, exposed
through the existing external-client menu (`claude-native`), not an ACP adapter.
Its process is session-scoped; `result` ends a foreground segment, not the
process. Preserve generation fences, PID-scoped shutdown, native tool IDs in
permission cards, and the canonical EventQueue ingress.

Focused verification (memory streams; no Claude account or SSH host required):

```bash
cargo test -p openbitfun-acp --lib claude_native
```

The tests cover projection, control correlation, permission wire compatibility,
turn cleanup, cancellation, stderr/EOF, retired generations, background notices,
and legacy metadata. They do not establish live SSH or relay reconnect behavior.
