//! Delegation between native agent sessions (decision D8).
//!
//! Behavior and output shapes follow codex-host's delegation contract
//! (`delegate_start` returns at once, children are ordinary writable
//! sessions, and the caller chooses `session_read` / `session_wait` /
//! `session_send` / `session_cancel` / `session_watch` / `session_list`).
//! The transport differs: each driver exposes these tools on the protocol
//! pipe it already owns (Claude SDK MCP, Codex dynamic tools, Pi extension
//! tools), so SSH workspaces need only the agent CLI.
//!
//! Children are created in the caller's workspace storage and driven by the
//! runtime scheduler (D4), which queues work for a busy session; their turns
//! are persisted by `turn_recorder.rs` (D6). Hosts without a registered
//! external-session port answer with an explicit unsupported error.
#[cfg(test)]
mod live_tests;
mod service;
mod snapshot;
mod tools;

pub use service::DelegationCaller;
pub use tools::{call_tool, tool_specs, DelegationToolOutput, DelegationToolSpec, TOOL_NAMESPACE};
