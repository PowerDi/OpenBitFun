//! Native Claude Code driver exposed through the existing external-client UI.
//! No ACP adapter or ACP protocol connection is used.
mod projector;
mod session;
pub(crate) use session::local_executable;
pub use session::{ClaudeNativeSession, CLIENT_ID, DEFAULT_MODEL_ID};
