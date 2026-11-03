//! Process registration for the external-session port.
//!
//! Sessions persisted with `provider: "acp"` are driven by an external agent
//! process (native Claude Code / Codex / Pi drivers, generic ACP clients),
//! not by this Runtime's execution engine. The scheduler still owns their
//! admission, queue and active-turn slot; the registered port drives the
//! agent. Hosts that run external agents register the port at startup.

use std::sync::{Arc, OnceLock};

use openbitfun_runtime_ports::ExternalSessionPort;

static EXTERNAL_SESSION_PORT: OnceLock<Arc<dyn ExternalSessionPort>> = OnceLock::new();

/// Register the external-session driver. The first registration owns the process.
pub fn set_external_session_port(port: Arc<dyn ExternalSessionPort>) {
    let _ = EXTERNAL_SESSION_PORT.set(port);
}

pub(crate) fn external_session_port() -> Option<Arc<dyn ExternalSessionPort>> {
    EXTERNAL_SESSION_PORT.get().cloned()
}

/// Whether this host can run `client_id` sessions through the scheduler, so
/// callers can refuse before creating anything (e.g. delegated children).
pub fn external_sessions_supported(client_id: &str) -> bool {
    EXTERNAL_SESSION_PORT
        .get()
        .is_some_and(|port| port.supports_client(client_id))
}
