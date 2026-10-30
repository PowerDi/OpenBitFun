//! Preserve causal native stream ordering at the canonical EventQueue ingress.
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::{AgenticEvent, AgenticEventPriority};

pub(super) async fn enqueue(
    queue: &EventQueue,
    events: Vec<AgenticEvent>,
    name: &str,
) -> OpenBitFunResult<()> {
    for event in events {
        // A native stream is already causally ordered. The legacy priority
        // heap must not put tool/terminal events ahead of their turn/round.
        queue
            .enqueue(event, Some(AgenticEventPriority::Normal))
            .await
            .map_err(|e| OpenBitFunError::service(format!("{name} event enqueue failed: {e}")))?;
    }
    Ok(())
}
