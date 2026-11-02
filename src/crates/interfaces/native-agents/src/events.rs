//! Preserve causal native stream ordering at the canonical EventQueue ingress,
//! and report runtime-started turns' terminal events to their waiter.
use std::sync::{Mutex, OnceLock};

use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::runtime_ports::{ExternalTurnOutcome, ExternalTurnOutcomeSink};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::{AgenticEvent, AgenticEventPriority};

type Waiters = Mutex<std::collections::HashMap<(String, String), ExternalTurnOutcomeSink>>;

fn waiters() -> &'static Waiters {
    static WAITERS: OnceLock<Waiters> = OnceLock::new();
    WAITERS.get_or_init(Default::default)
}

fn lock() -> std::sync::MutexGuard<
    'static,
    std::collections::HashMap<(String, String), ExternalTurnOutcomeSink>,
> {
    waiters()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Report the end of `turn_id` in `session_id` to `sink` (exactly once).
pub(crate) fn watch_turn(session_id: &str, turn_id: &str, sink: ExternalTurnOutcomeSink) {
    lock().insert((session_id.to_string(), turn_id.to_string()), sink);
}

/// Settle a watched turn that never produced a terminal event (start failed).
pub(crate) fn settle_turn(session_id: &str, turn_id: &str, outcome: ExternalTurnOutcome) {
    let sink = lock().remove(&(session_id.to_string(), turn_id.to_string()));
    if let Some(sink) = sink {
        sink(outcome);
    }
}

/// Drop a watch without reporting (the caller reports the error itself).
pub(crate) fn unwatch_turn(session_id: &str, turn_id: &str) -> bool {
    lock()
        .remove(&(session_id.to_string(), turn_id.to_string()))
        .is_some()
}

fn terminal_outcome(event: &AgenticEvent) -> Option<(&str, &str, ExternalTurnOutcome)> {
    match event {
        AgenticEvent::DialogTurnCompleted {
            session_id,
            turn_id,
            ..
        } => Some((session_id, turn_id, ExternalTurnOutcome::Completed)),
        AgenticEvent::DialogTurnCancelled {
            session_id,
            turn_id,
        } => Some((session_id, turn_id, ExternalTurnOutcome::Cancelled)),
        AgenticEvent::DialogTurnFailed {
            session_id,
            turn_id,
            error,
            ..
        } => Some((
            session_id,
            turn_id,
            ExternalTurnOutcome::Failed {
                error: error.clone(),
            },
        )),
        _ => None,
    }
}

pub(super) async fn enqueue(
    queue: &EventQueue,
    events: Vec<AgenticEvent>,
    name: &str,
) -> OpenBitFunResult<()> {
    for event in events {
        let terminal = terminal_outcome(&event)
            .map(|(session, turn, outcome)| (session.to_string(), turn.to_string(), outcome));
        // A native stream is already causally ordered. The legacy priority
        // heap must not put tool/terminal events ahead of their turn/round.
        queue
            .enqueue(event, Some(AgenticEventPriority::Normal))
            .await
            .map_err(|e| OpenBitFunError::service(format!("{name} event enqueue failed: {e}")))?;
        // Report after the terminal event is queued, so a follow-up turn's
        // events always come after this turn's end.
        if let Some((session_id, turn_id, outcome)) = terminal {
            settle_turn(&session_id, &turn_id, outcome);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_watched_turn_is_reported_once() {
        let (tx, rx) = std::sync::mpsc::channel();
        watch_turn(
            "events-s1",
            "t1",
            Box::new(move |outcome| tx.send(outcome).unwrap()),
        );
        // A child session's end with the same turn id is not the parent's.
        settle_turn("events-s1-subagent", "t1", ExternalTurnOutcome::Completed);
        assert!(rx.try_recv().is_err());
        settle_turn("events-s1", "t1", ExternalTurnOutcome::Cancelled);
        settle_turn("events-s1", "t1", ExternalTurnOutcome::Completed);
        assert_eq!(rx.try_recv().unwrap(), ExternalTurnOutcome::Cancelled);
        assert!(rx.try_recv().is_err());
    }
}
