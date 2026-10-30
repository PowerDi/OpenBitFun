//! Time-to-first-output for native model rounds.
//!
//! Only a round opened when the host submits the prompt has a known request
//! start. Later rounds begin inside the external CLI (after tool results), so
//! the host cannot see their request time and reports no TTFT for them.
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub(super) struct RoundLatency {
    requested_at: Option<Instant>,
    ttft_ms: Option<u64>,
}

impl RoundLatency {
    /// Latency of a round whose request the host just sent.
    pub(super) fn requested_now() -> Self {
        Self {
            requested_at: Some(Instant::now()),
            ttft_ms: None,
        }
    }

    /// Record the first visible text, thinking, or tool call of the round.
    pub(super) fn observe_output(&mut self) {
        if self.ttft_ms.is_none() {
            self.ttft_ms = self
                .requested_at
                .map(|at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX));
        }
    }

    pub(super) fn ttft_ms(&self) -> Option<u64> {
        self.ttft_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_only_the_first_output_of_a_requested_round() {
        let mut latency = RoundLatency::requested_now();
        assert_eq!(latency.ttft_ms(), None);
        latency.observe_output();
        let first = latency.ttft_ms().expect("ttft after first output");
        std::thread::sleep(std::time::Duration::from_millis(5));
        latency.observe_output();
        assert_eq!(latency.ttft_ms(), Some(first));
    }

    #[test]
    fn rounds_without_a_known_request_start_report_none() {
        let mut latency = RoundLatency::default();
        latency.observe_output();
        assert_eq!(latency.ttft_ms(), None);
    }
}
