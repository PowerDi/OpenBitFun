//! Codex thread anchors persisted in session metadata (`codexNativeHistory`).
//!
//! The Codex thread id lives in `acpRemoteSessionId`. Each OpenBitFun turn
//! records the Codex turn id it ran as, so fork and rollback can address
//! `thread/fork { lastTurnId }`. A pending fork is applied by the next launch.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(super) const METADATA_KEY: &str = "codexNativeHistory";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct TurnAnchor {
    pub turn_id: String,
    pub turn_index: usize,
    /// Codex turn id; absent when the turn never reached Codex.
    pub codex_turn_id: Option<String>,
}

/// Branch the next launch applies instead of `thread/start` / `thread/resume`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct PendingFork {
    pub thread_id: String,
    /// Inclusive last Codex turn to keep; `None` keeps the whole thread.
    pub last_turn_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct CodexHistory {
    pub turns: Vec<TurnAnchor>,
    pub pending: Option<PendingFork>,
}

impl CodexHistory {
    /// Tolerant read: unknown or malformed history yields an empty record.
    pub fn from_metadata(custom: Option<&Value>) -> Self {
        custom
            .and_then(|custom| custom.get(METADATA_KEY))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn anchor_mut(&mut self, turn_id: &str) -> Option<&mut TurnAnchor> {
        self.turns.iter_mut().rev().find(|a| a.turn_id == turn_id)
    }

    /// Codex turn id of `turn_id` plus the anchors up to and including it.
    pub fn fork_point(&self, turn_id: &str) -> Option<(Vec<TurnAnchor>, String)> {
        let position = self.turns.iter().position(|a| a.turn_id == turn_id)?;
        let codex = self.turns[position].codex_turn_id.clone()?;
        Some((self.turns[..=position].to_vec(), codex))
    }

    /// Rebind kept anchors to the turn ids a fork reported, matching from the
    /// end: the fork keeps the source prefix in order, whether or not Codex
    /// preserves turn ids across threads.
    pub fn rebind_after_fork(&mut self, forked_turn_ids: &[String]) {
        let mut ids = forked_turn_ids.iter().rev();
        for anchor in self
            .turns
            .iter_mut()
            .rev()
            .filter(|a| a.codex_turn_id.is_some())
        {
            match ids.next() {
                Some(id) => anchor.codex_turn_id = Some(id.clone()),
                None => anchor.codex_turn_id = None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_history_is_ignored_and_round_trips() {
        assert_eq!(
            CodexHistory::from_metadata(Some(&json!({"codexNativeHistory": 3}))),
            CodexHistory::default()
        );
        let history = CodexHistory {
            turns: vec![TurnAnchor {
                turn_id: "t1".into(),
                turn_index: 0,
                codex_turn_id: Some("c1".into()),
            }],
            pending: Some(PendingFork {
                thread_id: "th".into(),
                last_turn_id: Some("c1".into()),
            }),
        };
        let custom = json!({"codexNativeHistory": history.to_value()});
        assert_eq!(CodexHistory::from_metadata(Some(&custom)), history);
    }

    #[test]
    fn rebind_maps_kept_anchors_from_the_end() {
        let anchor = |id: &str, codex: Option<&str>| TurnAnchor {
            turn_id: id.into(),
            turn_index: 0,
            codex_turn_id: codex.map(str::to_string),
        };
        let mut history = CodexHistory {
            turns: vec![anchor("t1", Some("a")), anchor("t2", None), anchor("t3", Some("b"))],
            pending: None,
        };
        history.rebind_after_fork(&["x".into(), "y".into()]);
        let ids: Vec<_> = history.turns.iter().map(|a| a.codex_turn_id.clone()).collect();
        assert_eq!(ids, [Some("x".into()), None, Some("y".into())]);
    }
}
