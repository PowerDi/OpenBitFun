//! Pi session-tree anchors persisted in session metadata (`piNativeHistory`).
//!
//! The Pi session id lives in `acpRemoteSessionId`. Pi stores a session as an
//! append-only tree of entries whose ids survive `fork` and `--fork` copies.
//! Each OpenBitFun turn records the id of the user message entry it prompted
//! with, so rollback can `fork` before that message, and a fork through turn
//! T copies the source (`--fork`) and cuts it before the next prompt after T.
//! Sessions recorded before these anchors existed keep working; their old
//! turns simply cannot be rolled back or forked.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(super) const METADATA_KEY: &str = "piNativeHistory";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct TurnAnchor {
    pub turn_id: String,
    pub turn_index: usize,
    /// A `prompt` was sent to Pi for this turn (false for `/compact` and for
    /// turns that failed before reaching Pi).
    pub prompted: bool,
    /// Entry id of the user message the turn prompted with, once known.
    pub entry_id: Option<String>,
}

/// Branch the next launch applies instead of `--session-id`: copy
/// `session_id` with `--fork`, then cut it before `cut_before` (a user
/// message entry) when set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct PendingFork {
    pub session_id: String,
    pub cut_before: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct PiHistory {
    pub turns: Vec<TurnAnchor>,
    pub pending: Option<PendingFork>,
    /// Last entry id the driver has seen (a `get_entries` cursor).
    pub cursor: Option<String>,
    /// `cursor` marks the end of everything Pi held before the latest
    /// prompt, so entries after it belong to that prompt. False for records
    /// written before these anchors existed and after a branch switch.
    pub synced: bool,
}

impl PiHistory {
    /// Tolerant read: unknown or malformed history yields an empty record.
    pub(super) fn from_metadata(custom: Option<&Value>) -> Self {
        custom
            .and_then(|custom| custom.get(METADATA_KEY))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    pub(super) fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub(super) fn anchor_mut(&mut self, turn_id: &str) -> Option<&mut TurnAnchor> {
        self.turns.iter_mut().rev().find(|a| a.turn_id == turn_id)
    }

    /// Attribute newly appended Pi entries (`get_entries` rows after the
    /// cursor): the latest prompted anchor, when its entry is unknown, takes
    /// the first user message. Only that anchor can be waiting because
    /// entries are synced before every prompt (a turn that has not prompted
    /// yet is skipped). Returns whether the record changed.
    pub(super) fn absorb_entries(&mut self, entries: &[Value], attribute: bool) -> bool {
        let mut changed = !self.synced;
        if attribute && self.synced {
            if let Some(anchor) = self
                .turns
                .iter_mut()
                .rev()
                .find(|a| a.prompted)
                .filter(|a| a.entry_id.is_none())
            {
                let first_user = entries
                    .iter()
                    .find(|e| e["type"] == "message" && e["message"]["role"] == "user")
                    .and_then(|e| e["id"].as_str());
                if let Some(id) = first_user {
                    anchor.entry_id = Some(id.to_string());
                    changed = true;
                }
            }
        }
        if let Some(last) = entries.iter().rev().find_map(|e| e["id"].as_str()) {
            if self.cursor.as_deref() != Some(last) {
                self.cursor = Some(last.to_string());
                changed = true;
            }
        }
        self.synced = true;
        changed
    }

    /// Anchors through `turn_id` and the user message entry to cut before
    /// (the next prompt after the turn), for forking through `turn_id`.
    pub(super) fn fork_point(
        &self,
        turn_id: &str,
    ) -> Result<(Vec<TurnAnchor>, Option<String>), String> {
        let position = self
            .turns
            .iter()
            .position(|a| a.turn_id == turn_id)
            .ok_or_else(|| {
                "This turn was recorded before Pi fork support; it cannot be forked".to_string()
            })?;
        let cut_before = match self.turns[position + 1..].iter().find(|a| a.prompted) {
            Some(next) => Some(next.entry_id.clone().ok_or_else(|| {
                "Pi has not recorded a later turn of this session yet; retry shortly".to_string()
            })?),
            // Nothing later in this session: an unlaunched fork still cuts
            // its source where it was told to.
            None => self.pending.as_ref().and_then(|p| p.cut_before.clone()),
        };
        Ok((self.turns[..=position].to_vec(), cut_before))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn anchor(id: &str, prompted: bool, entry: Option<&str>) -> TurnAnchor {
        TurnAnchor {
            turn_id: id.into(),
            turn_index: 0,
            prompted,
            entry_id: entry.map(str::to_string),
        }
    }

    #[test]
    fn malformed_history_is_ignored_and_round_trips() {
        assert_eq!(
            PiHistory::from_metadata(Some(&json!({"piNativeHistory": "x"}))),
            PiHistory::default()
        );
        assert_eq!(PiHistory::from_metadata(None), PiHistory::default());
        let history = PiHistory {
            turns: vec![anchor("t1", true, Some("e1"))],
            pending: Some(PendingFork {
                session_id: "src".into(),
                cut_before: Some("e9".into()),
            }),
            cursor: Some("e4".into()),
            synced: true,
        };
        let custom = json!({"piNativeHistory": history.to_value()});
        assert_eq!(PiHistory::from_metadata(Some(&custom)), history);
        // Older and newer writers may omit or add fields.
        let partial = json!({"piNativeHistory": {"turns": [{"turnId": "t1", "future": 1}]}});
        let read = PiHistory::from_metadata(Some(&partial));
        assert_eq!(read.turns[0].turn_id, "t1");
        assert!(!read.turns[0].prompted && read.turns[0].entry_id.is_none());
    }

    #[test]
    fn entries_attribute_only_the_waiting_latest_turn() {
        let entries = vec![
            json!({"type":"model_change","id":"a"}),
            json!({"type":"message","id":"b","message":{"role":"user"}}),
            json!({"type":"message","id":"c","message":{"role":"assistant"}}),
            json!({"type":"message","id":"d","message":{"role":"user"}}),
        ];
        let mut history = PiHistory {
            turns: vec![anchor("t0", true, None), anchor("t1", true, None)],
            ..Default::default()
        };
        // Baseline sync (legacy session): cursor only, even when asked to
        // attribute, because nothing marks where the latest prompt began.
        assert!(history.absorb_entries(&entries, true));
        assert_eq!(history.turns[1].entry_id, None);
        assert_eq!(history.cursor.as_deref(), Some("d"));
        history.absorb_entries(&entries, true);
        assert_eq!(history.turns[1].entry_id.as_deref(), Some("b"));
        assert_eq!(history.turns[0].entry_id, None);
        assert!(!history.absorb_entries(&[], true));
        // A begun turn that has not prompted yet does not hide the one that did.
        history.turns[1].entry_id = None;
        history.turns.push(anchor("t2", false, None));
        history.absorb_entries(&entries, true);
        assert_eq!(history.turns[1].entry_id.as_deref(), Some("b"));
        assert_eq!(history.turns[2].entry_id, None);
    }

    #[test]
    fn fork_cuts_before_the_next_prompt_and_refuses_legacy_turns() {
        let history = PiHistory {
            turns: vec![
                anchor("t1", true, Some("e1")),
                anchor("t2", false, None),
                anchor("t3", true, Some("e3")),
                anchor("t4", true, None),
            ],
            ..Default::default()
        };
        let (anchors, cut) = history.fork_point("t1").unwrap();
        assert_eq!((anchors.len(), cut.as_deref()), (1, Some("e3")));
        assert!(history.fork_point("t3").is_err());
        assert_eq!(history.fork_point("t4").unwrap().1, None);
        assert!(history.fork_point("legacy").is_err());
        let unlaunched = PiHistory {
            turns: vec![anchor("t1", true, Some("e1"))],
            pending: Some(PendingFork {
                session_id: "src".into(),
                cut_before: Some("e7".into()),
            }),
            cursor: None,
            synced: false,
        };
        assert_eq!(
            unlaunched.fork_point("t1").unwrap().1.as_deref(),
            Some("e7")
        );
    }
}
