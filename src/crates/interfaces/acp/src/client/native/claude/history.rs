//! Claude transcript anchors per OpenBitFun turn, used by rollback and fork.
//!
//! Stored under one tolerant custom-metadata key. Sessions recorded before
//! this key existed have no anchors and refuse rollback/fork with a clear
//! error instead of guessing a transcript position.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const METADATA_KEY: &str = "claudeNativeHistory";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct TurnAnchor {
    pub turn_id: String,
    /// OpenBitFun storage index of the turn.
    pub turn_index: usize,
    /// Transcript uuid of the user prompt; file checkpoints are keyed by it.
    pub prompt_uuid: Option<String>,
    /// Last main-chain entry of the turn; `--resume-session-at` keeps through it.
    pub last_uuid: Option<String>,
    /// The CLI echoed the prompt; its uuid supersedes the one we submitted.
    #[serde(skip)]
    pub prompt_replayed: bool,
}

/// Launch override applied until a user message lands on the new branch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct PendingResume {
    /// Claude session to fork from; `None` resumes this session's own transcript.
    pub fork_from: Option<String>,
    pub resume_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct ClaudeHistory {
    pub turns: Vec<TurnAnchor>,
    pub pending: Option<PendingResume>,
}

impl ClaudeHistory {
    pub(crate) fn from_metadata(custom: Option<&Value>) -> Self {
        let Some(value) = custom.and_then(|custom| custom.get(METADATA_KEY)) else {
            return Self::default();
        };
        if value.is_null() {
            return Self::default();
        }
        serde_json::from_value(value.clone()).unwrap_or_else(|error| {
            log::warn!("Ignoring unreadable Claude native history anchors: {error}");
            Self::default()
        })
    }

    pub(crate) fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub(crate) fn anchor_mut(&mut self, turn_id: &str) -> Option<&mut TurnAnchor> {
        self.turns
            .iter_mut()
            .rev()
            .find(|anchor| anchor.turn_id == turn_id)
    }

    /// Anchors through `turn_id` plus the transcript position to fork at.
    pub(crate) fn fork_point(&self, turn_id: &str) -> Option<(Vec<TurnAnchor>, String)> {
        let index = self
            .turns
            .iter()
            .rposition(|anchor| anchor.turn_id == turn_id)?;
        let at = self.turns[index].last_uuid.clone()?;
        Some((self.turns[..=index].to_vec(), at))
    }
}

/// Where the transcript continues after dropping the latest turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RollbackPlan {
    /// No earlier turn reached Claude: start a fresh Claude session.
    Fresh,
    /// Resume keeping the transcript through this chain entry.
    ResumeAt(String),
}

impl ClaudeHistory {
    /// Plan dropping the latest turn `turn_id`. Earlier turns without
    /// transcript entries (failed before submission) are skipped; a missing
    /// anchor means the position is unknown (legacy history) and is refused.
    pub(crate) fn rollback_plan(
        &self,
        turn_id: &str,
    ) -> Result<(TurnAnchor, RollbackPlan), String> {
        let target = self
            .turns
            .last()
            .filter(|anchor| anchor.turn_id == turn_id)
            .ok_or_else(|| {
                "Only the latest Claude turn recorded by this version can be rolled back"
                    .to_string()
            })?;
        let mut expected = target.turn_index;
        for anchor in self.turns[..self.turns.len() - 1].iter().rev() {
            if expected == 0 {
                break;
            }
            if anchor.turn_index != expected - 1 {
                break;
            }
            expected = anchor.turn_index;
            if let Some(at) = &anchor.last_uuid {
                return Ok((target.clone(), RollbackPlan::ResumeAt(at.clone())));
            }
        }
        if expected == 0 {
            Ok((target.clone(), RollbackPlan::Fresh))
        } else {
            Err("Earlier turns predate Claude rollback support; fork or start a new session instead"
                .to_string())
        }
    }
}

/// Session id and position flags for one launch.
pub(crate) fn resume_args(id: &str, resume: bool, pending: Option<&PendingResume>) -> Vec<String> {
    let owned = |items: &[&str]| {
        items
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
    };
    match pending {
        Some(PendingResume {
            fork_from: Some(source),
            resume_at,
        }) => {
            // `--session-id` with `--resume` is accepted only with `--fork-session`.
            let mut args = owned(&["--resume", source, "--fork-session", "--session-id", id]);
            if let Some(at) = resume_at {
                args.extend(owned(&["--resume-session-at", at]));
            }
            args
        }
        Some(PendingResume {
            fork_from: None,
            resume_at: Some(at),
        }) if resume => owned(&["--resume", id, "--resume-session-at", at]),
        _ => owned(&[if resume { "--resume" } else { "--session-id" }, id]),
    }
}
