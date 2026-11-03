//! Read-only view of a delegated session, built from persisted turns.
//!
//! Mirrors codex-host `thread read`: the default view is the latest turn's
//! status and final agent message; `messages` adds visible user and agent
//! messages. Tool calls, tool output, file activity and reasoning are never
//! returned, so a delegator's context is not flooded with the child's trace.
use openbitfun_core::service::session::{DialogTurnData, SessionMetadata, TurnStatus};
use serde::Serialize;

/// Upper bound for one message in a snapshot, to keep tool results small.
const MAX_MESSAGE_CHARS: usize = 12_000;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SnapshotResult {
    /// `pending` while running, `available` with a final message, else `unavailable`.
    pub(super) availability: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SnapshotMessage {
    pub(super) turn_id: String,
    /// `user` or `agent`.
    pub(super) role: &'static str,
    pub(super) text: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SessionSnapshot {
    pub(super) session_id: String,
    pub(super) agent: String,
    pub(super) title: String,
    /// `running`, `completed`, `failed`, `cancelled`, or `idle` (no turn yet).
    pub(super) status: String,
    #[serde(skip)]
    pub(super) running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) turn_id: Option<String>,
    pub(super) result: SnapshotResult,
    /// Latest visible agent text of the running turn.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) progress: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) messages: Option<Vec<SnapshotMessage>>,
}

fn clip(text: &str) -> String {
    if text.chars().count() <= MAX_MESSAGE_CHARS {
        return text.to_string();
    }
    let mut clipped: String = text.chars().take(MAX_MESSAGE_CHARS).collect();
    clipped.push_str("\n[truncated]");
    clipped
}

/// Agent text of the last model round that produced any (the final answer
/// of a completed turn). Subagent items belong to child transcripts.
pub(super) fn final_text(turn: &DialogTurnData) -> Option<String> {
    turn.model_rounds.iter().rev().find_map(|round| {
        let text = round
            .text_items
            .iter()
            .filter(|item| item.is_subagent_item != Some(true))
            .map(|item| item.content.as_str())
            .collect::<Vec<_>>()
            .join("");
        let text = text.trim();
        (!text.is_empty()).then(|| clip(text))
    })
}

fn status_of(turn: &DialogTurnData) -> &'static str {
    super::service::status_label(&turn.status)
}

pub(super) fn build(
    metadata: &SessionMetadata,
    turns: &[DialogTurnData],
    with_messages: bool,
    running: bool,
) -> SessionSnapshot {
    let latest = turns.iter().max_by_key(|turn| turn.turn_index);
    let status = if running {
        "running".to_string()
    } else {
        latest
            .map(|turn| match turn.status {
                // A turn file left in progress by a crash is not running.
                TurnStatus::InProgress => "interrupted",
                _ => status_of(turn),
            })
            .unwrap_or("idle")
            .to_string()
    };
    let final_message = latest
        .filter(|turn| turn.status == TurnStatus::Completed)
        .and_then(final_text);
    let result = if running {
        SnapshotResult {
            availability: "pending",
            text: None,
        }
    } else {
        SnapshotResult {
            availability: if final_message.is_some() {
                "available"
            } else {
                "unavailable"
            },
            text: final_message,
        }
    };
    let messages = with_messages.then(|| {
        let mut ordered: Vec<&DialogTurnData> = turns.iter().collect();
        ordered.sort_by_key(|turn| turn.turn_index);
        ordered
            .into_iter()
            .flat_map(|turn| {
                let user = Some(SnapshotMessage {
                    turn_id: turn.turn_id.clone(),
                    role: "user",
                    text: clip(&turn.user_message.content),
                })
                .filter(|message| !message.text.trim().is_empty());
                let agent = final_text(turn).map(|text| SnapshotMessage {
                    turn_id: turn.turn_id.clone(),
                    role: "agent",
                    text,
                });
                user.into_iter().chain(agent)
            })
            .collect()
    });
    SessionSnapshot {
        session_id: metadata.session_id.clone(),
        agent: metadata
            .external_client_id()
            .unwrap_or(metadata.agent_type.as_str())
            .to_string(),
        title: metadata.session_name.clone(),
        status,
        running,
        turn_id: latest.map(|turn| turn.turn_id.clone()),
        result,
        progress: latest.filter(|_| running).and_then(final_text),
        error: latest
            .filter(|turn| !running && turn.status == TurnStatus::Error)
            .and_then(|turn| turn.error.clone()),
        messages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openbitfun_core::service::session::{
        DialogTurnKind, ModelRoundData, TextItemData, UserMessageData,
    };

    fn turn(index: usize, status: TurnStatus, rounds: &[&[&str]]) -> DialogTurnData {
        DialogTurnData {
            turn_id: format!("t{index}"),
            turn_index: index,
            session_id: "s".into(),
            timestamp: 0,
            kind: DialogTurnKind::UserDialog,
            agent_type: None,
            user_message: UserMessageData {
                id: format!("u{index}"),
                content: format!("task {index}"),
                timestamp: 0,
                metadata: None,
            },
            model_rounds: rounds
                .iter()
                .enumerate()
                .map(|(round_index, texts)| {
                    let mut round: ModelRoundData = serde_json::from_value(serde_json::json!({
                        "id": format!("r{round_index}"), "turnId": format!("t{index}"),
                        "roundIndex": round_index, "timestamp": 0, "startTime": 0,
                        "status": "completed"
                    }))
                    .unwrap();
                    round.text_items = texts
                        .iter()
                        .map(|text| -> TextItemData {
                            serde_json::from_value(serde_json::json!({
                                "id": "x", "content": text, "isStreaming": false, "timestamp": 0,
                                "status": "completed"
                            }))
                            .unwrap()
                        })
                        .collect();
                    round
                })
                .collect(),
            start_time: 0,
            end_time: None,
            duration_ms: None,
            token_usage: None,
            finish_reason: None,
            has_final_response: None,
            error: None,
            error_detail: None,
            recovery: None,
            recovery_epoch: None,
            status,
        }
    }

    fn metadata() -> SessionMetadata {
        let mut metadata = SessionMetadata::new(
            "s".into(),
            "Fix tests".into(),
            "acp:codex-native".into(),
            "m".into(),
        );
        metadata.custom_metadata = Some(serde_json::json!({"provider": "acp"}));
        metadata
    }

    #[test]
    fn result_is_the_final_round_text_without_trace() {
        let turns = vec![
            turn(0, TurnStatus::Completed, &[&["old"]]),
            turn(
                1,
                TurnStatus::Completed,
                &[&["looking"], &[], &["Done: ", "3 fixed"]],
            ),
        ];
        let snapshot = build(&metadata(), &turns, false, false);
        assert_eq!(snapshot.agent, "codex-native");
        assert_eq!(snapshot.status, "completed");
        assert_eq!(snapshot.turn_id.as_deref(), Some("t1"));
        assert_eq!(snapshot.result.availability, "available");
        assert_eq!(snapshot.result.text.as_deref(), Some("Done: 3 fixed"));
        assert!(snapshot.messages.is_none());
    }

    #[test]
    fn running_and_failed_turns_have_no_result() {
        let turns = vec![turn(0, TurnStatus::InProgress, &[&["working on it"]])];
        let running = build(&metadata(), &turns, false, true);
        assert_eq!(running.status, "running");
        assert_eq!(running.result.availability, "pending");
        assert_eq!(running.progress.as_deref(), Some("working on it"));

        // Not running any more but never settled: the host died mid-turn.
        let stale = build(&metadata(), &turns, false, false);
        assert_eq!(stale.status, "interrupted");
        assert_eq!(stale.result.availability, "unavailable");

        let mut failed = turn(0, TurnStatus::Error, &[&["partial"]]);
        failed.error = Some("rate limited".into());
        let failed = build(&metadata(), &[failed], false, false);
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.result.availability, "unavailable");
        assert_eq!(failed.error.as_deref(), Some("rate limited"));
    }

    #[test]
    fn messages_view_lists_user_and_final_agent_text_in_order() {
        let turns = vec![
            turn(1, TurnStatus::Completed, &[&["second answer"]]),
            turn(0, TurnStatus::Completed, &[&["first answer"]]),
        ];
        let snapshot = build(&metadata(), &turns, true, false);
        let messages = snapshot.messages.unwrap();
        let flat: Vec<_> = messages.iter().map(|m| (m.role, m.text.as_str())).collect();
        assert_eq!(
            flat,
            vec![
                ("user", "task 0"),
                ("agent", "first answer"),
                ("user", "task 1"),
                ("agent", "second answer"),
            ]
        );
    }
}
