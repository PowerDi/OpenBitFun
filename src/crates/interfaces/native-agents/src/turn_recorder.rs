//! Backend-owned persistence of native agent turns (decision D6).
//!
//! The native drivers already project every turn into canonical
//! `AgenticEvent`s. This recorder folds the same events into the persisted
//! `DialogTurnData` shape and saves it when the turn starts (so a crash keeps
//! the user message), at each model-round end, and when the turn settles. The
//! Web UI no longer has to be open for a native conversation to reach disk.
//!
//! The UI-side projection still saves the same turn file; both writers
//! produce the same identity (session id, turn index, turn id), and the
//! last write wins. Turn files are written atomically by `PersistenceManager`.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use openbitfun_core::agentic::persistence::PersistenceManager;
use openbitfun_core::service::session::{
    DialogTurnData, DialogTurnKind, ModelRoundData, TextItemData, ThinkingItemData, ToolCallData,
    ToolItemData, ToolResultData, TurnStatus, UserMessageData,
};
use openbitfun_events::{AgenticEvent, ToolEventData};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Turn being folded for one session.
struct OpenTurn {
    turn: DialogTurnData,
    /// Monotonic order across text, thinking and tool items of a round.
    next_order: usize,
}

impl OpenTurn {
    fn round_mut(&mut self, round_id: &str) -> &mut ModelRoundData {
        if let Some(index) = self.turn.model_rounds.iter().position(|r| r.id == round_id) {
            return &mut self.turn.model_rounds[index];
        }
        // Chunks can arrive for a round the recorder has not seen start.
        let index = self.turn.model_rounds.len();
        self.turn
            .model_rounds
            .push(new_round(&self.turn.turn_id, round_id, index));
        self.turn.model_rounds.last_mut().expect("round pushed")
    }

    fn order(&mut self) -> usize {
        let order = self.next_order;
        self.next_order += 1;
        order
    }
}

fn new_round(turn_id: &str, round_id: &str, round_index: usize) -> ModelRoundData {
    let now = now_ms();
    ModelRoundData {
        id: round_id.to_string(),
        turn_id: turn_id.to_string(),
        round_index,
        round_group_id: None,
        timestamp: now,
        text_items: Vec::new(),
        tool_items: Vec::new(),
        thinking_items: Vec::new(),
        start_time: now,
        end_time: None,
        duration_ms: None,
        provider_id: None,
        model_config_id: None,
        effective_model_name: None,
        routing: None,
        first_chunk_ms: None,
        first_visible_output_ms: None,
        stream_duration_ms: None,
        ttft_ms: None,
        attempt_count: None,
        attempt_diagnostics: Vec::new(),
        failure_category: None,
        token_details: None,
        status: "streaming".to_string(),
    }
}

/// What to do after folding one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordAction {
    None,
    /// Save the turn snapshot (still open).
    Checkpoint,
    /// Save the final turn and forget it.
    Settle,
}

/// Fold native turn events into persisted turns.
#[derive(Default)]
pub(crate) struct TurnRecorder {
    turns: HashMap<(String, String), OpenTurn>,
}

impl TurnRecorder {
    /// Fold `event`; returns the action and, for saves, the snapshot to write.
    pub(crate) fn record(
        &mut self,
        event: &AgenticEvent,
    ) -> (RecordAction, Option<DialogTurnData>) {
        match event {
            AgenticEvent::DialogTurnStarted {
                session_id,
                turn_id,
                turn_index,
                user_input,
                original_user_input,
                user_message_metadata,
            } => {
                let now = now_ms();
                let content = original_user_input
                    .clone()
                    .filter(|text| !text.trim().is_empty())
                    .unwrap_or_else(|| user_input.clone());
                let kind = match user_message_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("kind"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some("manual_compaction") => DialogTurnKind::ManualCompaction,
                    _ => DialogTurnKind::UserDialog,
                };
                let turn = DialogTurnData {
                    turn_id: turn_id.clone(),
                    turn_index: *turn_index,
                    session_id: session_id.clone(),
                    timestamp: now,
                    kind,
                    agent_type: None,
                    user_message: UserMessageData {
                        id: format!("user_{turn_id}"),
                        content,
                        timestamp: now,
                        metadata: user_message_metadata.clone(),
                    },
                    model_rounds: Vec::new(),
                    start_time: now,
                    end_time: None,
                    duration_ms: None,
                    token_usage: None,
                    finish_reason: None,
                    has_final_response: None,
                    error: None,
                    error_detail: None,
                    recovery: None,
                    recovery_epoch: None,
                    status: TurnStatus::InProgress,
                };
                self.turns.insert(
                    (session_id.clone(), turn_id.clone()),
                    OpenTurn {
                        turn: turn.clone(),
                        next_order: 0,
                    },
                );
                (RecordAction::Checkpoint, Some(turn))
            }
            AgenticEvent::ModelRoundStarted {
                session_id,
                turn_id,
                round_id,
                round_index,
                model_config_id,
                effective_model_name,
                ..
            } => {
                let Some(open) = self.open(session_id, turn_id) else {
                    return (RecordAction::None, None);
                };
                if !open.turn.model_rounds.iter().any(|r| &r.id == round_id) {
                    let mut round = new_round(turn_id, round_id, *round_index);
                    round.model_config_id =
                        Some(model_config_id.clone()).filter(|id| !id.is_empty());
                    round.effective_model_name =
                        Some(effective_model_name.clone()).filter(|name| !name.is_empty());
                    open.turn.model_rounds.push(round);
                }
                (RecordAction::None, None)
            }
            AgenticEvent::TextChunk {
                session_id,
                turn_id,
                round_id,
                text,
                ..
            } => {
                let Some(open) = self.open(session_id, turn_id) else {
                    return (RecordAction::None, None);
                };
                let order = open.next_order;
                let round = open.round_mut(round_id);
                // Consecutive chunks extend the last text item of the round.
                let extend = round
                    .text_items
                    .last()
                    .is_some_and(|last| last.order_index.map(|index| index + 1) == Some(order));
                if extend {
                    round
                        .text_items
                        .last_mut()
                        .expect("text item")
                        .content
                        .push_str(text);
                } else {
                    let index = round.text_items.len();
                    round.text_items.push(TextItemData {
                        id: format!("{round_id}-text-{index}"),
                        content: text.clone(),
                        is_streaming: false,
                        timestamp: now_ms(),
                        is_markdown: true,
                        order_index: Some(order),
                        is_subagent_item: None,
                        parent_task_tool_id: None,
                        subagent_session_id: None,
                        status: Some("completed".to_string()),
                        attempt_id: None,
                        attempt_index: None,
                    });
                    open.next_order += 1;
                }
                (RecordAction::None, None)
            }
            AgenticEvent::ThinkingChunk {
                session_id,
                turn_id,
                round_id,
                content,
                reasoning_kind,
                ..
            } => {
                let Some(open) = self.open(session_id, turn_id) else {
                    return (RecordAction::None, None);
                };
                let order = open.next_order;
                let round = open.round_mut(round_id);
                let extend = round
                    .thinking_items
                    .last()
                    .is_some_and(|last| last.order_index.map(|index| index + 1) == Some(order));
                if extend {
                    round
                        .thinking_items
                        .last_mut()
                        .expect("thinking item")
                        .content
                        .push_str(content);
                } else {
                    let index = round.thinking_items.len();
                    round.thinking_items.push(ThinkingItemData {
                        id: format!("{round_id}-thinking-{index}"),
                        content: content.clone(),
                        reasoning_kind: *reasoning_kind,
                        is_streaming: false,
                        is_collapsed: true,
                        timestamp: now_ms(),
                        order_index: Some(order),
                        status: Some("completed".to_string()),
                        is_subagent_item: None,
                        parent_task_tool_id: None,
                        subagent_session_id: None,
                        attempt_id: None,
                        attempt_index: None,
                    });
                    open.next_order += 1;
                }
                (RecordAction::None, None)
            }
            AgenticEvent::ToolEvent {
                session_id,
                turn_id,
                round_id,
                tool_event,
                ..
            } => {
                let Some(open) = self.open(session_id, turn_id) else {
                    return (RecordAction::None, None);
                };
                record_tool(open, round_id, tool_event);
                (RecordAction::None, None)
            }
            AgenticEvent::SubagentSessionLinked {
                session_id,
                parent_session_id,
                parent_dialog_turn_id,
                parent_tool_call_id,
                subagent_dialog_turn_id,
                ..
            } => {
                let Some(open) = self.open(parent_session_id, parent_dialog_turn_id) else {
                    return (RecordAction::None, None);
                };
                if let Some(tool) = open
                    .turn
                    .model_rounds
                    .iter_mut()
                    .flat_map(|round| round.tool_items.iter_mut())
                    .find(|tool| &tool.id == parent_tool_call_id)
                {
                    tool.subagent_session_id = Some(session_id.clone());
                    tool.subagent_dialog_turn_id = Some(subagent_dialog_turn_id.clone());
                }
                (RecordAction::None, None)
            }
            AgenticEvent::ModelRoundCompleted {
                session_id,
                turn_id,
                round_id,
                duration_ms,
                ..
            } => {
                let Some(open) = self.open(session_id, turn_id) else {
                    return (RecordAction::None, None);
                };
                let round = open.round_mut(round_id);
                round.end_time = Some(now_ms());
                round.duration_ms = *duration_ms;
                round.status = "completed".to_string();
                (RecordAction::Checkpoint, Some(open.turn.clone()))
            }
            AgenticEvent::DialogTurnCompleted {
                session_id,
                turn_id,
                duration_ms,
                finish_reason,
                has_final_response,
                ..
            } => self.settle(session_id, turn_id, |turn| {
                turn.status = TurnStatus::Completed;
                turn.duration_ms = Some(*duration_ms);
                turn.finish_reason = finish_reason.clone();
                turn.has_final_response = *has_final_response;
            }),
            AgenticEvent::DialogTurnCancelled {
                session_id,
                turn_id,
            } => self.settle(session_id, turn_id, |turn| {
                turn.status = TurnStatus::Cancelled;
            }),
            AgenticEvent::DialogTurnFailed {
                session_id,
                turn_id,
                error,
                error_detail,
                ..
            } => self.settle(session_id, turn_id, |turn| {
                turn.status = TurnStatus::Error;
                turn.error = Some(error.clone());
                turn.error_detail = error_detail.clone();
            }),
            _ => (RecordAction::None, None),
        }
    }

    fn open(&mut self, session_id: &str, turn_id: &str) -> Option<&mut OpenTurn> {
        self.turns
            .get_mut(&(session_id.to_string(), turn_id.to_string()))
    }

    fn settle(
        &mut self,
        session_id: &str,
        turn_id: &str,
        apply: impl FnOnce(&mut DialogTurnData),
    ) -> (RecordAction, Option<DialogTurnData>) {
        let Some(mut open) = self
            .turns
            .remove(&(session_id.to_string(), turn_id.to_string()))
        else {
            return (RecordAction::None, None);
        };
        let now = now_ms();
        open.turn.end_time = Some(now);
        open.turn.duration_ms = Some(now.saturating_sub(open.turn.start_time));
        apply(&mut open.turn);
        // A settled turn has no streaming rounds or running tools left.
        for round in &mut open.turn.model_rounds {
            if round.status == "streaming" {
                round.status = "completed".to_string();
                round.end_time.get_or_insert(now);
            }
            for tool in &mut round.tool_items {
                if matches!(tool.status.as_deref(), Some("running" | "pending")) {
                    tool.status = Some("cancelled".to_string());
                    tool.end_time.get_or_insert(now);
                }
            }
        }
        (RecordAction::Settle, Some(open.turn))
    }
}

fn record_tool(open: &mut OpenTurn, round_id: &str, event: &ToolEventData) {
    let identity = match event {
        ToolEventData::EarlyDetected { identity, .. }
        | ToolEventData::ParamsPartial { identity, .. }
        | ToolEventData::Started { identity, .. }
        | ToolEventData::ConfirmationNeeded { identity, .. }
        | ToolEventData::Completed { identity, .. }
        | ToolEventData::Failed { identity, .. }
        | ToolEventData::Cancelled { identity, .. }
        | ToolEventData::Rejected { identity, .. } => identity,
        _ => return,
    };
    let order = open.order();
    let round = open.round_mut(round_id);
    let index = match round
        .tool_items
        .iter()
        .position(|t| t.id == identity.tool_id)
    {
        Some(index) => {
            // Not a new item: give the order slot back.
            open.next_order -= 1;
            index
        }
        None => {
            let now = now_ms();
            let round = open.round_mut(round_id);
            round.tool_items.push(ToolItemData {
                id: identity.tool_id.clone(),
                tool_name: identity.tool_name.clone(),
                tool_call: ToolCallData {
                    input: serde_json::json!({}),
                    id: identity.tool_id.clone(),
                },
                tool_result: None,
                ai_intent: None,
                start_time: now,
                end_time: None,
                duration_ms: None,
                queue_wait_ms: None,
                preflight_ms: None,
                confirmation_wait_ms: None,
                execution_ms: None,
                order_index: Some(order),
                is_subagent_item: None,
                parent_task_tool_id: None,
                subagent_session_id: None,
                subagent_dialog_turn_id: None,
                attempt_id: None,
                attempt_index: None,
                subagent_model_id: None,
                subagent_model_display_name: None,
                status: Some("running".to_string()),
                interruption_reason: None,
            });
            round.tool_items.len() - 1
        }
    };
    let round = open.round_mut(round_id);
    let tool = &mut round.tool_items[index];
    tool.tool_name = identity.tool_name.clone();
    match event {
        ToolEventData::Started { params, .. }
        | ToolEventData::ConfirmationNeeded { params, .. } => {
            tool.tool_call.input = params.clone();
        }
        ToolEventData::Completed {
            result,
            result_for_assistant,
            image_attachments,
            duration_ms,
            ..
        } => {
            tool.status = Some("completed".to_string());
            tool.end_time = Some(now_ms());
            tool.duration_ms = Some(*duration_ms);
            tool.tool_result = Some(ToolResultData {
                result: result.clone(),
                success: true,
                result_for_assistant: result_for_assistant.clone(),
                image_attachments: image_attachments.clone(),
                error: None,
                duration_ms: Some(*duration_ms),
            });
        }
        ToolEventData::Failed {
            error, duration_ms, ..
        } => {
            tool.status = Some("error".to_string());
            tool.end_time = Some(now_ms());
            tool.duration_ms = *duration_ms;
            tool.tool_result = Some(ToolResultData {
                result: serde_json::Value::Null,
                success: false,
                result_for_assistant: None,
                image_attachments: None,
                error: Some(error.clone()),
                duration_ms: *duration_ms,
            });
        }
        ToolEventData::Cancelled {
            reason,
            duration_ms,
            ..
        } => {
            tool.status = Some("cancelled".to_string());
            tool.end_time = Some(now_ms());
            tool.duration_ms = *duration_ms;
            tool.interruption_reason = Some(reason.clone());
        }
        ToolEventData::Rejected { .. } => {
            tool.status = Some("rejected".to_string());
            tool.end_time = Some(now_ms());
        }
        _ => {}
    }
}

/// Where a session's turns are saved.
#[derive(Clone)]
pub(crate) struct TurnSink {
    pub manager: Arc<PersistenceManager>,
    pub storage: PathBuf,
}

/// Process-wide recorder and per-session sinks. Sessions register their
/// storage when opened; events of unregistered sessions are not recorded.
pub(crate) struct NativeTurnPersistence {
    recorder: Mutex<TurnRecorder>,
    sinks: Mutex<HashMap<String, TurnSink>>,
    /// Serializes saves per session so a checkpoint never lands after the
    /// final turn write.
    write_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl NativeTurnPersistence {
    pub(crate) fn global() -> &'static Self {
        static GLOBAL: std::sync::OnceLock<NativeTurnPersistence> = std::sync::OnceLock::new();
        GLOBAL.get_or_init(|| Self {
            recorder: Mutex::new(TurnRecorder::default()),
            sinks: Mutex::new(HashMap::new()),
            write_locks: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn register(&self, session_id: &str, sink: TurnSink) {
        self.sinks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id.to_string(), sink);
    }

    /// Fold one event and save when it settles or checkpoints a turn.
    pub(crate) async fn observe(&self, event: &AgenticEvent) {
        let (action, turn) = self
            .recorder
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(event);
        let Some(turn) = turn.filter(|_| action != RecordAction::None) else {
            return;
        };
        let Some(sink) = self
            .sinks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&turn.session_id)
            .cloned()
        else {
            return;
        };
        let lock = self
            .write_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(turn.session_id.clone())
            .or_default()
            .clone();
        let _guard = lock.lock().await;
        if let Err(error) = sink.manager.save_dialog_turn(&sink.storage, &turn).await {
            log::warn!(
                "Native turn save failed: session_id={}, turn_id={}, error={}",
                turn.session_id,
                turn.turn_id,
                error
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openbitfun_events::ToolEventIdentity;

    fn started(turn: &str, index: usize) -> AgenticEvent {
        AgenticEvent::DialogTurnStarted {
            session_id: "s".into(),
            turn_id: turn.into(),
            turn_index: index,
            user_input: "raw".into(),
            original_user_input: Some("fix it".into()),
            user_message_metadata: None,
        }
    }

    #[test]
    fn folds_a_turn_into_the_persisted_shape() {
        let mut recorder = TurnRecorder::default();
        let (action, turn) = recorder.record(&started("t1", 3));
        assert_eq!(action, RecordAction::Checkpoint);
        let turn = turn.unwrap();
        assert_eq!(
            (turn.turn_index, turn.user_message.content.as_str()),
            (3, "fix it")
        );
        assert_eq!(turn.status, TurnStatus::InProgress);

        let text = |chunk: &str| AgenticEvent::TextChunk {
            session_id: "s".into(),
            turn_id: "t1".into(),
            round_id: "r1".into(),
            attempt_id: None,
            attempt_index: None,
            text: chunk.into(),
        };
        let tool = |event| AgenticEvent::ToolEvent {
            session_id: "s".into(),
            turn_id: "t1".into(),
            round_id: "r1".into(),
            attempt_id: None,
            attempt_index: None,
            tool_event: event,
        };
        recorder.record(&text("Hel"));
        recorder.record(&text("lo"));
        recorder.record(&tool(ToolEventData::Started {
            identity: ToolEventIdentity::direct("tool-1", "ExecCommand"),
            params: serde_json::json!({"cmd": "ls"}),
            timeout_seconds: None,
        }));
        recorder.record(&tool(ToolEventData::Completed {
            identity: ToolEventIdentity::direct("tool-1", "ExecCommand"),
            result: serde_json::json!({"output": "a"}),
            result_for_assistant: None,
            image_attachments: None,
            duration_ms: 5,
            queue_wait_ms: None,
            preflight_ms: None,
            confirmation_wait_ms: None,
            execution_ms: None,
        }));
        recorder.record(&text("done"));
        // A different session's events never touch this turn.
        recorder.record(&AgenticEvent::TextChunk {
            session_id: "other".into(),
            turn_id: "t1".into(),
            round_id: "r1".into(),
            attempt_id: None,
            attempt_index: None,
            text: "x".into(),
        });
        let (action, turn) = recorder.record(&AgenticEvent::DialogTurnCompleted {
            session_id: "s".into(),
            turn_id: "t1".into(),
            total_rounds: 1,
            total_tools: 1,
            duration_ms: 42,
            partial_recovery_reason: None,
            success: Some(true),
            finish_reason: Some("complete".into()),
            has_final_response: Some(true),
            background_delivery: None,
        });
        assert_eq!(action, RecordAction::Settle);
        let turn = turn.unwrap();
        assert_eq!(turn.status, TurnStatus::Completed);
        let round = &turn.model_rounds[0];
        let texts: Vec<_> = round
            .text_items
            .iter()
            .map(|t| (t.order_index, t.content.as_str()))
            .collect();
        assert_eq!(texts, [(Some(0), "Hello"), (Some(2), "done")]);
        assert_eq!(round.tool_items[0].order_index, Some(1));
        assert_eq!(round.tool_items[0].status.as_deref(), Some("completed"));
        assert_eq!(round.tool_items[0].tool_call.input["cmd"], "ls");
        // Settled turns are forgotten.
        assert_eq!(recorder.record(&text("late")).0, RecordAction::None);
    }

    #[test]
    fn cancellation_closes_running_tools() {
        let mut recorder = TurnRecorder::default();
        recorder.record(&started("t2", 0));
        recorder.record(&AgenticEvent::ToolEvent {
            session_id: "s".into(),
            turn_id: "t2".into(),
            round_id: "r".into(),
            attempt_id: None,
            attempt_index: None,
            tool_event: ToolEventData::Started {
                identity: ToolEventIdentity::direct("tool", "Read"),
                params: serde_json::json!({}),
                timeout_seconds: None,
            },
        });
        let (_, turn) = recorder.record(&AgenticEvent::DialogTurnCancelled {
            session_id: "s".into(),
            turn_id: "t2".into(),
        });
        let turn = turn.unwrap();
        assert_eq!(turn.status, TurnStatus::Cancelled);
        assert_eq!(
            turn.model_rounds[0].tool_items[0].status.as_deref(),
            Some("cancelled")
        );
    }
}
