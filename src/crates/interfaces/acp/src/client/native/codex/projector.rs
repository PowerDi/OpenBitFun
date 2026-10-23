//! Projects Codex app-server v2 turn notifications onto OpenBitFun events.
//!
//! - text and reasoning stream from `item/*/delta`; `item/completed` only
//!   contributes what the deltas did not cover;
//! - tool cards start once from `item/started` (or an approval request that
//!   precedes it) and settle from `item/completed`;
//! - Codex has no explicit model rounds: a round closes when the model
//!   produces output again after it ran tools;
//! - `turn/completed` settles the turn by `turn.status`.
use std::collections::{HashMap, HashSet};

use openbitfun_events::{AgenticEvent, ToolEventData, ToolEventIdentity};
use serde_json::{json, Value};

use crate::client::tool_card_bridge::{normalize_tool_params, normalize_tool_result};

pub(crate) const CODEX_NATIVE_MODEL_CONFIG_ID: &str = "codex-native";

#[derive(Debug, Clone)]
struct ToolCall {
    tool_name: String,
    round_id: String,
    started_at: std::time::Instant,
}

#[derive(Debug, Clone)]
struct OpenRound {
    round_id: String,
    has_tool_calls: bool,
    placeholder: bool,
}

#[derive(Debug)]
pub(crate) struct CodexTurnProjector {
    session_id: String,
    turn_id: String,
    model_name: String,
    round_index: usize,
    open_round: Option<OpenRound>,
    tools: HashMap<String, ToolCall>,
    started_tools: HashSet<String>,
    /// Card ids of each file-change item: one card per changed file.
    file_cards: HashMap<String, Vec<String>>,
    /// Agent message text streamed per item id.
    streamed_text: HashMap<String, String>,
    /// Reasoning items that streamed at least one delta.
    streamed_reasoning: HashSet<String>,
    thinking_open: bool,
    total_rounds: usize,
    total_tools: usize,
    has_text: bool,
    started_at: std::time::Instant,
    cancel_requested: bool,
    finished: bool,
    compaction: Option<(String, std::time::Instant)>,
    compression_count: usize,
    compaction_trigger: &'static str,
    last_context_tokens: usize,
    /// Last non-retried `error` notification (message, codexErrorInfo tag).
    error: Option<(String, Option<String>)>,
}

impl CodexTurnProjector {
    pub(crate) fn new(session_id: &str, turn_id: &str, model_name: Option<&str>) -> Self {
        Self {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            model_name: model_name.unwrap_or("codex").to_string(),
            round_index: 0,
            open_round: None,
            tools: HashMap::new(),
            started_tools: HashSet::new(),
            file_cards: HashMap::new(),
            streamed_text: HashMap::new(),
            streamed_reasoning: HashSet::new(),
            thinking_open: false,
            total_rounds: 0,
            total_tools: 0,
            has_text: false,
            started_at: std::time::Instant::now(),
            cancel_requested: false,
            finished: false,
            compaction: None,
            compression_count: 0,
            compaction_trigger: "auto",
            last_context_tokens: 0,
            error: None,
        }
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn current_round_index(&self) -> usize {
        self.round_index.saturating_sub(1)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) fn mark_cancel_requested(&mut self) {
        self.cancel_requested = true;
    }

    pub(crate) fn mark_manual_compaction(&mut self) {
        self.compaction_trigger = "manual";
    }

    pub(crate) fn open_pending_round(&mut self) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if self.open_round.is_none() && !self.finished {
            self.start_round(&mut out);
            if let Some(round) = self.open_round.as_mut() {
                round.placeholder = true;
            }
        }
        out
    }

    /// Ensure the card of an approval request exists; `item/started` for the
    /// same item id is deduplicated later.
    pub(crate) fn permission_tool(
        &mut self,
        item_id: &str,
        tool_name: &str,
        params: Value,
    ) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if self.finished {
            return out;
        }
        self.begin_tool_round(&mut out);
        self.start_tool(item_id, tool_name, params, &mut out);
        out
    }

    /// Consume one notification of this turn.
    pub(crate) fn consume(&mut self, method: &str, params: &Value) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if self.finished {
            return out;
        }
        match method {
            "item/started" => self.item_started(&params["item"], &mut out),
            "item/completed" => self.item_completed(&params["item"], &mut out),
            "item/agentMessage/delta" => {
                let (Some(id), Some(delta)) = (params["itemId"].as_str(), params["delta"].as_str())
                else {
                    return out;
                };
                if delta.is_empty() {
                    return out;
                }
                self.close_thinking(&mut out);
                self.begin_output_round(&mut out);
                self.streamed_text
                    .entry(id.to_string())
                    .or_default()
                    .push_str(delta);
                out.extend(self.text_event(delta.to_string()));
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let (Some(id), Some(delta)) = (params["itemId"].as_str(), params["delta"].as_str())
                else {
                    return out;
                };
                if delta.is_empty() {
                    return out;
                }
                self.begin_output_round(&mut out);
                self.streamed_reasoning.insert(id.to_string());
                self.thinking_open = true;
                out.extend(self.thinking_event(delta.to_string(), false));
            }
            "item/reasoning/summaryPartAdded" => {
                if params["itemId"]
                    .as_str()
                    .is_some_and(|id| self.streamed_reasoning.contains(id))
                {
                    out.extend(self.thinking_event("\n\n".to_string(), false));
                }
            }
            "item/fileChange/patchUpdated" => {
                if let Some(id) = params["itemId"].as_str() {
                    self.start_file_change(id, &params["changes"], &mut out);
                }
            }
            "thread/tokenUsage/updated" => {
                if let Some(event) = self.usage_event(&params["tokenUsage"]) {
                    out.push(event);
                }
            }
            "thread/compacted" => {
                if self.compaction.is_none() {
                    self.start_compaction("thread-compacted", &mut out);
                }
                self.complete_compaction(&mut out);
            }
            "error" => {
                if params["willRetry"].as_bool() != Some(true) {
                    let error = &params["error"];
                    self.error = Some((
                        error["message"]
                            .as_str()
                            .unwrap_or("Codex error")
                            .to_string(),
                        super::errors::error_info_tag(&error["codexErrorInfo"]),
                    ));
                }
            }
            "turn/completed" => self.turn_completed(&params["turn"], &mut out),
            _ => {}
        }
        out
    }

    /// Close the turn when the process faulted before `turn/completed`.
    pub(crate) fn fail(&mut self, error: &str) -> Vec<AgenticEvent> {
        self.fail_classified(error, None)
    }

    fn fail_classified(&mut self, error: &str, code: Option<&str>) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        if self.cancel_requested {
            return self.cancel();
        }
        let failure = super::errors::classify(error, code);
        let mut out = Vec::new();
        self.fail_compaction(&failure.message, &mut out);
        self.settle_open_tools(Some(&failure.message), &mut out);
        self.close_round(&mut out);
        out.push(AgenticEvent::DialogTurnFailed {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            error: failure.display(),
            error_category: Some(failure.category.clone()),
            error_detail: Some(failure.detail()),
        });
        self.finished = true;
        out
    }

    pub(crate) fn cancel(&mut self) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.fail_compaction("Codex context compaction was cancelled", &mut out);
        self.settle_open_tools(None, &mut out);
        self.close_round(&mut out);
        out.push(AgenticEvent::DialogTurnCancelled {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
        });
        self.finished = true;
        out
    }

    fn turn_completed(&mut self, turn: &Value, out: &mut Vec<AgenticEvent>) {
        match turn["status"].as_str() {
            Some("completed") => {
                self.complete_compaction(out);
                self.settle_open_tools(Some("Tool did not report a result"), out);
                self.close_round(out);
                out.push(AgenticEvent::DialogTurnCompleted {
                    session_id: self.session_id.clone(),
                    turn_id: self.turn_id.clone(),
                    total_rounds: self.total_rounds,
                    total_tools: self.total_tools,
                    duration_ms: self.started_at.elapsed().as_millis() as u64,
                    partial_recovery_reason: None,
                    success: Some(true),
                    finish_reason: Some("complete".to_string()),
                    has_final_response: Some(self.has_text),
                    background_delivery: None,
                });
                self.finished = true;
            }
            Some("interrupted") => out.extend(self.cancel()),
            _ => {
                let error = &turn["error"];
                let (mut message, mut code) = match error["message"].as_str() {
                    Some(message) => (
                        message.to_string(),
                        super::errors::error_info_tag(&error["codexErrorInfo"]),
                    ),
                    None => self
                        .error
                        .clone()
                        .unwrap_or_else(|| ("Codex turn failed".to_string(), None)),
                };
                if code.is_none() {
                    code = self.error.as_ref().and_then(|(_, code)| code.clone());
                }
                if let Some(details) = error["additionalDetails"]
                    .as_str()
                    .filter(|d| !d.trim().is_empty())
                {
                    message = format!("{message}\n{details}");
                }
                out.extend(self.fail_classified(&message, code.as_deref()));
            }
        }
    }

    fn item_started(&mut self, item: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(id) = item["id"].as_str() else {
            return;
        };
        match item["type"].as_str() {
            Some("commandExecution") => {
                self.close_thinking(out);
                self.begin_tool_round(out);
                self.start_tool(id, "ExecCommand", command_params(item), out);
            }
            Some("fileChange") => self.start_file_change(id, &item["changes"], out),
            Some("contextCompaction") => self.start_compaction(id, out),
            Some(_) => {
                if let Some((name, params)) = generic_tool(item) {
                    self.close_thinking(out);
                    self.begin_tool_round(out);
                    self.start_tool(id, &name, params, out);
                }
            }
            None => {}
        }
    }

    fn item_completed(&mut self, item: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(id) = item["id"].as_str() else {
            return;
        };
        match item["type"].as_str() {
            Some("agentMessage") => {
                let full = item["text"].as_str().unwrap_or_default();
                let streamed = self.streamed_text.remove(id).unwrap_or_default();
                let suffix = if full.starts_with(&streamed) {
                    &full[streamed.len()..]
                } else {
                    log::warn!("Codex native: final message diverged from streamed deltas");
                    ""
                };
                if !suffix.is_empty() {
                    self.close_thinking(out);
                    self.begin_output_round(out);
                    out.extend(self.text_event(suffix.to_string()));
                }
            }
            Some("reasoning") => {
                if !self.streamed_reasoning.remove(id) {
                    let text = reasoning_text(item);
                    if !text.trim().is_empty() {
                        self.begin_output_round(out);
                        out.extend(self.thinking_event(text, false));
                        self.thinking_open = true;
                    }
                }
                self.close_thinking(out);
            }
            Some("commandExecution") => {
                if !self.started_tools.contains(id) {
                    self.begin_tool_round(out);
                    self.start_tool(id, "ExecCommand", command_params(item), out);
                }
                let output = item["aggregatedOutput"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                let exit_code = item["exitCode"].as_i64();
                match item["status"].as_str() {
                    Some("declined") => self.fail_tool(id, "The command was declined", out),
                    Some("failed") if exit_code.is_none() => {
                        let error = if output.trim().is_empty() {
                            "Command failed".to_string()
                        } else {
                            output
                        };
                        self.fail_tool(id, &error, out)
                    }
                    _ => {
                        let result = json!({"output": output, "exit_code": exit_code, "result": output,
                            "duration_ms": item["durationMs"]});
                        self.complete_tool(id, result, output.clone(), out)
                    }
                }
            }
            Some("fileChange") => {
                self.start_file_change(id, &item["changes"], out);
                let ids = self.file_card_ids(id);
                for card in ids {
                    match item["status"].as_str() {
                        Some("declined") => {
                            self.fail_tool(&card, "The file change was declined", out)
                        }
                        Some("failed") => self.fail_tool(&card, "The file change failed", out),
                        _ => self.complete_tool(
                            &card,
                            json!({"result": "Applied", "success": true}),
                            "Applied".into(),
                            out,
                        ),
                    }
                }
            }
            Some("contextCompaction") => {
                if self.compaction.is_none() {
                    self.start_compaction(id, out);
                }
                self.complete_compaction(out);
            }
            Some(_) => {
                let Some((name, params)) = generic_tool(item) else {
                    return;
                };
                if !self.started_tools.contains(id) {
                    self.begin_tool_round(out);
                    self.start_tool(id, &name, params, out);
                }
                match generic_outcome(item) {
                    Ok(text) => self.complete_tool(id, json!({"result": text}), text.clone(), out),
                    Err(error) => self.fail_tool(id, &error, out),
                }
            }
            None => {}
        }
    }

    /// One card per changed file; the first keeps the item id so approval
    /// cards (keyed by item id) and the change card are the same card.
    fn start_file_change(&mut self, item_id: &str, changes: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(changes) = changes.as_array().filter(|changes| !changes.is_empty()) else {
            return;
        };
        self.close_thinking(out);
        self.begin_tool_round(out);
        let mut cards = Vec::with_capacity(changes.len());
        for (index, change) in changes.iter().enumerate() {
            let card = if index == 0 {
                item_id.to_string()
            } else {
                format!("{item_id}:{index}")
            };
            let (name, params) = file_change_params(change);
            self.start_tool(&card, name, params, out);
            cards.push(card);
        }
        let known = self.file_cards.entry(item_id.to_string()).or_default();
        for card in cards {
            if !known.contains(&card) {
                known.push(card);
            }
        }
    }

    fn file_card_ids(&self, item_id: &str) -> Vec<String> {
        match self.file_cards.get(item_id) {
            Some(cards) => cards.clone(),
            None if self.started_tools.contains(item_id) => vec![item_id.to_string()],
            None => Vec::new(),
        }
    }

    fn start_tool(
        &mut self,
        tool_id: &str,
        tool_name: &str,
        params: Value,
        out: &mut Vec<AgenticEvent>,
    ) {
        if !self.started_tools.insert(tool_id.to_string()) {
            return;
        }
        let Some(round) = self.open_round.as_mut() else {
            return;
        };
        round.has_tool_calls = true;
        let round_id = round.round_id.clone();
        self.total_tools += 1;
        self.tools.insert(
            tool_id.to_string(),
            ToolCall {
                tool_name: tool_name.to_string(),
                round_id: round_id.clone(),
                started_at: std::time::Instant::now(),
            },
        );
        out.push(self.tool_event(
            &round_id,
            ToolEventData::Started {
                identity: ToolEventIdentity::direct(tool_id, tool_name),
                params: normalize_tool_params(tool_name, params),
                timeout_seconds: None,
            },
        ));
    }

    fn complete_tool(
        &mut self,
        tool_id: &str,
        result: Value,
        text: String,
        out: &mut Vec<AgenticEvent>,
    ) {
        let Some(call) = self.tools.remove(tool_id) else {
            return;
        };
        let event = ToolEventData::Completed {
            identity: ToolEventIdentity::direct(tool_id, call.tool_name.clone()),
            result: normalize_tool_result(&call.tool_name, result),
            result_for_assistant: Some(text),
            image_attachments: None,
            duration_ms: call.started_at.elapsed().as_millis() as u64,
            queue_wait_ms: None,
            preflight_ms: None,
            confirmation_wait_ms: None,
            execution_ms: None,
        };
        out.push(self.tool_event(&call.round_id, event));
    }

    fn fail_tool(&mut self, tool_id: &str, error: &str, out: &mut Vec<AgenticEvent>) {
        let Some(call) = self.tools.remove(tool_id) else {
            return;
        };
        let event = ToolEventData::Failed {
            identity: ToolEventIdentity::direct(tool_id, call.tool_name),
            error: error.to_string(),
            error_detail: None,
            duration_ms: Some(call.started_at.elapsed().as_millis() as u64),
            queue_wait_ms: None,
            preflight_ms: None,
            confirmation_wait_ms: None,
            execution_ms: None,
        };
        out.push(self.tool_event(&call.round_id, event));
    }

    /// Close cards still open at turn end: failed with `error`, or cancelled.
    fn settle_open_tools(&mut self, error: Option<&str>, out: &mut Vec<AgenticEvent>) {
        let mut open: Vec<(String, ToolCall)> = self.tools.drain().collect();
        open.sort_by(|a, b| a.0.cmp(&b.0));
        for (tool_id, call) in open {
            let identity = ToolEventIdentity::direct(tool_id, call.tool_name);
            let duration_ms = Some(call.started_at.elapsed().as_millis() as u64);
            let event = match error {
                Some(error) => ToolEventData::Failed {
                    identity,
                    error: error.to_string(),
                    error_detail: None,
                    duration_ms,
                    queue_wait_ms: None,
                    preflight_ms: None,
                    confirmation_wait_ms: None,
                    execution_ms: None,
                },
                None => ToolEventData::Cancelled {
                    identity,
                    reason: "Cancelled".to_string(),
                    duration_ms,
                    queue_wait_ms: None,
                    preflight_ms: None,
                    confirmation_wait_ms: None,
                    execution_ms: None,
                },
            };
            out.push(self.tool_event(&call.round_id, event));
        }
    }

    fn start_compaction(&mut self, id: &str, out: &mut Vec<AgenticEvent>) {
        if self.compaction.is_some() {
            return;
        }
        let compression_id = format!("{}-compaction-{id}", self.turn_id);
        out.push(AgenticEvent::ContextCompressionStarted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            compression_id: compression_id.clone(),
            trigger: self.compaction_trigger.to_string(),
            tokens_before: self.last_context_tokens,
            context_window: 0,
        });
        self.compaction = Some((compression_id, std::time::Instant::now()));
    }

    fn complete_compaction(&mut self, out: &mut Vec<AgenticEvent>) {
        let Some((compression_id, started_at)) = self.compaction.take() else {
            return;
        };
        self.compression_count += 1;
        let tokens_before = self.last_context_tokens;
        // Codex reports the compacted size with the next usage update.
        out.push(AgenticEvent::ContextCompressionCompleted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            compression_id,
            compression_count: self.compression_count,
            tokens_before,
            tokens_after: tokens_before,
            compression_ratio: 1.0,
            duration_ms: started_at.elapsed().as_millis() as u64,
            has_summary: true,
            summary_source: "model".into(),
            applied: true,
        });
    }

    fn fail_compaction(&mut self, error: &str, out: &mut Vec<AgenticEvent>) {
        if let Some((compression_id, _)) = self.compaction.take() {
            out.push(AgenticEvent::ContextCompressionFailed {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id,
                error: error.to_string(),
            });
        }
    }

    /// `thread/tokenUsage/updated`: `last` is the latest model call, whose
    /// prompt size is the current context occupancy.
    fn usage_event(&mut self, usage: &Value) -> Option<AgenticEvent> {
        let last = usage.get("last")?;
        let tokens = |key: &str| last.get(key).and_then(Value::as_u64).unwrap_or(0) as usize;
        let input = tokens("inputTokens");
        let output = tokens("outputTokens");
        let total = last
            .get("totalTokens")
            .and_then(Value::as_u64)
            .map(|t| t as usize)
            .unwrap_or(input + output);
        self.last_context_tokens = total;
        Some(AgenticEvent::TokenUsageUpdated {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            model_config_id: CODEX_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            input_tokens: input,
            output_tokens: Some(output),
            total_tokens: total,
            max_context_tokens: usage
                .get("modelContextWindow")
                .and_then(Value::as_u64)
                .map(|w| w as usize),
            is_subagent: false,
            cached_tokens: Some(tokens("cachedInputTokens")),
            token_details: None,
        })
    }

    /// Text/reasoning after tools belongs to the next model round.
    fn begin_output_round(&mut self, out: &mut Vec<AgenticEvent>) {
        match self.open_round.as_ref() {
            Some(round) if round.placeholder => self.start_round(out),
            Some(round) if !round.has_tool_calls => {}
            _ => self.start_round(out),
        }
    }

    fn begin_tool_round(&mut self, out: &mut Vec<AgenticEvent>) {
        match self.open_round.as_ref() {
            Some(round) if !round.placeholder => {}
            _ => self.start_round(out),
        }
    }

    fn start_round(&mut self, out: &mut Vec<AgenticEvent>) {
        if let Some(round) = self.open_round.as_mut().filter(|round| round.placeholder) {
            round.placeholder = false;
            return;
        }
        self.close_round(out);
        let round_id = format!("{}-round-{}", self.turn_id, self.round_index);
        out.push(AgenticEvent::ModelRoundStarted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id: round_id.clone(),
            round_group_id: None,
            round_index: self.round_index,
            model_config_id: CODEX_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
        });
        self.round_index += 1;
        self.total_rounds += 1;
        self.open_round = Some(OpenRound {
            round_id,
            has_tool_calls: false,
            placeholder: false,
        });
    }

    fn close_thinking(&mut self, out: &mut Vec<AgenticEvent>) {
        if std::mem::take(&mut self.thinking_open) {
            out.extend(self.thinking_event(String::new(), true));
        }
    }

    fn close_round(&mut self, out: &mut Vec<AgenticEvent>) {
        self.close_thinking(out);
        let Some(round) = self.open_round.take() else {
            return;
        };
        out.push(AgenticEvent::ModelRoundCompleted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id: round.round_id,
            has_tool_calls: round.has_tool_calls,
            duration_ms: None,
            provider_id: None,
            model_config_id: CODEX_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            first_chunk_ms: None,
            first_visible_output_ms: None,
            stream_duration_ms: None,
            attempt_count: None,
            failure_category: None,
            token_details: None,
        });
    }

    /// Host-authored text at the start of the turn (not Codex output).
    /// Does not count as a final response.
    pub(super) fn notice_event(&self, text: String) -> Option<AgenticEvent> {
        let round_id = self.open_round.as_ref()?.round_id.clone();
        Some(AgenticEvent::TextChunk {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id,
            attempt_id: None,
            attempt_index: None,
            text,
        })
    }

    fn text_event(&mut self, text: String) -> Option<AgenticEvent> {
        let round_id = self.open_round.as_ref()?.round_id.clone();
        self.has_text = true;
        Some(AgenticEvent::TextChunk {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id,
            attempt_id: None,
            attempt_index: None,
            text,
        })
    }

    fn thinking_event(&self, content: String, is_end: bool) -> Option<AgenticEvent> {
        let round_id = self.open_round.as_ref()?.round_id.clone();
        Some(AgenticEvent::ThinkingChunk {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id,
            attempt_id: None,
            attempt_index: None,
            content,
            reasoning_kind: None,
            is_end,
        })
    }

    fn tool_event(&self, round_id: &str, tool_event: ToolEventData) -> AgenticEvent {
        AgenticEvent::ToolEvent {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            round_id: round_id.to_string(),
            attempt_id: None,
            attempt_index: None,
            tool_event,
        }
    }
}

fn command_params(item: &Value) -> Value {
    json!({"command": item["command"], "cwd": item["cwd"], "commandActions": item["commandActions"]})
}

/// Card name and params of a non-command, non-file item, or `None` for items
/// that are not tool calls (messages, reasoning, review markers, ...).
pub(crate) fn generic_tool(item: &Value) -> Option<(String, Value)> {
    match item["type"].as_str()? {
        "mcpToolCall" => Some((
            format!(
                "mcp__{}__{}",
                item["server"].as_str().unwrap_or("mcp"),
                item["tool"].as_str().unwrap_or("tool")
            ),
            item["arguments"].clone(),
        )),
        "dynamicToolCall" => Some((
            item["tool"].as_str().unwrap_or("tool").to_string(),
            item["arguments"].clone(),
        )),
        "webSearch" => Some((
            "WebSearch".into(),
            json!({"query": item["query"], "action": item["action"]}),
        )),
        "imageView" => Some(("Read".into(), json!({"file_path": item["path"]}))),
        "collabAgentToolCall" => Some((
            "Task".into(),
            json!({"description": item["tool"], "prompt": item["prompt"], "model": item["model"]}),
        )),
        "imageGeneration" => Some(("ImageGeneration".into(), item.clone())),
        _ => None,
    }
}

/// Result text of a completed generic item, or its error.
fn generic_outcome(item: &Value) -> Result<String, String> {
    match item["type"].as_str() {
        Some("mcpToolCall") => {
            if let Some(error) = item["error"]["message"].as_str() {
                return Err(error.to_string());
            }
            if item["status"] == "failed" {
                return Err("MCP tool call failed".into());
            }
            let result = &item["result"];
            let text = result["content"]
                .as_array()
                .map(|blocks| {
                    blocks
                        .iter()
                        .filter_map(|block| block["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .filter(|text| !text.is_empty())
                .unwrap_or_else(|| result["structuredContent"].to_string());
            Ok(text)
        }
        Some("dynamicToolCall") => {
            let text = item["contentItems"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            if item["success"] == false || item["status"] == "failed" {
                Err(if text.is_empty() {
                    "Tool call failed".into()
                } else {
                    text
                })
            } else {
                Ok(text)
            }
        }
        Some("webSearch") => Ok(item["query"].as_str().unwrap_or_default().to_string()),
        _ if item["status"] == "failed" => Err("Tool call failed".into()),
        _ => Ok(String::new()),
    }
}

fn reasoning_text(item: &Value) -> String {
    let join = |key: &str| {
        item[key]
            .as_array()
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n\n")
            })
            .unwrap_or_default()
    };
    let summary = join("summary");
    if summary.trim().is_empty() {
        join("content")
    } else {
        summary
    }
}

/// Card for one `FileUpdateChange { path, kind, diff }`. Updates render as an
/// edit diff built from the unified diff; adds as a write; deletes as delete.
pub(crate) fn file_change_params(change: &Value) -> (&'static str, Value) {
    let path = change["path"].clone();
    let diff = change["diff"].as_str().unwrap_or_default();
    match change["kind"]["type"].as_str() {
        Some("add") => {
            let content = if diff
                .lines()
                .all(|line| line.starts_with('+') || line.is_empty())
            {
                diff.lines()
                    .filter(|line| !line.starts_with("+++"))
                    .map(|line| line.strip_prefix('+').unwrap_or(line))
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                diff.to_string()
            };
            ("Write", json!({"file_path": path, "content": content}))
        }
        Some("delete") => ("Delete", json!({"file_path": path})),
        _ => {
            let (mut old, mut new) = (Vec::new(), Vec::new());
            for line in diff.lines() {
                if line.starts_with("---") || line.starts_with("+++") || line.starts_with("@@") {
                    continue;
                }
                match line.chars().next() {
                    Some('-') => old.push(&line[1..]),
                    Some('+') => new.push(&line[1..]),
                    Some(' ') => {
                        old.push(&line[1..]);
                        new.push(&line[1..]);
                    }
                    _ => {}
                }
            }
            let mut params = json!({"file_path": path, "old_string": old.join("\n"),
                "new_string": new.join("\n"), "diff": diff});
            if let Some(target) = change["kind"]["move_path"].as_str() {
                params["move_path"] = json!(target);
            }
            ("Edit", params)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(events: &[AgenticEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                AgenticEvent::ToolEvent { tool_event, .. } => match tool_event {
                    ToolEventData::Started { identity, .. } => {
                        format!("tool_started:{}", identity.tool_name)
                    }
                    ToolEventData::Completed { identity, .. } => {
                        format!("tool_completed:{}", identity.tool_name)
                    }
                    ToolEventData::Failed { identity, .. } => {
                        format!("tool_failed:{}", identity.tool_name)
                    }
                    ToolEventData::Cancelled { .. } => "tool_cancelled".into(),
                    _ => "tool_other".into(),
                },
                AgenticEvent::ModelRoundStarted { .. } => "round_started".into(),
                AgenticEvent::ModelRoundCompleted { .. } => "round_completed".into(),
                AgenticEvent::TextChunk { text, .. } => format!("text:{text}"),
                AgenticEvent::ThinkingChunk { is_end, .. } => format!("thinking:{is_end}"),
                AgenticEvent::TokenUsageUpdated { .. } => "usage".into(),
                AgenticEvent::DialogTurnCompleted { .. } => "turn_completed".into(),
                AgenticEvent::DialogTurnFailed { .. } => "turn_failed".into(),
                AgenticEvent::DialogTurnCancelled { .. } => "turn_cancelled".into(),
                other => format!("{other:?}")
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_string(),
            })
            .collect()
    }

    fn run(projector: &mut CodexTurnProjector, frames: &[(&str, Value)]) -> Vec<AgenticEvent> {
        frames
            .iter()
            .flat_map(|(method, params)| projector.consume(method, params))
            .collect()
    }

    #[test]
    fn projects_reasoning_command_file_change_and_text_into_rounds() {
        let mut projector = CodexTurnProjector::new("s1", "t1", Some("gpt-5.5"));
        let mut events = projector.open_pending_round();
        events.extend(run(
            &mut projector,
            &[
                ("item/started", json!({"item":{"type":"reasoning","id":"r1","summary":[],"content":[]}})),
                ("item/reasoning/summaryTextDelta", json!({"itemId":"r1","delta":"Look"})),
                ("item/completed", json!({"item":{"type":"reasoning","id":"r1","summary":["Look"],"content":[]}})),
                ("item/started", json!({"item":{"type":"commandExecution","id":"c1","command":"ls","cwd":"/w",
                    "status":"inProgress","commandActions":[]}})),
                ("item/completed", json!({"item":{"type":"commandExecution","id":"c1","command":"ls","cwd":"/w",
                    "status":"completed","aggregatedOutput":"a.txt\n","exitCode":0,"commandActions":[]}})),
                ("item/started", json!({"item":{"type":"fileChange","id":"f1","status":"inProgress","changes":[
                    {"path":"a.txt","kind":{"type":"update","move_path":null},"diff":"@@ -1 +1 @@\n-old\n+new\n"}]}})),
                ("item/completed", json!({"item":{"type":"fileChange","id":"f1","status":"completed","changes":[
                    {"path":"a.txt","kind":{"type":"update","move_path":null},"diff":"@@ -1 +1 @@\n-old\n+new\n"}]}})),
                ("item/agentMessage/delta", json!({"itemId":"m1","delta":"Do"})),
                ("item/completed", json!({"item":{"type":"agentMessage","id":"m1","text":"Done"}})),
                ("thread/tokenUsage/updated", json!({"tokenUsage":{"modelContextWindow":258400,
                    "last":{"totalTokens":1200,"inputTokens":1000,"cachedInputTokens":800,"outputTokens":200},
                    "total":{"totalTokens":1200,"inputTokens":1000,"cachedInputTokens":800,"outputTokens":200}}})),
                ("turn/completed", json!({"turn":{"id":"ct1","status":"completed","error":null,"items":[]}})),
            ],
        ));
        assert_eq!(
            kinds(&events),
            [
                "round_started",
                "thinking:false",
                "thinking:true",
                "tool_started:ExecCommand",
                "tool_completed:ExecCommand",
                "tool_started:Edit",
                "tool_completed:Edit",
                "round_completed",
                "round_started",
                "text:Do",
                "text:ne",
                "usage",
                "round_completed",
                "turn_completed",
            ]
        );
        let edit = events.iter().find_map(|event| match event {
            AgenticEvent::ToolEvent {
                tool_event:
                    ToolEventData::Started {
                        identity, params, ..
                    },
                ..
            } if identity.tool_name == "Edit" => Some(params.clone()),
            _ => None,
        });
        let edit = edit.unwrap();
        assert_eq!(edit["old_string"], "old");
        assert_eq!(edit["new_string"], "new");
    }

    #[test]
    fn approval_card_is_reused_and_failed_turn_is_classified() {
        let mut projector = CodexTurnProjector::new("s1", "t1", None);
        let mut events =
            projector.permission_tool("c1", "ExecCommand", json!({"command":"rm -rf x"}));
        events.extend(run(
            &mut projector,
            &[
                (
                    "item/started",
                    json!({"item":{"type":"commandExecution","id":"c1","command":"rm -rf x",
                    "status":"inProgress"}}),
                ),
                (
                    "item/completed",
                    json!({"item":{"type":"commandExecution","id":"c1","status":"declined"}}),
                ),
                (
                    "error",
                    json!({"willRetry":true,"error":{"message":"retrying"}}),
                ),
                (
                    "turn/completed",
                    json!({"turn":{"id":"ct1","status":"failed",
                    "error":{"message":"unexpected status 401","codexErrorInfo":"unauthorized"}}}),
                ),
            ],
        ));
        assert_eq!(
            kinds(&events),
            [
                "round_started",
                "tool_started:ExecCommand",
                "tool_failed:ExecCommand",
                "round_completed",
                "turn_failed"
            ]
        );
        let Some(AgenticEvent::DialogTurnFailed {
            error,
            error_category,
            ..
        }) = events.last()
        else {
            panic!("turn failure");
        };
        assert!(error.contains("codex login"), "{error}");
        assert_eq!(
            error_category,
            &Some(openbitfun_events::agentic::ErrorCategory::Auth)
        );
    }

    #[test]
    fn interrupted_turn_cancels_open_tools() {
        let mut projector = CodexTurnProjector::new("s1", "t1", None);
        let events = run(
            &mut projector,
            &[
                (
                    "item/started",
                    json!({"item":{"type":"mcpToolCall","id":"m1","server":"docs","tool":"search",
                    "arguments":{"q":"x"},"status":"inProgress"}}),
                ),
                (
                    "turn/completed",
                    json!({"turn":{"id":"ct1","status":"interrupted"}}),
                ),
            ],
        );
        assert_eq!(
            kinds(&events),
            [
                "round_started",
                "tool_started:mcp__docs__search",
                "tool_cancelled",
                "round_completed",
                "turn_cancelled"
            ]
        );
        assert!(projector.is_finished());
    }
}
