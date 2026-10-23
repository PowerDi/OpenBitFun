//! Projects Claude Code `stream-json` frames onto OpenBitFun `AgenticEvent`s.
//!
//! Modeled on codexhost's `ClaudeNativeTurnAccumulator` and paseo's
//! `TimelineAssembler`:
//! - text/thinking stream from `content_block_delta`; the final `assistant`
//!   frame only contributes the suffix the deltas did not cover;
//! - the CLI sends one `assistant` frame per content block, all sharing the
//!   message id, so dedupe is keyed by message id + block kind order;
//! - a tool card starts once, from the complete `tool_use` block;
//! - frames carrying `parent_tool_use_id` belong to a subagent and never
//!   leak into the parent transcript;
//! - a round stays open until the next round starts or the turn ends, because
//!   `tool_result` frames arrive after the round's `message_stop`.

use std::collections::{HashMap, HashSet};

use openbitfun_events::{AgenticEvent, ToolEventData, ToolEventIdentity};
use serde_json::{json, Value};

use crate::client::tool_card_bridge::{normalize_tool_params, normalize_tool_result};

/// Model identifiers reported on native rounds. Native harnesses pick their
/// own model, so the config id is a synthetic, stable label.
pub(crate) const CLAUDE_NATIVE_MODEL_CONFIG_ID: &str = "claude-native";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockKind {
    Text,
    Thinking,
}

#[derive(Debug, Default)]
struct StreamedMessage {
    /// Streamed blocks in arrival order: kind and accumulated text.
    blocks: Vec<(BlockKind, String)>,
    /// Block index (stream) -> position in `blocks`.
    index_map: HashMap<u64, usize>,
    /// How many streamed blocks of each kind the final frames consumed.
    consumed_text: usize,
    consumed_thinking: usize,
}

#[derive(Debug, Clone)]
struct ToolCall {
    tool_name: String,
    round_id: String,
    started_at: std::time::Instant,
}

/// Outcome of a terminal `result` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TurnOutcome {
    Completed,
    Cancelled,
    Failed(String),
}

#[derive(Debug)]
pub(crate) struct ClaudeTurnProjector {
    session_id: String,
    turn_id: String,
    model_name: String,
    round_index: usize,
    open_round: Option<OpenRound>,
    messages: HashMap<String, StreamedMessage>,
    /// Message id of the frame currently streaming at the root level.
    active_message_id: Option<String>,
    open_thinking_block: Option<u64>,
    tools: HashMap<String, ToolCall>,
    started_tools: HashSet<String>,
    total_rounds: usize,
    total_tools: usize,
    has_text: bool,
    started_at: std::time::Instant,
    cancel_requested: bool,
    finished: bool,
    /// Child (subagent) projectors consume sidechain frames for their own
    /// transcript; the parent projector keeps dropping them.
    sidechain: bool,
    /// Usage of the latest root assistant message: its prompt size is the
    /// current context occupancy (result usage sums every API call instead).
    last_message_usage: Option<Value>,
    compaction: Option<Compaction>,
    compaction_settled: bool,
    compression_count: usize,
    compaction_trigger: &'static str,
    /// Last `error` code Claude attached to a root assistant frame
    /// (e.g. `authentication_failed`); used to classify the turn failure.
    assistant_error: Option<String>,
    /// Cumulative session cost reported by the last `result` frame.
    total_cost_usd: Option<f64>,
}

#[derive(Debug)]
struct Compaction {
    id: String,
    tokens_before: usize,
    started_at: std::time::Instant,
    /// `compact_result: success` arrived; the token counts come with the
    /// `compact_boundary` frame, which the CLI may send afterwards.
    succeeded: bool,
}

#[derive(Debug, Clone)]
struct OpenRound {
    round_id: String,
    has_tool_calls: bool,
    /// Opened at turn start so the UI shows a waiting indicator before Claude
    /// streams anything; adopted by the first real message instead of a new round.
    placeholder: bool,
}

impl ClaudeTurnProjector {
    pub(crate) fn new(session_id: &str, turn_id: &str, model_name: Option<&str>) -> Self {
        Self {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            model_name: model_name.unwrap_or("claude").to_string(),
            round_index: 0,
            open_round: None,
            messages: HashMap::new(),
            active_message_id: None,
            open_thinking_block: None,
            tools: HashMap::new(),
            started_tools: HashSet::new(),
            total_rounds: 0,
            total_tools: 0,
            has_text: false,
            started_at: std::time::Instant::now(),
            cancel_requested: false,
            finished: false,
            sidechain: false,
            last_message_usage: None,
            compaction: None,
            compaction_settled: false,
            compression_count: 0,
            compaction_trigger: "auto",
            assistant_error: None,
            total_cost_usd: None,
        }
    }

    /// Projector for one subagent transcript (frames with `parent_tool_use_id`).
    pub(crate) fn for_subagent(session_id: &str, turn_id: &str, model_name: Option<&str>) -> Self {
        Self {
            sidechain: true,
            ..Self::new(session_id, turn_id, model_name)
        }
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    /// Index of the latest model round (0 before the first one).
    pub(crate) fn current_round_index(&self) -> usize {
        self.round_index.saturating_sub(1)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) fn mark_manual_compaction(&mut self) {
        self.compaction_trigger = "manual";
    }

    /// `total_cost_usd` from the terminal `result`, when Claude reported one.
    pub(crate) fn total_cost_usd(&self) -> Option<f64> {
        self.total_cost_usd
    }

    pub(crate) fn mark_cancel_requested(&mut self) {
        self.cancel_requested = true;
    }

    /// Ensure the permission card exists even when stdio control arrives before
    /// the final tool_use frame, without inventing an assistant message id.
    pub(crate) fn permission_tool(
        &mut self,
        tool_id: &str,
        name: &str,
        input: Value,
    ) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if self
            .open_round
            .as_ref()
            .map_or(true, |round| round.placeholder)
        {
            self.start_round(&mut out);
        }
        self.start_tool(&json!({"id":tool_id, "name":name, "input":input}), &mut out);
        out
    }

    /// Tool ids still waiting for a result (used to close cards on faults).
    pub(crate) fn open_tool_ids(&self) -> Vec<String> {
        self.tools.keys().cloned().collect()
    }

    /// Consume one stdout frame. Control frames are handled by the session and
    /// ignored here.
    pub(crate) fn consume(&mut self, frame: &Value) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        // Subagent (sidechain) traffic never enters the parent transcript.
        if !self.sidechain
            && frame
                .get("parent_tool_use_id")
                .is_some_and(|value| !value.is_null())
        {
            return Vec::new();
        }
        let mut out = Vec::new();
        match frame.get("type").and_then(Value::as_str) {
            Some("system") => {
                self.consume_compaction(frame, &mut out);
                if frame["subtype"] == "local_command_output" {
                    if let Some(text) = frame["content"].as_str().filter(|text| !text.is_empty()) {
                        if self.open_round.is_none() {
                            self.start_round(&mut out);
                        }
                        out.extend(self.text_event(text.to_string()));
                    }
                }
                if frame.get("subtype").and_then(Value::as_str) == Some("init") {
                    if let Some(model) = frame.get("model").and_then(Value::as_str) {
                        self.model_name = model.to_string();
                    }
                }
            }
            Some("stream_event") => {
                if let Some(event) = frame.get("event") {
                    self.consume_stream_event(event, &mut out);
                }
            }
            Some("assistant") => self.consume_assistant(frame, &mut out),
            Some("user") => self.consume_user(frame, &mut out),
            Some("result") => self.consume_result(frame, &mut out),
            _ => {}
        }
        out
    }

    fn consume_compaction(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        let status = frame["subtype"] == "status";
        let boundary = frame["subtype"] == "compact_boundary";
        let outcome = frame["compact_result"]
            .as_str()
            .filter(|value| matches!(*value, "success" | "failed"));
        let starting = status && frame["status"] == "compacting";
        if !starting && !boundary && !(status && outcome.is_some()) {
            return;
        }
        // CLI versions may send both compact_result and compact_boundary.
        if !starting && self.compaction_settled {
            return;
        }
        let metadata = &frame["compact_metadata"];
        if self.compaction.is_none() {
            let tokens_before = metadata["pre_tokens"]
                .as_u64()
                .map(|n| n as usize)
                .unwrap_or_else(|| {
                    self.last_message_usage.as_ref().map_or(0, |usage| {
                        [
                            "input_tokens",
                            "cache_read_input_tokens",
                            "cache_creation_input_tokens",
                        ]
                        .iter()
                        .map(|key| usage[*key].as_u64().unwrap_or(0) as usize)
                        .sum()
                    })
                });
            let id = uuid::Uuid::new_v4().to_string();
            out.push(AgenticEvent::ContextCompressionStarted {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id: id.clone(),
                trigger: metadata["trigger"]
                    .as_str()
                    .unwrap_or(self.compaction_trigger)
                    .to_string(),
                tokens_before,
                context_window: 0,
            });
            self.compaction = Some(Compaction {
                id,
                tokens_before,
                started_at: std::time::Instant::now(),
                succeeded: false,
            });
            self.compaction_settled = false;
        }
        if !boundary && outcome.is_none() {
            return;
        }
        if !boundary && outcome == Some("success") && metadata["pre_tokens"].as_u64().is_none() {
            // Wait for the boundary's token counts instead of settling on 0.
            if let Some(compaction) = self.compaction.as_mut() {
                compaction.succeeded = true;
            }
            return;
        }
        let compaction = self.compaction.take().expect("started compaction");
        self.compaction_settled = true;
        if outcome == Some("failed") {
            out.push(AgenticEvent::ContextCompressionFailed {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id: compaction.id,
                error: frame["error"]
                    .as_str()
                    .or_else(|| frame["message"].as_str())
                    .unwrap_or("Claude context compaction failed")
                    .to_string(),
            });
            return;
        }
        self.complete_compaction(compaction, metadata, out);
    }

    fn complete_compaction(
        &mut self,
        compaction: Compaction,
        metadata: &Value,
        out: &mut Vec<AgenticEvent>,
    ) {
        self.compression_count += 1;
        let tokens_before = metadata["pre_tokens"]
            .as_u64()
            .map(|n| n as usize)
            .unwrap_or(compaction.tokens_before);
        // Claude normally reports only pre_tokens. Keep the last known occupancy
        // until a later usage report, rather than treating missing post_tokens as zero.
        let tokens_after = metadata["post_tokens"]
            .as_u64()
            .map(|n| n as usize)
            .unwrap_or(tokens_before);
        out.push(AgenticEvent::ContextCompressionCompleted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            compression_id: compaction.id,
            compression_count: self.compression_count,
            tokens_before,
            tokens_after,
            compression_ratio: if tokens_before == 0 {
                1.0
            } else {
                tokens_after as f64 / tokens_before as f64
            },
            duration_ms: compaction.started_at.elapsed().as_millis() as u64,
            has_summary: true,
            summary_source: "model".into(),
            applied: true,
        });
    }

    fn fail_compaction(&mut self, error: &str, out: &mut Vec<AgenticEvent>) {
        if let Some(compaction) = self.compaction.take() {
            self.compaction_settled = true;
            out.push(AgenticEvent::ContextCompressionFailed {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id: compaction.id,
                error: error.to_string(),
            });
        }
    }

    /// Close the turn when the process faulted before a `result` arrived.
    pub(crate) fn fail(&mut self, error: &str) -> Vec<AgenticEvent> {
        self.fail_classified(error, None)
    }

    fn fail_classified(&mut self, error: &str, subtype: Option<&str>) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        if self.cancel_requested {
            return self.cancel();
        }
        let failure = super::errors::classify(error, subtype, self.assistant_error.as_deref());
        let mut out = Vec::new();
        self.fail_compaction(&failure.message, &mut out);
        self.fail_open_tools(&failure.message, &mut out);
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

    /// Close the turn as cancelled without waiting for a `result`.
    pub(crate) fn cancel(&mut self) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.fail_compaction("Claude context compaction was cancelled", &mut out);
        self.cancel_open_tools(&mut out);
        self.close_round(&mut out);
        out.push(AgenticEvent::DialogTurnCancelled {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
        });
        self.finished = true;
        out
    }
    fn consume_stream_event(&mut self, event: &Value, out: &mut Vec<AgenticEvent>) {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                let message_id = event
                    .pointer("/message/id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let Some(model) = event.pointer("/message/model").and_then(Value::as_str) {
                    self.model_name = model.to_string();
                }
                if let Some(usage) = event.pointer("/message/usage") {
                    self.last_message_usage = Some(usage.clone());
                }
                self.start_round(out);
                if let Some(id) = message_id.clone() {
                    self.messages.entry(id).or_default();
                }
                self.active_message_id = message_id;
            }
            Some("content_block_start") => {
                let index = event.get("index").and_then(Value::as_u64).unwrap_or(0);
                let kind = match event.pointer("/content_block/type").and_then(Value::as_str) {
                    Some("text") => BlockKind::Text,
                    Some("thinking") => BlockKind::Thinking,
                    _ => return,
                };
                if let Some(message) = self.active_message_mut() {
                    message.blocks.push((kind, String::new()));
                    let position = message.blocks.len() - 1;
                    message.index_map.insert(index, position);
                }
            }
            Some("content_block_delta") => self.consume_delta(event, out),
            Some("content_block_stop") => {
                let index = event.get("index").and_then(Value::as_u64).unwrap_or(0);
                if self.open_thinking_block == Some(index) {
                    self.open_thinking_block = None;
                    let event = self.thinking_event(String::new(), true);
                    out.extend(event);
                }
            }
            _ => {}
        }
    }

    fn active_message_mut(&mut self) -> Option<&mut StreamedMessage> {
        let id = self.active_message_id.clone()?;
        self.messages.get_mut(&id)
    }
    fn consume_delta(&mut self, event: &Value, out: &mut Vec<AgenticEvent>) {
        let index = event.get("index").and_then(Value::as_u64).unwrap_or(0);
        let delta = event.get("delta").unwrap_or(&Value::Null);
        let (kind, text) = match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => (
                BlockKind::Text,
                delta
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            Some("thinking_delta") => (
                BlockKind::Thinking,
                delta
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            // input_json_delta / signature_delta: the complete tool_use block
            // and signature arrive with the final assistant frame.
            _ => return,
        };
        if text.is_empty() {
            return;
        }
        let Some(message) = self.active_message_mut() else {
            return;
        };
        let Some(position) = message.index_map.get(&index).copied() else {
            return;
        };
        message.blocks[position].1.push_str(text);
        let text = text.to_string();
        match kind {
            BlockKind::Text => {
                out.extend(self.text_event(text));
            }
            BlockKind::Thinking => {
                self.open_thinking_block = Some(index);
                out.extend(self.thinking_event(text, false));
            }
        }
    }

    fn consume_assistant(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        if let Some(error) = frame.get("error").and_then(Value::as_str) {
            self.assistant_error = Some(error.to_string());
        }
        if let Some(usage) = frame.pointer("/message/usage") {
            self.last_message_usage = Some(usage.clone());
        }
        let message_id = frame
            .pointer("/message/id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if self.open_round.is_none() || !self.messages.contains_key(&message_id) {
            // Non-streaming path (no partial messages): the frame is the round.
            self.start_round(out);
            self.messages.entry(message_id.clone()).or_default();
            self.active_message_id = Some(message_id.clone());
        }
        let Some(blocks) = frame.pointer("/message/content").and_then(Value::as_array) else {
            return;
        };
        for block in blocks {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let full = block
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let suffix = self.consume_final_block(&message_id, BlockKind::Text, full);
                    if let Some(suffix) = suffix.filter(|s| !s.is_empty()) {
                        out.extend(self.text_event(suffix));
                    }
                }
                Some("thinking") => {
                    let full = block
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let suffix = self.consume_final_block(&message_id, BlockKind::Thinking, full);
                    if let Some(suffix) = suffix.filter(|s| !s.is_empty()) {
                        out.extend(self.thinking_event(suffix, false));
                        out.extend(self.thinking_event(String::new(), true));
                    }
                }
                Some("tool_use") | Some("server_tool_use") | Some("mcp_tool_use") => {
                    self.start_tool(block, out);
                }
                _ => {}
            }
        }
    }

    /// Match a final block against the next streamed block of the same kind and
    /// return the part the deltas did not cover.
    fn consume_final_block(
        &mut self,
        message_id: &str,
        kind: BlockKind,
        full: &str,
    ) -> Option<String> {
        let message = self.messages.entry(message_id.to_string()).or_default();
        let consumed = match kind {
            BlockKind::Text => &mut message.consumed_text,
            BlockKind::Thinking => &mut message.consumed_thinking,
        };
        let streamed = message
            .blocks
            .iter()
            .filter(|(block_kind, _)| *block_kind == kind)
            .nth(*consumed)
            .map(|(_, text)| text.clone());
        *consumed += 1;
        match streamed {
            None => Some(full.to_string()),
            Some(streamed) if full.starts_with(&streamed) => {
                Some(full[streamed.len()..].to_string())
            }
            // The final text disagrees with what streamed; keep the streamed
            // copy rather than duplicating content in the transcript.
            Some(_) => {
                log::warn!("Claude native: final block diverged from streamed deltas");
                None
            }
        }
    }
    fn start_tool(&mut self, block: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(tool_id) = block.get("id").and_then(Value::as_str) else {
            return;
        };
        if !self.started_tools.insert(tool_id.to_string()) {
            return;
        }
        let native_name = block.get("name").and_then(Value::as_str).unwrap_or("tool");
        let tool_name = native_tool_name(native_name);
        let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
        let params = normalize_tool_params(&tool_name, input);
        let Some(round) = self.open_round.as_mut() else {
            return;
        };
        round.has_tool_calls = true;
        let round_id = round.round_id.clone();
        self.total_tools += 1;
        self.tools.insert(
            tool_id.to_string(),
            ToolCall {
                tool_name: tool_name.clone(),
                round_id: round_id.clone(),
                started_at: std::time::Instant::now(),
            },
        );
        out.push(self.tool_event(
            &round_id,
            ToolEventData::Started {
                identity: ToolEventIdentity::direct(tool_id, tool_name),
                params,
                timeout_seconds: None,
            },
        ));
    }

    fn consume_user(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(blocks) = frame.pointer("/message/content").and_then(Value::as_array) else {
            return;
        };
        let structured = frame.get("tool_use_result");
        for block in blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let Some(tool_id) = block.get("tool_use_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(call) = self.tools.remove(tool_id) else {
                continue;
            };
            let text = tool_result_text(block.get("content"));
            let duration_ms = call.started_at.elapsed().as_millis() as u64;
            let identity = ToolEventIdentity::direct(tool_id, call.tool_name.clone());
            let is_error = block
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let event = if is_error {
                ToolEventData::Failed {
                    identity,
                    error: text,
                    error_detail: None,
                    duration_ms: Some(duration_ms),
                    queue_wait_ms: None,
                    preflight_ms: None,
                    confirmation_wait_ms: None,
                    execution_ms: None,
                }
            } else {
                let result = build_tool_result(&call.tool_name, &text, structured);
                ToolEventData::Completed {
                    identity,
                    result: normalize_tool_result(&call.tool_name, result),
                    result_for_assistant: Some(text),
                    image_attachments: None,
                    duration_ms,
                    queue_wait_ms: None,
                    preflight_ms: None,
                    confirmation_wait_ms: None,
                    execution_ms: None,
                }
            };
            out.push(self.tool_event(&call.round_id, event));
        }
    }

    fn consume_result(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        let subtype = frame.get("subtype").and_then(Value::as_str);
        let is_error = frame
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.total_cost_usd = frame
            .get("total_cost_usd")
            .and_then(Value::as_f64)
            .filter(|cost| cost.is_finite() && *cost >= 0.0);
        let outcome = if self.cancel_requested {
            TurnOutcome::Cancelled
        } else if subtype == Some("success") && !is_error && self.assistant_error.is_none() {
            TurnOutcome::Completed
        } else {
            let detail = frame
                .get("result")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(str::to_string)
                .or_else(|| {
                    frame.get("errors").and_then(Value::as_array).map(|errors| {
                        errors
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(
                                "
",
                            )
                    })
                })
                .filter(|text| !text.trim().is_empty())
                .or_else(|| subtype.map(str::to_string))
                .unwrap_or_else(|| "Claude Code turn failed".to_string());
            TurnOutcome::Failed(detail)
        };
        if let Some(event) = self.usage_event(frame) {
            out.push(event);
        }
        match outcome {
            TurnOutcome::Completed => {
                if self.compaction.as_ref().is_some_and(|c| c.succeeded) {
                    // Success was reported but no boundary followed.
                    let compaction = self.compaction.take().expect("pending compaction");
                    self.compaction_settled = true;
                    self.complete_compaction(compaction, &Value::Null, out);
                }
                self.fail_compaction("Claude turn ended before compaction completion", out);
                // A result with tools still open is a protocol gap; close them
                // so cards do not spin forever.
                self.fail_open_tools("Tool did not report a result", out);
                self.close_round(out);
                out.push(AgenticEvent::DialogTurnCompleted {
                    session_id: self.session_id.clone(),
                    turn_id: self.turn_id.clone(),
                    total_rounds: self.total_rounds,
                    total_tools: self.total_tools,
                    duration_ms: self.started_at.elapsed().as_millis() as u64,
                    partial_recovery_reason: None,
                    success: Some(true),
                    finish_reason: Some(
                        turn_finish_reason(frame.get("stop_reason").and_then(Value::as_str))
                            .to_string(),
                    ),
                    has_final_response: Some(self.has_text),
                    background_delivery: None,
                });
                self.finished = true;
            }
            TurnOutcome::Cancelled => out.extend(self.cancel()),
            TurnOutcome::Failed(error) => out.extend(self.fail_classified(&error, subtype)),
        }
    }
    /// One usage update per turn, from the terminal `result`. Context
    /// occupancy counts cached prompt tokens too; the window comes from the
    /// CLI-reported `modelUsage.*.contextWindow` (e.g. 1M for `[1m]` models).
    fn usage_event(&self, frame: &Value) -> Option<AgenticEvent> {
        let tokens = |usage: &Value, key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
        let result_usage = frame.get("usage");
        let prompt_usage = self.last_message_usage.as_ref().or(result_usage)?;
        let cached = tokens(prompt_usage, "cache_read_input_tokens");
        let input = tokens(prompt_usage, "input_tokens")
            + cached
            + tokens(prompt_usage, "cache_creation_input_tokens");
        let output = result_usage
            .map(|usage| tokens(usage, "output_tokens"))
            .unwrap_or_else(|| tokens(prompt_usage, "output_tokens"));
        let max_context = frame
            .get("modelUsage")
            .and_then(Value::as_object)
            .and_then(|models| {
                models
                    .values()
                    .filter_map(|entry| entry.get("contextWindow").and_then(Value::as_u64))
                    .max()
            });
        Some(AgenticEvent::TokenUsageUpdated {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            model_config_id: CLAUDE_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            input_tokens: input as usize,
            output_tokens: Some(output as usize),
            total_tokens: (input + output) as usize,
            max_context_tokens: max_context.map(|value| value as usize),
            is_subagent: false,
            cached_tokens: Some(cached as usize),
            token_details: None,
        })
    }

    /// Open the first round as soon as the user turn starts, matching the core
    /// agent, so the waiting indicator and elapsed timer appear immediately.
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
            model_config_id: CLAUDE_NATIVE_MODEL_CONFIG_ID.to_string(),
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

    fn close_round(&mut self, out: &mut Vec<AgenticEvent>) {
        if self.open_thinking_block.take().is_some() {
            out.extend(self.thinking_event(String::new(), true));
        }
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
            model_config_id: CLAUDE_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            first_chunk_ms: None,
            first_visible_output_ms: None,
            stream_duration_ms: None,
            attempt_count: None,
            failure_category: None,
            token_details: None,
        });
    }

    fn fail_open_tools(&mut self, error: &str, out: &mut Vec<AgenticEvent>) {
        let mut open: Vec<(String, ToolCall)> = self.tools.drain().collect();
        open.sort_by(|a, b| a.0.cmp(&b.0));
        for (tool_id, call) in open {
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
    }

    fn cancel_open_tools(&mut self, out: &mut Vec<AgenticEvent>) {
        let mut open: Vec<(String, ToolCall)> = self.tools.drain().collect();
        open.sort_by(|a, b| a.0.cmp(&b.0));
        for (tool_id, call) in open {
            let event = ToolEventData::Cancelled {
                identity: ToolEventIdentity::direct(tool_id, call.tool_name),
                reason: "Cancelled".to_string(),
                duration_ms: Some(call.started_at.elapsed().as_millis() as u64),
                queue_wait_ms: None,
                preflight_ms: None,
                confirmation_wait_ms: None,
                execution_ms: None,
            };
            out.push(self.tool_event(&call.round_id, event));
        }
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

/// Claude Code tool name -> OpenBitFun card name. Unknown tools keep their own
/// name and render on the generic card.
pub(crate) fn native_tool_name(name: &str) -> String {
    match name {
        "Bash" | "BashOutput" => "ExecCommand",
        "Agent" | "Task" => "Task",
        "MultiEdit" => "Edit",
        "NotebookEdit" => "Edit",
        other => other,
    }
    .to_string()
}

/// Flatten a `tool_result.content` (string or text blocks) to plain text.
fn tool_result_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Build the card-facing result. The CLI attaches a structured
/// `tool_use_result` next to the model-facing text; cards prefer structure.
fn build_tool_result(tool_name: &str, text: &str, structured: Option<&Value>) -> Value {
    let mut object = match structured {
        Some(Value::Object(object)) => object.clone(),
        _ => serde_json::Map::new(),
    };
    if tool_name == "ExecCommand" && !object.contains_key("output") {
        let stdout = object
            .get("stdout")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let stderr = object
            .get("stderr")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let output = match (stdout.is_empty(), stderr.is_empty()) {
            (false, false) => format!("{stdout}\n{stderr}"),
            (false, true) => stdout.to_string(),
            (true, false) => stderr.to_string(),
            (true, true) => text.to_string(),
        };
        object.insert("output".to_string(), Value::String(output));
    }
    object
        .entry("result".to_string())
        .or_insert_with(|| Value::String(text.to_string()));
    Value::Object(object)
}
/// Map Claude API `stop_reason` onto the core turn finish vocabulary. A turn
/// that reached `result` successfully ended normally (`end_turn`, `stop_sequence`,
/// or a trailing tool step); only real truncation is surfaced to the user.
fn turn_finish_reason(stop_reason: Option<&str>) -> &'static str {
    match stop_reason {
        Some("max_tokens") | Some("model_context_window_exceeded") => "interrupted",
        Some("refusal") => "empty_round",
        _ => "complete",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(frames: &[Value]) -> Vec<AgenticEvent> {
        let mut projector = ClaudeTurnProjector::new("s1", "t1", None);
        frames.iter().flat_map(|f| projector.consume(f)).collect()
    }

    fn texts(events: &[AgenticEvent]) -> String {
        events
            .iter()
            .filter_map(|e| match e {
                AgenticEvent::TextChunk { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    fn se(event: Value) -> Value {
        json!({"type": "stream_event", "parent_tool_use_id": null, "event": event})
    }

    /// Mirrors the captured sequence from claude 2.1.302: streamed tool_use,
    /// one assistant frame per block, tool_result in a later user frame,
    /// then a second round with thinking + text.
    #[test]
    fn projects_probe_sequence_without_duplicates() {
        let frames = vec![
            json!({"type": "system", "subtype": "init", "model": "claude-x"}),
            se(json!({"type": "message_start", "message": {"id": "m1"}})),
            se(json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "tool_use", "id": "tu1", "name": "Bash", "input": {}}})),
            se(json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": "{\"command\""}})),
            json!({"type": "assistant", "parent_tool_use_id": null, "message": {"id": "m1",
                "content": [{"type": "tool_use", "id": "tu1", "name": "Bash",
                    "input": {"command": "echo hi"}}]}}),
            se(json!({"type": "content_block_stop", "index": 0})),
            se(json!({"type": "message_stop"})),
            json!({"type": "user", "parent_tool_use_id": null,
                "message": {"content": [{"type": "tool_result", "tool_use_id": "tu1",
                    "content": "hi", "is_error": false}]},
                "tool_use_result": {"stdout": "hi", "stderr": ""}}),
            se(json!({"type": "message_start", "message": {"id": "m2"}})),
            se(json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "thinking", "thinking": ""}})),
            se(json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "signature_delta", "signature": "x"}})),
            json!({"type": "assistant", "parent_tool_use_id": null, "message": {"id": "m2",
                "content": [{"type": "thinking", "thinking": ""}]}}),
            se(json!({"type": "content_block_stop", "index": 0})),
            se(json!({"type": "content_block_start", "index": 1,
                "content_block": {"type": "text", "text": ""}})),
            se(json!({"type": "content_block_delta", "index": 1,
                "delta": {"type": "text_delta", "text": "Hel"}})),
            se(json!({"type": "content_block_delta", "index": 1,
                "delta": {"type": "text_delta", "text": "lo"}})),
            json!({"type": "assistant", "parent_tool_use_id": null, "message": {"id": "m2",
                "content": [{"type": "text", "text": "Hello!"}]}}),
            se(json!({"type": "message_stop"})),
            json!({"type": "result", "subtype": "success", "is_error": false,
                "stop_reason": "end_turn"}),
        ];
        let events = run(&frames);

        assert_eq!(
            texts(&events),
            "Hello!",
            "deltas plus only the uncovered suffix"
        );
        let started = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    AgenticEvent::ToolEvent {
                        tool_event: ToolEventData::Started { .. },
                        ..
                    }
                )
            })
            .count();
        assert_eq!(started, 1);
        let completed = events.iter().find_map(|e| match e {
            AgenticEvent::ToolEvent {
                round_id,
                tool_event:
                    ToolEventData::Completed {
                        identity, result, ..
                    },
                ..
            } => Some((round_id.clone(), identity.tool_name.clone(), result.clone())),
            _ => None,
        });
        let (round_id, name, result) = completed.expect("tool completed");
        assert_eq!(
            round_id, "t1-round-0",
            "result attaches to the issuing round"
        );
        assert_eq!(name, "ExecCommand");
        assert_eq!(result["output"], "hi");
        assert!(matches!(
            events.last(),
            Some(AgenticEvent::DialogTurnCompleted {
                total_rounds: 2,
                total_tools: 1,
                ..
            })
        ));
        // Round 0 must close before round 1 opens.
        let order: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                AgenticEvent::ModelRoundStarted { .. } => Some("start"),
                AgenticEvent::ModelRoundCompleted { .. } => Some("end"),
                _ => None,
            })
            .collect();
        assert_eq!(order, vec!["start", "end", "start", "end"]);
    }

    #[test]
    fn ignores_subagent_sidechain_frames() {
        let frames = vec![
            se(json!({"type": "message_start", "message": {"id": "m1"}})),
            json!({"type": "assistant", "parent_tool_use_id": "tu-task",
                "message": {"id": "child", "content": [{"type": "text", "text": "child says"}]}}),
            json!({"type": "assistant", "parent_tool_use_id": null,
                "message": {"id": "m1", "content": [{"type": "text", "text": "parent"}]}}),
        ];
        assert_eq!(texts(&run(&frames)), "parent");
    }

    #[test]
    fn non_streaming_assistant_frames_open_their_own_round() {
        let frames = vec![
            json!({"type": "assistant", "message": {"id": "m1",
                "content": [{"type": "text", "text": "one"}]}}),
            json!({"type": "result", "subtype": "success", "is_error": false}),
        ];
        let events = run(&frames);
        assert_eq!(texts(&events), "one");
        assert!(matches!(
            events.first(),
            Some(AgenticEvent::ModelRoundStarted { .. })
        ));
    }

    #[test]
    fn cancel_closes_open_tools_and_turn() {
        let mut projector = ClaudeTurnProjector::new("s1", "t1", None);
        projector.consume(&se(
            json!({"type": "message_start", "message": {"id": "m1"}}),
        ));
        projector.consume(&json!({"type": "assistant", "message": {"id": "m1",
            "content": [{"type": "tool_use", "id": "tu1", "name": "Read", "input": {"file_path": "a"}}]}}));
        projector.mark_cancel_requested();
        let events = projector.consume(
            &json!({"type": "result", "subtype": "error_during_execution", "is_error": true}),
        );
        assert!(events.iter().any(|e| matches!(
            e,
            AgenticEvent::ToolEvent {
                tool_event: ToolEventData::Cancelled { .. },
                ..
            }
        )));
        assert!(matches!(
            events.last(),
            Some(AgenticEvent::DialogTurnCancelled { .. })
        ));
        assert!(projector.is_finished());
        assert!(projector
            .consume(&json!({"type": "result", "subtype": "success"}))
            .is_empty());
    }

    #[test]
    fn maps_claude_tool_names_to_native_cards() {
        assert_eq!(native_tool_name("Bash"), "ExecCommand");
        assert_eq!(native_tool_name("Agent"), "Task");
        assert_eq!(native_tool_name("Read"), "Read");
        assert_eq!(native_tool_name("mcp__x__y"), "mcp__x__y");
    }
}
