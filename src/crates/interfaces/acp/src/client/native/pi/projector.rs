//! Projects Pi RPC session events (`docs/json.md`) onto OpenBitFun `AgenticEvent`s.
//!
//! - one Pi `turn_start`..`turn_end` (assistant response plus its tool calls)
//!   is one OpenBitFun model round;
//! - text/thinking stream from `message_update` deltas; `text_end` /
//!   `thinking_end` only contribute a suffix the deltas did not cover;
//! - tool cards start at `tool_execution_start` and settle at
//!   `tool_execution_end` (falling back to the `toolcall_end` block);
//! - `agent_settled` ends the OpenBitFun turn (`agent_end` does not: retries,
//!   compaction recovery, steering and follow-ups can still continue).
use std::collections::HashMap;

use openbitfun_events::{AgenticEvent, ToolEventData, ToolEventIdentity};
use serde_json::{json, Value};

use crate::client::tool_card_bridge::{normalize_tool_params, normalize_tool_result};

pub(crate) const PI_NATIVE_MODEL_CONFIG_ID: &str = "pi-native";

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
    /// Opened at turn start for the waiting indicator; adopted by `turn_start`.
    placeholder: bool,
}

#[derive(Debug)]
struct Compaction {
    id: String,
    started_at: std::time::Instant,
}

#[derive(Debug)]
pub(crate) struct PiTurnProjector {
    session_id: String,
    turn_id: String,
    model_name: String,
    context_window: Option<u64>,
    round_index: usize,
    open_round: Option<OpenRound>,
    /// contentIndex -> streamed text/thinking of the current assistant message.
    text_blocks: HashMap<u64, String>,
    thinking_blocks: HashMap<u64, String>,
    thinking_open: bool,
    tools: HashMap<String, ToolCall>,
    total_rounds: usize,
    total_tools: usize,
    has_text: bool,
    started_at: std::time::Instant,
    cancel_requested: bool,
    finished: bool,
    /// `errorMessage` of the last assistant message that ended with `error`.
    assistant_error: Option<String>,
    /// `auto_retry_end.finalError` when retries were exhausted.
    retry_error: Option<String>,
    compaction: Option<Compaction>,
    compression_count: usize,
    /// Number of user messages Pi echoed; the first is the turn's own prompt.
    user_messages_seen: usize,
}

impl PiTurnProjector {
    pub(crate) fn new(
        session_id: &str,
        turn_id: &str,
        model_name: Option<&str>,
        context_window: Option<u64>,
    ) -> Self {
        Self {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            model_name: model_name.unwrap_or("pi").to_string(),
            context_window,
            round_index: 0,
            open_round: None,
            text_blocks: HashMap::new(),
            thinking_blocks: HashMap::new(),
            thinking_open: false,
            tools: HashMap::new(),
            total_rounds: 0,
            total_tools: 0,
            has_text: false,
            started_at: std::time::Instant::now(),
            cancel_requested: false,
            finished: false,
            assistant_error: None,
            retry_error: None,
            compaction: None,
            compression_count: 0,
            user_messages_seen: 0,
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

    /// True when `frame` is a user message after the turn's own prompt, i.e.
    /// a steering or follow-up message Pi took in.
    pub(crate) fn is_injected_user_message(&mut self, frame: &Value) -> bool {
        if frame["type"] != "message_start" || frame["message"]["role"] != "user" {
            return false;
        }
        self.user_messages_seen += 1;
        self.user_messages_seen > 1
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

    pub(crate) fn consume(&mut self, frame: &Value) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        match frame["type"].as_str() {
            Some("turn_start") => self.start_round(&mut out),
            Some("message_start") if frame["message"]["role"] == "assistant" => {
                self.text_blocks.clear();
                self.thinking_blocks.clear();
                if self.open_round.is_none() {
                    self.start_round(&mut out);
                }
            }
            Some("message_update") => {
                self.consume_update(&frame["assistantMessageEvent"], &mut out)
            }
            Some("message_end") if frame["message"]["role"] == "assistant" => {
                self.consume_assistant_end(&frame["message"], &mut out)
            }
            Some("turn_end") => {
                // Results of every tool call arrive before `turn_end`.
                self.close_round(&mut out);
            }
            Some("tool_execution_start") => self.start_tool(
                frame["toolCallId"].as_str(),
                frame["toolName"].as_str(),
                frame.get("args"),
                &mut out,
            ),
            Some("tool_execution_end") => self.finish_tool(frame, &mut out),
            Some("compaction_start") => self.start_compaction(frame, &mut out),
            Some("compaction_end") => self.end_compaction(frame, &mut out),
            Some("auto_retry_end") if frame["success"] == false => {
                self.retry_error = frame["finalError"].as_str().map(str::to_string);
            }
            Some("auto_retry_end") => {
                self.assistant_error = None;
                self.retry_error = None;
            }
            Some("agent_settled") => self.settle(frame["aborted"] == true, &mut out),
            _ => {}
        }
        out
    }

    fn consume_update(&mut self, event: &Value, out: &mut Vec<AgenticEvent>) {
        let index = event["contentIndex"].as_u64().unwrap_or(0);
        match event["type"].as_str() {
            Some("text_delta") => {
                if let Some(delta) = event["delta"].as_str().filter(|d| !d.is_empty()) {
                    self.close_thinking(out);
                    self.text_blocks.entry(index).or_default().push_str(delta);
                    out.extend(self.text_event(delta.to_string()));
                }
            }
            Some("text_end") => {
                let streamed = self.text_blocks.remove(&index).unwrap_or_default();
                if let Some(suffix) = uncovered_suffix(&streamed, event["content"].as_str()) {
                    self.close_thinking(out);
                    out.extend(self.text_event(suffix));
                }
            }
            Some("thinking_delta") => {
                if let Some(delta) = event["delta"].as_str().filter(|d| !d.is_empty()) {
                    self.thinking_blocks
                        .entry(index)
                        .or_default()
                        .push_str(delta);
                    self.thinking_open = true;
                    out.extend(self.thinking_event(delta.to_string(), false));
                }
            }
            Some("thinking_end") => {
                let streamed = self.thinking_blocks.remove(&index).unwrap_or_default();
                if let Some(suffix) = uncovered_suffix(&streamed, event["content"].as_str()) {
                    out.extend(self.thinking_event(suffix, false));
                    self.thinking_open = true;
                }
                self.close_thinking(out);
            }
            _ => {}
        }
    }

    fn consume_assistant_end(&mut self, message: &Value, out: &mut Vec<AgenticEvent>) {
        self.close_thinking(out);
        if let Some(model) = message["model"].as_str().filter(|m| !m.is_empty()) {
            self.model_name = model.to_string();
        }
        match message["stopReason"].as_str() {
            Some("error") => {
                self.assistant_error = Some(
                    message["errorMessage"]
                        .as_str()
                        .filter(|text| !text.trim().is_empty())
                        .unwrap_or("Pi model request failed")
                        .to_string(),
                );
            }
            Some("aborted") => {}
            _ => self.assistant_error = None,
        }
        if let Some(event) = self.usage_event(&message["usage"]) {
            out.push(event);
        }
    }

    /// Context occupancy of the latest response: prompt tokens including cache.
    fn usage_event(&self, usage: &Value) -> Option<AgenticEvent> {
        let tokens = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
        if !usage.is_object() {
            return None;
        }
        let cached = tokens("cacheRead");
        let input = tokens("input") + cached + tokens("cacheWrite");
        let output = tokens("output");
        if input == 0 && output == 0 {
            return None;
        }
        Some(AgenticEvent::TokenUsageUpdated {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            model_config_id: PI_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            input_tokens: input as usize,
            output_tokens: Some(output as usize),
            total_tokens: (input + output) as usize,
            max_context_tokens: self.context_window.map(|value| value as usize),
            is_subagent: false,
            cached_tokens: Some(cached as usize),
            token_details: None,
        })
    }

    /// Show the card a permission request is about. Pi starts the tool
    /// before its `tool_call` hook asks, so this is usually a no-op; it covers
    /// a request that arrives first.
    pub(crate) fn permission_tool(
        &mut self,
        tool_id: &str,
        name: &str,
        args: &Value,
    ) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if !self.finished {
            self.start_tool(Some(tool_id), Some(name), Some(args), &mut out);
        }
        out
    }

    fn start_tool(
        &mut self,
        tool_id: Option<&str>,
        name: Option<&str>,
        args: Option<&Value>,
        out: &mut Vec<AgenticEvent>,
    ) {
        let Some(tool_id) = tool_id else {
            return;
        };
        if self.tools.contains_key(tool_id) {
            return;
        }
        if self.open_round.is_none() {
            self.start_round(out);
        }
        let tool_name = native_tool_name(name.unwrap_or("tool"));
        let params = normalize_tool_params(
            &tool_name,
            card_params(&tool_name, args.cloned().unwrap_or_else(|| json!({}))),
        );
        let round = self.open_round.as_mut().expect("open round");
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

    fn finish_tool(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        let Some(tool_id) = frame["toolCallId"].as_str() else {
            return;
        };
        if !self.tools.contains_key(tool_id) {
            // A tool that never reported a start (e.g. blocked before running).
            self.start_tool(
                Some(tool_id),
                frame["toolName"].as_str(),
                frame.get("args"),
                out,
            );
        }
        let Some(call) = self.tools.remove(tool_id) else {
            return;
        };
        let result = &frame["result"];
        let text = content_text(result.get("content"));
        let duration_ms = frame["durationMs"]
            .as_u64()
            .unwrap_or_else(|| call.started_at.elapsed().as_millis() as u64);
        let identity = ToolEventIdentity::direct(tool_id, call.tool_name.clone());
        let event = if frame["isError"] == true {
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
            let card = card_result(&call.tool_name, &text, result.get("details"));
            ToolEventData::Completed {
                identity,
                result: normalize_tool_result(&call.tool_name, card),
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

    /// Close a manual `compact` turn from the command's response `data`. The
    /// `compaction_end` event normally arrives first; if it did not, the
    /// response carries the same result.
    pub(crate) fn settle_manual_compaction(&mut self, data: &Value) -> Vec<AgenticEvent> {
        let mut out = Vec::new();
        if self.compaction.is_some() || (self.compression_count == 0 && data.is_object()) {
            self.end_compaction(
                &json!({"type":"compaction_end", "reason":"manual", "result": data}),
                &mut out,
            );
        }
        out.extend(self.complete());
        out
    }

    fn start_compaction(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        if self.compaction.is_some() {
            return;
        }
        let id = uuid::Uuid::new_v4().to_string();
        let trigger = match frame["reason"].as_str() {
            Some("manual") => "manual",
            _ => "auto",
        };
        out.push(AgenticEvent::ContextCompressionStarted {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
            compression_id: id.clone(),
            trigger: trigger.to_string(),
            tokens_before: 0,
            context_window: self.context_window.unwrap_or(0) as usize,
        });
        self.compaction = Some(Compaction {
            id,
            started_at: std::time::Instant::now(),
        });
    }

    fn end_compaction(&mut self, frame: &Value, out: &mut Vec<AgenticEvent>) {
        if self.compaction.is_none() {
            self.start_compaction(frame, out);
        }
        let compaction = self.compaction.take().expect("started compaction");
        let result = &frame["result"];
        if !result.is_object() {
            let error = if frame["aborted"] == true {
                "Pi context compaction was cancelled".to_string()
            } else {
                frame["errorMessage"]
                    .as_str()
                    .unwrap_or("Pi context compaction failed")
                    .to_string()
            };
            out.push(AgenticEvent::ContextCompressionFailed {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id: compaction.id,
                error,
            });
            return;
        }
        self.compression_count += 1;
        let tokens_before = result["tokensBefore"].as_u64().unwrap_or(0) as usize;
        let tokens_after = result["estimatedTokensAfter"]
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
            has_summary: result["summary"].as_str().is_some_and(|s| !s.is_empty()),
            summary_source: "model".into(),
            applied: true,
        });
    }

    /// `agent_settled`: Pi will not continue automatically.
    fn settle(&mut self, aborted: bool, out: &mut Vec<AgenticEvent>) {
        if aborted || self.cancel_requested {
            out.extend(self.cancel());
        } else if let Some(error) = self
            .retry_error
            .clone()
            .or_else(|| self.assistant_error.clone())
        {
            out.extend(self.fail(&error));
        } else {
            out.extend(self.complete());
        }
    }

    /// Close a turn that ended without a run, e.g. a manual `compact`.
    pub(crate) fn complete(&mut self) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.fail_compaction("Pi turn ended before compaction completion", &mut out);
        self.fail_open_tools("Tool did not report a result", &mut out);
        self.close_round(&mut out);
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
        out
    }

    pub(crate) fn fail(&mut self, error: &str) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        if self.cancel_requested {
            return self.cancel();
        }
        let failure = super::errors::classify(error);
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

    pub(crate) fn cancel(&mut self) -> Vec<AgenticEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.fail_compaction("Pi context compaction was cancelled", &mut out);
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
        self.close_round(&mut out);
        out.push(AgenticEvent::DialogTurnCancelled {
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
        });
        self.finished = true;
        out
    }

    fn fail_compaction(&mut self, error: &str, out: &mut Vec<AgenticEvent>) {
        if let Some(compaction) = self.compaction.take() {
            out.push(AgenticEvent::ContextCompressionFailed {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                compression_id: compaction.id,
                error: error.to_string(),
            });
        }
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
            model_config_id: PI_NATIVE_MODEL_CONFIG_ID.to_string(),
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
            model_config_id: PI_NATIVE_MODEL_CONFIG_ID.to_string(),
            effective_model_name: self.model_name.clone(),
            first_chunk_ms: None,
            first_visible_output_ms: None,
            stream_duration_ms: None,
            attempt_count: None,
            failure_category: None,
            token_details: None,
        });
    }

    fn close_thinking(&mut self, out: &mut Vec<AgenticEvent>) {
        if std::mem::take(&mut self.thinking_open) {
            out.extend(self.thinking_event(String::new(), true));
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

/// The part of `content` the deltas did not stream, if any.
fn uncovered_suffix(streamed: &str, content: Option<&str>) -> Option<String> {
    let content = content?;
    let suffix = content
        .strip_prefix(streamed)
        .unwrap_or(if streamed.is_empty() { content } else { "" });
    (!suffix.is_empty()).then(|| suffix.to_string())
}

/// Pi built-in tool name -> OpenBitFun card name. Unknown (extension/MCP)
/// tools keep their own name and render on the generic card.
pub(crate) fn native_tool_name(name: &str) -> String {
    match name {
        "bash" => "ExecCommand",
        "read" => "Read",
        "edit" => "Edit",
        "write" => "Write",
        "grep" => "Grep",
        "find" => "Glob",
        "ls" => "LS",
        other => other,
    }
    .to_string()
}

/// Card name and normalized card params of a Pi tool call.
pub(crate) fn card_input(name: &str, args: &Value) -> (String, Value) {
    let tool_name = native_tool_name(name);
    let params = normalize_tool_params(&tool_name, card_params(&tool_name, args.clone()));
    (tool_name, params)
}

/// Pi argument shapes -> the shapes the shared cards read. A single-entry
/// `edits[]` becomes `old_string`/`new_string` so the Edit card shows a diff.
fn card_params(tool_name: &str, args: Value) -> Value {
    let Value::Object(mut object) = args else {
        return args;
    };
    if tool_name == "Edit" && !object.contains_key("old_string") {
        let single = object
            .get("edits")
            .and_then(Value::as_array)
            .filter(|edits| edits.len() == 1)
            .map(|edits| edits[0].clone());
        if let Some(edit) = single {
            if let Some(old) = edit.get("oldText") {
                object.insert("old_string".into(), old.clone());
            }
            if let Some(new) = edit.get("newText") {
                object.insert("new_string".into(), new.clone());
            }
        }
    }
    Value::Object(object)
}

fn content_text(content: Option<&Value>) -> String {
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

fn card_result(tool_name: &str, text: &str, details: Option<&Value>) -> Value {
    let mut object = match details {
        Some(Value::Object(object)) => object.clone(),
        _ => serde_json::Map::new(),
    };
    if tool_name == "ExecCommand" && !object.contains_key("output") {
        object.insert("output".into(), Value::String(text.to_string()));
    }
    object
        .entry("result".to_string())
        .or_insert_with(|| Value::String(text.to_string()));
    Value::Object(object)
}

#[cfg(test)]
#[path = "projector_tests.rs"]
mod tests;
