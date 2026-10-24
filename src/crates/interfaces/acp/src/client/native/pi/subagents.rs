//! Runs of the `subagent` tool (a Pi user extension) as linked read-only
//! child sessions, like Claude subagents.
//!
//! The extension runs each subagent as a child `pi --mode json` process and
//! reports progress through `tool_execution_update` with a full snapshot in
//! `partialResult.details.results[]` (one `SingleResult` per single, parallel
//! or chain step). Each result's `messages` grows monotonically, so only the
//! messages after the last projected one are replayed into the child
//! projector as the RPC events it already understands. The run is
//! synchronous within the tool call: children settle at the parent's
//! `tool_execution_end` (or with the parent turn), never later.
use std::collections::HashMap;

use openbitfun_core::agentic::core::SessionContinuationPolicy;
use openbitfun_events::AgenticEvent;
use serde_json::{json, Value};

use super::projector::PiTurnProjector;

/// Raw Pi name of the extension tool.
pub(super) const SUBAGENT_TOOL: &str = "subagent";

struct ChildRun {
    projector: PiTurnProjector,
    /// `messages` already projected.
    seen: usize,
}

/// A child session whose hidden record must exist before its events.
#[derive(Debug, Clone)]
pub(super) struct ChildRecord {
    pub(super) session_id: String,
    pub(super) parent_turn_id: String,
    pub(super) parent_tool_call_id: String,
    pub(super) agent: Option<String>,
    pub(super) task: String,
    pub(super) model: Option<String>,
}

#[derive(Default)]
pub(super) struct SubagentUpdate {
    /// Records to persist before `events` are delivered.
    pub(super) records: Vec<ChildRecord>,
    pub(super) events: Vec<AgenticEvent>,
}

/// Child runs of the active parent turn, keyed by (tool call id, result index).
#[derive(Default)]
pub(super) struct PiSubagentRuns {
    runs: HashMap<(String, usize), ChildRun>,
}

pub(super) fn is_subagent_frame(frame: &Value) -> bool {
    matches!(
        frame["type"].as_str(),
        Some("tool_execution_update" | "tool_execution_end")
    ) && frame["toolName"] == SUBAGENT_TOOL
}

impl PiSubagentRuns {
    /// Project one `subagent` update or end frame of the parent turn.
    pub(super) fn consume(
        &mut self,
        parent_session_id: &str,
        parent_turn_id: &str,
        parent_model: Option<&str>,
        frame: &Value,
    ) -> SubagentUpdate {
        let mut update = SubagentUpdate::default();
        let Some(tool_id) = frame["toolCallId"].as_str() else {
            return update;
        };
        let end = frame["type"] == "tool_execution_end";
        let details = if end {
            &frame["result"]["details"]
        } else {
            &frame["partialResult"]["details"]
        };
        let Some(results) = details["results"].as_array() else {
            if end {
                update
                    .events
                    .extend(self.settle_tool(tool_id, frame["isError"] == true));
            }
            return update;
        };
        let chain = details["mode"] == "chain";
        for (index, result) in results.iter().enumerate() {
            let key = (tool_id.to_string(), index);
            if !self.runs.contains_key(&key) {
                let Some(record) =
                    child_record(parent_session_id, parent_turn_id, tool_id, index, result).map(
                        |mut record| {
                            record.model = record.model.or(parent_model.map(str::to_string));
                            record
                        },
                    )
                else {
                    log::debug!("Pi subagent result without a task skipped: index={index}");
                    continue;
                };
                update
                    .events
                    .extend(self.link(&record, key.clone(), parent_session_id));
                update.records.push(record);
            }
            let Some(run) = self.runs.get_mut(&key) else {
                continue;
            };
            let messages = result["messages"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for message in messages.iter().skip(run.seen) {
                for event in replay_frames(message) {
                    update.events.extend(run.projector.consume(&event));
                }
            }
            run.seen = run.seen.max(messages.len());
            // A later chain step means this one has returned.
            let done = end || (chain && index + 1 < results.len());
            if done && !run.projector.is_finished() {
                update.events.extend(settle(&mut run.projector, result));
            }
        }
        if end {
            update
                .events
                .extend(self.settle_tool(tool_id, frame["isError"] == true));
        }
        update
    }

    fn link(
        &mut self,
        record: &ChildRecord,
        key: (String, usize),
        parent_session_id: &str,
    ) -> Vec<AgenticEvent> {
        let turn_id = uuid::Uuid::new_v4().to_string();
        let mut events = vec![
            AgenticEvent::SubagentSessionLinked {
                session_id: record.session_id.clone(),
                subagent_dialog_turn_id: turn_id.clone(),
                parent_session_id: parent_session_id.to_string(),
                parent_dialog_turn_id: record.parent_turn_id.clone(),
                parent_tool_call_id: record.parent_tool_call_id.clone(),
                agent_type: record.agent.clone(),
                model_id: None,
                focused_review_display_label: None,
                continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
            },
            AgenticEvent::DialogTurnStarted {
                session_id: record.session_id.clone(),
                turn_id: turn_id.clone(),
                turn_index: 0,
                user_input: record.task.clone(),
                original_user_input: None,
                user_message_metadata: None,
            },
        ];
        let mut projector =
            PiTurnProjector::new(&record.session_id, &turn_id, record.model.as_deref(), None);
        events.extend(projector.open_pending_round());
        self.runs.insert(key, ChildRun { projector, seen: 0 });
        events
    }

    /// Close the children of one tool call that are still open.
    fn settle_tool(&mut self, tool_id: &str, failed: bool) -> Vec<AgenticEvent> {
        let mut keys: Vec<_> = self
            .runs
            .keys()
            .filter(|(id, _)| id == tool_id)
            .cloned()
            .collect();
        keys.sort();
        let mut events = Vec::new();
        for key in keys {
            if let Some(mut run) = self.runs.remove(&key) {
                if !run.projector.is_finished() {
                    events.extend(if failed {
                        run.projector.fail("Subagent did not complete")
                    } else {
                        run.projector
                            .consume(&json!({"type":"agent_settled", "aborted":false}))
                    });
                }
            }
        }
        events
    }

    /// The parent turn ended (cancelled, failed, or settled without the
    /// tool's end): close every open child.
    pub(super) fn settle_all(&mut self, cancelled: bool) -> Vec<AgenticEvent> {
        let mut runs: Vec<_> = self.runs.drain().collect();
        runs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut events = Vec::new();
        for (_, mut run) in runs {
            events.extend(if cancelled {
                run.projector.cancel()
            } else {
                run.projector
                    .fail("Pi turn ended before the subagent finished")
            });
        }
        events
    }
}

fn child_record(
    parent_session_id: &str,
    parent_turn_id: &str,
    tool_id: &str,
    index: usize,
    result: &Value,
) -> Option<ChildRecord> {
    let task = result["task"].as_str()?.to_string();
    // Path-safe and stable per tool call and result (ids may become file names).
    let safe: String = tool_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    Some(ChildRecord {
        session_id: format!("{parent_session_id}-subagent-{safe}-{index}"),
        parent_turn_id: parent_turn_id.to_string(),
        parent_tool_call_id: tool_id.to_string(),
        agent: result["agent"]
            .as_str()
            .filter(|agent| !agent.is_empty())
            .map(str::to_string),
        task,
        model: result["model"]
            .as_str()
            .filter(|model| !model.is_empty())
            .map(str::to_string),
    })
}

/// Finish a returned child the way the extension judges it.
fn settle(projector: &mut PiTurnProjector, result: &Value) -> Vec<AgenticEvent> {
    let failed = result["exitCode"].as_i64().is_some_and(|code| code > 0)
        || matches!(result["stopReason"].as_str(), Some("error" | "aborted"));
    if !failed {
        return projector.consume(&json!({"type":"agent_settled", "aborted":false}));
    }
    if result["stopReason"] == "aborted" {
        return projector.cancel();
    }
    let error = [&result["errorMessage"], &result["stderr"]]
        .into_iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .find(|text| !text.is_empty())
        .unwrap_or("Subagent failed");
    projector.fail(error)
}

/// RPC events equivalent to one completed child message.
fn replay_frames(message: &Value) -> Vec<Value> {
    match message["role"].as_str() {
        Some("assistant") => {
            let mut frames = vec![
                json!({"type":"turn_start"}),
                json!({"type":"message_start", "message":{"role":"assistant"}}),
            ];
            let blocks = message["content"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for (index, block) in blocks.iter().enumerate() {
                match block["type"].as_str() {
                    Some("text") => frames.push(json!({"type":"message_update",
                        "assistantMessageEvent":{"type":"text_end", "contentIndex":index,
                            "content":block["text"]}})),
                    Some("thinking") => frames.push(json!({"type":"message_update",
                        "assistantMessageEvent":{"type":"thinking_end", "contentIndex":index,
                            "content":block["thinking"]}})),
                    Some("toolCall") => frames.push(json!({"type":"tool_execution_start",
                        "toolCallId":block["id"], "toolName":block["name"],
                        "args":block["arguments"]})),
                    _ => {}
                }
            }
            frames.push(json!({"type":"message_end", "message":message}));
            frames
        }
        Some("toolResult") => vec![json!({"type":"tool_execution_end",
            "toolCallId":message["toolCallId"], "toolName":message["toolName"],
            "result":{"content":message["content"], "details":message["details"]},
            "isError":message["isError"] == true, "durationMs":message["durationMs"]})],
        // The task prompt is the child turn's input already.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(messages: Value, exit_code: i64) -> Value {
        json!({"mode":"single", "results":[{"agent":"scout", "task":"find main",
            "exitCode":exit_code, "messages":messages, "model":"m1"}]})
    }

    fn assistant_tool() -> Value {
        json!({"role":"assistant", "content":[{"type":"text", "text":"Looking."},
            {"type":"toolCall", "id":"c1", "name":"grep", "arguments":{"pattern":"main"}}]})
    }

    fn tool_result() -> Value {
        json!({"role":"toolResult", "toolCallId":"c1", "toolName":"grep",
            "content":[{"type":"text", "text":"src/main.rs"}], "isError":false})
    }

    fn answer() -> Value {
        json!({"role":"assistant", "content":[{"type":"text", "text":"In src/main.rs."}],
            "stopReason":"stop"})
    }

    #[test]
    fn snapshots_project_each_child_message_once_and_settle_at_end() {
        let mut runs = PiSubagentRuns::default();
        let user = json!({"role":"user", "content":"Task: find main"});
        let frames = [
            json!({"type":"tool_execution_update", "toolCallId":"call/1", "toolName":"subagent",
                "partialResult":{"details":single(json!([user, assistant_tool()]), 0)}}),
            json!({"type":"tool_execution_update", "toolCallId":"call/1", "toolName":"subagent",
                "partialResult":{"details":single(json!([user, assistant_tool(), tool_result()]), 0)}}),
            json!({"type":"tool_execution_end", "toolCallId":"call/1", "toolName":"subagent",
                "result":{"details":single(json!([user, assistant_tool(), tool_result(), answer()]), 0)},
                "isError":false}),
        ];
        let mut records = Vec::new();
        let mut events = Vec::new();
        for frame in &frames {
            assert!(is_subagent_frame(frame));
            let update = runs.consume("s1", "parent-turn", Some("pm"), frame);
            records.extend(update.records);
            events.extend(update.events);
        }
        assert_eq!(records.len(), 1);
        let child = "s1-subagent-call_1-0";
        assert_eq!(records[0].session_id, child);
        assert_eq!(records[0].model.as_deref(), Some("m1"));
        let linked = events
            .iter()
            .filter(|e| {
                matches!(e, AgenticEvent::SubagentSessionLinked {
                session_id, parent_tool_call_id, parent_dialog_turn_id, .. }
                if session_id == child && parent_tool_call_id == "call/1"
                    && parent_dialog_turn_id == "parent-turn")
            })
            .count();
        assert_eq!(linked, 1);
        let texts: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                AgenticEvent::TextChunk {
                    session_id, text, ..
                } if session_id == child => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, ["Looking.", "In src/main.rs."]);
        let tool_events = events
            .iter()
            .filter(
                |e| matches!(e, AgenticEvent::ToolEvent { session_id, .. } if session_id == child),
            )
            .count();
        assert_eq!(tool_events, 2, "one start and one completion");
        assert!(matches!(
            events.last(),
            Some(AgenticEvent::DialogTurnCompleted { session_id, total_tools: 1, .. })
                if session_id == child
        ));
        assert!(runs.runs.is_empty());
    }

    #[test]
    fn failed_or_unfinished_children_settle_with_the_parent() {
        let mut runs = PiSubagentRuns::default();
        let parallel = json!({"mode":"parallel", "results":[
            {"agent":"a", "task":"one", "exitCode":-1, "messages":[]},
            {"agent":"b", "task":"two", "exitCode":-1, "messages":[]}]});
        let update = runs.consume(
            "s1",
            "t",
            None,
            &json!({"type":"tool_execution_update", "toolCallId":"c", "toolName":"subagent",
                "partialResult":{"details":parallel}}),
        );
        assert_eq!(update.records.len(), 2);
        let end = json!({"mode":"parallel", "results":[
            {"agent":"a", "task":"one", "exitCode":0, "messages":[answer()]},
            {"agent":"b", "task":"two", "exitCode":1, "messages":[], "stderr":"boom"}]});
        let events = runs
            .consume(
                "s1",
                "t",
                None,
                &json!({"type":"tool_execution_end", "toolCallId":"c", "toolName":"subagent",
                    "result":{"details":end}, "isError":false}),
            )
            .events;
        assert!(events.iter().any(|e| matches!(e,
            AgenticEvent::DialogTurnCompleted { session_id, .. } if session_id == "s1-subagent-c-0")));
        assert!(events.iter().any(|e| matches!(e,
            AgenticEvent::DialogTurnFailed { session_id, .. } if session_id == "s1-subagent-c-1")));

        runs.consume(
            "s1",
            "t",
            None,
            &json!({"type":"tool_execution_update", "toolCallId":"d", "toolName":"subagent",
                "partialResult":{"details":single(json!([]), 0)}}),
        );
        let events = runs.settle_all(true);
        assert!(matches!(
            events.last(),
            Some(AgenticEvent::DialogTurnCancelled { session_id, .. }) if session_id == "s1-subagent-d-0"
        ));
    }
}
