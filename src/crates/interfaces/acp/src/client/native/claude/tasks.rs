//! Session-scoped Claude todos. Observe complete tool calls/results without
//! changing their normal transcript projection.
use std::collections::HashMap;

use serde_json::Value;

use crate::client::AcpPlanEntry;

#[derive(Default)]
pub(super) struct ClaudeTasks {
    tasks: Vec<(String, AcpPlanEntry)>,
    pending: HashMap<String, (String, Value)>,
}

impl ClaudeTasks {
    pub(super) fn entries(&self) -> Vec<AcpPlanEntry> {
        self.tasks.iter().map(|(_, entry)| entry.clone()).collect()
    }

    pub(super) fn clear_pending(&mut self) {
        self.pending.clear();
    }

    pub(super) fn consume(&mut self, frame: &Value) -> Option<Vec<AcpPlanEntry>> {
        let blocks = frame.pointer("/message/content")?.as_array()?;
        if frame["type"] == "assistant" {
            for block in blocks.iter().filter(|block| block["type"] == "tool_use") {
                let (Some(id), Some(name)) = (block["id"].as_str(), block["name"].as_str()) else {
                    continue;
                };
                if matches!(name, "TodoWrite" | "TaskCreate" | "TaskUpdate" | "TaskList") {
                    self.pending
                        .entry(id.to_string())
                        .or_insert_with(|| (name.to_string(), block["input"].clone()));
                }
            }
            return None;
        }
        if frame["type"] != "user" {
            return None;
        }
        let results: Vec<_> = blocks
            .iter()
            .filter(|block| block["type"] == "tool_result")
            .collect();
        let before = self.entries();
        for block in &results {
            let Some(id) = block["tool_use_id"].as_str() else {
                continue;
            };
            let Some((name, input)) = self.pending.remove(id) else {
                continue;
            };
            if block["is_error"] == true {
                continue;
            }
            // A frame-wide structured result is unambiguous only for one tool.
            let structured = (results.len() == 1)
                .then(|| {
                    frame
                        .get("tool_use_result")
                        .or_else(|| frame.get("toolUseResult"))
                })
                .flatten()
                .filter(|value| !value.is_null());
            let parsed = parse_result(&block["content"]);
            let result = structured.or(parsed.as_ref()).unwrap_or(&Value::Null);
            self.apply(&name, &input, result);
        }
        let after = self.entries();
        (before != after).then_some(after)
    }

    fn apply(&mut self, name: &str, input: &Value, result: &Value) {
        match name {
            "TodoWrite" => {
                if let Some(todos) = input["todos"].as_array() {
                    self.tasks = todos
                        .iter()
                        .enumerate()
                        .filter_map(|(index, todo)| {
                            Some((
                                index.to_string(),
                                entry(text(todo, &["content"])?, status(todo)?.to_string()),
                            ))
                        })
                        .collect();
                }
            }
            "TaskCreate" => {
                let task = &result["task"];
                if let (Some(id), Some(subject)) = (
                    text(task, &["id"]),
                    text(input, &["subject"]).or_else(|| text(task, &["subject"])),
                ) {
                    self.upsert(id, entry(subject, "pending".into()));
                }
            }
            "TaskUpdate" => {
                let Some(id) = text(input, &["taskId", "id", "task_id"]) else {
                    return;
                };
                if input["status"] == "deleted" {
                    self.tasks.retain(|(key, _)| key != &id);
                    return;
                }
                let existing = self
                    .tasks
                    .iter()
                    .find(|(key, _)| key == &id)
                    .map(|(_, entry)| entry);
                let Some(subject) = text(input, &["subject"])
                    .or_else(|| existing.map(|entry| entry.content.clone()))
                else {
                    return;
                };
                let state = status(input)
                    .map(str::to_string)
                    .or_else(|| existing.map(|entry| entry.status.clone()))
                    .unwrap_or_else(|| "pending".into());
                self.upsert(id, entry(subject, state));
            }
            "TaskList" => {
                if let Some(tasks) = result["tasks"].as_array() {
                    self.tasks.clear();
                    for task in tasks {
                        if let (Some(id), Some(subject), Some(state)) =
                            (text(task, &["id"]), text(task, &["subject"]), status(task))
                        {
                            self.upsert(id, entry(subject, state.into()));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn upsert(&mut self, id: String, entry: AcpPlanEntry) {
        if let Some((_, existing)) = self.tasks.iter_mut().find(|(key, _)| key == &id) {
            *existing = entry;
        } else {
            self.tasks.push((id, entry));
        }
    }
}

fn text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value[*key]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn status(value: &Value) -> Option<&str> {
    value["status"]
        .as_str()
        .filter(|status| matches!(*status, "pending" | "in_progress" | "completed"))
}

fn entry(content: String, status: String) -> AcpPlanEntry {
    AcpPlanEntry {
        content,
        priority: "medium".into(),
        status,
    }
}

fn parse_result(content: &Value) -> Option<Value> {
    if let Some(text) = content.as_str() {
        return serde_json::from_str(text).ok();
    }
    let text = content
        .as_array()?
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    serde_json::from_str(&text).ok()
}
