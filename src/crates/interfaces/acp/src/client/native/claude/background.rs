//! Claude background tasks (background shells, backgrounded subagents, ...)
//! tracked from the stream-json `task_*` and `background_tasks_changed`
//! system frames, which arrive whether or not a turn is active.
use serde_json::Value;

use super::super::NativeBackgroundTask;

/// Finished tasks kept for the UI after they leave the live set.
const FINISHED_RETENTION: usize = 10;

struct Tracked {
    task: NativeBackgroundTask,
    /// Foreground tasks (a blocking Bash or subagent call) are tracked so a
    /// later move to the background can promote them, but are not listed.
    backgrounded: bool,
    process_id: String,
}

#[derive(Default)]
pub(super) struct ClaudeBackgroundTasks {
    tasks: Vec<Tracked>,
}

impl ClaudeBackgroundTasks {
    pub(super) fn snapshot(&self) -> Vec<NativeBackgroundTask> {
        self.tasks
            .iter()
            .filter(|tracked| tracked.backgrounded)
            .map(|tracked| tracked.task.clone())
            .collect()
    }

    pub(super) fn has_running(&self) -> bool {
        self.tasks
            .iter()
            .any(|tracked| tracked.backgrounded && tracked.task.is_running())
    }

    pub(super) fn is_running(&self, task_id: &str) -> bool {
        self.tasks.iter().any(|tracked| {
            tracked.backgrounded && tracked.task.id == task_id && tracked.task.is_running()
        })
    }

    /// Apply one frame; returns true when the listed snapshot changed.
    pub(super) fn consume(&mut self, process_id: &str, frame: &Value, now_ms: u64) -> bool {
        if frame["type"] != "system" {
            return false;
        }
        let before = self.snapshot();
        match frame["subtype"].as_str() {
            Some("task_started") => self.started(process_id, frame, now_ms),
            Some("task_updated") => self.updated(frame, now_ms),
            Some("task_notification") => self.finished(frame, now_ms),
            Some("background_tasks_changed") => self.level(process_id, frame, now_ms),
            _ => return false,
        }
        self.prune();
        self.snapshot() != before
    }

    /// The process that ran these tasks ended; nothing it started survives.
    pub(super) fn process_ended(&mut self, process_id: &str, now_ms: u64) -> bool {
        let mut changed = false;
        for tracked in self
            .tasks
            .iter_mut()
            .filter(|tracked| tracked.process_id == process_id && tracked.task.is_running())
        {
            tracked.task.status = "killed".into();
            tracked.task.ended_at_ms = Some(now_ms);
            changed |= tracked.backgrounded;
        }
        self.prune();
        changed
    }

    /// A listed (backgrounded) task by id, running or finished.
    pub(super) fn backgrounded(&self, task_id: &str) -> Option<&NativeBackgroundTask> {
        self.tasks
            .iter()
            .find(|tracked| tracked.backgrounded && tracked.task.id == task_id)
            .map(|tracked| &tracked.task)
    }

    fn find(&mut self, task_id: &str) -> Option<&mut Tracked> {
        self.tasks
            .iter_mut()
            .find(|tracked| tracked.task.id == task_id)
    }

    fn started(&mut self, process_id: &str, frame: &Value, now_ms: u64) {
        let Some(id) = frame["task_id"].as_str() else {
            return;
        };
        // Ambient tasks are CLI housekeeping, not user work.
        if frame["ambient"] == true {
            return;
        }
        let backgrounded = frame["is_backgrounded"] != false;
        let task = NativeBackgroundTask {
            id: id.to_string(),
            task_type: frame["task_type"].as_str().map(str::to_string),
            subagent_type: frame["subagent_type"].as_str().map(str::to_string),
            description: frame["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            tool_use_id: frame["tool_use_id"].as_str().map(str::to_string),
            status: "running".into(),
            started_at_ms: now_ms,
            ended_at_ms: None,
            summary: None,
            output_file: None,
        };
        match self.find(id) {
            Some(tracked) => {
                // The level frame may precede the bookend; keep its start time.
                tracked.backgrounded |= backgrounded;
                tracked.task.task_type = task.task_type.or(tracked.task.task_type.take());
                tracked.task.subagent_type =
                    task.subagent_type.or(tracked.task.subagent_type.take());
                tracked.task.tool_use_id = task.tool_use_id.or(tracked.task.tool_use_id.take());
                if !task.description.is_empty() {
                    tracked.task.description = task.description;
                }
            }
            None => self.tasks.push(Tracked {
                task,
                backgrounded,
                process_id: process_id.to_string(),
            }),
        }
    }

    fn updated(&mut self, frame: &Value, now_ms: u64) {
        let (Some(id), Some(patch)) = (frame["task_id"].as_str(), frame["patch"].as_object())
        else {
            return;
        };
        let Some(tracked) = self.find(id) else {
            return;
        };
        if patch.get("is_backgrounded") == Some(&Value::Bool(true)) {
            tracked.backgrounded = true;
        }
        if let Some(description) = patch.get("description").and_then(Value::as_str) {
            tracked.task.description = description.to_string();
        }
        if let Some(status) = patch.get("status").and_then(Value::as_str) {
            tracked.task.status = normalize_status(status);
            if !tracked.task.is_running() && tracked.task.ended_at_ms.is_none() {
                tracked.task.ended_at_ms = Some(now_ms);
            }
        }
        if let Some(error) = patch.get("error").and_then(Value::as_str) {
            tracked.task.summary = Some(error.to_string());
        }
    }

    fn finished(&mut self, frame: &Value, now_ms: u64) {
        let Some(id) = frame["task_id"].as_str() else {
            return;
        };
        let Some(tracked) = self.find(id) else {
            return;
        };
        tracked.task.status = normalize_status(frame["status"].as_str().unwrap_or("completed"));
        tracked.task.ended_at_ms.get_or_insert(now_ms);
        if let Some(summary) = frame["summary"].as_str().filter(|s| !s.trim().is_empty()) {
            tracked.task.summary = Some(summary.to_string());
        }
        if let Some(output) = frame["output_file"].as_str().filter(|s| !s.is_empty()) {
            tracked.task.output_file = Some(output.to_string());
        }
    }

    /// REPLACE semantics: the payload is every live background task.
    fn level(&mut self, process_id: &str, frame: &Value, now_ms: u64) {
        let Some(live) = frame["tasks"].as_array() else {
            return;
        };
        let live: Vec<&Value> = live
            .iter()
            .filter(|task| task["task_id"].is_string() && task["ambient"] != true)
            .collect();
        // A missed bookend must not wedge a stale running row.
        for tracked in self.tasks.iter_mut().filter(|tracked| {
            tracked.backgrounded
                && tracked.task.is_running()
                && !live
                    .iter()
                    .any(|task| task["task_id"] == tracked.task.id.as_str())
        }) {
            tracked.task.status = "completed".into();
            tracked.task.ended_at_ms = Some(now_ms);
        }
        for entry in live {
            let id = entry["task_id"].as_str().expect("filtered");
            let description = entry["description"].as_str().unwrap_or_default();
            match self.find(id) {
                Some(tracked) => {
                    tracked.backgrounded = true;
                    if !description.is_empty() {
                        tracked.task.description = description.to_string();
                    }
                    if tracked.task.task_type.is_none() {
                        tracked.task.task_type = entry["task_type"].as_str().map(str::to_string);
                    }
                }
                None => self.tasks.push(Tracked {
                    task: NativeBackgroundTask {
                        id: id.to_string(),
                        task_type: entry["task_type"].as_str().map(str::to_string),
                        subagent_type: entry["subagent_type"].as_str().map(str::to_string),
                        description: description.to_string(),
                        tool_use_id: None,
                        status: "running".into(),
                        started_at_ms: now_ms,
                        ended_at_ms: None,
                        summary: None,
                        output_file: None,
                    },
                    backgrounded: true,
                    process_id: process_id.to_string(),
                }),
            }
        }
    }

    fn prune(&mut self) {
        // Finished foreground tasks were never listed.
        self.tasks
            .retain(|tracked| tracked.backgrounded || tracked.task.is_running());
        let finished = self
            .tasks
            .iter()
            .filter(|tracked| !tracked.task.is_running())
            .count();
        let mut excess = finished.saturating_sub(FINISHED_RETENTION);
        self.tasks.retain(|tracked| {
            if excess > 0 && !tracked.task.is_running() {
                excess -= 1;
                return false;
            }
            true
        });
    }
}

/// Wire statuses: `stopped` (notification) and `killed` (patch) both mean the
/// task was stopped before finishing.
fn normalize_status(status: &str) -> String {
    match status {
        "stopped" => "killed",
        other => other,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn system(subtype: &str, fields: Value) -> Value {
        let mut frame = json!({"type":"system", "subtype":subtype, "uuid":"u", "session_id":"s"});
        frame
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        frame
    }

    #[test]
    fn task_frames_build_background_task_list() {
        let mut tasks = ClaudeBackgroundTasks::default();
        // Level frame first, as the CLI emits it before the bookend.
        assert!(tasks.consume(
            "p1",
            &system(
                "background_tasks_changed",
                json!({"tasks":[
            {"task_id":"b1", "task_type":"local_bash", "description":"npm run dev"}]})
            ),
            10
        ));
        tasks.consume(
            "p1",
            &system(
                "task_started",
                json!({"task_id":"b1", "tool_use_id":"tu1", "description":"npm run dev",
                "task_type":"local_bash", "is_backgrounded":true}),
            ),
            20,
        );
        // A foreground subagent is not listed until it is backgrounded.
        assert!(!tasks.consume(
            "p1",
            &system(
                "task_started",
                json!({"task_id":"a1", "description":"Explore",
                "task_type":"local_agent", "subagent_type":"Explore", "is_backgrounded":false}),
            ),
            30,
        ));
        assert!(tasks.consume(
            "p1",
            &system(
                "task_updated",
                json!({"task_id":"a1", "patch":{"is_backgrounded":true}})
            ),
            40,
        ));
        // Ambient housekeeping is ignored.
        tasks.consume(
            "p1",
            &system(
                "task_started",
                json!({"task_id":"d1", "description":"dream", "ambient":true}),
            ),
            40,
        );
        let listed = tasks.snapshot();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, "b1");
        assert_eq!(listed[0].started_at_ms, 10);
        assert_eq!(listed[0].tool_use_id.as_deref(), Some("tu1"));
        assert_eq!(listed[1].subagent_type.as_deref(), Some("Explore"));
        assert!(tasks.has_running() && tasks.is_running("a1"));

        tasks.consume(
            "p1",
            &system(
                "task_notification",
                json!({"task_id":"b1", "status":"stopped",
                "output_file":"/tmp/b1.out", "summary":"Stopped"}),
            ),
            50,
        );
        tasks.consume(
            "p1",
            &system(
                "task_updated",
                json!({"task_id":"a1", "patch":{"status":"failed", "error":"boom"}}),
            ),
            60,
        );
        let listed = tasks.snapshot();
        assert_eq!(listed[0].status, "killed");
        assert_eq!(listed[0].ended_at_ms, Some(50));
        assert_eq!(listed[0].output_file.as_deref(), Some("/tmp/b1.out"));
        assert_eq!(listed[1].status, "failed");
        assert!(!tasks.has_running());
    }

    #[test]
    fn level_frame_settles_missed_bookends_and_process_exit_kills_its_tasks() {
        let mut tasks = ClaudeBackgroundTasks::default();
        for id in ["b1", "b2"] {
            tasks.consume(
                "p1",
                &system(
                    "task_started",
                    json!({"task_id":id, "description":id, "task_type":"local_bash"}),
                ),
                1,
            );
        }
        tasks.consume(
            "p1",
            &system(
                "background_tasks_changed",
                json!({"tasks":[{"task_id":"b2", "task_type":"local_bash", "description":"b2"}]}),
            ),
            2,
        );
        assert!(!tasks.is_running("b1") && tasks.is_running("b2"));
        assert!(!tasks.process_ended("p0", 3));
        assert!(tasks.process_ended("p1", 3));
        assert!(!tasks.has_running());
        assert_eq!(tasks.snapshot()[1].status, "killed");
    }
}
