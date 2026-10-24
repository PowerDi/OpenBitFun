//! Codex multi-agent threads (`spawn_agent`) as linked read-only child
//! sessions, and waking the parent when a child finishes while it is idle.
//!
//! Codex runs each spawned agent as its own thread on the same app-server;
//! its notifications carry the child `threadId`. A child can outlive the
//! parent turn. When it finishes during a parent turn, Codex hands the result
//! to that turn; when the parent is idle, Codex only queues the notification
//! in the parent thread and starts nothing. The driver then starts one
//! input-less parent turn (`turn/start` with empty input), which runs on the
//! queued notification. Completions inside one debounce window coalesce.
use std::time::Duration;

use openbitfun_core::agentic::core::SessionContinuationPolicy;
use openbitfun_events::AgenticEvent;
use serde_json::{json, Value};

use super::super::super::NativeTurnInput;
use super::super::super::NATIVE_WAKE_TRIGGER_SOURCE;
use super::*;

/// Wait for sibling completions before waking the parent once.
pub(super) const WAKE_DEBOUNCE: Duration = Duration::from_millis(1500);
/// Frames kept per not-yet-linked thread while a spawn may be in flight.
const ORPHAN_FRAMES: usize = 500;
const ORPHAN_THREADS: usize = 8;

/// One spawned Codex agent thread projected as a child session.
pub(super) struct CodexChild {
    pub(super) session_id: String,
    /// App-server process running the thread.
    process_id: Option<String>,
    prompt: String,
    description: String,
    /// Projector of the child's running turn.
    pub(super) projector: Option<CodexTurnProjector>,
    pub(super) codex_turn_id: Option<String>,
    turn_index: usize,
}

/// A child that finished while the parent was idle.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct WakeReason {
    pub(super) thread_id: String,
    pub(super) description: String,
    pub(super) status: String,
}

#[derive(Default)]
pub(super) struct ChildThreads {
    pub(super) children: std::collections::HashMap<String, CodexChild>,
    /// Frames of threads not linked yet (child events can precede the
    /// parent's `spawnAgent` completion).
    orphans: std::collections::HashMap<String, Vec<(String, Value)>>,
    pub(super) wake_reasons: Vec<WakeReason>,
    pub(super) wake_scheduled: bool,
    /// Set by a user cancel; cleared when the user starts a turn.
    pub(super) wake_suppressed: bool,
}

impl ChildThreads {
    pub(super) fn any_running(&self) -> bool {
        self.children
            .values()
            .any(|child| child.projector.is_some())
    }

    /// The parent turn ended: unlinked frames will never be claimed.
    pub(super) fn drop_orphans(&mut self) {
        self.orphans.clear();
    }
}

/// Visible (English) line and metadata of a wake turn.
pub(super) fn wake_input(reasons: &[WakeReason]) -> (String, Value) {
    let subject = if reasons.len() == 1 {
        "Background agent finished"
    } else {
        "Background agents finished"
    };
    let list = reasons
        .iter()
        .map(|reason| {
            let mut label = reason.description.clone();
            if reason.status != "completed" {
                label.push_str(" (failed)");
            }
            label
        })
        .collect::<Vec<_>>()
        .join("; ");
    let threads: Vec<Value> = reasons
        .iter()
        .map(|reason| {
            json!({"threadId": reason.thread_id, "description": reason.description,
                "status": reason.status})
        })
        .collect();
    (
        format!("{subject}: {list}"),
        json!({
            "triggerSource": NATIVE_WAKE_TRIGGER_SOURCE,
            "nativeWake": {"source": "subagent_thread", "threads": threads},
        }),
    )
}

/// First line of a spawn prompt, shortened, as the child's label.
fn describe(prompt: &str) -> String {
    let line = prompt
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("Subagent");
    let mut text: String = line.trim().chars().take(60).collect();
    if line.trim().chars().count() > 60 {
        text.push_str("...");
    }
    text
}

fn path_safe(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl CodexNativeSession {
    /// A notification of another thread on this app-server.
    pub(super) async fn child_notification(
        &self,
        process: &Arc<RpcProcess>,
        thread: &str,
        method: &str,
        params: &Value,
    ) -> OpenBitFunResult<()> {
        if lock(&self.thread_process).as_deref() != Some(process.id.as_str()) {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if !state.threads.children.contains_key(thread) {
            // Only a running parent turn can still link it.
            if state.projector.is_some() {
                let orphans = &mut state.threads.orphans;
                if orphans.contains_key(thread) || orphans.len() < ORPHAN_THREADS {
                    let frames = orphans.entry(thread.to_string()).or_default();
                    if frames.len() < ORPHAN_FRAMES {
                        frames.push((method.to_string(), params.clone()));
                    }
                }
            }
            return Ok(());
        }
        let finished = self
            .project_child(&mut state, thread, method, params)
            .await?;
        if let Some(status) = finished {
            self.child_finished(&mut state, thread, &status);
        }
        Ok(())
    }

    /// Link the threads a completed `spawnAgent` call created.
    pub(super) async fn link_spawned(
        &self,
        state: &mut TurnState,
        item: &Value,
    ) -> OpenBitFunResult<()> {
        if item["type"] != "collabAgentToolCall" || item["tool"] != "spawnAgent" {
            return Ok(());
        }
        let (Some(tool_id), Some(parent_turn_id)) = (
            item["id"].as_str(),
            state.projector.as_ref().map(|p| p.turn_id().to_string()),
        ) else {
            return Ok(());
        };
        let prompt = item["prompt"].as_str().unwrap_or_default().to_string();
        let model = item["model"]
            .as_str()
            .map(str::to_string)
            .or_else(|| state.model.clone());
        let threads: Vec<String> = item["receiverThreadIds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|id| id.as_str().map(str::to_string))
            .collect();
        for thread in threads {
            if state.threads.children.contains_key(&thread) {
                continue;
            }
            let session_id = format!("{}-subagent-{}", self.session_id, path_safe(&thread));
            let description = describe(&prompt);
            self.persist_child_record(
                &session_id,
                &parent_turn_id,
                tool_id,
                &description,
                model.as_deref(),
            )
            .await;
            self.enqueue(vec![AgenticEvent::SubagentSessionLinked {
                session_id: session_id.clone(),
                subagent_dialog_turn_id: format!("{session_id}-pending"),
                parent_session_id: self.session_id.clone(),
                parent_dialog_turn_id: parent_turn_id.clone(),
                parent_tool_call_id: tool_id.to_string(),
                agent_type: Some("codex".to_string()),
                model_id: None,
                focused_review_display_label: None,
                continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
            }])
            .await?;
            state.threads.children.insert(
                thread.clone(),
                CodexChild {
                    session_id,
                    process_id: state.process_id.clone(),
                    prompt: prompt.clone(),
                    description,
                    projector: None,
                    codex_turn_id: None,
                    turn_index: 0,
                },
            );
            let buffered = state.threads.orphans.remove(&thread).unwrap_or_default();
            for (method, params) in buffered {
                if let Some(status) = self.project_child(state, &thread, &method, &params).await? {
                    self.child_finished(state, &thread, &status);
                }
            }
        }
        Ok(())
    }

    /// Project one child-thread notification; returns the child turn's
    /// final status once it ends.
    async fn project_child(
        &self,
        state: &mut TurnState,
        thread: &str,
        method: &str,
        params: &Value,
    ) -> OpenBitFunResult<Option<String>> {
        let model = state.model.clone();
        let Some(child) = state.threads.children.get_mut(thread) else {
            return Ok(None);
        };
        let notification_turn = params["turnId"]
            .as_str()
            .or_else(|| params["turn"]["id"].as_str())
            .map(str::to_string);
        if method == "turn/started" && child.projector.is_none() {
            let turn_id = uuid::Uuid::new_v4().to_string();
            let input = if child.turn_index == 0 {
                child.prompt.clone()
            } else {
                String::new()
            };
            self.enqueue(vec![AgenticEvent::DialogTurnStarted {
                session_id: child.session_id.clone(),
                turn_id: turn_id.clone(),
                turn_index: child.turn_index,
                user_input: input,
                original_user_input: None,
                user_message_metadata: None,
            }])
            .await?;
            child.turn_index += 1;
            let mut projector =
                CodexTurnProjector::new(&child.session_id, &turn_id, model.as_deref());
            let events = projector.open_pending_round();
            child.projector = Some(projector);
            child.codex_turn_id = notification_turn;
            self.enqueue(events).await?;
            return Ok(None);
        }
        if matches!(method, "turn/plan/updated" | "thread/status/changed") {
            return Ok(None);
        }
        let Some(projector) = child.projector.as_mut() else {
            return Ok(None);
        };
        if let (Some(current), Some(turn)) = (&child.codex_turn_id, &notification_turn) {
            if current != turn {
                return Ok(None);
            }
        }
        let events = projector.consume(method, params);
        let finished = projector.is_finished();
        let child_turn = projector.turn_id().to_string();
        self.enqueue(events).await?;
        if !finished {
            return Ok(None);
        }
        child.projector = None;
        child.codex_turn_id = None;
        self.clear_approvals(&child_turn).await;
        let status = params["turn"]["status"]
            .as_str()
            .unwrap_or("failed")
            .to_string();
        Ok(Some(status))
    }

    /// A child turn ended. During a parent turn Codex delivers the result
    /// there; an idle parent is woken once per debounce window.
    fn child_finished(&self, state: &mut TurnState, thread: &str, status: &str) {
        if status == "interrupted" || state.projector.is_some() || state.threads.wake_suppressed {
            return;
        }
        let Some(child) = state.threads.children.get(thread) else {
            return;
        };
        if state
            .threads
            .wake_reasons
            .iter()
            .any(|reason| reason.thread_id == thread)
        {
            return;
        }
        state.threads.wake_reasons.push(WakeReason {
            thread_id: thread.to_string(),
            description: child.description.clone(),
            status: status.to_string(),
        });
        if state.threads.wake_scheduled {
            return;
        }
        let Some(session) = self.weak_self.upgrade() else {
            return;
        };
        state.threads.wake_scheduled = true;
        tokio::spawn(async move {
            tokio::time::sleep(WAKE_DEBOUNCE).await;
            session.wake_parent().await;
        });
    }

    /// Start the input-less parent turn that runs on Codex's queued
    /// subagent notifications.
    pub(super) async fn wake_parent(self: Arc<Self>) {
        let _guard = self.lifecycle.lock().await;
        let reasons = {
            let mut state = self.state.lock().await;
            state.threads.wake_scheduled = false;
            let reasons = std::mem::take(&mut state.threads.wake_reasons);
            // A user turn already took the notifications in.
            if state.threads.wake_suppressed || state.projector.is_some() {
                return;
            }
            reasons
        };
        if reasons.is_empty() {
            return;
        }
        log::info!(
            "Waking Codex parent after subagent completion: session_id={}, children={}",
            self.session_id,
            reasons.len()
        );
        let (input, metadata) = wake_input(&reasons);
        let turn_id = uuid::Uuid::new_v4().to_string();
        if let Err(error) = self
            .begin_turn_with(turn_id.clone(), input, None, Some(metadata))
            .await
        {
            log::warn!("Codex wake turn could not start: {error}");
            return;
        }
        // Empty input: Codex runs the turn on the queued notifications.
        if let Err(error) = self.submit_turn(&turn_id, &NativeTurnInput::from("")).await {
            self.fail_turn(&error.to_string()).await;
        }
    }

    /// Stop running child turns with the parent (user cancel); best effort.
    pub(super) async fn interrupt_children(&self, process: Option<&RpcProcess>) {
        let running: Vec<(String, String)> = {
            let mut state = self.state.lock().await;
            state.threads.wake_suppressed = true;
            state.threads.wake_reasons.clear();
            state
                .threads
                .children
                .iter()
                .filter_map(|(thread, child)| {
                    child
                        .codex_turn_id
                        .clone()
                        .map(|turn| (thread.clone(), turn))
                })
                .collect()
        };
        let Some(process) = process else {
            return;
        };
        for (thread, turn) in running {
            if let Err(error) = process
                .request(
                    "turn/interrupt",
                    json!({"threadId": thread, "turnId": turn}),
                    INTERRUPT_GRACE,
                )
                .await
            {
                log::debug!("Codex subagent interrupt failed: {error}");
            }
        }
    }

    /// An app-server exited: the child threads it ran are gone.
    pub(super) async fn settle_children(&self, state: &mut TurnState, process_id: &str) {
        let gone: Vec<String> = state
            .threads
            .children
            .iter()
            .filter(|(_, child)| {
                child
                    .process_id
                    .as_deref()
                    .is_none_or(|id| id == process_id)
            })
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in gone {
            let Some(mut child) = state.threads.children.remove(&thread) else {
                continue;
            };
            if let Some(mut projector) = child.projector.take() {
                let events = projector.fail("Codex process ended before the subagent finished");
                if let Err(error) = self.enqueue(events).await {
                    log::error!("Codex subagent settle failed: {error}");
                }
            }
        }
        state.threads.orphans.clear();
        state.threads.wake_reasons.clear();
    }

    async fn persist_child_record(
        &self,
        child_session_id: &str,
        parent_turn_id: &str,
        tool_id: &str,
        description: &str,
        model: Option<&str>,
    ) {
        let Some((persistence, storage)) = &self.persistence else {
            return;
        };
        let result = async {
            let parent = persistence
                .load_metadata(storage, &self.session_id)
                .await?
                .ok_or_else(|| service_error("Codex parent session metadata is missing"))?;
            let relationship = openbitfun_core::service::session::SessionRelationship {
                kind: Some(openbitfun_core::service::session::SessionRelationshipKind::Subagent),
                parent_session_id: Some(self.session_id.clone()),
                parent_request_id: None,
                parent_dialog_turn_id: Some(parent_turn_id.to_string()),
                parent_turn_index: None,
                parent_tool_call_id: Some(tool_id.to_string()),
                subagent_type: Some("codex".to_string()),
                continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
            };
            persistence
                .create_subagent_session_record(
                    storage,
                    child_session_id,
                    &parent,
                    relationship,
                    format!("Subagent: {description}"),
                    model.map(str::to_string),
                )
                .await
        }
        .await;
        if let Err(error) = result {
            log::warn!("Codex subagent record persistence failed: {error}");
        }
    }
}
