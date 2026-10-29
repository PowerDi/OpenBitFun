//! Long-lived Claude Code SDK stdio transport, separate from ACP JSON-RPC.
//!
//! Like codexhost's SDK transport and paseo's query pump, process lifetime is
//! session-scoped, control replies are independent of turn output, and a result
//! settles a segment without closing the input stream or background workers.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use openbitfun_core::agentic::core::SessionContinuationPolicy;
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::infrastructure::events::{emit_global_event, BackendEvent};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::AgenticEvent;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{oneshot, Mutex, Notify};

use super::super::preferences::NativeAgentPreferences;
use super::super::process::{Reader, Writer};
use super::super::title::{self, TitleResolver};
use super::super::{
    NativeAgentDriver, NativeAgentNotification, NativeAgentNotificationSink, NativeBackgroundTask,
    NativeRollbackOutcome, NativeTurnInput,
};
use super::background::ClaudeBackgroundTasks;
use super::history::{resume_args, ClaudeHistory, PendingResume, RollbackPlan, TurnAnchor};
use super::projector::{native_tool_name, ClaudeTurnProjector};
use super::tasks::ClaudeTasks;
use crate::client::remote_shell::{remote_user_shell_command, shell_escape};
use crate::client::session_persistence::AcpSessionPersistence;
use crate::client::{
    AcpSessionConfigKind, AcpSessionConfigOption, AcpSessionConfigSelectOption,
    AcpSessionConfigValue, AcpSessionContextUsage, AcpSessionModelOption, AcpSessionOptions,
    SubmitAcpPermissionResponseRequest,
};

pub const CLIENT_ID: &str = "claude-native";
/// Follow the Claude CLI's own configured model (no `--model` flag).
pub const DEFAULT_MODEL_ID: &str = "default";
/// `triggerSource` of a turn the CLI started after a background task
/// finished (not typed by the user; the UI disables editing it).
pub(crate) use super::super::NATIVE_WAKE_TRIGGER_SOURCE as WAKE_TRIGGER_SOURCE;
const PERMISSION_MODE_CONFIG_ID: &str = "permission-mode";
const THINKING_CONFIG_ID: &str = "thinking";
/// Claude Code permission modes (same catalog as codexhost). `auto` is only
/// offered when a model reports `supportsAutoMode`.
const PERMISSION_MODES: &[(&str, &str, &str)] = &[
    ("default", "默认", "编辑和其他受保护操作前询问。"),
    (
        "acceptEdits",
        "接受编辑",
        "允许文件编辑；其他受保护操作前询问。",
    ),
    ("plan", "规划模式", "只探索并制定计划，不修改文件。"),
    ("auto", "自动模式", "由 Claude 判断权限请求。"),
    (
        "bypassPermissions",
        "绕过权限",
        "跳过 Claude Code 权限检查。",
    ),
];
/// Thinking options: `auto` leaves thinking to Claude Code settings and the
/// model default.
const THINKING_LEVELS: &[(&str, &str)] = &[
    ("off", "Off"),
    ("auto", "Auto"),
    ("low", "Low"),
    ("medium", "Medium"),
    ("high", "High"),
    ("xhigh", "Extra High"),
    ("max", "Max"),
];
const INTERRUPT_GRACE: Duration = Duration::from_secs(3);
/// Stop an idle CLI process (no turn, permission or running background task)
/// after this long; the next turn resumes it with `--resume`.
const IDLE_RELEASE_AFTER: Duration = Duration::from_secs(30 * 60);
/// What the Agent SDK's `enableFileCheckpointing` sets; enables `rewind_files`.
const FILE_CHECKPOINT_ENV: &str = "CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING";
/// Permission option ids beyond the ACP-standard allow_once/allow_always/reject_once.
const ALLOW_SESSION_OPTION: &str = "allow_session";
const APPROVE_PLAN_OPTION: &str = "approve_plan";
const KEEP_PLANNING_OPTION: &str = "keep_planning";
/// Deny text codexhost sends when the user keeps Claude in plan mode.
const KEEP_PLANNING_MESSAGE: &str = "User chose to stay in plan mode. Do not begin implementation.";

struct NativeProcess {
    id: String,
    writer: Mutex<Writer>,
    controls: DashMap<String, oneshot::Sender<Value>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    alive: AtomicBool,
    terminated: Mutex<Option<oneshot::Receiver<()>>>,
    exit: tokio::sync::watch::Receiver<Option<String>>,
    stderr: Mutex<VecDeque<String>>,
}

impl NativeProcess {
    async fn write(&self, frame: &Value) -> OpenBitFunResult<()> {
        super::super::process::write_json_line(
            &self.writer,
            frame,
            INTERRUPT_GRACE,
            "Claude stdin write timed out",
        )
        .await
    }

    async fn request(&self, request: Value, timeout: Duration) -> OpenBitFunResult<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.controls.insert(id.clone(), tx);
        let result = async {
            self.write(&json!({"type":"control_request", "request_id":id, "request":request}))
                .await?;
            let response = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| service_error("Claude control request timed out"))?
                .map_err(|_| service_error("Claude process exited during control request"))?;
            if response["subtype"] != "success" {
                return Err(service_error(format!(
                    "Claude control request failed: {}",
                    response["error"]
                )));
            }
            Ok(response["response"].clone())
        }
        .await;
        self.controls.remove(&id);
        result
    }

    async fn kill(&self) {
        self.alive.store(false, Ordering::Release);
        self.controls.clear();
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

/// How a `can_use_tool` request is presented and answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionKind {
    /// Generic tool approval with optional session/always scopes.
    Approval,
    /// `AskUserQuestion`: answered with `updatedInput.answers`.
    Question,
    /// `ExitPlanMode`: approve the plan or keep planning.
    PlanReview,
}

impl PermissionKind {
    fn of(tool_name: &str) -> Self {
        match tool_name {
            "AskUserQuestion" => Self::Question,
            "ExitPlanMode" => Self::PlanReview,
            _ => Self::Approval,
        }
    }
}

struct PendingPermission {
    process: Arc<NativeProcess>,
    turn_id: String,
    request_id: String,
    tool_id: String,
    input: Value,
    suggestions: Value,
    kind: PermissionKind,
}

#[derive(Default)]
struct TurnState {
    projector: Option<ClaudeTurnProjector>,
    autonomous: bool,
    index: usize,
    model: Option<String>,
    autonomous_result_pending: bool,
    streaming: bool,
    process_id: Option<String>,
    /// Subagent transcripts keyed by the parent `Agent`/`Task` tool_use id.
    children: std::collections::HashMap<String, ChildTranscript>,
    /// Claude `agent_id` (= task_id) to the `Agent` tool_use id that spawned
    /// it. A resumed agent keeps that id on its sidechain frames, while the
    /// resuming `SendMessage` gets its own task frames; both map back here.
    agent_children: std::collections::HashMap<String, String>,
    /// Later tool_use ids (`SendMessage`) aliased to the spawning tool_use id.
    child_aliases: std::collections::HashMap<String, String>,
    /// Next child turn index per spawning tool_use id, so a resumed agent
    /// appends a turn instead of overwriting turn 0.
    child_turns: std::collections::HashMap<String, usize>,
    tasks: ClaudeTasks,
    /// Steering messages written to stdin and not yet taken in by Claude.
    steers: Vec<PendingSteer>,
    /// Finished background tasks whose `<task-notification>` the CLI queued
    /// for the main agent. When the CLI runs one between turns, the next
    /// autonomous turn is labelled with it (oldest first).
    wake_reasons: VecDeque<WakeReason>,
}

/// Why Claude Code woke the main agent between turns: a backgrounded task
/// (subagent or shell) finished. The CLI itself injects the notification and
/// starts the turn; the driver only labels it.
#[derive(Debug, Clone, PartialEq)]
struct WakeReason {
    task_id: String,
    tool_use_id: Option<String>,
    task_type: Option<String>,
    subagent_type: Option<String>,
    description: String,
    status: String,
    summary: Option<String>,
}

impl WakeReason {
    fn is_agent(&self) -> bool {
        self.task_type.as_deref() == Some("local_agent") || self.subagent_type.is_some()
    }

    /// Visible (English) prompt line of the woken turn.
    fn display_input(&self) -> String {
        let subject = if self.is_agent() {
            "Background agent"
        } else {
            "Background task"
        };
        let verb = if self.status == "completed" {
            "finished"
        } else {
            "failed"
        };
        let mut text = format!("{subject} {verb}");
        if !self.description.trim().is_empty() {
            text.push_str(": ");
            text.push_str(self.description.trim());
        }
        text
    }

    fn metadata(&self) -> Value {
        json!({
            "triggerSource": WAKE_TRIGGER_SOURCE,
            "nativeWake": {
                "source": "background_task",
                "taskId": self.task_id,
                "toolUseId": self.tool_use_id,
                "taskType": self.task_type,
                "subagentType": self.subagent_type,
                "description": self.description,
                "status": self.status,
                "summary": self.summary,
            },
        })
    }
}

/// A mid-turn user message queued in the CLI with `priority:"next"`. Claude
/// folds it into the running turn between tool rounds; if the turn ends
/// first, the CLI runs it as its own turn.
#[derive(Debug, Clone)]
struct PendingSteer {
    uuid: String,
    text: String,
    display: Option<String>,
    process_id: String,
}

/// One Claude subagent projected as a read-only child session. Claude Code
/// has no user-to-subagent input channel, so the child is `FreshOnly`.
struct ChildTranscript {
    session_id: String,
    projector: ClaudeTurnProjector,
    /// Claude `agent_id` (= task_id); subagent permission requests carry it.
    agent_id: Option<String>,
}

pub struct ClaudeNativeSession {
    session_id: String,
    /// Replaced only when rolling back the first turn starts a fresh transcript.
    claude_session_id: std::sync::Mutex<String>,
    workspace_path: String,
    remote_connection_id: Option<String>,
    resume: AtomicBool,
    process: Mutex<Option<Arc<NativeProcess>>>,
    /// Serialize launch, submission and cancellation; output never takes this lock.
    lifecycle: Mutex<()>,
    state: Mutex<TurnState>,
    permissions: DashMap<String, PendingPermission>,
    settled: Notify,
    queue: Arc<EventQueue>,
    persistence: Option<(Arc<AcpSessionPersistence>, PathBuf)>,
    selected_model: std::sync::Mutex<String>,
    permission_mode: std::sync::Mutex<String>,
    thinking: std::sync::Mutex<String>,
    /// Models reported by the SDK `initialize` response (`response.models`).
    sdk_models: std::sync::Mutex<Vec<Value>>,
    commands: std::sync::Mutex<Vec<crate::client::AcpAvailableCommand>>,
    notifier: Option<NativeAgentNotificationSink>,
    /// Mode active before Claude entered plan mode; restored on plan approval.
    pre_plan_mode: std::sync::Mutex<Option<String>>,
    /// Latest context occupancy and cumulative cost, replayed via `options()`.
    context_usage: std::sync::Mutex<Option<AcpSessionContextUsage>>,
    /// Claude transcript anchors per turn and the pending branch launch.
    history: std::sync::Mutex<ClaudeHistory>,
    /// Background shells/subagents; they outlive turns but not the process.
    background: std::sync::Mutex<ClaudeBackgroundTasks>,
    /// Last process output or session request; drives the idle release.
    last_activity: std::sync::Mutex<tokio::time::Instant>,
    /// First-turn title generation; replaced in tests.
    title_resolver: std::sync::Mutex<TitleResolver>,
}

impl ClaudeNativeSession {
    pub(in crate::client) fn new(
        session_id: String,
        claude_session_id: String,
        workspace_path: String,
        remote_connection_id: Option<String>,
        resume: bool,
        queue: Arc<EventQueue>,
        persistence: Option<(Arc<AcpSessionPersistence>, PathBuf)>,
        turn_index: usize,
        notifier: Option<NativeAgentNotificationSink>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            claude_session_id: std::sync::Mutex::new(claude_session_id),
            workspace_path,
            remote_connection_id,
            resume: AtomicBool::new(resume),
            process: Mutex::new(None),
            lifecycle: Mutex::new(()),
            state: Mutex::new(TurnState {
                index: turn_index,
                ..Default::default()
            }),
            permissions: DashMap::new(),
            settled: Notify::new(),
            queue,
            persistence,
            selected_model: std::sync::Mutex::new(DEFAULT_MODEL_ID.to_string()),
            permission_mode: std::sync::Mutex::new("default".to_string()),
            thinking: std::sync::Mutex::new("auto".to_string()),
            sdk_models: std::sync::Mutex::new(Vec::new()),
            commands: std::sync::Mutex::new(Vec::new()),
            notifier,
            pre_plan_mode: std::sync::Mutex::new(None),
            context_usage: std::sync::Mutex::new(None),
            history: std::sync::Mutex::new(ClaudeHistory::default()),
            background: std::sync::Mutex::new(ClaudeBackgroundTasks::default()),
            last_activity: std::sync::Mutex::new(tokio::time::Instant::now()),
            title_resolver: std::sync::Mutex::new(title::default_resolver()),
        })
    }

    fn background(&self) -> std::sync::MutexGuard<'_, ClaudeBackgroundTasks> {
        self.background
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn background_tasks(&self) -> Vec<NativeBackgroundTask> {
        self.background().snapshot()
    }

    fn track_background(&self, process: &NativeProcess, frame: &Value) {
        let mut background = self.background();
        if background.consume(&process.id, frame, unix_ms()) {
            let tasks = background.snapshot();
            drop(background);
            self.notify(NativeAgentNotification::BackgroundTasksUpdated(tasks));
        }
    }

    /// A finished backgrounded task the CLI will report to the main agent.
    /// Stopped tasks (user stop, interrupt) do not wake it.
    fn wake_reason(&self, frame: &Value) -> Option<WakeReason> {
        let task_id = frame["task_id"].as_str()?;
        let status = frame["status"].as_str().unwrap_or("completed");
        if !matches!(status, "completed" | "failed") {
            return None;
        }
        let background = self.background();
        let task = background.backgrounded(task_id)?;
        Some(WakeReason {
            task_id: task_id.to_string(),
            tool_use_id: frame["tool_use_id"]
                .as_str()
                .map(str::to_string)
                .or_else(|| task.tool_use_id.clone()),
            task_type: task.task_type.clone(),
            subagent_type: task.subagent_type.clone(),
            description: task.description.clone(),
            status: status.to_string(),
            summary: frame["summary"]
                .as_str()
                .filter(|summary| !summary.trim().is_empty())
                .map(str::to_string),
        })
    }

    fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = tokio::time::Instant::now();
    }

    fn last_activity(&self) -> tokio::time::Instant {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// One watcher per process: release it once the session has been idle for
    /// `after`. Any frame or request resets the clock; a busy session (turn,
    /// permission, running background task) is never released.
    fn spawn_idle_release(
        self: &Arc<Self>,
        process: Arc<NativeProcess>,
        after: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let session = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let deadline = match session.upgrade() {
                    Some(session) => session.last_activity() + after,
                    None => return,
                };
                tokio::time::sleep_until(deadline).await;
                let Some(session) = session.upgrade() else {
                    return;
                };
                if !process.alive.load(Ordering::Acquire) {
                    return;
                }
                if session.last_activity() + after > tokio::time::Instant::now() {
                    continue;
                }
                if session.release_if_idle(&process).await {
                    return;
                }
                // Busy: check again a full period after it was last seen busy.
                session.touch();
            }
        })
    }

    async fn release_if_idle(&self, process: &Arc<NativeProcess>) -> bool {
        // Serialize with launch/submission so a starting turn is never cut.
        let _guard = self.lifecycle.lock().await;
        let slot = self.process.lock().await;
        if !slot
            .as_ref()
            .is_some_and(|current| current.id == process.id)
        {
            // A newer process owns the session; this one is already retired.
            return true;
        }
        let busy = self.state.lock().await.projector.is_some()
            || !self.permissions.is_empty()
            || self.background().has_running();
        if busy {
            return false;
        }
        log::info!(
            "Releasing idle Claude native process: session_id={}",
            self.session_id
        );
        drop(slot);
        process.kill().await;
        true
    }

    /// Stop one running background task through the live CLI process.
    pub async fn stop_task(&self, task_id: &str) -> OpenBitFunResult<()> {
        if !self.background().is_running(task_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Claude background task is not running: {task_id}"
            )));
        }
        let process = self
            .process
            .lock()
            .await
            .clone()
            .filter(|p| p.alive.load(Ordering::Acquire))
            .ok_or_else(|| service_error("Claude process is not running"))?;
        self.touch();
        process
            .request(stop_task_request(task_id), Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    pub(crate) fn claude_id(&self) -> String {
        read_lock(&self.claude_session_id)
    }

    fn history(&self) -> std::sync::MutexGuard<'_, ClaudeHistory> {
        self.history
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(in crate::client) fn apply_history(&self, history: ClaudeHistory) {
        *self.history() = history;
    }

    async fn persist_history(&self) -> OpenBitFunResult<()> {
        let Some((persistence, storage)) = &self.persistence else {
            return Ok(());
        };
        let value = self.history().to_value();
        persistence
            .set_custom_metadata(
                storage,
                &self.session_id,
                super::history::METADATA_KEY,
                value,
            )
            .await
    }

    /// Record transcript uuids of root chain entries for the active turn.
    /// `folded` marks the echo of a steering message taken into the turn: it
    /// extends the chain but is never the turn's prompt.
    fn track_anchor(&self, turn_id: &str, frame: &Value, folded: bool) {
        if !frame.get("parent_tool_use_id").is_none_or(Value::is_null) {
            return;
        }
        let Some(uuid) = frame["uuid"].as_str().filter(|uuid| !uuid.is_empty()) else {
            return;
        };
        let chain_entry = matches!(frame["type"].as_str(), Some("assistant" | "user"))
            || (frame["type"] == "system" && frame["subtype"] == "compact_boundary");
        if !chain_entry {
            return;
        }
        let mut history = self.history();
        let Some(anchor) = history.anchor_mut(turn_id) else {
            return;
        };
        // `--replay-user-messages` echoes the prompt; the first echo of the
        // turn is authoritative even if the CLI assigned its own uuid.
        let local_output = frame
            .pointer("/message/content")
            .and_then(Value::as_str)
            .is_some_and(|text| text.trim_start().starts_with("<local-command"));
        if frame["type"] == "user"
            && frame["isReplay"] == true
            && frame["isSynthetic"] != true
            && !local_output
            && !folded
            && !anchor.prompt_replayed
        {
            anchor.prompt_replayed = true;
            anchor.prompt_uuid = Some(uuid.to_string());
        }
        anchor.last_uuid = Some(uuid.to_string());
    }

    fn notify(&self, update: NativeAgentNotification) {
        if let Some(notifier) = &self.notifier {
            notifier(update);
        }
    }

    fn cache_initialize_response(&self, response: &Value) {
        self.replace_commands(&response["commands"]);
        if let Some(models) = response["models"].as_array() {
            *self
                .sdk_models
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = models.clone();
        }
    }

    fn replace_commands(&self, value: &Value) {
        if !value.is_array() {
            return;
        }
        self.publish_commands(super::commands::parse(value));
    }

    fn publish_commands(&self, commands: Vec<crate::client::AcpAvailableCommand>) {
        let mut cached = self
            .commands
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *cached == commands {
            return;
        }
        *cached = commands.clone();
        drop(cached);
        self.notify(NativeAgentNotification::AvailableCommandsUpdated(commands));
    }

    fn init_commands(&self, frame: &Value) {
        // Some CLI releases advertise names here instead of in initialize.
        // Preserve descriptions/hints already supplied by the SDK catalog.
        let mut commands = self
            .commands
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for key in ["slash_commands", "skills"] {
            for command in super::commands::parse(&frame[key]) {
                if !commands
                    .iter()
                    .any(|existing| existing.name == command.name)
                {
                    commands.push(command);
                }
            }
        }
        self.publish_commands(commands);
    }

    /// Seed model / permission mode / thinking from remembered choices.
    /// The model is re-validated once the SDK reports its catalog.
    pub(in crate::client) fn apply_preferences(&self, preferences: NativeAgentPreferences) {
        let assign = |slot: &std::sync::Mutex<String>, value: Option<String>| {
            if let Some(value) = value {
                *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
            }
        };
        assign(
            &self.selected_model,
            preferences.model.filter(|value| !value.trim().is_empty()),
        );
        assign(
            &self.permission_mode,
            preferences
                .permission_mode
                .filter(|value| PERMISSION_MODES.iter().any(|(id, _, _)| id == value)),
        );
        assign(
            &self.thinking,
            preferences
                .thinking
                .filter(|value| THINKING_LEVELS.iter().any(|(id, _)| id == value)),
        );
    }

    async fn remember(&self, update: impl Fn(&mut NativeAgentPreferences)) {
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .save_native_preference(CLIENT_ID, storage, &self.session_id, update)
                .await
            {
                log::warn!("Failed to remember Claude native preference: {error}");
            }
        }
    }

    fn selected_model(&self) -> String {
        self.selected_model
            .lock()
            .map(|model| model.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    /// Options for the external-client model selector, built from the SDK
    /// `initialize` model list. Empty until the CLI has been initialized.
    pub fn options(&self) -> AcpSessionOptions {
        let models = self
            .sdk_models
            .lock()
            .map(|models| models.clone())
            .unwrap_or_default();
        let available_models: Vec<AcpSessionModelOption> =
            models.iter().filter_map(sdk_model_option).collect();
        let current = self.selected_model();
        let current_model_id = if available_models.iter().any(|m| m.id == current) {
            Some(current)
        } else {
            available_models.first().map(|m| m.id.clone())
        };
        AcpSessionOptions {
            current_model_id,
            available_models,
            model_config_id: None,
            config_options: self.config_options(&models),
            context_usage: self
                .context_usage
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
            capabilities: Some(super::ClaudeNativeDriver.capabilities()),
            background_tasks: self.background_tasks(),
        }
    }

    fn config_options(&self, models: &[Value]) -> Vec<AcpSessionConfigOption> {
        let supports_auto = models
            .iter()
            .any(|model| model["supportsAutoMode"].as_bool() == Some(true));
        let select = |id: &str, name: &str, category: &str, current: String, options| {
            AcpSessionConfigOption {
                id: id.to_string(),
                name: name.to_string(),
                description: None,
                category: Some(category.to_string()),
                kind: AcpSessionConfigKind::Select {
                    current_value: current,
                    options,
                },
            }
        };
        let modes = PERMISSION_MODES
            .iter()
            .filter(|(id, _, _)| supports_auto || *id != "auto")
            .map(|(id, name, description)| AcpSessionConfigSelectOption {
                value: (*id).to_string(),
                name: (*name).to_string(),
                description: Some((*description).to_string()),
            })
            .collect();
        let levels = THINKING_LEVELS
            .iter()
            .map(|(id, name)| AcpSessionConfigSelectOption {
                value: (*id).to_string(),
                name: (*name).to_string(),
                description: None,
            })
            .collect();
        vec![
            select(
                PERMISSION_MODE_CONFIG_ID,
                "Permission mode",
                "mode",
                read_lock(&self.permission_mode),
                modes,
            ),
            select(
                THINKING_CONFIG_ID,
                "Thinking",
                "thought_level",
                read_lock(&self.thinking),
                levels,
            ),
        ]
    }

    /// Apply a permission mode or thinking level. Live processes switch via
    /// SDK control requests; later launches pass the matching CLI flags.
    pub async fn set_config_option(
        &self,
        config_id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        let AcpSessionConfigValue::Select { value } = value else {
            return Err(OpenBitFunError::Validation(
                "Claude native options take a select value".into(),
            ));
        };
        let (slot, control) = match config_id {
            PERMISSION_MODE_CONFIG_ID => {
                if !PERMISSION_MODES.iter().any(|(id, _, _)| *id == value) {
                    return Err(OpenBitFunError::Validation(format!(
                        "Unsupported Claude permission mode: {value}"
                    )));
                }
                (
                    &self.permission_mode,
                    json!({"subtype":"set_permission_mode", "mode":value}),
                )
            }
            THINKING_CONFIG_ID => {
                if !THINKING_LEVELS.iter().any(|(id, _)| *id == value) {
                    return Err(OpenBitFunError::Validation(format!(
                        "Unsupported Claude thinking level: {value}"
                    )));
                }
                (
                    &self.thinking,
                    json!({"subtype":"apply_flag_settings", "settings":thinking_flag_settings(&value)}),
                )
            }
            other => {
                return Err(OpenBitFunError::Validation(format!(
                    "Unknown Claude native option: {other}"
                )))
            }
        };
        let process = self.process.lock().await.clone();
        if let Some(process) = process.filter(|p| p.alive.load(Ordering::Acquire)) {
            process.request(control, Duration::from_secs(10)).await?;
        }
        if config_id == PERMISSION_MODE_CONFIG_ID {
            self.note_mode_transition(&value);
        }
        *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = value.clone();
        if config_id == PERMISSION_MODE_CONFIG_ID {
            self.remember(|p| p.permission_mode = Some(value.clone()))
                .await;
        } else {
            self.remember(|p| p.thinking = Some(value.clone())).await;
        }
        Ok(self.options())
    }

    /// Launch the CLI and complete `initialize` without sending a user turn, so
    /// the model list is available before the first message.
    /// Once the catalog is cached (e.g. after an idle release or an exit
    /// between turns), options are served without relaunching; the next
    /// turn resumes the process.
    pub async fn warm_up(self: &Arc<Self>) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        let catalog_cached = !self
            .sdk_models
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty();
        if catalog_cached {
            return Ok(());
        }
        self.ensure_process().await.map(|_| ())
    }

    /// Select a Claude model alias. Applies to the live process through the
    /// SDK `set_model` control request and to later launches via `--model`.
    pub async fn set_model(&self, model_id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        if model_id != DEFAULT_MODEL_ID
            && !self
                .options()
                .available_models
                .iter()
                .any(|m| m.id == model_id)
        {
            return Err(OpenBitFunError::Validation(format!(
                "Unsupported Claude native model: {model_id}"
            )));
        }
        {
            let mut selected = self
                .selected_model
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *selected = model_id.to_string();
        }
        let process = self.process.lock().await.clone();
        if let Some(process) = process.filter(|p| p.alive.load(Ordering::Acquire)) {
            let model = if model_id == DEFAULT_MODEL_ID {
                Value::Null
            } else {
                Value::String(model_id.to_string())
            };
            if let Err(error) = process
                .request(
                    json!({"subtype":"set_model", "model":model}),
                    Duration::from_secs(10),
                )
                .await
            {
                // The selection is kept; the next launch passes `--model`.
                log::warn!(
                    "Claude native live model switch failed; restarting on next turn: {error}"
                );
                process.kill().await;
            }
        }
        self.remember(|p| p.model = Some(model_id.to_string()))
            .await;
        Ok(self.options())
    }

    pub(crate) fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        self.workspace_path == path && self.remote_connection_id.as_deref() == remote
    }

    /// Claude stores transcripts under `~/.claude/projects/<encoded cwd>`, so
    /// once one exists (or is resumed/forked from) the cwd is fixed.
    pub(crate) async fn workspace_bound(&self) -> bool {
        self.resume.load(Ordering::Acquire)
            || self.history().pending.is_some()
            || !self.history().turns.is_empty()
            || self.state.lock().await.projector.is_some()
    }

    pub async fn start_turn(
        self: &Arc<Self>,
        turn_id: String,
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        self.touch();
        if self.state.lock().await.projector.is_some() {
            return Err(service_error("Claude session already has an active turn"));
        }
        let process = match self.ensure_process().await {
            Ok(process) => process,
            Err(error) => {
                self.fail_turn(&error.to_string()).await;
                self.begin_turn(
                    &mut *self.state.lock().await,
                    turn_id,
                    input.text,
                    original,
                    false,
                )
                .await?;
                self.fail_turn(&error.to_string()).await;
                return Err(error);
            }
        };
        // Serialize admission with the reader's generation fence. Background
        // output may have opened an autonomous turn while initialize awaited.
        let slot = self.process.lock().await;
        let mut state = self.state.lock().await;
        if state.projector.is_some() {
            return Err(service_error(
                "Claude background response is still active; retry after it settles",
            ));
        }
        let title_message = (state.index == 0)
            .then(|| title::title_source(&input.text, original.as_deref()))
            .flatten();
        self.begin_turn(
            &mut state,
            turn_id.clone(),
            input.text.clone(),
            original,
            false,
        )
        .await?;
        state.process_id = Some(process.id.clone());
        drop(state);
        let result: OpenBitFunResult<()> = async {
            if !process.alive.load(Ordering::Acquire) {
                return Err(service_error(
                    "Claude process exited before submission; retry to resume",
                ));
            }
            let prompt_uuid = uuid::Uuid::new_v4().to_string();
            process
                .write(&user_frame(&self.claude_id(), &prompt_uuid, &input, None))
                .await?;
            self.resume.store(true, Ordering::Release);
            {
                // The branch now holds a user message; plain resume finds it.
                let mut history = self.history();
                history.pending = None;
                if let Some(anchor) = history.anchor_mut(&turn_id) {
                    if !anchor.prompt_replayed {
                        anchor.prompt_uuid = Some(prompt_uuid);
                    }
                }
            }
            if let Some((persistence, storage)) = &self.persistence {
                persistence
                    .update_remote_session_state(
                        storage,
                        &self.session_id,
                        &self.claude_id(),
                        "claude-native-stream-json",
                        None,
                    )
                    .await?;
                if let Err(error) = self.persist_history().await {
                    log::warn!("Failed to persist Claude native history anchors: {error}");
                }
            }
            Ok(())
        }
        .await;
        drop(slot);
        if let Err(error) = &result {
            process.kill().await;
            self.fail_turn(&error.to_string()).await;
        } else if let Some(message) = title_message {
            self.spawn_title_generation(message);
        }
        result
    }

    /// Title the session from its first user message in the background; the
    /// turn never waits for it. A rename made meanwhile is kept.
    fn spawn_title_generation(self: &Arc<Self>, message: String) {
        let session = self.clone();
        let resolver = self
            .title_resolver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        tokio::spawn(async move {
            if let Some((persistence, storage)) = &session.persistence {
                match persistence
                    .load_metadata(storage, &session.session_id)
                    .await
                {
                    Ok(Some(metadata)) if !title::user_edited(&metadata) => {}
                    Ok(_) => return,
                    Err(error) => {
                        log::warn!("Claude session title skipped: {error}");
                        return;
                    }
                }
            }
            let resolved = resolver(message).await;
            if let Some((persistence, storage)) = &session.persistence {
                match persistence
                    .set_generated_title(storage, &session.session_id, &resolved.title)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        log::debug!(
                            "Claude session title skipped because the user renamed the session"
                        );
                        return;
                    }
                    Err(error) => {
                        log::warn!("Claude session title persistence failed: {error}");
                        return;
                    }
                }
            }
            let event = AgenticEvent::SessionTitleGenerated {
                session_id: session.session_id.clone(),
                title: resolved.title,
                method: resolved.method.as_str().to_string(),
            };
            if let Err(error) = session.enqueue(vec![event]).await {
                log::warn!("Claude session title event failed: {error}");
            }
        });
    }

    /// Write a user message for the running turn `turn_id`. The CLI queues it
    /// with `priority:"next"` and folds it in between tool rounds; the echo
    /// (`--replay-user-messages`) or the result's `user_message_uuids`
    /// confirms it. Returns the steering id (the message uuid).
    pub async fn steer(
        self: &Arc<Self>,
        turn_id: &str,
        input: NativeTurnInput,
        display: Option<String>,
    ) -> OpenBitFunResult<String> {
        if input.text.trim().is_empty() && input.images.is_empty() {
            return Err(OpenBitFunError::Validation(
                "Steering content cannot be empty".into(),
            ));
        }
        let _guard = self.lifecycle.lock().await;
        self.touch();
        let process = self
            .process
            .lock()
            .await
            .clone()
            .filter(|process| process.alive.load(Ordering::Acquire))
            .ok_or_else(|| service_error("Claude process is not running"))?;
        let uuid = uuid::Uuid::new_v4().to_string();
        {
            let mut state = self.state.lock().await;
            if state.projector.as_ref().map(ClaudeTurnProjector::turn_id) != Some(turn_id) {
                return Err(service_error("Claude turn is no longer running"));
            }
            state.steers.push(PendingSteer {
                uuid: uuid.clone(),
                text: input.text.clone(),
                display,
                process_id: process.id.clone(),
            });
        }
        // Never hold the turn state across a write: a large image may block on
        // a full pipe while the reader needs the state to drain stdout.
        let frame = user_frame(&self.claude_id(), &uuid, &input, Some("next"));
        if let Err(error) = process.write(&frame).await {
            self.state
                .lock()
                .await
                .steers
                .retain(|steer| steer.uuid != uuid);
            return Err(error);
        }
        Ok(uuid)
    }

    /// Settle pending steering messages the CLI reports as taken in: their
    /// replay echo (`uuid`) or a result's `user_message_uuids`. Inside a turn
    /// they render as steering items; the echo of one that the CLI runs after
    /// the turn ended opens that turn instead.
    async fn settle_steers(&self, state: &mut TurnState, frame: &Value) -> OpenBitFunResult<()> {
        if state.steers.is_empty() || !frame.get("parent_tool_use_id").is_none_or(Value::is_null) {
            return Ok(());
        }
        let taken: Vec<&str> = match frame["type"].as_str() {
            Some("user") if frame["isReplay"] == true => {
                frame["uuid"].as_str().into_iter().collect()
            }
            Some("result") => frame["user_message_uuids"]
                .as_array()
                .map(|ids| ids.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        for uuid in taken {
            let Some(position) = state.steers.iter().position(|steer| steer.uuid == uuid) else {
                continue;
            };
            let steer = state.steers.remove(position);
            let Some(projector) = state.projector.as_ref() else {
                // The turn ended before Claude took the message in; the CLI
                // now runs it as its own turn, anchored on this echo.
                self.begin_turn(
                    state,
                    uuid::Uuid::new_v4().to_string(),
                    steer.text.clone(),
                    steer.display.clone(),
                    false,
                )
                .await?;
                continue;
            };
            let event = AgenticEvent::UserSteeringInjected {
                session_id: self.session_id.clone(),
                turn_id: projector.turn_id().to_string(),
                round_index: projector.current_round_index(),
                steering_id: steer.uuid.clone(),
                display_content: steer.display.clone().unwrap_or_else(|| steer.text.clone()),
                content: steer.text,
            };
            self.enqueue(vec![event]).await?;
        }
        Ok(())
    }

    /// Whether `frame` is the echo of a steering message folded into the turn
    /// (not the turn's own prompt).
    fn is_steer_echo(state: &TurnState, frame: &Value) -> bool {
        frame["type"] == "user"
            && frame["isReplay"] == true
            && frame["uuid"]
                .as_str()
                .is_some_and(|uuid| state.steers.iter().any(|steer| steer.uuid == uuid))
    }

    async fn begin_turn(
        &self,
        state: &mut TurnState,
        turn_id: String,
        input: String,
        original: Option<String>,
        autonomous: bool,
    ) -> OpenBitFunResult<()> {
        self.begin_turn_with(state, turn_id, input, original, autonomous, None)
            .await
    }

    /// Open the turn Claude Code started on its own (background output or a
    /// queued task notification), labelled with the oldest wake reason.
    async fn begin_autonomous_turn(&self, state: &mut TurnState) -> OpenBitFunResult<()> {
        let reason = state.wake_reasons.pop_front();
        if let Some(reason) = &reason {
            log::info!(
                "Claude woke the main agent after a background task: session_id={}, task_id={}, status={}",
                self.session_id,
                reason.task_id,
                reason.status
            );
        }
        let (input, metadata) = match reason {
            Some(reason) => (reason.display_input(), Some(reason.metadata())),
            None => (String::new(), None),
        };
        self.begin_turn_with(
            state,
            uuid::Uuid::new_v4().to_string(),
            input,
            None,
            true,
            metadata,
        )
        .await
    }

    async fn begin_turn_with(
        &self,
        state: &mut TurnState,
        turn_id: String,
        input: String,
        original: Option<String>,
        autonomous: bool,
        metadata: Option<Value>,
    ) -> OpenBitFunResult<()> {
        let manual_compaction = input.split_whitespace().next() == Some("/compact");
        self.enqueue(vec![AgenticEvent::DialogTurnStarted {
            session_id: self.session_id.clone(),
            turn_id: turn_id.clone(),
            turn_index: state.index,
            user_input: input,
            original_user_input: original,
            user_message_metadata: metadata,
        }])
        .await?;
        state.index += 1;
        self.history().turns.push(TurnAnchor {
            turn_id: turn_id.clone(),
            turn_index: state.index - 1,
            ..Default::default()
        });
        let mut projector =
            ClaudeTurnProjector::new(&self.session_id, &turn_id, state.model.as_deref());
        if manual_compaction {
            projector.mark_manual_compaction();
        }
        if !autonomous {
            // Show the waiting indicator immediately, before Claude streams.
            self.enqueue(projector.open_pending_round()).await?;
        }
        state.projector = Some(projector);
        state.autonomous = autonomous;
        state.streaming = false;
        state.process_id = None;
        state.tasks.clear_pending();
        let entries = state.tasks.entries();
        if !entries.is_empty() {
            self.notify(NativeAgentNotification::PlanUpdated {
                turn_id: state
                    .projector
                    .as_ref()
                    .expect("new turn")
                    .turn_id()
                    .to_string(),
                entries,
            });
        }
        Ok(())
    }

    async fn enqueue(&self, events: Vec<AgenticEvent>) -> OpenBitFunResult<()> {
        super::super::events::enqueue(&self.queue, events, "Claude").await
    }

    async fn ensure_process(self: &Arc<Self>) -> OpenBitFunResult<Arc<NativeProcess>> {
        let mut slot = self.process.lock().await;
        if let Some(process) = slot.as_ref().filter(|p| p.alive.load(Ordering::Acquire)) {
            return Ok(process.clone());
        }
        self.state.lock().await.autonomous_result_pending = false;
        let pending = self.history().pending.clone();
        if pending.as_ref().is_some_and(|p| p.fork_from.is_some()) {
            // The CLI refuses `--session-id` of an existing transcript, and a
            // prior fork launch may have created one. Until a user message
            // lands on the branch the id is unreferenced, so each fork launch
            // takes a new one; `start_turn` persists the id that was used.
            *self
                .claude_session_id
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = uuid::Uuid::new_v4().to_string();
        }
        let mut args = cli_args(
            &self.claude_id(),
            self.resume.load(Ordering::Acquire),
            pending.as_ref(),
        );
        set_flag(
            &mut args,
            "--permission-mode",
            read_lock(&self.permission_mode),
        );
        args.extend(thinking_cli_args(&read_lock(&self.thinking)));
        let model = self.selected_model();
        if model != DEFAULT_MODEL_ID {
            args.extend(["--model".to_string(), model]);
        }
        if let Some(previous) = slot.as_ref() {
            let mut completion = previous.terminated.lock().await;
            if let Some(done) = completion.as_mut() {
                tokio::time::timeout(Duration::from_secs(5), done).await
                    .map_err(|_| service_error("Previous Claude process has not terminated; refusing concurrent resume"))?
                    .map_err(|_| service_error("Claude process supervisor was lost"))?;
                *completion = None;
            }
        }
        if self.remote_connection_id.is_none() {
            if let Some(message) = super::super::missing_local_workspace(
                &self.workspace_path,
                self.resume.load(Ordering::Acquire),
            ) {
                return Err(service_error(message));
            }
        }
        let executable = local_executable();
        let remote_command = remote_command(&self.workspace_path, &args);
        let io = super::super::process::spawn(super::super::process::ProcessConfig {
            client_id: CLIENT_ID,
            name: "Claude",
            executable: &executable,
            args: &args,
            workspace_path: &self.workspace_path,
            remote_connection_id: self.remote_connection_id.as_deref(),
            remote_command: &remote_command,
            env: &[(FILE_CHECKPOINT_ENV, "true")],
        })
        .await?;
        let reader = io.reader;
        let stderr = io.stderr;
        let process = Arc::new(NativeProcess {
            id: uuid::Uuid::new_v4().to_string(),
            writer: Mutex::new(io.writer),
            controls: DashMap::new(),
            shutdown: Mutex::new(Some(io.shutdown)),
            alive: AtomicBool::new(true),
            terminated: Mutex::new(Some(io.terminated)),
            exit: io.exit,
            stderr: Mutex::new(VecDeque::new()),
        });
        *slot = Some(process.clone());
        let tail_process = process.clone();
        let stderr_task = tokio::spawn(async move {
            super::super::process::collect_stderr(stderr, &tail_process.stderr).await;
        });
        let session = self.clone();
        let reader_process = process.clone();
        tokio::spawn(async move {
            session.read_loop(reader_process, reader, stderr_task).await;
        });
        drop(slot);
        self.touch();
        self.spawn_idle_release(process.clone(), IDLE_RELEASE_AFTER);
        match process
            .request(
                json!({"subtype":"initialize", "hooks":null, "forwardSubagentText":true}),
                Duration::from_secs(30),
            )
            .await
        {
            Ok(response) => {
                self.cache_initialize_response(&response);
                if let Some(models) = response["models"].as_array() {
                    // A remembered model the CLI no longer offers falls back
                    // to the CLI default instead of failing the first turn.
                    let model = self.selected_model();
                    if model != DEFAULT_MODEL_ID
                        && !models
                            .iter()
                            .any(|row| row["value"].as_str() == Some(model.as_str()))
                    {
                        log::warn!("Remembered Claude model {model} is unavailable; using default");
                        *self
                            .selected_model
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            DEFAULT_MODEL_ID.to_string();
                        let _ = process
                            .request(
                                json!({"subtype":"set_model", "model":null}),
                                Duration::from_secs(10),
                            )
                            .await;
                    }
                }
            }
            Err(error) => {
                process.kill().await;
                let tail = process
                    .stderr
                    .lock()
                    .await
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(service_error(format!("{error}\n{tail}")));
            }
        }
        Ok(process)
    }

    async fn read_loop(
        self: Arc<Self>,
        process: Arc<NativeProcess>,
        reader: Reader,
        mut stderr_task: tokio::task::JoinHandle<()>,
    ) {
        let mut lines = BufReader::new(reader).lines();
        let mut exit = process.exit.clone();
        let error = loop {
            let line = tokio::select! {
                biased;
                line = lines.next_line() => line,
                changed = exit.changed() => {
                    break if changed.is_ok() {
                        exit.borrow().clone().unwrap_or_else(|| "Claude process exited".into())
                    } else { "Claude process supervisor ended".into() };
                }
            };
            match line {
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(Some(line)) => match serde_json::from_str::<Value>(&line) {
                    Ok(frame) => {
                        self.touch();
                        let refresh_usage = frame["type"] == "result"
                            && frame.get("parent_tool_use_id").is_none_or(Value::is_null);
                        if let Err(error) = self.route_frame(&process, frame).await {
                            break error.to_string();
                        }
                        if refresh_usage {
                            // Never block the reader on a control round-trip.
                            let session = self.clone();
                            let process = process.clone();
                            tokio::spawn(async move {
                                session.refresh_context_usage(&process).await;
                            });
                        }
                    }
                    Err(error) => break format!("Invalid Claude stream-json frame: {error}"),
                },
                Ok(None) => break "Claude output stream closed".to_string(),
                Err(error) => break format!("Claude output read failed: {error}"),
            }
        };
        process.controls.clear();
        process.kill().await;
        if tokio::time::timeout(Duration::from_millis(250), &mut stderr_task)
            .await
            .is_err()
        {
            stderr_task.abort();
        }
        let tail = process
            .stderr
            .lock()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        // Background tasks die with the process that ran them.
        let ended = {
            let mut background = self.background();
            background
                .process_ended(&process.id, unix_ms())
                .then(|| background.snapshot())
        };
        if let Some(tasks) = ended {
            self.notify(NativeAgentNotification::BackgroundTasksUpdated(tasks));
        }
        // Steering messages die with the CLI queue that held them.
        self.state
            .lock()
            .await
            .steers
            .retain(|steer| steer.process_id != process.id);
        // Hold the process fence while failing its turn. An old reader must
        // never fail a turn submitted after a resume/restart. Between turns
        // there is nothing to fail: an idle exit or crash stays silent and
        // the next turn lazily resumes the transcript.
        let slot = self.process.lock().await;
        let owns_turn = self.state.lock().await.process_id.as_deref() == Some(process.id.as_str());
        if slot.as_ref().is_some_and(|p| p.id == process.id) && owns_turn {
            self.fail_turn(&format!("{error}\n{tail}")).await;
        }
    }

    async fn route_frame(
        &self,
        process: &Arc<NativeProcess>,
        frame: Value,
    ) -> OpenBitFunResult<()> {
        if frame["type"] == "control_response" {
            if let Some(id) = frame["response"]["request_id"].as_str() {
                if let Some((_, tx)) = process.controls.remove(id) {
                    let _ = tx.send(frame["response"].clone());
                }
            }
            return Ok(());
        }
        let slot = self.process.lock().await;
        if slot
            .as_ref()
            .is_some_and(|current| current.id != process.id)
            || !process.alive.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if frame["type"] != "control_request" {
            if let Some(tool_id) = frame["parent_tool_use_id"].as_str() {
                let tool_id = tool_id.to_string();
                return self.route_child_frame(&mut state, &tool_id, &frame).await;
            }
        }
        if frame["type"] == "result" && state.autonomous_result_pending {
            state.autonomous_result_pending = false;
            return Ok(());
        }
        if opens_autonomous_turn(&frame) {
            state.autonomous_result_pending = false;
        }
        if frame["type"] == "system" && frame["subtype"] == "init" {
            // Ends the reader, which fails the turn with the update hint.
            super::cli::check_reported_version(frame["claude_code_version"].as_str())?;
            state.model = frame["model"].as_str().map(str::to_string);
            self.init_commands(&frame);
            self.adopt_reported_session_id(&frame).await;
        }
        if frame["type"] == "system" && matches!(frame["subtype"].as_str(), Some("init" | "status"))
        {
            if let Some(mode) = frame["permissionMode"].as_str() {
                self.observe_permission_mode(mode);
            }
        }
        if frame["type"] == "control_request" {
            if frame["request"]["subtype"] == "can_use_tool" {
                self.handle_permission(process, &frame, &mut state).await?;
            } else {
                process.write(&json!({"type":"control_response", "response":{"subtype":"error",
                    "request_id":frame["request_id"], "error":"Unsupported Claude control request"}})).await?;
            }
            return Ok(());
        }
        if !process.alive.load(Ordering::Acquire) {
            return Ok(());
        }
        if frame["type"] == "system" {
            self.track_background(process, &frame);
            match frame["subtype"].as_str() {
                Some("commands_changed") => {
                    self.replace_commands(&frame["commands"]);
                    return Ok(());
                }
                Some("task_started") => {
                    // Background shells also carry a tool_use_id; only agents
                    // get a child transcript.
                    if let (Some(tool_id), true) =
                        (frame["tool_use_id"].as_str(), is_agent_task(&frame))
                    {
                        let tool_id = tool_id.to_string();
                        self.start_child_task(&mut state, &tool_id, &frame).await?;
                    }
                    return Ok(());
                }
                Some("task_notification") => {
                    if let Some(reason) = self.wake_reason(&frame) {
                        state.wake_reasons.push_back(reason);
                    }
                    if let Some(tool_id) = Self::task_child_key(&state, &frame) {
                        let failed = frame["status"].as_str().is_some_and(|s| s != "completed");
                        self.finish_child(&mut state, &tool_id, failed, frame["summary"].as_str())
                            .await?;
                    }
                    return Ok(());
                }
                Some("task_progress" | "task_updated" | "background_tasks_changed") => {
                    return Ok(());
                }
                _ => {}
            }
        }
        let folded = state.projector.is_some() && Self::is_steer_echo(&state, &frame);
        self.settle_steers(&mut state, &frame).await?;
        if let Some(task_id) = injected_notification_task(&frame) {
            if state.projector.is_some() {
                // Taken into the running turn: it wakes nothing later.
                state
                    .wake_reasons
                    .retain(|reason| reason.task_id != task_id);
            } else {
                // The CLI starts the wake turn with the notification itself.
                if let Some(position) = state
                    .wake_reasons
                    .iter()
                    .position(|reason| reason.task_id == task_id)
                {
                    let reason = state.wake_reasons.remove(position).expect("position");
                    state.wake_reasons.push_front(reason);
                }
                self.begin_autonomous_turn(&mut state).await?;
            }
        }
        if state.projector.is_none() && opens_autonomous_turn(&frame) {
            self.begin_autonomous_turn(&mut state).await?;
        }
        if state.projector.is_some() {
            state.process_id = Some(process.id.clone());
        }
        if frame["type"] == "stream_event" && frame["event"]["type"] == "message_start" {
            state.streaming = true;
        }
        if state.projector.is_some() {
            if let Some(entries) = state.tasks.consume(&frame) {
                self.notify(NativeAgentNotification::PlanUpdated {
                    turn_id: state
                        .projector
                        .as_ref()
                        .expect("active turn")
                        .turn_id()
                        .to_string(),
                    entries,
                });
            }
        }
        if let Some(turn_id) = state.projector.as_ref().map(|p| p.turn_id().to_string()) {
            self.track_anchor(&turn_id, &frame, folded);
        }
        if let Some(projector) = state.projector.as_mut() {
            let events = projector.consume(&frame);
            if frame["type"] == "result" {
                self.publish_result_usage(&events, projector.total_cost_usd());
            }
            self.enqueue(events).await?;
            // Background root responses sometimes end at message_stop without
            // a result; close that autonomous segment, but retain the process.
            if state.autonomous
                && ((frame["type"] == "stream_event" && frame["event"]["type"] == "message_stop")
                    || (!state.streaming
                        && frame["type"] == "assistant"
                        && frame["message"]["stop_reason"] == "end_turn"))
            {
                let projector = state.projector.as_mut().expect("active autonomous turn");
                if projector.open_tool_ids().is_empty() {
                    self.enqueue(
                        projector.consume(
                            &json!({"type":"result", "subtype":"success", "is_error":false}),
                        ),
                    )
                    .await?;
                    state.autonomous_result_pending = true;
                }
            }
            if state
                .projector
                .as_ref()
                .is_some_and(ClaudeTurnProjector::is_finished)
            {
                let id = state
                    .projector
                    .take()
                    .expect("finished turn")
                    .turn_id()
                    .to_string();
                self.clear_permissions(&id).await;
                self.settled.notify_waiters();
                if let Err(error) = self.persist_history().await {
                    log::warn!("Failed to persist Claude native history anchors: {error}");
                }
            }
        }
        Ok(())
    }

    /// Open the child for an agent `task_started`. A `SendMessage` that
    /// resumes a known agent reopens that agent's child from its own card.
    async fn start_child_task(
        &self,
        state: &mut TurnState,
        tool_id: &str,
        frame: &Value,
    ) -> OpenBitFunResult<()> {
        let key = frame["task_id"]
            .as_str()
            .and_then(|agent| state.agent_children.get(agent))
            .or_else(|| state.child_aliases.get(tool_id))
            .cloned()
            .unwrap_or_else(|| tool_id.to_string());
        if key != tool_id {
            state.child_aliases.insert(tool_id.to_string(), key.clone());
        }
        self.ensure_child(state, &key, tool_id, frame).await
    }

    /// Child key of a `task_notification`: by agent id, else by tool_use id.
    fn task_child_key(state: &TurnState, frame: &Value) -> Option<String> {
        if let Some(key) = frame["task_id"]
            .as_str()
            .and_then(|agent| state.agent_children.get(agent))
            .filter(|key| state.children.contains_key(*key))
        {
            return Some(key.clone());
        }
        let tool_id = frame["tool_use_id"].as_str()?;
        Some(
            state
                .child_aliases
                .get(tool_id)
                .cloned()
                .unwrap_or_else(|| tool_id.to_string()),
        )
    }

    /// Create (or reopen) the child session keyed by the spawning tool_use id
    /// and link it to the `link_tool_id` card. Order mirrors the core agent:
    /// link first, then the turn.
    async fn ensure_child(
        &self,
        state: &mut TurnState,
        tool_id: &str,
        link_tool_id: &str,
        info: &Value,
    ) -> OpenBitFunResult<()> {
        if state.children.contains_key(tool_id) {
            return Ok(());
        }
        // The card lives in the turn that issued the tool call.
        let Some(parent_turn_id) = state.projector.as_ref().map(|p| p.turn_id().to_string()) else {
            return Ok(());
        };
        // Path-safe and stable per tool call (session ids may become file names).
        let session_id = format!("{}-subagent-{tool_id}", self.session_id);
        let turn_id = uuid::Uuid::new_v4().to_string();
        let turn_index = match state.child_turns.get(tool_id) {
            Some(index) => *index,
            None => self.persisted_child_turns(&session_id).await,
        };
        state
            .child_turns
            .insert(tool_id.to_string(), turn_index + 1);
        let agent_id = info["task_id"]
            .as_str()
            .or_else(|| info["agent_id"].as_str())
            .map(str::to_string);
        if let Some(agent) = &agent_id {
            state
                .agent_children
                .entry(agent.clone())
                .or_insert_with(|| tool_id.to_string());
        }
        let prompt = info["prompt"].as_str().unwrap_or_default().to_string();
        let subagent_type = info["subagent_type"].as_str().map(str::to_string);
        // The frontend persists child turns and restores the child from disk,
        // so the hidden child record must exist before any child event.
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = self
                .persist_child_record(
                    persistence,
                    storage,
                    &session_id,
                    &parent_turn_id,
                    tool_id,
                    &subagent_type,
                    info,
                    state.model.as_deref(),
                )
                .await
            {
                log::warn!("Claude subagent record persistence failed: {error}");
            }
        }
        self.enqueue(vec![
            AgenticEvent::SubagentSessionLinked {
                session_id: session_id.clone(),
                subagent_dialog_turn_id: turn_id.clone(),
                parent_session_id: self.session_id.clone(),
                parent_dialog_turn_id: parent_turn_id,
                parent_tool_call_id: link_tool_id.to_string(),
                agent_type: subagent_type,
                model_id: None,
                focused_review_display_label: None,
                continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
            },
            AgenticEvent::DialogTurnStarted {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                turn_index,
                user_input: prompt,
                original_user_input: None,
                user_message_metadata: None,
            },
        ])
        .await?;
        let mut projector =
            ClaudeTurnProjector::for_subagent(&session_id, &turn_id, state.model.as_deref());
        self.enqueue(projector.open_pending_round()).await?;
        state.children.insert(
            tool_id.to_string(),
            ChildTranscript {
                session_id,
                projector,
                agent_id,
            },
        );
        Ok(())
    }

    /// Turns already saved for a child, e.g. one reopened after a restart.
    async fn persisted_child_turns(&self, child_session_id: &str) -> usize {
        let Some((persistence, storage)) = &self.persistence else {
            return 0;
        };
        match persistence.load_metadata(storage, child_session_id).await {
            Ok(metadata) => metadata.map_or(0, |metadata| metadata.turn_count),
            Err(error) => {
                log::warn!("Claude subagent metadata read failed: {error}");
                0
            }
        }
    }

    async fn persist_child_record(
        &self,
        persistence: &AcpSessionPersistence,
        storage: &std::path::Path,
        child_session_id: &str,
        parent_turn_id: &str,
        tool_id: &str,
        subagent_type: &Option<String>,
        info: &Value,
        model: Option<&str>,
    ) -> OpenBitFunResult<()> {
        let parent = persistence
            .load_metadata(storage, &self.session_id)
            .await?
            .ok_or_else(|| service_error("Claude parent session metadata is missing"))?;
        let relationship = openbitfun_core::service::session::SessionRelationship {
            kind: Some(openbitfun_core::service::session::SessionRelationshipKind::Subagent),
            parent_session_id: Some(self.session_id.clone()),
            parent_request_id: None,
            parent_dialog_turn_id: Some(parent_turn_id.to_string()),
            parent_turn_index: None,
            parent_tool_call_id: Some(tool_id.to_string()),
            subagent_type: subagent_type.clone(),
            continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
        };
        let label = subagent_type.as_deref().unwrap_or("Subagent");
        let title = match info["description"]
            .as_str()
            .filter(|d| !d.trim().is_empty())
        {
            Some(description) => format!("{label}: {description}"),
            None => label.to_string(),
        };
        persistence
            .create_subagent_session_record(
                storage,
                child_session_id,
                &parent,
                relationship,
                title,
                model.map(str::to_string),
            )
            .await
    }

    async fn route_child_frame(
        &self,
        state: &mut TurnState,
        tool_id: &str,
        frame: &Value,
    ) -> OpenBitFunResult<()> {
        let tool_id = state
            .child_aliases
            .get(tool_id)
            .cloned()
            .unwrap_or_else(|| tool_id.to_string());
        if !state.children.contains_key(&tool_id) {
            // Synchronous subagents may stream before (or without) task_started.
            let info = json!({"subagent_type": frame["subagent_type"],
                "description": frame["task_description"],
                "prompt": frame["task_description"],
                "agent_id": frame["agent_id"]});
            self.ensure_child(state, &tool_id, &tool_id, &info).await?;
        }
        if let Some(child) = state.children.get_mut(&tool_id) {
            let events = child.projector.consume(frame);
            self.enqueue(events).await?;
        }
        Ok(())
    }

    async fn finish_child(
        &self,
        state: &mut TurnState,
        tool_id: &str,
        failed: bool,
        summary: Option<&str>,
    ) -> OpenBitFunResult<()> {
        let Some(mut child) = state.children.remove(tool_id) else {
            return Ok(());
        };
        let events = if failed {
            child
                .projector
                .fail(summary.unwrap_or("Subagent did not complete"))
        } else {
            child
                .projector
                .consume(&json!({"type":"result", "subtype":"success", "is_error":false}))
        };
        self.enqueue(events).await?;
        log::debug!(
            "Claude subagent finished: child_session_id={}",
            child.session_id
        );
        Ok(())
    }

    /// Settle every open child, e.g. when the process exits or the user cancels.
    async fn settle_children(&self, state: &mut TurnState, cancelled: bool) {
        for (_, mut child) in state.children.drain() {
            let events = if cancelled {
                child.projector.cancel()
            } else {
                child
                    .projector
                    .fail("Claude process ended before the subagent finished")
            };
            if let Err(error) = self.enqueue(events).await {
                log::error!("Claude subagent settle failed: {error}");
            }
        }
    }

    async fn handle_permission(
        &self,
        process: &Arc<NativeProcess>,
        frame: &Value,
        state: &mut TurnState,
    ) -> OpenBitFunResult<()> {
        let request = &frame["request"];
        let request_id = frame["request_id"]
            .as_str()
            .ok_or_else(|| service_error("Claude permission has no request_id"))?
            .to_string();
        let tool_id = request["tool_use_id"]
            .as_str()
            .ok_or_else(|| service_error("Claude permission has no tool_use_id"))?
            .to_string();
        // A subagent asking for permission carries its `agent_id`; its card
        // belongs to the child session, not the parent turn.
        let child_key = request["agent_id"].as_str().and_then(|agent| {
            state
                .children
                .iter()
                .find(|(_, child)| child.agent_id.as_deref() == Some(agent))
                .map(|(key, _)| key.clone())
        });
        if child_key.is_none() && state.projector.is_none() {
            self.begin_turn(
                state,
                uuid::Uuid::new_v4().to_string(),
                String::new(),
                None,
                true,
            )
            .await?;
        }
        state.process_id = Some(process.id.clone());
        let (projector, card_session_id) = match child_key {
            Some(key) => {
                let child = state.children.get_mut(&key).expect("child transcript");
                (&mut child.projector, child.session_id.clone())
            }
            None => (
                state.projector.as_mut().expect("permission turn"),
                self.session_id.clone(),
            ),
        };
        // The control request can precede the final assistant tool_use block.
        // Project it now with the same id; the projector deduplicates the latter.
        self.enqueue(projector.permission_tool(
            &tool_id,
            request["tool_name"].as_str().unwrap_or("tool"),
            request["input"].clone(),
        ))
        .await?;
        let permission_id = format!("claude_native_permission_{}", uuid::Uuid::new_v4());
        let pending = PendingPermission {
            process: process.clone(),
            turn_id: projector.turn_id().to_string(),
            request_id,
            tool_id: tool_id.clone(),
            input: request["input"].clone(),
            suggestions: request["permission_suggestions"].clone(),
            kind: PermissionKind::of(request["tool_name"].as_str().unwrap_or_default()),
        };
        self.permissions.insert(permission_id.clone(), pending);
        let payload = permission_payload(&permission_id, &card_session_id, request);
        if let Err(error) = emit_global_event(BackendEvent::Custom {
            event_name: "backend-event-acppermissionrequest".into(),
            payload,
        })
        .await
        {
            if let Some((_, pending)) = self.permissions.remove(&permission_id) {
                let _ = cancel_permission(pending).await;
            }
            return Err(service_error(format!(
                "Claude permission delivery failed: {error}"
            )));
        }
        Ok(())
    }

    pub(crate) async fn submit_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> Option<OpenBitFunResult<()>> {
        let (_, pending) = self.permissions.remove(&request.permission_id)?;
        let reply = match permission_reply(&pending, request) {
            Ok(reply) => reply,
            Err(error) => {
                self.permissions
                    .insert(request.permission_id.clone(), pending);
                return Some(Err(error));
            }
        };
        let approved_plan = (pending.kind == PermissionKind::PlanReview
            && reply["behavior"] == "allow")
            .then(|| self.plan_exit_mode());
        let result = write_permission_reply(&pending, reply).await;
        if let (Ok(()), Some(mode)) = (&result, approved_plan) {
            // Claude leaves plan mode with the approval; mirror it right away.
            self.observe_permission_mode(&mode);
            self.remember(|p| p.permission_mode = Some(mode.clone()))
                .await;
        }
        Some(result)
    }

    /// Permission mode restored when a plan is approved.
    fn plan_exit_mode(&self) -> String {
        self.pre_plan_mode
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Remember the mode active before entering plan mode.
    fn note_mode_transition(&self, next: &str) {
        let current = read_lock(&self.permission_mode);
        let mut pre_plan = self
            .pre_plan_mode
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if next == "plan" && current != "plan" {
            *pre_plan = Some(current);
        } else if next != "plan" {
            *pre_plan = None;
        }
    }

    /// Track a mode reported by Claude (system init/status) or implied by a
    /// plan approval, and tell the UI to refresh its options.
    fn observe_permission_mode(&self, mode: &str) {
        if !PERMISSION_MODES.iter().any(|(id, _, _)| *id == mode)
            || read_lock(&self.permission_mode) == mode
        {
            return;
        }
        self.note_mode_transition(mode);
        *self
            .permission_mode
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = mode.to_string();
        self.notify(NativeAgentNotification::OptionsChanged);
    }

    fn publish_context_usage(&self, used: u64, size: u64, cost: Option<f64>) {
        if size == 0 {
            return;
        }
        let mut slot = self
            .context_usage
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let cost = cost
            .map(usd_cost)
            .or_else(|| slot.as_ref().and_then(|usage| usage.cost.clone()));
        let usage = AcpSessionContextUsage { used, size, cost };
        *slot = Some(usage.clone());
        drop(slot);
        self.notify(NativeAgentNotification::ContextUsageUpdated(usage));
    }

    /// Context occupancy from the turn's usage event plus the result's cost.
    fn publish_result_usage(&self, events: &[AgenticEvent], cost: Option<f64>) {
        let usage = events.iter().find_map(|event| match event {
            AgenticEvent::TokenUsageUpdated {
                input_tokens,
                max_context_tokens: Some(max),
                ..
            } => Some((*input_tokens as u64, *max as u64)),
            _ => None,
        });
        if let Some((used, size)) = usage {
            self.publish_context_usage(used, size, cost);
        } else if let Some(amount) = cost {
            let mut slot = self
                .context_usage
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(usage) = slot.as_mut() {
                usage.cost = Some(usd_cost(amount));
            }
        }
    }

    /// Ask the CLI for its own context accounting (`get_context_usage`, as
    /// codexhost does); it includes system prompt, tools and memory files.
    async fn refresh_context_usage(&self, process: &Arc<NativeProcess>) {
        if !process.alive.load(Ordering::Acquire) {
            return;
        }
        match process
            .request(
                json!({"subtype":"get_context_usage"}),
                Duration::from_secs(10),
            )
            .await
        {
            Ok(response) => {
                if let Some((used, size)) = parse_context_usage(&response) {
                    self.publish_context_usage(used, size, None);
                }
            }
            Err(error) => log::debug!("Claude get_context_usage failed: {error}"),
        }
    }

    async fn clear_permissions(&self, turn_id: &str) {
        let ids: Vec<String> = self
            .permissions
            .iter()
            .filter(|p| p.turn_id == turn_id)
            .map(|p| p.key().clone())
            .collect();
        for id in ids {
            if let Some((_, pending)) = self.permissions.remove(&id) {
                let _ = cancel_permission(pending).await;
            }
        }
    }

    async fn fail_turn(&self, error: &str) {
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            if let Err(error) = self.enqueue(projector.fail(error)).await {
                log::error!("Claude failure event delivery failed: {error}");
            }
            self.clear_permissions(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
        self.settle_children(&mut state, false).await;
    }

    pub async fn cancel(&self) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        {
            let mut state = self.state.lock().await;
            // The interrupt stops background agents and drops queued
            // notifications in the CLI; nothing is left to wake for.
            state.wake_reasons.clear();
            if let Some(projector) = state.projector.as_mut() {
                projector.mark_cancel_requested();
            }
        }
        let process = self.process.lock().await.clone();
        if let Some(process) = process.as_ref().filter(|p| p.alive.load(Ordering::Acquire)) {
            let interrupted = tokio::time::timeout(INTERRUPT_GRACE, async {
                process
                    .request(json!({"subtype":"interrupt"}), INTERRUPT_GRACE)
                    .await?;
                loop {
                    let notified = self.settled.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.state.lock().await.projector.is_none() {
                        return Ok::<_, OpenBitFunError>(());
                    }
                    notified.await;
                }
            })
            .await;
            if !matches!(interrupted, Ok(Ok(()))) {
                process.kill().await;
            }
        }
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            self.enqueue(projector.cancel()).await?;
            self.clear_permissions(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
        self.settle_children(&mut state, true).await;
        Ok(())
    }

    /// A fork launch should keep the `--session-id` we chose; if the CLI
    /// reports another id, follow it so later resumes find the transcript.
    async fn adopt_reported_session_id(&self, frame: &Value) {
        let Some(reported) = frame["session_id"]
            .as_str()
            .filter(|id| uuid::Uuid::parse_str(id).is_ok())
        else {
            return;
        };
        if reported == self.claude_id() {
            return;
        }
        log::warn!("Claude reported a different session id than requested; adopting it");
        *self
            .claude_session_id
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = reported.to_string();
        if let Some((persistence, storage)) = &self.persistence {
            let strategy = if self.resume.load(Ordering::Acquire) {
                "claude-native-stream-json"
            } else {
                "claude-native-prepared"
            };
            if let Err(error) = persistence
                .update_remote_session_state(storage, &self.session_id, reported, strategy, None)
                .await
            {
                log::warn!("Failed to persist adopted Claude session id: {error}");
            }
        }
    }

    /// Drop the latest turn: restore its file edits from Claude's checkpoint,
    /// stop the CLI, and make the next launch resume before that turn.
    pub async fn rollback_last_turn(
        self: &Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        let _guard = self.lifecycle.lock().await;
        if self.state.lock().await.projector.is_some() || !self.permissions.is_empty() {
            return Err(OpenBitFunError::Validation(
                "Wait until the Claude turn finishes before rolling back".into(),
            ));
        }
        let (target, plan) = self
            .history()
            .rollback_plan(turn_id)
            .map_err(OpenBitFunError::Validation)?;
        if let Some((persistence, storage)) = &self.persistence {
            persistence
                .ensure_latest_turn(storage, &self.session_id, &target.turn_id)
                .await?;
        }
        let mut outcome = NativeRollbackOutcome::default();
        match target.prompt_uuid.as_deref() {
            Some(prompt) => match self.rewind_files(prompt).await {
                Ok(files) => {
                    outcome.files_rewound = true;
                    outcome.files_changed = files;
                }
                Err(error) => {
                    log::warn!("Claude file rewind skipped during rollback: {error}");
                    outcome.note = Some(format!("Files were not restored: {error}"));
                }
            },
            None => {
                outcome.note =
                    Some("Files were not restored: the turn has no Claude checkpoint".into())
            }
        }
        if let Some(process) = self.process.lock().await.clone() {
            process.kill().await;
        }
        {
            let mut history = self.history();
            history
                .turns
                .retain(|anchor| anchor.turn_index < target.turn_index);
            let fork_from = history.pending.as_ref().and_then(|p| p.fork_from.clone());
            history.pending = match &plan {
                RollbackPlan::Fresh => None,
                RollbackPlan::ResumeAt(at) => Some(PendingResume {
                    fork_from,
                    resume_at: Some(at.clone()),
                }),
            };
        }
        let fresh = plan == RollbackPlan::Fresh;
        if fresh {
            // Nothing earlier reached Claude: continue in a new transcript.
            *self
                .claude_session_id
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = uuid::Uuid::new_v4().to_string();
            self.resume.store(false, Ordering::Release);
        }
        self.state.lock().await.index = target.turn_index;
        if let Some((persistence, storage)) = &self.persistence {
            if fresh {
                persistence
                    .update_remote_session_state(
                        storage,
                        &self.session_id,
                        &self.claude_id(),
                        "claude-native-prepared",
                        None,
                    )
                    .await?;
            }
            self.persist_history().await?;
            persistence
                .truncate_turns_from(storage, &self.session_id, target.turn_index)
                .await?;
        }
        Ok(outcome)
    }

    /// `rewind_files` against the live (or freshly resumed) CLI process.
    async fn rewind_files(self: &Arc<Self>, prompt_uuid: &str) -> OpenBitFunResult<Vec<String>> {
        let process = self.ensure_process().await?;
        let response = process
            .request(
                json!({"subtype":"rewind_files", "user_message_id":prompt_uuid, "dry_run":false}),
                Duration::from_secs(60),
            )
            .await?;
        if response["canRewind"] == false {
            return Err(service_error(
                response["error"]
                    .as_str()
                    .unwrap_or("Claude cannot rewind files for this turn")
                    .to_string(),
            ));
        }
        Ok(response["filesChanged"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect())
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.cancel().await;
        let _guard = self.lifecycle.lock().await;
        if let Some(process) = self.process.lock().await.as_ref() {
            process.kill().await;
        }
    }
}

#[async_trait::async_trait]
impl super::super::NativeAgentSession for ClaudeNativeSession {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn options(&self) -> AcpSessionOptions {
        ClaudeNativeSession::options(self)
    }
    async fn warm_up(self: Arc<Self>) -> OpenBitFunResult<()> {
        ClaudeNativeSession::warm_up(&self).await
    }
    async fn start_turn(
        self: Arc<Self>,
        turn_id: String,
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        ClaudeNativeSession::start_turn(&self, turn_id, input, original).await
    }
    async fn steer(
        self: Arc<Self>,
        turn_id: &str,
        input: NativeTurnInput,
        display: Option<String>,
    ) -> OpenBitFunResult<String> {
        ClaudeNativeSession::steer(&self, turn_id, input, display).await
    }
    async fn cancel(&self) -> OpenBitFunResult<()> {
        ClaudeNativeSession::cancel(self).await
    }
    async fn set_model(&self, id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        ClaudeNativeSession::set_model(self, id).await
    }
    async fn set_config_option(
        &self,
        id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        ClaudeNativeSession::set_config_option(self, id, value).await
    }
    async fn respond_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> OpenBitFunResult<bool> {
        match self.submit_permission(request).await {
            Some(result) => result.map(|()| true),
            None => Ok(false),
        }
    }
    fn commands(&self) -> Vec<crate::client::AcpAvailableCommand> {
        self.commands
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        ClaudeNativeSession::matches_scope(self, path, remote)
    }
    async fn workspace_bound(&self) -> bool {
        ClaudeNativeSession::workspace_bound(self).await
    }
    async fn rollback_last_turn(
        self: Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        ClaudeNativeSession::rollback_last_turn(&self, turn_id).await
    }
    fn background_tasks(&self) -> Vec<NativeBackgroundTask> {
        ClaudeNativeSession::background_tasks(self)
    }
    async fn stop_task(&self, task_id: &str) -> OpenBitFunResult<()> {
        ClaudeNativeSession::stop_task(self, task_id).await
    }
    async fn shutdown(&self) {
        ClaudeNativeSession::shutdown(self).await
    }
}

/// Stream-json user frame. Images become `image` content blocks after the
/// text; text-only input keeps the plain string content.
fn user_frame(
    claude_id: &str,
    uuid: &str,
    input: &NativeTurnInput,
    priority: Option<&str>,
) -> Value {
    let content = if input.images.is_empty() {
        Value::String(input.text.clone())
    } else {
        let mut blocks = Vec::with_capacity(input.images.len() + 1);
        if !input.text.trim().is_empty() {
            blocks.push(json!({"type":"text", "text":input.text}));
        }
        blocks.extend(input.images.iter().map(|image| {
            json!({"type":"image", "source":{"type":"base64",
                "media_type":image.media_type, "data":image.data}})
        }));
        Value::Array(blocks)
    };
    let mut frame = json!({"type":"user", "session_id":claude_id, "uuid":uuid,
        "parent_tool_use_id":null, "message":{"role":"user", "content":content}});
    if let Some(priority) = priority {
        frame["priority"] = json!(priority);
    }
    frame
}

/// `stop_task` control request (also allowed while a turn is running).
fn stop_task_request(task_id: &str) -> Value {
    json!({"subtype":"stop_task", "task_id":task_id})
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Task id of a `<task-notification>` the CLI injected into the main
/// conversation as a user message.
/// A `task_started` for a subagent (not a background shell or other task).
fn is_agent_task(frame: &Value) -> bool {
    match frame["task_type"].as_str() {
        Some(task_type) => task_type == "local_agent",
        None => frame["subagent_type"].is_string(),
    }
}

fn injected_notification_task(frame: &Value) -> Option<String> {
    if frame["type"] != "user" || !frame.get("parent_tool_use_id").is_none_or(Value::is_null) {
        return None;
    }
    let content = &frame["message"]["content"];
    let text = match content {
        Value::String(text) => text.as_str(),
        Value::Array(blocks) => blocks
            .iter()
            .find_map(|block| block["text"].as_str())
            .unwrap_or_default(),
        _ => return None,
    };
    let text = text.trim_start();
    if !text.starts_with("<task-notification>") {
        return None;
    }
    let start = text.find("<task-id>")? + "<task-id>".len();
    let end = start + text[start..].find("</task-id>")?;
    Some(text[start..end].trim().to_string()).filter(|id| !id.is_empty())
}

fn opens_autonomous_turn(frame: &Value) -> bool {
    frame.get("parent_tool_use_id").is_none_or(Value::is_null)
        && (frame["type"] == "assistant"
            || (frame["type"] == "stream_event" && frame["event"]["type"] == "message_start"))
}

fn denied(tool_id: &str, message: &str) -> Value {
    json!({"behavior":"deny", "message":message, "toolUseID":tool_id,
        "decisionClassification":"user_reject"})
}

fn allowed(tool_id: &str, input: Value, permanent: bool) -> Value {
    json!({"behavior":"allow", "updatedInput":input, "toolUseID":tool_id,
        "decisionClassification": if permanent { "user_permanent" } else { "user_temporary" }})
}

fn usd_cost(amount: f64) -> agent_client_protocol::schema::Cost {
    agent_client_protocol::schema::Cost::new(amount, "USD")
}

/// Permission updates Claude suggested for one scope. Session scope covers
/// `session`/`cliArg`; always scope covers the persisted settings files.
fn scoped_suggestions(suggestions: &Value, always: bool) -> Vec<Value> {
    suggestions
        .as_array()
        .into_iter()
        .flatten()
        .filter(|update| {
            matches!(
                (always, update["destination"].as_str()),
                (false, Some("session" | "cliArg"))
                    | (
                        true,
                        Some("userSettings" | "projectSettings" | "localSettings")
                    )
            )
        })
        .cloned()
        .collect()
}

/// `AskUserQuestion` answers keyed by question text, as Claude expects in
/// `updatedInput.answers`. The UI keys answers by question index (or text);
/// multi-select values are comma-joined like codexhost.
fn question_answers(input: &Value, answers: Option<&Value>) -> OpenBitFunResult<Value> {
    let questions = input["questions"]
        .as_array()
        .filter(|questions| !questions.is_empty())
        .ok_or_else(|| service_error("Claude question request has no questions"))?;
    let answers = answers
        .and_then(Value::as_object)
        .ok_or_else(|| service_error("Claude question answers are missing"))?;
    let mut out = serde_json::Map::new();
    for (index, question) in questions.iter().enumerate() {
        let text = question["question"]
            .as_str()
            .ok_or_else(|| service_error("Claude question has no text"))?;
        let answer = match answers
            .get(text)
            .or_else(|| answers.get(&index.to_string()))
        {
            Some(Value::String(answer)) => answer.trim().to_string(),
            Some(Value::Array(values)) => values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join(", "),
            _ => String::new(),
        };
        if answer.is_empty() {
            return Err(service_error(format!(
                "Claude question is unanswered: {text}"
            )));
        }
        out.insert(text.to_string(), Value::String(answer));
    }
    Ok(Value::Object(out))
}

/// Build the `can_use_tool` response for a UI decision.
fn permission_reply(
    pending: &PendingPermission,
    request: &SubmitAcpPermissionResponseRequest,
) -> OpenBitFunResult<Value> {
    let option = request.option_id.as_deref();
    let tool_id = pending.tool_id.as_str();
    match pending.kind {
        PermissionKind::Question => {
            let answer = match option {
                Some("allow_once") => true,
                Some("reject_once") => false,
                None => request.approve,
                Some(_) => return Err(service_error("Unknown Claude question option")),
            };
            if !answer {
                return Ok(denied(tool_id, "User cancelled the Question"));
            }
            let mut input = pending.input.clone();
            input["answers"] = question_answers(&pending.input, request.answers.as_ref())?;
            Ok(allowed(tool_id, input, false))
        }
        PermissionKind::PlanReview => {
            let approve = match option {
                Some(APPROVE_PLAN_OPTION | "allow_once") => true,
                Some(KEEP_PLANNING_OPTION | "reject_once") => false,
                None => request.approve,
                Some(_) => return Err(service_error("Unknown Claude plan review option")),
            };
            if !approve {
                let message = match request
                    .feedback
                    .as_deref()
                    .map(str::trim)
                    .filter(|feedback| !feedback.is_empty())
                {
                    Some(feedback) => {
                        format!("{KEEP_PLANNING_MESSAGE}\n\nUser feedback: {feedback}")
                    }
                    None => KEEP_PLANNING_MESSAGE.to_string(),
                };
                return Ok(denied(tool_id, &message));
            }
            if pending.input["plan"]
                .as_str()
                .is_none_or(|plan| plan.trim().is_empty())
            {
                return Err(service_error(
                    "Claude did not provide plan text; keep planning instead",
                ));
            }
            // Same wire shape as codexhost: the CLI itself leaves plan mode and
            // restores the pre-plan mode; the session only mirrors that.
            Ok(allowed(tool_id, pending.input.clone(), false))
        }
        PermissionKind::Approval => {
            // None = deny; Some(None) = once; Some(Some(always)) = scoped.
            let decision = match option {
                Some("allow_once") => Some(None),
                Some(ALLOW_SESSION_OPTION) => Some(Some(false)),
                Some("allow_always") => Some(Some(true)),
                Some("reject_once") => None,
                None => request.approve.then_some(None),
                Some(_) => return Err(service_error("Unknown Claude permission option")),
            };
            match decision {
                None => Ok(denied(tool_id, "User denied the Tool request")),
                Some(None) => Ok(allowed(tool_id, pending.input.clone(), false)),
                Some(Some(always)) => {
                    let updates = scoped_suggestions(&pending.suggestions, always);
                    if updates.is_empty() {
                        return Err(service_error("Claude did not offer this permission scope"));
                    }
                    let mut reply = allowed(tool_id, pending.input.clone(), true);
                    reply["updatedPermissions"] = Value::Array(updates);
                    Ok(reply)
                }
            }
        }
    }
}

async fn write_permission_reply(pending: &PendingPermission, reply: Value) -> OpenBitFunResult<()> {
    pending
        .process
        .write(
            &json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":pending.request_id, "response":reply}}),
        )
        .await
}

/// Settle a request nobody will answer (turn ended, delivery failed).
async fn cancel_permission(pending: PendingPermission) -> OpenBitFunResult<()> {
    let message = match pending.kind {
        PermissionKind::Question => "Claude Question is no longer pending",
        _ => "Claude Tool approval is no longer pending",
    };
    let reply = denied(&pending.tool_id, message);
    write_permission_reply(&pending, reply).await
}

/// `get_context_usage` response: `{totalTokens, maxTokens, model}`.
fn parse_context_usage(response: &Value) -> Option<(u64, u64)> {
    let used = response["totalTokens"].as_u64()?;
    let size = response["maxTokens"].as_u64().filter(|size| *size > 0)?;
    Some((used, size))
}

fn permission_payload(id: &str, session_id: &str, request: &Value) -> Value {
    let tool_name = request["tool_name"].as_str().unwrap_or("tool");
    let option =
        |id: &str, name: &str, kind: &str| json!({"optionId":id, "name":name, "kind":kind});
    let options = match PermissionKind::of(tool_name) {
        // The question card answers through `answers`; the bar only skips.
        PermissionKind::Question => vec![option("reject_once", "Skip", "reject_once")],
        // The plan review bar renders these with the plan and a feedback box.
        PermissionKind::PlanReview => vec![
            option(APPROVE_PLAN_OPTION, "Approve plan", "allow_once"),
            option(KEEP_PLANNING_OPTION, "Keep planning", "reject_once"),
        ],
        PermissionKind::Approval => {
            let suggestions = &request["permission_suggestions"];
            let mut options = vec![option("allow_once", "Allow once", "allow_once")];
            if !scoped_suggestions(suggestions, false).is_empty() {
                options.push(option(
                    ALLOW_SESSION_OPTION,
                    "Allow for this session",
                    "allow_session",
                ));
            }
            if !scoped_suggestions(suggestions, true).is_empty() {
                options.push(option("allow_always", "Always allow", "allow_always"));
            }
            options.push(option("reject_once", "Reject", "reject_once"));
            options
        }
    };
    json!({"permissionId":id, "sessionId":session_id, "toolCall":{
        "toolCallId":request["tool_use_id"], "title":request["display_name"].as_str().unwrap_or(tool_name),
        "kind":match native_tool_name(tool_name).as_str() { "ExecCommand" => "execute", "Read" => "read", "Write" | "Edit" => "edit", _ => "other" },
        "status":"pending", "rawInput":request["input"], "content":[]},
        "options":options})
}

fn read_lock(slot: &std::sync::Mutex<String>) -> String {
    slot.lock()
        .map(|value| value.clone())
        .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
}

fn set_flag(args: &mut [String], flag: &str, value: String) {
    if let Some(position) = args.iter().position(|arg| arg == flag) {
        if let Some(slot) = args.get_mut(position + 1) {
            *slot = value;
        }
    }
}

/// Launch flags for a thinking level. Adaptive thinking uses summarized
/// display so readable thinking streams (codexhost does the same).
fn thinking_cli_args(level: &str) -> Vec<String> {
    match level {
        "off" => vec!["--thinking".into(), "disabled".into()],
        // Not chosen: the CLI's own settings (alwaysThinkingEnabled,
        // effortLevel) and model defaults apply, as when running `claude`.
        "auto" => vec!["--thinking-display".into(), "summarized".into()],
        effort => vec![
            "--thinking".into(),
            "adaptive".into(),
            "--thinking-display".into(),
            "summarized".into(),
            "--effort".into(),
            effort.to_string(),
        ],
    }
}

/// Runtime flag settings for `apply_flag_settings` (codexhost wire shape).
fn thinking_flag_settings(level: &str) -> Value {
    match level {
        "off" => json!({"alwaysThinkingEnabled": false}),
        // Clear the flag layer so the user's settings apply again.
        "auto" => json!({"alwaysThinkingEnabled": null, "effortLevel": null}),
        effort => json!({"alwaysThinkingEnabled": true, "effortLevel": effort}),
    }
}

fn cli_args(id: &str, resume: bool, pending: Option<&PendingResume>) -> Vec<String> {
    let mut args: Vec<String> = [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--permission-prompt-tool",
        "stdio",
        "--permission-mode",
        "default",
        // Only makes bypass *selectable* at runtime; the session still starts
        // in the chosen mode (codexhost passes the same flag).
        "--allow-dangerously-skip-permissions",
        // Same sources as running `claude` in the workspace: project/local
        // settings carry `.claude/agents`, hooks, skills and permissions.
        // Fresh, resume and fork launches all go through here.
        "--setting-sources",
        "user,project,local",
        // Echo prompts with their transcript uuid (rollback/fork anchors).
        "--replay-user-messages",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    args.extend(resume_args(id, resume, pending));
    args
}

fn remote_command(path: &str, args: &[String]) -> String {
    let args = args
        .iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ");
    remote_user_shell_command(&format!(
        "cd {} && export {FILE_CHECKPOINT_ENV}=true && exec claude {args}",
        shell_escape(path)
    ))
}

/// Map one SDK `models` entry to the external-client option shape. The SDK
/// `value` is what `--model` and `set_model` accept; `displayName` is the label.
fn sdk_model_option(row: &Value) -> Option<AcpSessionModelOption> {
    let id = row.get("value")?.as_str()?.trim();
    if id.is_empty() {
        return None;
    }
    let name = row
        .get("displayName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(id)
        .to_string();
    let provider_name = row
        .get("resolvedModel")
        .and_then(Value::as_str)
        .map(str::to_string);
    let description = row
        .get("description")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some(AcpSessionModelOption {
        id: id.to_string(),
        name,
        provider_name,
        description,
    })
}

pub(crate) fn local_executable() -> PathBuf {
    #[cfg(windows)]
    let name = "claude.exe";
    #[cfg(not(windows))]
    let name = "claude";
    if let Some(home) = dirs::home_dir() {
        let path = home.join(".local").join("bin").join(name);
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from(name)
}

fn service_error(message: impl Into<String>) -> OpenBitFunError {
    OpenBitFunError::service(message.into())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
