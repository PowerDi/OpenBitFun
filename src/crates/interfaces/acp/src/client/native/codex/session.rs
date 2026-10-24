//! Session-scoped `codex app-server` JSON-RPC (newline-delimited) transport.
//!
//! One app-server per OpenBitFun session, launched lazily and released when
//! idle. The Codex thread id is persisted; the next launch resumes it. Server
//! requests (approvals) are answered on the same stdio pipe.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
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
    NativeAgentDriver, NativeAgentNotification, NativeAgentNotificationSink, NativeRollbackOutcome,
    NativeTurnInput,
};
use super::history::{CodexHistory, PendingFork, TurnAnchor};
use super::projector::CodexTurnProjector;
use crate::client::remote_shell::{remote_user_shell_command, shell_escape};
use crate::client::session_persistence::AcpSessionPersistence;
use crate::client::{
    AcpPlanEntry, AcpSessionConfigKind, AcpSessionConfigOption, AcpSessionConfigSelectOption,
    AcpSessionConfigValue, AcpSessionContextUsage, AcpSessionModelOption, AcpSessionOptions,
    SubmitAcpPermissionResponseRequest,
};

pub const CLIENT_ID: &str = "codex-native";
/// Follow the Codex CLI's own `config.toml` (no override is sent).
pub(super) const DEFAULT_ID: &str = "default";
pub(super) const STRATEGY_RESUME: &str = "codex-native-app-server";
pub(super) const STRATEGY_PREPARED: &str = "codex-native-prepared";
const PERMISSION_MODE_CONFIG_ID: &str = "permission-mode";
const THINKING_CONFIG_ID: &str = "thinking";
/// Codex approval presets (same catalog as the Codex TUI `/approvals`):
/// id, label, description. `default` sends no override.
const PERMISSION_MODES: &[(&str, &str, &str)] = &[
    (
        "default",
        "Default",
        "Use the approval policy and sandbox from Codex config.toml.",
    ),
    (
        "read-only",
        "Read only",
        "Read files only; ask before edits, commands or network.",
    ),
    (
        "auto",
        "Auto",
        "Edit and run commands in the workspace; ask before anything outside it.",
    ),
    (
        "full-access",
        "Full access",
        "No sandbox and no approvals. Use with care.",
    ),
];
/// Used until `model/list` reports the selected model's efforts.
const FALLBACK_EFFORTS: &[&str] = &["minimal", "low", "medium", "high", "xhigh"];
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// Stop an idle app-server (no turn or approval) after this long; the next
/// turn relaunches it and resumes the thread.
const IDLE_RELEASE_AFTER: Duration = Duration::from_secs(30 * 60);
const ALLOW_SESSION_OPTION: &str = "allow_session";

pub(super) struct RpcProcess {
    pub(super) id: String,
    writer: Mutex<Writer>,
    next_id: AtomicU64,
    pending: DashMap<u64, oneshot::Sender<Result<Value, Value>>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    pub(super) alive: AtomicBool,
    terminated: Mutex<Option<oneshot::Receiver<()>>>,
    exit: tokio::sync::watch::Receiver<Option<String>>,
    stderr: Mutex<VecDeque<String>>,
}

impl RpcProcess {
    pub(super) fn new(
        writer: Writer,
        shutdown: Option<oneshot::Sender<()>>,
        terminated: Option<oneshot::Receiver<()>>,
        exit: tokio::sync::watch::Receiver<Option<String>>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            writer: Mutex::new(writer),
            next_id: AtomicU64::new(1),
            pending: DashMap::new(),
            shutdown: Mutex::new(shutdown),
            alive: AtomicBool::new(true),
            terminated: Mutex::new(terminated),
            exit,
            stderr: Mutex::new(VecDeque::new()),
        }
    }

    async fn write(&self, frame: &Value) -> OpenBitFunResult<()> {
        super::super::process::write_json_line(
            &self.writer,
            frame,
            INTERRUPT_GRACE,
            "Codex stdin write timed out",
        )
        .await
    }

    /// JSON-RPC request; the error object of a failed call becomes the error text.
    pub(super) async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> OpenBitFunResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        let result = async {
            self.write(&json!({"id": id, "method": method, "params": params}))
                .await?;
            let reply = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| service_error(format!("Codex {method} timed out")))?
                .map_err(|_| service_error(format!("Codex exited during {method}")))?;
            reply.map_err(|error| {
                service_error(format!(
                    "Codex {method} failed: {}",
                    error["message"].as_str().unwrap_or(&error.to_string())
                ))
            })
        }
        .await;
        self.pending.remove(&id);
        result
    }

    async fn notify(&self, method: &str) -> OpenBitFunResult<()> {
        self.write(&json!({"method": method})).await
    }

    pub(super) async fn respond(&self, id: &Value, result: Value) -> OpenBitFunResult<()> {
        self.write(&json!({"id": id, "result": result})).await
    }

    async fn respond_error(&self, id: &Value, code: i64, message: &str) -> OpenBitFunResult<()> {
        self.write(&json!({"id": id, "error": {"code": code, "message": message}}))
            .await
    }

    /// Route a response frame to its waiting request.
    fn resolve(&self, frame: &Value) {
        let Some(id) = frame["id"].as_u64() else {
            return;
        };
        if let Some((_, tx)) = self.pending.remove(&id) {
            let _ = tx.send(match frame.get("error") {
                Some(error) if !error.is_null() => Err(error.clone()),
                _ => Ok(frame["result"].clone()),
            });
        }
    }

    pub(super) async fn kill(&self) {
        self.alive.store(false, Ordering::Release);
        self.pending.clear();
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }

    async fn stderr_tail(&self) -> String {
        self.stderr
            .lock()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// One approval request waiting for the user.
struct PendingApproval {
    process: Arc<RpcProcess>,
    request_id: Value,
    method: String,
    params: Value,
    turn_id: String,
}

#[derive(Default)]
struct TurnState {
    projector: Option<CodexTurnProjector>,
    index: usize,
    /// Codex turn id of the active turn, once known.
    codex_turn_id: Option<String>,
    process_id: Option<String>,
    /// Model reported by the thread (used for round labels).
    model: Option<String>,
    /// Spawned agent threads and pending parent wake-ups.
    threads: subagents::ChildThreads,
}

pub struct CodexNativeSession {
    session_id: String,
    workspace_path: String,
    remote_connection_id: Option<String>,
    /// Codex thread id once a thread exists (persisted as `acpRemoteSessionId`).
    thread_id: std::sync::Mutex<Option<String>>,
    process: Mutex<Option<Arc<RpcProcess>>>,
    /// Owner handle for detached work (subagent wake-ups).
    weak_self: std::sync::Weak<Self>,
    /// Process id whose app-server has the thread loaded.
    thread_process: std::sync::Mutex<Option<String>>,
    /// Serialize launch, submission and cancellation; output never takes this lock.
    lifecycle: Mutex<()>,
    state: Mutex<TurnState>,
    approvals: DashMap<String, PendingApproval>,
    settled: Notify,
    queue: Arc<EventQueue>,
    persistence: Option<(Arc<AcpSessionPersistence>, PathBuf)>,
    selected_model: std::sync::Mutex<String>,
    permission_mode: std::sync::Mutex<String>,
    effort: std::sync::Mutex<String>,
    /// `model/list` rows (non-hidden).
    models: std::sync::Mutex<Vec<Value>>,
    /// Model the thread runs when no override is selected.
    active_model: std::sync::Mutex<Option<String>>,
    /// Model and reasoning effort Codex applies when nothing is selected:
    /// `config/read` (config.toml layers), refined by the thread's report.
    config_defaults: std::sync::Mutex<ConfigDefaults>,
    notifier: Option<NativeAgentNotificationSink>,
    context_usage: std::sync::Mutex<Option<AcpSessionContextUsage>>,
    history: std::sync::Mutex<CodexHistory>,
    last_activity: std::sync::Mutex<tokio::time::Instant>,
    title_resolver: std::sync::Mutex<TitleResolver>,
    /// Every frame routed through this session (live protocol tests only).
    #[cfg(test)]
    frame_tap: std::sync::Mutex<Vec<Value>>,
}

fn lock<T>(slot: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    slot.lock().unwrap_or_else(|error| error.into_inner())
}

impl CodexNativeSession {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::client) fn new(
        session_id: String,
        thread_id: Option<String>,
        workspace_path: String,
        remote_connection_id: Option<String>,
        queue: Arc<EventQueue>,
        persistence: Option<(Arc<AcpSessionPersistence>, PathBuf)>,
        turn_index: usize,
        notifier: Option<NativeAgentNotificationSink>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak_self| Self {
            weak_self: weak_self.clone(),
            session_id,
            workspace_path,
            remote_connection_id,
            thread_id: std::sync::Mutex::new(thread_id),
            process: Mutex::new(None),
            thread_process: std::sync::Mutex::new(None),
            lifecycle: Mutex::new(()),
            state: Mutex::new(TurnState {
                index: turn_index,
                ..Default::default()
            }),
            approvals: DashMap::new(),
            settled: Notify::new(),
            queue,
            persistence,
            selected_model: std::sync::Mutex::new(DEFAULT_ID.to_string()),
            permission_mode: std::sync::Mutex::new(DEFAULT_ID.to_string()),
            effort: std::sync::Mutex::new(DEFAULT_ID.to_string()),
            models: std::sync::Mutex::new(Vec::new()),
            active_model: std::sync::Mutex::new(None),
            config_defaults: std::sync::Mutex::new(ConfigDefaults::default()),
            notifier,
            context_usage: std::sync::Mutex::new(None),
            history: std::sync::Mutex::new(CodexHistory::default()),
            last_activity: std::sync::Mutex::new(tokio::time::Instant::now()),
            title_resolver: std::sync::Mutex::new(title::default_resolver()),
            #[cfg(test)]
            frame_tap: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub(super) fn thread_id(&self) -> Option<String> {
        lock(&self.thread_id).clone()
    }

    pub(super) fn apply_history(&self, history: CodexHistory) {
        *lock(&self.history) = history;
    }

    pub(in crate::client) fn apply_preferences(&self, preferences: NativeAgentPreferences) {
        if let Some(model) = preferences.model.filter(|m| !m.trim().is_empty()) {
            *lock(&self.selected_model) = model;
        }
        if let Some(mode) = preferences
            .permission_mode
            .filter(|mode| PERMISSION_MODES.iter().any(|(id, _, _)| id == mode))
        {
            *lock(&self.permission_mode) = mode;
        }
        if let Some(effort) = preferences.thinking.filter(|e| !e.trim().is_empty()) {
            *lock(&self.effort) = effort;
        }
    }

    async fn remember(&self, update: impl Fn(&mut NativeAgentPreferences)) {
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .save_native_preference(CLIENT_ID, storage, &self.session_id, update)
                .await
            {
                log::warn!("Failed to remember Codex native preference: {error}");
            }
        }
    }

    fn touch(&self) {
        *lock(&self.last_activity) = tokio::time::Instant::now();
    }

    fn notify(&self, update: NativeAgentNotification) {
        if let Some(notifier) = &self.notifier {
            notifier(update);
        }
    }

    async fn enqueue(&self, events: Vec<AgenticEvent>) -> OpenBitFunResult<()> {
        super::super::events::enqueue(&self.queue, events, "Codex").await
    }

    async fn persist_history(&self) -> OpenBitFunResult<()> {
        let Some((persistence, storage)) = &self.persistence else {
            return Ok(());
        };
        let value = lock(&self.history).to_value();
        persistence
            .set_custom_metadata(
                storage,
                &self.session_id,
                super::history::METADATA_KEY,
                value,
            )
            .await
    }

    fn model_rows(&self) -> Vec<Value> {
        lock(&self.models).clone()
    }

    /// Model the next turn runs: the selection, else the thread's model,
    /// else the configured default (config.toml), else the catalog default.
    /// Without a selection no model is sent, so Codex applies its own config;
    /// this only mirrors that choice for display.
    fn effective_model(&self, rows: &[Value]) -> Option<String> {
        let selected = lock(&self.selected_model).clone();
        let known = |id: &str| rows.iter().any(|row| model_slug(row) == Some(id));
        if selected != DEFAULT_ID && known(&selected) {
            return Some(selected);
        }
        if let Some(active) = lock(&self.active_model).clone() {
            return Some(active);
        }
        if let Some(configured) = lock(&self.config_defaults).model.clone() {
            return Some(configured);
        }
        rows.iter()
            .find(|row| row["isDefault"] == true)
            .or_else(|| rows.first())
            .and_then(model_slug)
            .map(str::to_string)
    }

    pub fn options(&self) -> AcpSessionOptions {
        let rows = self.model_rows();
        let mut available_models: Vec<AcpSessionModelOption> =
            rows.iter().filter_map(model_option).collect();
        let current_model_id = self.effective_model(&rows);
        // A configured model outside the catalog (custom provider) is still
        // what runs; list it so the picker can show it.
        if let Some(current) = current_model_id.as_deref() {
            if !available_models.iter().any(|m| m.id == current) {
                available_models.insert(
                    0,
                    AcpSessionModelOption {
                        id: current.to_string(),
                        name: current.to_string(),
                        provider_name: None,
                        description: Some("Codex configured default.".to_string()),
                    },
                );
            }
        }
        AcpSessionOptions {
            current_model_id,
            available_models,
            model_config_id: None,
            config_options: self.config_options(&rows),
            context_usage: lock(&self.context_usage).clone(),
            capabilities: Some(super::CodexNativeDriver.capabilities()),
            background_tasks: Vec::new(),
        }
    }

    fn config_options(&self, rows: &[Value]) -> Vec<AcpSessionConfigOption> {
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
            .map(|(id, name, description)| AcpSessionConfigSelectOption {
                value: (*id).to_string(),
                name: (*name).to_string(),
                description: Some((*description).to_string()),
            })
            .collect();
        let default_effort = lock(&self.config_defaults).effort.clone();
        let mut efforts = vec![AcpSessionConfigSelectOption {
            value: DEFAULT_ID.to_string(),
            name: match &default_effort {
                Some(effort) => format!("Default ({})", effort_label(effort)),
                None => "Default".to_string(),
            },
            description: Some(
                "The reasoning effort Codex is configured with (config.toml), else the model default."
                    .to_string(),
            ),
        }];
        efforts.extend(
            self.supported_efforts(rows)
                .into_iter()
                .map(|(value, description)| AcpSessionConfigSelectOption {
                    name: effort_label(&value),
                    value,
                    description,
                }),
        );
        vec![
            select(
                PERMISSION_MODE_CONFIG_ID,
                "Permission mode",
                "mode",
                lock(&self.permission_mode).clone(),
                modes,
            ),
            select(
                THINKING_CONFIG_ID,
                "Reasoning effort",
                "thought_level",
                lock(&self.effort).clone(),
                efforts,
            ),
        ]
    }

    fn supported_efforts(&self, rows: &[Value]) -> Vec<(String, Option<String>)> {
        let model = self.effective_model(rows);
        let reported: Vec<(String, Option<String>)> = rows
            .iter()
            .find(|row| model.is_some() && model_slug(row) == model.as_deref())
            .and_then(|row| row["supportedReasoningEfforts"].as_array())
            .map(|efforts| {
                efforts
                    .iter()
                    .filter_map(|effort| {
                        Some((
                            effort["reasoningEffort"].as_str()?.to_string(),
                            effort["description"].as_str().map(str::to_string),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if reported.is_empty() {
            FALLBACK_EFFORTS
                .iter()
                .map(|effort| (effort.to_string(), None))
                .collect()
        } else {
            reported
        }
    }

    /// Permission mode and reasoning effort apply to the next `turn/start`.
    pub async fn set_config_option(
        &self,
        config_id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        let AcpSessionConfigValue::Select { value } = value else {
            return Err(OpenBitFunError::Validation(
                "Codex native options take a select value".into(),
            ));
        };
        match config_id {
            PERMISSION_MODE_CONFIG_ID => {
                if !PERMISSION_MODES.iter().any(|(id, _, _)| *id == value) {
                    return Err(OpenBitFunError::Validation(format!(
                        "Unsupported Codex permission mode: {value}"
                    )));
                }
                let previous = std::mem::replace(&mut *lock(&self.permission_mode), value.clone());
                if value == DEFAULT_ID && previous != DEFAULT_ID {
                    // Overrides persist on a loaded thread; reload it so the
                    // config.toml policy applies again.
                    self.release_thread_when_idle().await;
                }
                self.remember(|p| p.permission_mode = Some(value.clone()))
                    .await;
            }
            THINKING_CONFIG_ID => {
                let rows = self.model_rows();
                if value != DEFAULT_ID
                    && !self
                        .supported_efforts(&rows)
                        .iter()
                        .any(|(id, _)| *id == value)
                {
                    return Err(OpenBitFunError::Validation(format!(
                        "Unsupported Codex reasoning effort: {value}"
                    )));
                }
                *lock(&self.effort) = value.clone();
                self.remember(|p| p.thinking = Some(value.clone())).await;
            }
            other => {
                return Err(OpenBitFunError::Validation(format!(
                    "Unknown Codex native option: {other}"
                )))
            }
        }
        Ok(self.options())
    }

    /// Model selection applies to the next `turn/start` (Codex keeps it for
    /// later turns of the thread).
    pub async fn set_model(&self, model_id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        let rows = self.model_rows();
        // Picking the configured default when it is outside the catalog is the
        // same as not choosing: Codex keeps applying its own config.
        let model_id = if self.is_uncataloged_default(model_id) {
            DEFAULT_ID
        } else {
            model_id
        };
        if model_id != DEFAULT_ID && !rows.iter().any(|row| model_slug(row) == Some(model_id)) {
            return Err(OpenBitFunError::Validation(format!(
                "Unsupported Codex native model: {model_id}"
            )));
        }
        *lock(&self.selected_model) = model_id.to_string();
        // An effort the new model does not offer falls back to its default.
        let effort = lock(&self.effort).clone();
        if effort != DEFAULT_ID
            && !self
                .supported_efforts(&rows)
                .iter()
                .any(|(id, _)| *id == effort)
        {
            *lock(&self.effort) = DEFAULT_ID.to_string();
        }
        self.remember(|p| p.model = Some(model_id.to_string()))
            .await;
        Ok(self.options())
    }

    /// Drop the loaded thread between turns; the next turn resumes it.
    async fn release_thread_when_idle(&self) {
        let busy = {
            let state = self.state.lock().await;
            state.projector.is_some() || state.threads.any_running()
        };
        if busy || !self.approvals.is_empty() {
            return;
        }
        if let Some(process) = self.process.lock().await.take() {
            process.kill().await;
        }
        *lock(&self.thread_process) = None;
    }

    /// Launch the app-server and complete `initialize` + `model/list`, so the
    /// model list is available before the first turn.
    pub async fn warm_up(self: &Arc<Self>) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        if !self.model_rows().is_empty() {
            return Ok(());
        }
        self.ensure_process().await.map(|_| ())
    }

    async fn ensure_process(self: &Arc<Self>) -> OpenBitFunResult<Arc<RpcProcess>> {
        let mut slot = self.process.lock().await;
        if let Some(process) = slot.as_ref().filter(|p| p.alive.load(Ordering::Acquire)) {
            return Ok(process.clone());
        }
        if let Some(previous) = slot.as_ref() {
            let mut completion = previous.terminated.lock().await;
            if let Some(done) = completion.as_mut() {
                tokio::time::timeout(Duration::from_secs(5), done)
                    .await
                    .map_err(|_| service_error("Previous Codex process has not terminated"))?
                    .map_err(|_| service_error("Codex process supervisor was lost"))?;
                *completion = None;
            }
        }
        if self.remote_connection_id.is_none() {
            if let Some(message) = super::super::missing_local_workspace(
                &self.workspace_path,
                self.thread_id().is_some(),
            ) {
                return Err(service_error(message));
            }
        }
        let executable = super::cli::local_executable();
        let args = vec!["app-server".to_string()];
        let remote_command = remote_user_shell_command(&format!(
            "cd {} && exec codex app-server",
            shell_escape(&self.workspace_path)
        ));
        let io = super::super::process::spawn(super::super::process::ProcessConfig {
            client_id: CLIENT_ID,
            name: "Codex",
            executable: &executable,
            args: &args,
            workspace_path: &self.workspace_path,
            remote_connection_id: self.remote_connection_id.as_deref(),
            remote_command: &remote_command,
            env: &[],
        })
        .await?;
        let process = Arc::new(RpcProcess::new(
            io.writer,
            Some(io.shutdown),
            Some(io.terminated),
            io.exit,
        ));
        *slot = Some(process.clone());
        *lock(&self.thread_process) = None;
        let tail = process.clone();
        let stderr = io.stderr;
        let stderr_task = tokio::spawn(async move {
            super::super::process::collect_stderr(stderr, &tail.stderr).await;
        });
        let session = self.clone();
        let reader_process = process.clone();
        let reader = io.reader;
        tokio::spawn(async move {
            session.read_loop(reader_process, reader, stderr_task).await;
        });
        drop(slot);
        self.touch();
        self.spawn_idle_release(process.clone());
        if let Err(error) = self.initialize(&process).await {
            process.kill().await;
            let tail = process.stderr_tail().await;
            return Err(service_error(format!("{error}\n{tail}")));
        }
        Ok(process)
    }

    pub(super) async fn initialize(&self, process: &RpcProcess) -> OpenBitFunResult<()> {
        let response = process
            .request("initialize", initialize_params(), REQUEST_TIMEOUT)
            .await?;
        super::cli::check_user_agent(response["userAgent"].as_str())?;
        process.notify("initialized").await?;
        if let Err(error) = self.load_models(process).await {
            log::warn!("Codex model/list failed: {error}");
        }
        if let Err(error) = self.load_config_defaults(process).await {
            log::warn!("Codex config/read failed: {error}");
        }
        Ok(())
    }

    /// Read the effective config for the workspace (user plus project
    /// layers) to show which model and effort Codex applies by default.
    /// Only `model` and `model_reasoning_effort` are kept.
    async fn load_config_defaults(&self, process: &RpcProcess) -> OpenBitFunResult<()> {
        let response = process
            .request(
                "config/read",
                json!({"cwd": self.workspace_path}),
                REQUEST_TIMEOUT,
            )
            .await?;
        let config = &response["config"];
        let mut defaults = lock(&self.config_defaults);
        defaults.model = non_empty(&config["model"]);
        defaults.effort = non_empty(&config["model_reasoning_effort"]);
        Ok(())
    }

    /// Whether `model_id` is the model Codex runs without a selection and
    /// the catalog does not list it (e.g. a custom provider model).
    fn is_uncataloged_default(&self, model_id: &str) -> bool {
        let active = lock(&self.active_model).clone();
        let configured = lock(&self.config_defaults).model.clone();
        active.or(configured).as_deref() == Some(model_id)
            && !self
                .model_rows()
                .iter()
                .any(|row| model_slug(row) == Some(model_id))
    }

    async fn load_models(&self, process: &RpcProcess) -> OpenBitFunResult<()> {
        let mut rows = Vec::new();
        let mut cursor = Value::Null;
        // Bounded paging: the catalog is small; never loop on a bad cursor.
        for _ in 0..5 {
            let page = process
                .request(
                    "model/list",
                    json!({"cursor": cursor, "limit": 100, "includeHidden": false}),
                    REQUEST_TIMEOUT,
                )
                .await?;
            rows.extend(
                page["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|row| row["hidden"] != true)
                    .cloned(),
            );
            match page["nextCursor"].as_str() {
                Some(next) if !next.is_empty() => cursor = json!(next),
                _ => break,
            }
        }
        // A remembered model the CLI no longer offers falls back to default.
        let selected = lock(&self.selected_model).clone();
        if selected != DEFAULT_ID
            && !rows
                .iter()
                .any(|row| model_slug(row) == Some(selected.as_str()))
        {
            log::warn!("Remembered Codex model {selected} is unavailable; using default");
            *lock(&self.selected_model) = DEFAULT_ID.to_string();
        }
        *lock(&self.models) = rows;
        Ok(())
    }

    /// Load the thread into this app-server: apply a pending fork, resume the
    /// persisted thread, or start a new one.
    async fn ensure_thread(&self, process: &RpcProcess) -> OpenBitFunResult<String> {
        if let Some(thread) = self.thread_id() {
            if lock(&self.thread_process).as_deref() == Some(process.id.as_str()) {
                return Ok(thread);
            }
        }
        let pending = lock(&self.history).pending.clone();
        let base = self.thread_base_params();
        let (method, params) = match (&pending, self.thread_id()) {
            (Some(fork), _) => {
                let mut params = base;
                params["threadId"] = json!(fork.thread_id);
                if let Some(last) = &fork.last_turn_id {
                    params["lastTurnId"] = json!(last);
                }
                ("thread/fork", params)
            }
            (None, Some(thread)) => {
                let mut params = base;
                params["threadId"] = json!(thread);
                params["excludeTurns"] = json!(true);
                ("thread/resume", params)
            }
            (None, None) => ("thread/start", base),
        };
        let (method, response) = match process.request(method, params, REQUEST_TIMEOUT).await {
            // Codex writes no rollout until a turn starts, so a thread whose
            // first `turn/start` never landed cannot be resumed. With no
            // Codex turn recorded there is nothing to lose: start afresh.
            Err(error)
                if method == "thread/resume"
                    && error.to_string().contains("no rollout found")
                    && lock(&self.history)
                        .turns
                        .iter()
                        .all(|anchor| anchor.codex_turn_id.is_none()) =>
            {
                log::warn!(
                    "Codex thread has no rollout and no turns; starting a new thread: session_id={}",
                    self.session_id
                );
                let params = self.thread_base_params();
                (
                    "thread/start",
                    process
                        .request("thread/start", params, REQUEST_TIMEOUT)
                        .await?,
                )
            }
            result => (method, result?),
        };
        let thread = response["thread"]["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| service_error(format!("Codex {method} returned no thread id")))?
            .to_string();
        if let Some(model) = response["model"].as_str().filter(|m| !m.is_empty()) {
            *lock(&self.active_model) = Some(model.to_string());
            self.state.lock().await.model = Some(model.to_string());
        }
        if let Some(effort) = non_empty(&response["reasoningEffort"]) {
            lock(&self.config_defaults).effort = Some(effort);
        }
        if pending.is_some() {
            let turn_ids: Vec<String> = response["thread"]["turns"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|turn| turn["id"].as_str().map(str::to_string))
                .collect();
            let mut history = lock(&self.history);
            history.rebind_after_fork(&turn_ids);
            history.pending = None;
        }
        *lock(&self.thread_id) = Some(thread.clone());
        *lock(&self.thread_process) = Some(process.id.clone());
        if let Some((persistence, storage)) = &self.persistence {
            persistence
                .update_remote_session_state(
                    storage,
                    &self.session_id,
                    &thread,
                    STRATEGY_RESUME,
                    None,
                )
                .await?;
            if pending.is_some() {
                self.persist_history().await?;
            }
        }
        Ok(thread)
    }

    fn thread_base_params(&self) -> Value {
        let mut params = json!({"cwd": self.workspace_path});
        let model = lock(&self.selected_model).clone();
        if model != DEFAULT_ID {
            params["model"] = json!(model);
        }
        if let Some((approval, sandbox, _)) = permission_preset(&lock(&self.permission_mode)) {
            params["approvalPolicy"] = json!(approval);
            params["sandbox"] = json!(sandbox);
        }
        params
    }

    /// One watcher per process: release it once the session has been idle.
    fn spawn_idle_release(self: &Arc<Self>, process: Arc<RpcProcess>) {
        let session = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let deadline = match session.upgrade() {
                    Some(session) => *lock(&session.last_activity) + IDLE_RELEASE_AFTER,
                    None => return,
                };
                tokio::time::sleep_until(deadline).await;
                let Some(session) = session.upgrade() else {
                    return;
                };
                if !process.alive.load(Ordering::Acquire) {
                    return;
                }
                if *lock(&session.last_activity) + IDLE_RELEASE_AFTER > tokio::time::Instant::now()
                {
                    continue;
                }
                let _guard = session.lifecycle.lock().await;
                let busy = {
                    let state = session.state.lock().await;
                    state.projector.is_some() || state.threads.any_running()
                } || !session.approvals.is_empty();
                if !busy {
                    log::info!(
                        "Releasing idle Codex native process: session_id={}",
                        session.session_id
                    );
                    process.kill().await;
                    return;
                }
                session.touch();
            }
        });
    }

    pub(crate) fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        self.workspace_path == path && self.remote_connection_id.as_deref() == remote
    }

    /// A Codex thread records its cwd; once one exists the session keeps it.
    pub(crate) async fn workspace_bound(&self) -> bool {
        self.thread_id().is_some()
            || lock(&self.history).pending.is_some()
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
            return Err(service_error("Codex session already has an active turn"));
        }
        {
            // The user is driving again; queued notifications go into this turn.
            let mut state = self.state.lock().await;
            state.threads.wake_suppressed = false;
            state.threads.wake_reasons.clear();
        }
        let title_message = (self.state.lock().await.index == 0)
            .then(|| title::title_source(&input.text, original.as_deref()))
            .flatten();
        self.begin_turn(turn_id.clone(), input.text.clone(), original)
            .await?;
        let result = self.submit_turn(&turn_id, &input).await;
        match &result {
            Err(error) => self.fail_turn(&error.to_string()).await,
            Ok(()) => {
                if let Some(message) = title_message {
                    self.spawn_title_generation(message);
                }
            }
        }
        result
    }

    async fn submit_turn(
        self: &Arc<Self>,
        turn_id: &str,
        input: &NativeTurnInput,
    ) -> OpenBitFunResult<()> {
        let process = self.ensure_process().await?;
        self.state.lock().await.process_id = Some(process.id.clone());
        let thread = self.ensure_thread(&process).await?;
        let params = self.turn_params(&thread, input);
        let response = process
            .request("turn/start", params, REQUEST_TIMEOUT)
            .await?;
        let codex_turn = response["turn"]["id"].as_str().map(str::to_string);
        {
            let mut state = self.state.lock().await;
            // The response is authoritative for the turn it started.
            if codex_turn.is_some()
                && state
                    .projector
                    .as_ref()
                    .is_some_and(|p| p.turn_id() == turn_id)
            {
                state.codex_turn_id = codex_turn.clone();
            }
        }
        if let Some(anchor) = lock(&self.history).anchor_mut(turn_id) {
            anchor.codex_turn_id = codex_turn;
        }
        if let Err(error) = self.persist_history().await {
            log::warn!("Failed to persist Codex native history anchors: {error}");
        }
        Ok(())
    }

    fn turn_params(&self, thread: &str, input: &NativeTurnInput) -> Value {
        let mut params = json!({"threadId": thread, "input": user_input(input)});
        let model = lock(&self.selected_model).clone();
        if model != DEFAULT_ID {
            params["model"] = json!(model);
        }
        let effort = lock(&self.effort).clone();
        if effort != DEFAULT_ID {
            params["effort"] = json!(effort);
        }
        if let Some((approval, _, sandbox)) = permission_preset(&lock(&self.permission_mode)) {
            params["approvalPolicy"] = json!(approval);
            params["sandboxPolicy"] = sandbox;
        }
        params
    }

    async fn begin_turn(
        &self,
        turn_id: String,
        input: String,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        self.begin_turn_with(turn_id, input, original, None).await
    }

    async fn begin_turn_with(
        &self,
        turn_id: String,
        input: String,
        original: Option<String>,
        metadata: Option<Value>,
    ) -> OpenBitFunResult<()> {
        let mut state = self.state.lock().await;
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
        lock(&self.history).turns.push(TurnAnchor {
            turn_id: turn_id.clone(),
            turn_index: state.index - 1,
            codex_turn_id: None,
        });
        let mut projector =
            CodexTurnProjector::new(&self.session_id, &turn_id, state.model.as_deref());
        if manual_compaction {
            projector.mark_manual_compaction();
        }
        self.enqueue(projector.open_pending_round()).await?;
        state.projector = Some(projector);
        state.codex_turn_id = None;
        state.process_id = None;
        Ok(())
    }

    /// Title the session from its first user message in the background.
    fn spawn_title_generation(self: &Arc<Self>, message: String) {
        let session = self.clone();
        let resolver = lock(&self.title_resolver).clone();
        tokio::spawn(async move {
            let expected = match &session.persistence {
                Some((persistence, storage)) => {
                    match persistence
                        .load_metadata(storage, &session.session_id)
                        .await
                    {
                        Ok(Some(metadata)) => match title::expected_title(&metadata) {
                            Some(expected) => Some(expected),
                            None => return,
                        },
                        Ok(None) => return,
                        Err(error) => {
                            log::warn!("Codex session title skipped: {error}");
                            return;
                        }
                    }
                }
                None => None,
            };
            let resolved = resolver(message).await;
            if let (Some((persistence, storage)), Some(expected)) =
                (&session.persistence, expected.as_deref())
            {
                match persistence
                    .set_generated_title(storage, &session.session_id, expected, &resolved.title)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        log::warn!("Codex session title persistence failed: {error}");
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
                log::warn!("Codex session title event failed: {error}");
            }
        });
    }

    /// `turn/steer` adds input to the running Codex turn; the precondition
    /// `expectedTurnId` rejects it if that turn already ended.
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
        self.touch();
        let process = self
            .process
            .lock()
            .await
            .clone()
            .filter(|p| p.alive.load(Ordering::Acquire))
            .ok_or_else(|| service_error("Codex process is not running"))?;
        let (codex_turn, round_index) = {
            let state = self.state.lock().await;
            let projector = state
                .projector
                .as_ref()
                .filter(|p| p.turn_id() == turn_id)
                .ok_or_else(|| service_error("Codex turn is no longer running"))?;
            let codex_turn = state
                .codex_turn_id
                .clone()
                .ok_or_else(|| service_error("Codex turn has not started yet; retry shortly"))?;
            (codex_turn, projector.current_round_index())
        };
        let thread = self
            .thread_id()
            .ok_or_else(|| service_error("Codex thread is not loaded"))?;
        let steering_id = uuid::Uuid::new_v4().to_string();
        process
            .request(
                "turn/steer",
                json!({"threadId": thread, "input": user_input(&input), "expectedTurnId": codex_turn,
                    "clientUserMessageId": steering_id}),
                REQUEST_TIMEOUT,
            )
            .await?;
        self.enqueue(vec![AgenticEvent::UserSteeringInjected {
            session_id: self.session_id.clone(),
            turn_id: turn_id.to_string(),
            round_index,
            steering_id: steering_id.clone(),
            display_content: display.unwrap_or_else(|| input.text.clone()),
            content: input.text,
        }])
        .await?;
        Ok(steering_id)
    }

    /// `turn/interrupt`, then wait for `turn/completed { interrupted }`; a
    /// process that does not settle in time is stopped.
    pub async fn cancel(&self) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        let codex_turn = {
            let mut state = self.state.lock().await;
            if let Some(projector) = state.projector.as_mut() {
                projector.mark_cancel_requested();
            }
            state.codex_turn_id.clone()
        };
        let process = self.process.lock().await.clone();
        // Stop running subagents too and do not wake for them afterwards.
        self.interrupt_children(
            process
                .as_deref()
                .filter(|p| p.alive.load(Ordering::Acquire)),
        )
        .await;
        if let (Some(process), Some(turn), Some(thread)) = (
            process.as_ref().filter(|p| p.alive.load(Ordering::Acquire)),
            codex_turn,
            self.thread_id(),
        ) {
            let interrupted = tokio::time::timeout(INTERRUPT_GRACE, async {
                process
                    .request(
                        "turn/interrupt",
                        json!({"threadId": thread, "turnId": turn}),
                        INTERRUPT_GRACE,
                    )
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
            self.clear_approvals(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
        Ok(())
    }

    /// Drop the latest turn: the session continues on a fork of the thread
    /// through the previous turn (or a fresh thread). The source thread and
    /// workspace files are left untouched.
    pub async fn rollback_last_turn(
        self: &Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        let _guard = self.lifecycle.lock().await;
        if self.state.lock().await.projector.is_some() || !self.approvals.is_empty() {
            return Err(OpenBitFunError::Validation(
                "Wait until the Codex turn finishes before rolling back".into(),
            ));
        }
        let (target, previous) = {
            let history = lock(&self.history);
            let target = history
                .turns
                .last()
                .filter(|anchor| anchor.turn_id == turn_id)
                .cloned()
                .ok_or_else(|| {
                    OpenBitFunError::Validation(
                        "Only the latest Codex turn can be rolled back".into(),
                    )
                })?;
            let previous = history
                .turns
                .iter()
                .rev()
                .skip(1)
                .find_map(|anchor| anchor.codex_turn_id.clone());
            (target, previous)
        };
        if let Some((persistence, storage)) = &self.persistence {
            persistence
                .ensure_latest_turn(storage, &self.session_id, &target.turn_id)
                .await?;
        }
        let source = self.thread_id().or_else(|| {
            lock(&self.history)
                .pending
                .as_ref()
                .map(|p| p.thread_id.clone())
        });
        let fresh = previous.is_none() || source.is_none();
        {
            let mut history = lock(&self.history);
            history
                .turns
                .retain(|anchor| anchor.turn_index < target.turn_index);
            history.pending = match (target.codex_turn_id.is_some(), fresh) {
                // The dropped turn never reached Codex: nothing to branch.
                (false, _) => history.pending.take(),
                (true, true) => None,
                (true, false) => Some(PendingFork {
                    thread_id: source.clone().unwrap_or_default(),
                    last_turn_id: previous.clone(),
                }),
            };
        }
        if target.codex_turn_id.is_some() {
            *lock(&self.thread_process) = None;
            if fresh {
                *lock(&self.thread_id) = None;
            }
        }
        self.state.lock().await.index = target.turn_index;
        if let Some((persistence, storage)) = &self.persistence {
            if target.codex_turn_id.is_some() && fresh {
                persistence
                    .set_custom_metadata(
                        storage,
                        &self.session_id,
                        crate::client::session_persistence::CUSTOM_METADATA_REMOTE_SESSION_ID_KEY,
                        Value::Null,
                    )
                    .await?;
                persistence
                    .set_custom_metadata(
                        storage,
                        &self.session_id,
                        crate::client::session_persistence::CUSTOM_METADATA_RESUME_STRATEGY_KEY,
                        json!(STRATEGY_PREPARED),
                    )
                    .await?;
            }
            self.persist_history().await?;
            persistence
                .truncate_turns_from(storage, &self.session_id, target.turn_index)
                .await?;
        }
        Ok(NativeRollbackOutcome {
            files_changed: Vec::new(),
            files_rewound: false,
            note: Some(
                "Codex does not restore workspace files; only the conversation was rolled back."
                    .into(),
            ),
        })
    }

    async fn fail_turn(&self, error: &str) {
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            if let Err(error) = self.enqueue(projector.fail(error)).await {
                log::error!("Codex failure event delivery failed: {error}");
            }
            self.clear_approvals(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
    }

    /// Settle approvals nobody will answer (turn ended, process gone).
    async fn clear_approvals(&self, turn_id: &str) {
        let ids: Vec<String> = self
            .approvals
            .iter()
            .filter(|entry| entry.turn_id == turn_id)
            .map(|entry| entry.key().clone())
            .collect();
        for id in ids {
            if let Some((_, pending)) = self.approvals.remove(&id) {
                let _ = pending
                    .process
                    .respond(&pending.request_id, cancelled_reply(&pending.method))
                    .await;
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        if let Some(process) = self.process.lock().await.take() {
            process.kill().await;
        }
        self.approvals.clear();
    }

    async fn read_loop(
        self: Arc<Self>,
        process: Arc<RpcProcess>,
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
                        exit.borrow().clone().unwrap_or_else(|| "Codex process exited".into())
                    } else { "Codex process supervisor ended".into() };
                }
            };
            match line {
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(Some(line)) => match serde_json::from_str::<Value>(&line) {
                    Ok(frame) => {
                        self.touch();
                        if let Err(error) = self.route_frame(&process, frame).await {
                            break error.to_string();
                        }
                    }
                    // Not JSON-RPC: keep it as a diagnostic, not a fault.
                    Err(_) => log::debug!(
                        "Codex app-server non-JSON output: {}",
                        line.chars().take(200).collect::<String>()
                    ),
                },
                Ok(None) => break "Codex output stream closed".to_string(),
                Err(error) => break format!("Codex output read failed: {error}"),
            }
        };
        process.kill().await;
        if tokio::time::timeout(Duration::from_millis(250), &mut stderr_task)
            .await
            .is_err()
        {
            stderr_task.abort();
        }
        let tail = process.stderr_tail().await;
        // Approvals die with the process that asked.
        let stale: Vec<String> = self
            .approvals
            .iter()
            .filter(|entry| entry.process.id == process.id)
            .map(|entry| entry.key().clone())
            .collect();
        for id in stale {
            self.approvals.remove(&id);
        }
        {
            let mut state = self.state.lock().await;
            self.settle_children(&mut state, &process.id).await;
        }
        // Only the process that owns the active turn may fail it; an idle
        // exit stays silent and the next turn relaunches and resumes.
        let slot = self.process.lock().await;
        let owns_turn = self.state.lock().await.process_id.as_deref() == Some(process.id.as_str());
        if slot.as_ref().is_some_and(|p| p.id == process.id) && owns_turn {
            self.fail_turn(&format!("{error}\n{tail}")).await;
        }
    }

    pub(super) async fn route_frame(
        &self,
        process: &Arc<RpcProcess>,
        frame: Value,
    ) -> OpenBitFunResult<()> {
        #[cfg(test)]
        lock(&self.frame_tap).push(frame.clone());
        let method = frame["method"].as_str().map(str::to_string);
        match (method, frame.get("id").filter(|id| !id.is_null())) {
            (None, Some(_)) => {
                process.resolve(&frame);
                Ok(())
            }
            (Some(method), Some(id)) => {
                self.handle_server_request(process, id.clone(), &method, &frame["params"])
                    .await
            }
            (Some(method), None) => {
                self.handle_notification(process, &method, &frame["params"])
                    .await
            }
            (None, None) => Ok(()),
        }
    }

    async fn handle_notification(
        &self,
        process: &Arc<RpcProcess>,
        method: &str,
        params: &Value,
    ) -> OpenBitFunResult<()> {
        // Subagent threads share the app-server; only this thread is projected.
        if let (Some(thread), Some(current)) = (params["threadId"].as_str(), self.thread_id()) {
            if thread != current {
                return self
                    .child_notification(process, thread, method, params)
                    .await;
            }
        }
        if method == "serverRequest/resolved" {
            let request = &params["requestId"];
            self.approvals
                .retain(|_, pending| pending.request_id != *request);
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if state
            .process_id
            .as_deref()
            .is_some_and(|id| id != process.id)
            || state.projector.is_none()
        {
            return Ok(());
        }
        let notification_turn = params["turnId"]
            .as_str()
            .or_else(|| params["turn"]["id"].as_str())
            .map(str::to_string);
        match (&state.codex_turn_id, &notification_turn) {
            (Some(current), Some(turn)) if current != turn => return Ok(()),
            (None, Some(turn)) if method == "turn/started" => {
                state.codex_turn_id = Some(turn.clone())
            }
            // `thread/resume` replays the previous turn's usage before the
            // new turn starts: it restores the context meter but neither
            // names the active turn nor belongs to it.
            (None, Some(_)) => {
                if method == "thread/tokenUsage/updated" {
                    self.publish_context_usage(&params["tokenUsage"]);
                }
                return Ok(());
            }
            _ => {}
        }
        if method == "turn/plan/updated" {
            let entries = plan_entries(params);
            let turn_id = state
                .projector
                .as_ref()
                .map(|p| p.turn_id().to_string())
                .unwrap_or_default();
            self.notify(NativeAgentNotification::PlanUpdated { turn_id, entries });
            return Ok(());
        }
        if method == "thread/tokenUsage/updated" {
            self.publish_context_usage(&params["tokenUsage"]);
        }
        if method == "item/completed" {
            self.link_spawned(&mut state, &params["item"]).await?;
        }
        let projector = state.projector.as_mut().expect("active projector");
        let events = projector.consume(method, params);
        let finished = projector.is_finished();
        self.enqueue(events).await?;
        if finished {
            let projector = state.projector.take().expect("finished projector");
            state.codex_turn_id = None;
            state.threads.drop_orphans();
            drop(state);
            self.clear_approvals(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
        Ok(())
    }

    fn publish_context_usage(&self, usage: &Value) {
        let used = usage["last"]["totalTokens"].as_u64().unwrap_or(0);
        let Some(size) = usage["modelContextWindow"]
            .as_u64()
            .filter(|size| *size > 0)
        else {
            return;
        };
        let usage = AcpSessionContextUsage {
            used,
            size,
            cost: None,
        };
        *lock(&self.context_usage) = Some(usage.clone());
        self.notify(NativeAgentNotification::ContextUsageUpdated(usage));
    }

    async fn handle_server_request(
        &self,
        process: &Arc<RpcProcess>,
        id: Value,
        method: &str,
        params: &Value,
    ) -> OpenBitFunResult<()> {
        match method {
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                self.handle_approval(process, id, method, params).await
            }
            // OpenBitFun has no Codex question UI yet: answer with no
            // answers so the turn continues instead of blocking.
            "item/tool/requestUserInput" => process.respond(&id, json!({"answers": {}})).await,
            "mcpServer/elicitation/request" => {
                process
                    .respond(
                        &id,
                        json!({"action": "decline", "content": null, "_meta": null}),
                    )
                    .await
            }
            other => {
                log::debug!("Codex server request not supported: {other}");
                process
                    .respond_error(&id, -32601, &format!("OpenBitFun does not support {other}"))
                    .await
            }
        }
    }

    async fn handle_approval(
        &self,
        process: &Arc<RpcProcess>,
        request_id: Value,
        method: &str,
        params: &Value,
    ) -> OpenBitFunResult<()> {
        let item_id = params["itemId"].as_str().unwrap_or_default().to_string();
        let (tool_name, card_params, kind) = approval_card(method, params);
        // A subagent thread's request belongs to its child session's turn.
        let current_thread = self.thread_id();
        let target = {
            let mut guard = self.state.lock().await;
            let state = &mut *guard;
            let child = params["threadId"]
                .as_str()
                .filter(|thread| current_thread.as_deref() != Some(*thread))
                .and_then(|thread| state.threads.children.get_mut(thread));
            let target = match child {
                Some(child) => child
                    .projector
                    .as_mut()
                    .map(|projector| (projector, child.session_id.clone())),
                None => state
                    .projector
                    .as_mut()
                    .map(|projector| (projector, self.session_id.clone())),
            };
            match target {
                Some((projector, card_session)) => {
                    let events =
                        projector.permission_tool(&item_id, tool_name, card_params.clone());
                    let turn_id = projector.turn_id().to_string();
                    self.enqueue(events).await?;
                    Some((turn_id, card_session))
                }
                None => None,
            }
        };
        let Some((turn_id, card_session)) = target else {
            // No turn to show it in: refuse rather than block Codex.
            return process.respond(&request_id, cancelled_reply(method)).await;
        };
        let permission_id = format!("codex_native_permission_{}", uuid::Uuid::new_v4());
        let payload = permission_payload(
            &permission_id,
            &card_session,
            method,
            &item_id,
            tool_name,
            kind,
            &card_params,
            params,
        );
        self.approvals.insert(
            permission_id.clone(),
            PendingApproval {
                process: process.clone(),
                request_id: request_id.clone(),
                method: method.to_string(),
                params: params.clone(),
                turn_id,
            },
        );
        if let Err(error) = emit_global_event(BackendEvent::Custom {
            event_name: "backend-event-acppermissionrequest".into(),
            payload,
        })
        .await
        {
            if let Some((_, pending)) = self.approvals.remove(&permission_id) {
                let _ = pending
                    .process
                    .respond(&pending.request_id, cancelled_reply(method))
                    .await;
            }
            return Err(service_error(format!(
                "Codex approval delivery failed: {error}"
            )));
        }
        Ok(())
    }

    pub(crate) async fn submit_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> Option<OpenBitFunResult<()>> {
        let (_, pending) = self.approvals.remove(&request.permission_id)?;
        self.touch();
        let reply = match approval_reply(&pending.method, &pending.params, request) {
            Ok(reply) => reply,
            Err(error) => {
                self.approvals
                    .insert(request.permission_id.clone(), pending);
                return Some(Err(error));
            }
        };
        Some(pending.process.respond(&pending.request_id, reply).await)
    }
}

#[async_trait::async_trait]
impl super::super::NativeAgentSession for CodexNativeSession {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn options(&self) -> AcpSessionOptions {
        CodexNativeSession::options(self)
    }
    async fn warm_up(self: Arc<Self>) -> OpenBitFunResult<()> {
        CodexNativeSession::warm_up(&self).await
    }
    async fn start_turn(
        self: Arc<Self>,
        turn_id: String,
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        CodexNativeSession::start_turn(&self, turn_id, input, original).await
    }
    async fn steer(
        self: Arc<Self>,
        turn_id: &str,
        input: NativeTurnInput,
        display: Option<String>,
    ) -> OpenBitFunResult<String> {
        CodexNativeSession::steer(&self, turn_id, input, display).await
    }
    async fn cancel(&self) -> OpenBitFunResult<()> {
        CodexNativeSession::cancel(self).await
    }
    async fn set_model(&self, id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        CodexNativeSession::set_model(self, id).await
    }
    async fn set_config_option(
        &self,
        id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        CodexNativeSession::set_config_option(self, id, value).await
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
        Vec::new()
    }
    fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        CodexNativeSession::matches_scope(self, path, remote)
    }
    async fn workspace_bound(&self) -> bool {
        CodexNativeSession::workspace_bound(self).await
    }
    async fn rollback_last_turn(
        self: Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        CodexNativeSession::rollback_last_turn(&self, turn_id).await
    }
    async fn shutdown(&self) {
        CodexNativeSession::shutdown(self).await
    }
}

pub(super) fn initialize_params() -> Value {
    json!({
        "clientInfo": {"name": "openbitfun", "title": "OpenBitFun", "version": env!("CARGO_PKG_VERSION")},
        "capabilities": {"experimentalApi": false, "requestAttestation": false},
    })
}

#[derive(Default, Clone)]
struct ConfigDefaults {
    model: Option<String>,
    effort: Option<String>,
}

fn non_empty(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// Id `turn/start { model }` accepts: the model slug, else the row id.
fn model_slug(row: &Value) -> Option<&str> {
    row["model"]
        .as_str()
        .or_else(|| row["id"].as_str())
        .map(str::trim)
        .filter(|id| !id.is_empty())
}

fn model_option(row: &Value) -> Option<AcpSessionModelOption> {
    let id = model_slug(row)?;
    let name = row["displayName"]
        .as_str()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(id);
    Some(AcpSessionModelOption {
        id: id.to_string(),
        name: name.to_string(),
        provider_name: None,
        description: row["description"].as_str().map(str::to_string),
    })
}

fn effort_label(effort: &str) -> String {
    match effort {
        "xhigh" => "Extra high".to_string(),
        other => {
            let mut chars = other.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect())
                .unwrap_or_default()
        }
    }
}

/// `(approvalPolicy, thread sandbox mode, turn sandboxPolicy)` of a preset;
/// `None` for `default` (Codex config.toml applies).
fn permission_preset(mode: &str) -> Option<(&'static str, &'static str, Value)> {
    match mode {
        "read-only" => Some((
            "on-request",
            "read-only",
            json!({"type": "readOnly", "networkAccess": false}),
        )),
        "auto" => Some((
            "on-request",
            "workspace-write",
            json!({"type": "workspaceWrite", "writableRoots": [], "networkAccess": false,
                "excludeTmpdirEnvVar": false, "excludeSlashTmp": false}),
        )),
        "full-access" => Some((
            "never",
            "danger-full-access",
            json!({"type": "dangerFullAccess"}),
        )),
        _ => None,
    }
}

/// `UserInput[]`: the text, then images as data URLs (they travel over stdin,
/// so remote workspaces need no local file).
pub(super) fn user_input(input: &NativeTurnInput) -> Value {
    let mut items = Vec::with_capacity(input.images.len() + 1);
    // Exactly empty with no images: an input-less (wake-up) turn.
    if !input.text.trim().is_empty() || (input.images.is_empty() && !input.text.is_empty()) {
        items.push(json!({"type": "text", "text": input.text, "text_elements": []}));
    }
    items.extend(input.images.iter().map(|image| {
        json!({"type": "image", "url": format!("data:{};base64,{}", image.media_type, image.data)})
    }));
    Value::Array(items)
}

fn plan_entries(params: &Value) -> Vec<AcpPlanEntry> {
    params["plan"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|step| {
            Some(AcpPlanEntry {
                content: step["step"].as_str()?.to_string(),
                priority: "medium".to_string(),
                status: match step["status"].as_str() {
                    Some("inProgress") => "in_progress",
                    Some("completed") => "completed",
                    _ => "pending",
                }
                .to_string(),
            })
        })
        .collect()
}

/// Card name, params and ACP tool kind of an approval request.
fn approval_card(method: &str, params: &Value) -> (&'static str, Value, &'static str) {
    match method {
        "item/commandExecution/requestApproval" => (
            "ExecCommand",
            json!({"command": params["command"], "cwd": params["cwd"], "reason": params["reason"]}),
            "execute",
        ),
        "item/fileChange/requestApproval" => (
            "Edit",
            json!({"file_path": params["grantRoot"], "reason": params["reason"]}),
            "edit",
        ),
        _ => (
            "RequestPermissions",
            json!({"permissions": params["permissions"], "reason": params["reason"], "cwd": params["cwd"]}),
            "other",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn permission_payload(
    permission_id: &str,
    session_id: &str,
    method: &str,
    item_id: &str,
    tool_name: &str,
    kind: &str,
    card_params: &Value,
    params: &Value,
) -> Value {
    let option =
        |id: &str, name: &str, kind: &str| json!({"optionId": id, "name": name, "kind": kind});
    // `availableDecisions` (when sent) is the server's list for this prompt;
    // e.g. Codex omits `acceptForSession` for commands it cannot cache.
    let offered = |decision: &str| match params["availableDecisions"].as_array() {
        Some(decisions) => decisions.iter().any(|d| {
            d.as_str() == Some(decision) || d.as_object().is_some_and(|o| o.contains_key(decision))
        }),
        None => true,
    };
    let mut options = vec![option("allow_once", "Allow once", "allow_once")];
    if offered("acceptForSession") {
        options.push(option(
            ALLOW_SESSION_OPTION,
            "Allow for this session",
            "allow_session",
        ));
    }
    if method == "item/commandExecution/requestApproval"
        && !params["proposedExecpolicyAmendment"].is_null()
        && offered("acceptWithExecpolicyAmendment")
    {
        options.push(option("allow_always", "Always allow", "allow_always"));
    }
    options.push(option("reject_once", "Reject", "reject_once"));
    let title = match method {
        "item/commandExecution/requestApproval" => params["command"]
            .as_str()
            .unwrap_or("Run command")
            .to_string(),
        "item/fileChange/requestApproval" => params["reason"]
            .as_str()
            .unwrap_or("Apply file changes")
            .to_string(),
        _ => params["reason"]
            .as_str()
            .unwrap_or("Grant additional permissions")
            .to_string(),
    };
    json!({"permissionId": permission_id, "sessionId": session_id, "toolCall": {
        "toolCallId": item_id, "title": title, "kind": kind, "toolName": tool_name,
        "status": "pending", "rawInput": card_params, "content": []},
        "options": options})
}

/// Codex reply for a user decision (see `CommandExecutionApprovalDecision`,
/// `FileChangeApprovalDecision`, `PermissionsRequestApprovalResponse`).
pub(super) fn approval_reply(
    method: &str,
    params: &Value,
    request: &SubmitAcpPermissionResponseRequest,
) -> OpenBitFunResult<Value> {
    #[derive(PartialEq)]
    enum Choice {
        Once,
        Session,
        Always,
        Deny,
    }
    let choice = match request.option_id.as_deref() {
        Some("allow_once") => Choice::Once,
        Some(ALLOW_SESSION_OPTION) => Choice::Session,
        Some("allow_always") => Choice::Always,
        Some("reject_once") => Choice::Deny,
        None if request.approve => Choice::Once,
        None => Choice::Deny,
        Some(other) => {
            return Err(service_error(format!(
                "Unknown Codex approval option: {other}"
            )))
        }
    };
    Ok(match method {
        "item/permissions/requestApproval" => {
            let granted = match choice {
                Choice::Deny => json!({}),
                _ => {
                    let mut granted = serde_json::Map::new();
                    for key in ["network", "fileSystem"] {
                        let value = &params["permissions"][key];
                        if !value.is_null() {
                            granted.insert(key.to_string(), value.clone());
                        }
                    }
                    Value::Object(granted)
                }
            };
            let scope = if matches!(choice, Choice::Session | Choice::Always) {
                "session"
            } else {
                "turn"
            };
            json!({"permissions": granted, "scope": scope})
        }
        _ => {
            let decision = match choice {
                Choice::Once => json!("accept"),
                Choice::Session => json!("acceptForSession"),
                Choice::Always => match &params["proposedExecpolicyAmendment"] {
                    amendment
                        if !amendment.is_null()
                            && method == "item/commandExecution/requestApproval" =>
                    {
                        json!({"acceptWithExecpolicyAmendment": {"execpolicy_amendment": amendment}})
                    }
                    _ => {
                        return Err(service_error(
                            "Codex did not offer a persistent rule for this request",
                        ))
                    }
                },
                Choice::Deny => json!("decline"),
            };
            json!({"decision": decision})
        }
    })
}

/// Reply for a request nobody will answer (turn ended, delivery failed).
fn cancelled_reply(method: &str) -> Value {
    match method {
        "item/permissions/requestApproval" => json!({"permissions": {}, "scope": "turn"}),
        _ => json!({"decision": "cancel"}),
    }
}

fn service_error(message: impl Into<String>) -> OpenBitFunError {
    OpenBitFunError::service(message.into())
}

#[path = "subagents.rs"]
mod subagents;

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;

// SESSION_APPEND
