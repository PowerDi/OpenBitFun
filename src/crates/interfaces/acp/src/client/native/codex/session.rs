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
    NativeAgentDriver, NativeAgentNotification, NativeAgentNotificationSink,
    NativeRollbackOutcome, NativeTurnInput,
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
pub const DEFAULT_ID: &str = "default";
pub(super) const STRATEGY_RESUME: &str = "codex-native-app-server";
pub(super) const STRATEGY_PREPARED: &str = "codex-native-prepared";
const PERMISSION_MODE_CONFIG_ID: &str = "permission-mode";
const THINKING_CONFIG_ID: &str = "thinking";
/// Codex approval presets (same catalog as the Codex TUI `/approvals`):
/// id, label, description. `default` sends no override.
const PERMISSION_MODES: &[(&str, &str, &str)] = &[
    ("default", "Default", "Use the approval policy and sandbox from Codex config.toml."),
    ("read-only", "Read only", "Read files only; ask before edits, commands or network."),
    ("auto", "Auto", "Edit and run commands in the workspace; ask before anything outside it."),
    ("full-access", "Full access", "No sandbox and no approvals. Use with care."),
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
        let Some(id) = frame["id"].as_u64() else { return };
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
        self.stderr.lock().await.iter().cloned().collect::<Vec<_>>().join("\n")
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
}

pub struct CodexNativeSession {
    session_id: String,
    workspace_path: String,
    remote_connection_id: Option<String>,
    /// Codex thread id once a thread exists (persisted as `acpRemoteSessionId`).
    thread_id: std::sync::Mutex<Option<String>>,
    process: Mutex<Option<Arc<RpcProcess>>>,
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
    notifier: Option<NativeAgentNotificationSink>,
    context_usage: std::sync::Mutex<Option<AcpSessionContextUsage>>,
    history: std::sync::Mutex<CodexHistory>,
    last_activity: std::sync::Mutex<tokio::time::Instant>,
    title_resolver: std::sync::Mutex<TitleResolver>,
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
        Arc::new(Self {
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
            notifier,
            context_usage: std::sync::Mutex::new(None),
            history: std::sync::Mutex::new(CodexHistory::default()),
            last_activity: std::sync::Mutex::new(tokio::time::Instant::now()),
            title_resolver: std::sync::Mutex::new(title::default_resolver()),
        })
    }

    pub(super) fn thread_id(&self) -> Option<String> {
        lock(&self.thread_id).clone()
    }

    pub(in crate::client) fn apply_history(&self, history: CodexHistory) {
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
            .set_custom_metadata(storage, &self.session_id, super::history::METADATA_KEY, value)
            .await
    }

    fn model_rows(&self) -> Vec<Value> {
        lock(&self.models).clone()
    }

    /// Model the next turn runs: the selection, else the thread's model,
    /// else the catalog default.
    fn effective_model(&self, rows: &[Value]) -> Option<String> {
        let selected = lock(&self.selected_model).clone();
        let known = |id: &str| rows.iter().any(|row| model_slug(row) == Some(id));
        if selected != DEFAULT_ID && known(&selected) {
            return Some(selected);
        }
        if let Some(active) = lock(&self.active_model).clone().filter(|m| known(m)) {
            return Some(active);
        }
        rows.iter()
            .find(|row| row["isDefault"] == true)
            .or_else(|| rows.first())
            .and_then(model_slug)
            .map(str::to_string)
    }

    pub fn options(&self) -> AcpSessionOptions {
        let rows = self.model_rows();
        let available_models: Vec<AcpSessionModelOption> =
            rows.iter().filter_map(model_option).collect();
        AcpSessionOptions {
            current_model_id: self.effective_model(&rows),
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
        let mut efforts = vec![AcpSessionConfigSelectOption {
            value: DEFAULT_ID.to_string(),
            name: "Default".to_string(),
            description: Some("The model's default reasoning effort.".to_string()),
        }];
        efforts.extend(self.supported_efforts(rows).into_iter().map(|(value, description)| {
            AcpSessionConfigSelectOption {
                name: effort_label(&value),
                value,
                description,
            }
        }));
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
                self.remember(|p| p.permission_mode = Some(value.clone())).await;
            }
            THINKING_CONFIG_ID => {
                let rows = self.model_rows();
                if value != DEFAULT_ID
                    && !self.supported_efforts(&rows).iter().any(|(id, _)| *id == value)
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
        if model_id != DEFAULT_ID && !rows.iter().any(|row| model_slug(row) == Some(model_id)) {
            return Err(OpenBitFunError::Validation(format!(
                "Unsupported Codex native model: {model_id}"
            )));
        }
        *lock(&self.selected_model) = model_id.to_string();
        // An effort the new model does not offer falls back to its default.
        let effort = lock(&self.effort).clone();
        if effort != DEFAULT_ID && !self.supported_efforts(&rows).iter().any(|(id, _)| *id == effort) {
            *lock(&self.effort) = DEFAULT_ID.to_string();
        }
        self.remember(|p| p.model = Some(model_id.to_string())).await;
        Ok(self.options())
    }

    /// Drop the loaded thread between turns; the next turn resumes it.
    async fn release_thread_when_idle(&self) {
        if self.state.lock().await.projector.is_some() || !self.approvals.is_empty() {
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
            if let Some(message) =
                super::super::missing_local_workspace(&self.workspace_path, self.thread_id().is_some())
            {
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
        Ok(())
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
        if selected != DEFAULT_ID && !rows.iter().any(|row| model_slug(row) == Some(selected.as_str())) {
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
        let response = process.request(method, params, REQUEST_TIMEOUT).await?;
        let thread = response["thread"]["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| service_error(format!("Codex {method} returned no thread id")))?
            .to_string();
        if let Some(model) = response["model"].as_str().filter(|m| !m.is_empty()) {
            *lock(&self.active_model) = Some(model.to_string());
            self.state.lock().await.model = Some(model.to_string());
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
                .update_remote_session_state(storage, &self.session_id, &thread, STRATEGY_RESUME, None)
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
                let Some(session) = session.upgrade() else { return };
                if !process.alive.load(Ordering::Acquire) {
                    return;
                }
                if *lock(&session.last_activity) + IDLE_RELEASE_AFTER > tokio::time::Instant::now() {
                    continue;
                }
                let _guard = session.lifecycle.lock().await;
                let busy = session.state.lock().await.projector.is_some()
                    || !session.approvals.is_empty();
                if !busy {
                    log::info!("Releasing idle Codex native process: session_id={}", session.session_id);
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
        let title_message = (self.state.lock().await.index == 0)
            .then(|| title::title_source(&input.text, original.as_deref()))
            .flatten();
        self.begin_turn(turn_id.clone(), input.text.clone(), original).await?;
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

    async fn submit_turn(self: &Arc<Self>, turn_id: &str, input: &NativeTurnInput) -> OpenBitFunResult<()> {
        let process = self.ensure_process().await?;
        self.state.lock().await.process_id = Some(process.id.clone());
        let thread = self.ensure_thread(&process).await?;
        let params = self.turn_params(&thread, input);
        let response = process.request("turn/start", params, REQUEST_TIMEOUT).await?;
        let codex_turn = response["turn"]["id"].as_str().map(str::to_string);
        {
            let mut state = self.state.lock().await;
            if state.codex_turn_id.is_none()
                && state.projector.as_ref().is_some_and(|p| p.turn_id() == turn_id)
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

    async fn begin_turn(&self, turn_id: String, input: String, original: Option<String>) -> OpenBitFunResult<()> {
        let mut state = self.state.lock().await;
        let manual_compaction = input.split_whitespace().next() == Some("/compact");
        self.enqueue(vec![AgenticEvent::DialogTurnStarted {
            session_id: self.session_id.clone(),
            turn_id: turn_id.clone(),
            turn_index: state.index,
            user_input: input,
            original_user_input: original,
            user_message_metadata: None,
        }])
        .await?;
        state.index += 1;
        lock(&self.history).turns.push(TurnAnchor {
            turn_id: turn_id.clone(),
            turn_index: state.index - 1,
            codex_turn_id: None,
        });
        let mut projector = CodexTurnProjector::new(&self.session_id, &turn_id, state.model.as_deref());
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
                    match persistence.load_metadata(storage, &session.session_id).await {
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
            if let (Some((persistence, storage)), Some(expected)) = (&session.persistence, expected.as_deref()) {
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

    // SESSION_STEER
}

// SESSION_APPEND
