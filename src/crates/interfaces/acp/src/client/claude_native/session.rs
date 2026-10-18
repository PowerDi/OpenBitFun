//! Long-lived Claude Code SDK stdio transport, separate from ACP JSON-RPC.
//!
//! Like codexhost's SDK transport and paseo's query pump, process lifetime is
//! session-scoped, control replies are independent of turn output, and a result
//! settles a segment without closing the input stream or background workers.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use openbitfun_core::agentic::core::SessionContinuationPolicy;
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::infrastructure::events::{emit_global_event, BackendEvent};
use openbitfun_core::service::remote_ssh::workspace_state::get_remote_workspace_manager;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::{AgenticEvent, AgenticEventPriority};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Mutex, Notify};

use super::super::manager::{configure_process_group, terminate_child_process_tree};
use super::super::remote_shell::{remote_user_shell_command, shell_escape};
use super::super::session_persistence::{AcpSessionPersistence, ClaudeNativePreferences};
use super::super::{
    AcpSessionConfigKind, AcpSessionConfigOption, AcpSessionConfigSelectOption,
    AcpSessionConfigValue, AcpSessionModelOption, AcpSessionOptions,
    SubmitAcpPermissionResponseRequest,
};
use super::projector::{native_tool_name, ClaudeTurnProjector};

pub const CLIENT_ID: &str = "claude-native";
/// Follow the Claude CLI's own configured model (no `--model` flag).
pub const DEFAULT_MODEL_ID: &str = "default";
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
/// Thinking options: `auto` keeps adaptive thinking at the model default.
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
type Reader = Pin<Box<dyn AsyncRead + Send>>;
type Writer = Pin<Box<dyn AsyncWrite + Send>>;

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
        let mut bytes = serde_json::to_vec(frame).map_err(|e| service_error(e.to_string()))?;
        bytes.push(b'\n');
        tokio::time::timeout(INTERRUPT_GRACE, async {
            let mut writer = self.writer.lock().await;
            writer
                .write_all(&bytes)
                .await
                .map_err(|e| service_error(e.to_string()))?;
            writer
                .flush()
                .await
                .map_err(|e| service_error(e.to_string()))
        })
        .await
        .map_err(|_| service_error("Claude stdin write timed out"))?
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

struct PendingPermission {
    process: Arc<NativeProcess>,
    turn_id: String,
    request_id: String,
    tool_id: String,
    input: Value,
    suggestions: Value,
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
    pub(crate) claude_session_id: String,
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
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            claude_session_id,
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
        })
    }

    /// Seed model / permission mode / thinking from remembered choices.
    /// The model is re-validated once the SDK reports its catalog.
    pub(in crate::client) fn apply_preferences(&self, preferences: ClaudeNativePreferences) {
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

    async fn remember(&self, update: impl Fn(&mut ClaudeNativePreferences)) {
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .save_claude_preference(storage, &self.session_id, update)
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
            context_usage: None,
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
        *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = value.clone();
        if config_id == PERMISSION_MODE_CONFIG_ID {
            self.remember(|p| p.permission_mode = Some(value.clone())).await;
        } else {
            self.remember(|p| p.thinking = Some(value.clone())).await;
        }
        Ok(self.options())
    }

    /// Launch the CLI and complete `initialize` without sending a user turn, so
    /// the model list is available before the first message.
    pub async fn warm_up(self: &Arc<Self>) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
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
        self.remember(|p| p.model = Some(model_id.to_string())).await;
        Ok(self.options())
    }

    pub(crate) fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        self.workspace_path == path && self.remote_connection_id.as_deref() == remote
    }

    pub async fn start_turn(
        self: &Arc<Self>,
        turn_id: String,
        input: String,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
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
                    input,
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
        self.begin_turn(&mut state, turn_id, input.clone(), original, false)
            .await?;
        state.process_id = Some(process.id.clone());
        drop(state);
        let result: OpenBitFunResult<()> = async {
            if !process.alive.load(Ordering::Acquire) {
                return Err(service_error(
                    "Claude process exited before submission; retry to resume",
                ));
            }
            process
                .write(&json!({"type":"user", "session_id":self.claude_session_id,
                "parent_tool_use_id":null, "message":{"role":"user", "content":input}}))
                .await?;
            self.resume.store(true, Ordering::Release);
            if let Some((persistence, storage)) = &self.persistence {
                persistence
                    .update_remote_session_state(
                        storage,
                        &self.session_id,
                        &self.claude_session_id,
                        "claude-native-stream-json",
                        None,
                    )
                    .await?;
            }
            Ok(())
        }
        .await;
        drop(slot);
        if let Err(error) = &result {
            process.kill().await;
            self.fail_turn(&error.to_string()).await;
        }
        result
    }

    async fn begin_turn(
        &self,
        state: &mut TurnState,
        turn_id: String,
        input: String,
        original: Option<String>,
        autonomous: bool,
    ) -> OpenBitFunResult<()> {
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
        let mut projector =
            ClaudeTurnProjector::new(&self.session_id, &turn_id, state.model.as_deref());
        if !autonomous {
            // Show the waiting indicator immediately, before Claude streams.
            self.enqueue(projector.open_pending_round()).await?;
        }
        state.projector = Some(projector);
        state.autonomous = autonomous;
        state.streaming = false;
        state.process_id = None;
        Ok(())
    }

    async fn enqueue(&self, events: Vec<AgenticEvent>) -> OpenBitFunResult<()> {
        for event in events {
            // A native stream is already causally ordered. The legacy priority
            // heap must not put tool/terminal events ahead of their turn/round.
            self.queue
                .enqueue(event, Some(AgenticEventPriority::Normal))
                .await
                .map_err(|e| service_error(format!("Claude event enqueue failed: {e}")))?;
        }
        Ok(())
    }

    async fn ensure_process(self: &Arc<Self>) -> OpenBitFunResult<Arc<NativeProcess>> {
        let mut slot = self.process.lock().await;
        if let Some(process) = slot.as_ref().filter(|p| p.alive.load(Ordering::Acquire)) {
            return Ok(process.clone());
        }
        self.state.lock().await.autonomous_result_pending = false;
        let mut args = cli_args(&self.claude_session_id, self.resume.load(Ordering::Acquire));
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
        let (kill_tx, kill_rx) = oneshot::channel();
        let (terminated_tx, terminated_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = tokio::sync::watch::channel(None);
        let (writer, reader, stderr): (Writer, Reader, Reader) = if let Some(connection) =
            &self.remote_connection_id
        {
            let manager = get_remote_workspace_manager()
                .ok_or_else(|| service_error("Remote workspace manager is not initialized"))?;
            let ssh = manager
                .get_ssh_manager()
                .await
                .ok_or_else(|| service_error("SSH manager is unavailable for Claude native"))?;
            let command = remote_command(&self.workspace_path, &args);
            let transport = ssh
                .open_workspace_stdio(connection, &command)
                .await
                .map_err(|e| service_error(format!("Claude SSH launch failed: {e}")))?;
            let (writer, reader, stderr, control, completion) = transport.into_parts();
            tokio::spawn(async move {
                tokio::select! {
                    _ = kill_rx => {
                        if let Err(error) = control.kill().await {
                            log::warn!("Claude SSH process termination failed: {error}");
                        }
                        completion.wait().await;
                    }
                    exit = completion.clone().wait() => {
                        let _ = exit_tx.send(Some(format!("Claude SSH process exited: {:?}", exit.exit_code)));
                    }
                }
                let _ = terminated_tx.send(());
            });
            (Box::pin(writer), Box::pin(reader), Box::pin(stderr))
        } else {
            let mut command =
                openbitfun_core::util::process_manager::create_tokio_command(local_executable());
            configure_process_group(&mut command);
            command
                .args(&args)
                .current_dir(&self.workspace_path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            let mut child = command
                .spawn()
                .map_err(|e| service_error(format!("Claude native launch failed: {e}")))?;
            let writer = child
                .stdin
                .take()
                .ok_or_else(|| service_error("Claude stdin unavailable"))?;
            let reader = child
                .stdout
                .take()
                .ok_or_else(|| service_error("Claude stdout unavailable"))?;
            let stderr = child
                .stderr
                .take()
                .ok_or_else(|| service_error("Claude stderr unavailable"))?;
            tokio::spawn(async move {
                tokio::select! {
                    _ = kill_rx => terminate_child_process_tree(CLIENT_ID, child).await,
                    exit = child.wait() => {
                        let _ = exit_tx.send(Some(format!("Claude process exited: {exit:?}")));
                    }
                }
                let _ = terminated_tx.send(());
            });
            (Box::pin(writer), Box::pin(reader), Box::pin(stderr))
        };
        let process = Arc::new(NativeProcess {
            id: uuid::Uuid::new_v4().to_string(),
            writer: Mutex::new(writer),
            controls: DashMap::new(),
            shutdown: Mutex::new(Some(kill_tx)),
            alive: AtomicBool::new(true),
            terminated: Mutex::new(Some(terminated_rx)),
            exit: exit_rx,
            stderr: Mutex::new(VecDeque::new()),
        });
        *slot = Some(process.clone());
        let tail_process = process.clone();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut tail = tail_process.stderr.lock().await;
                // Bound diagnostics, not protocol frames or agent behavior.
                tail.push_back(line.chars().take(2048).collect());
                while tail.len() > 20 {
                    tail.pop_front();
                }
            }
        });
        let session = self.clone();
        let reader_process = process.clone();
        tokio::spawn(async move {
            session.read_loop(reader_process, reader, stderr_task).await;
        });
        drop(slot);
        match process
            .request(
                json!({"subtype":"initialize", "hooks":null, "forwardSubagentText":true}),
                Duration::from_secs(30),
            )
            .await
        {
            Ok(response) => {
                if let Some(models) = response["models"].as_array() {
                    if let Ok(mut cache) = self.sdk_models.lock() {
                        *cache = models.clone();
                    }
                    // A remembered model the CLI no longer offers falls back
                    // to the CLI default instead of failing the first turn.
                    let model = self.selected_model();
                    if model != DEFAULT_MODEL_ID
                        && !models.iter().any(|row| row["value"].as_str() == Some(model.as_str()))
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
                        if let Err(error) = self.route_frame(&process, frame).await {
                            break error.to_string();
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
        // Hold the process fence while failing its turn. An old reader must
        // never fail a turn submitted after a resume/restart.
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
            state.model = frame["model"].as_str().map(str::to_string);
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
            match frame["subtype"].as_str() {
                Some("task_started") => {
                    if let Some(tool_id) = frame["tool_use_id"].as_str() {
                        let tool_id = tool_id.to_string();
                        self.ensure_child(&mut state, &tool_id, &frame).await?;
                    }
                    return Ok(());
                }
                Some("task_notification") => {
                    if let Some(tool_id) = frame["tool_use_id"].as_str() {
                        let tool_id = tool_id.to_string();
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
        if state.projector.is_none() && opens_autonomous_turn(&frame) {
            self.begin_turn(
                &mut state,
                uuid::Uuid::new_v4().to_string(),
                String::new(),
                None,
                true,
            )
            .await?;
        }
        if state.projector.is_some() {
            state.process_id = Some(process.id.clone());
        }
        if frame["type"] == "stream_event" && frame["event"]["type"] == "message_start" {
            state.streaming = true;
        }
        if let Some(projector) = state.projector.as_mut() {
            let events = projector.consume(&frame);
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
            }
        }
        Ok(())
    }

    /// Create the child session for a subagent and link it to the parent
    /// `Task` card. Order mirrors the core agent: link first, then the turn.
    async fn ensure_child(
        &self,
        state: &mut TurnState,
        tool_id: &str,
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
                parent_tool_call_id: tool_id.to_string(),
                agent_type: subagent_type,
                model_id: None,
                focused_review_display_label: None,
                continuation_policy: Some(SessionContinuationPolicy::FreshOnly),
            },
            AgenticEvent::DialogTurnStarted {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                turn_index: 0,
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
                agent_id: info["task_id"]
                    .as_str()
                    .or_else(|| info["agent_id"].as_str())
                    .map(str::to_string),
            },
        );
        Ok(())
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
        if !state.children.contains_key(tool_id) {
            // Synchronous subagents may stream before (or without) task_started.
            let info = json!({"subagent_type": frame["subagent_type"],
                "description": frame["task_description"],
                "prompt": frame["task_description"],
                "agent_id": frame["agent_id"]});
            self.ensure_child(state, tool_id, &info).await?;
        }
        if let Some(child) = state.children.get_mut(tool_id) {
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
                let _ = answer_permission(pending, false, false).await;
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
        let option = request.option_id.as_deref();
        let allow = match option {
            Some("allow_once" | "allow_always") => true,
            Some("reject_once") => false,
            None => request.approve,
            Some(_) => {
                self.permissions
                    .insert(request.permission_id.clone(), pending);
                return Some(Err(service_error("Unknown Claude permission option")));
            }
        };
        Some(answer_permission(pending, allow, option == Some("allow_always")).await)
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
                let _ = answer_permission(pending, false, false).await;
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

    pub(crate) async fn shutdown(&self) {
        let _ = self.cancel().await;
        let _guard = self.lifecycle.lock().await;
        if let Some(process) = self.process.lock().await.as_ref() {
            process.kill().await;
        }
    }
}

fn opens_autonomous_turn(frame: &Value) -> bool {
    frame.get("parent_tool_use_id").is_none_or(Value::is_null)
        && (frame["type"] == "assistant"
            || (frame["type"] == "stream_event" && frame["event"]["type"] == "message_start"))
}

async fn answer_permission(
    pending: PendingPermission,
    allow: bool,
    always: bool,
) -> OpenBitFunResult<()> {
    let mut reply = if allow {
        json!({"behavior":"allow", "updatedInput":pending.input, "toolUseID":pending.tool_id})
    } else {
        json!({"behavior":"deny", "message":"Permission denied by OpenBitFun", "toolUseID":pending.tool_id})
    };
    if allow && always && pending.suggestions.is_array() {
        reply["updatedPermissions"] = pending.suggestions;
    }
    pending
        .process
        .write(
            &json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":pending.request_id, "response":reply}}),
        )
        .await
}

fn permission_payload(id: &str, session_id: &str, request: &Value) -> Value {
    json!({"permissionId":id, "sessionId":session_id, "toolCall":{
        "toolCallId":request["tool_use_id"], "title":request["display_name"].as_str().unwrap_or_else(|| request["tool_name"].as_str().unwrap_or("tool")),
        "kind":match native_tool_name(request["tool_name"].as_str().unwrap_or("tool")).as_str() { "ExecCommand" => "execute", "Read" => "read", "Write" | "Edit" => "edit", _ => "other" },
        "status":"pending", "rawInput":request["input"], "content":[]},
        "options":[{"optionId":"allow_once", "name":"Allow once", "kind":"allow_once"},
            {"optionId":"allow_always", "name":"Always allow", "kind":"allow_always"},
            {"optionId":"reject_once", "name":"Reject", "kind":"reject_once"}]})
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
        "auto" => vec![
            "--thinking".into(),
            "adaptive".into(),
            "--thinking-display".into(),
            "summarized".into(),
        ],
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
        "auto" => json!({"alwaysThinkingEnabled": true, "effortLevel": null}),
        effort => json!({"alwaysThinkingEnabled": true, "effortLevel": effort}),
    }
}

fn cli_args(id: &str, resume: bool) -> Vec<String> {
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
        "--setting-sources",
        "user",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    args.extend([
        if resume { "--resume" } else { "--session-id" }.into(),
        id.into(),
    ]);
    args
}

fn remote_command(path: &str, args: &[String]) -> String {
    let args = args
        .iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ");
    remote_user_shell_command(&format!("cd {} && exec claude {args}", shell_escape(path)))
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
