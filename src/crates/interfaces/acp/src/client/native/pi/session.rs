//! Long-lived `pi --mode rpc` stdio transport (JSONL commands, `response`
//! records correlated by `id`, and session events; see Pi `docs/rpc.md`).
//!
//! The process is session-scoped. A turn starts with `prompt`, streams
//! events, and ends at `agent_settled`; `steer` folds a message into the
//! running turn; `abort` cancels it. Tool approvals arrive as dialogs of the
//! OpenBitFun approval extension (see `approval.rs`).
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
    NativeAgentNotification, NativeAgentNotificationSink, NativeRollbackOutcome, NativeTurnInput,
};
use super::approval::{self, ApprovalRequest};
use super::history::{PendingFork, PiHistory, TurnAnchor};
use super::projector::PiTurnProjector;
use super::subagents::{is_subagent_frame, ChildRecord, PiSubagentRuns};
use crate::client::remote_shell::{remote_user_shell_command, shell_escape};
use crate::client::session_persistence::AcpSessionPersistence;
use crate::client::{
    AcpAvailableCommand, AcpSessionConfigKind, AcpSessionConfigOption,
    AcpSessionConfigSelectOption, AcpSessionConfigValue, AcpSessionContextUsage,
    AcpSessionModelOption, AcpSessionOptions, SubmitAcpPermissionResponseRequest,
};

pub const CLIENT_ID: &str = "pi-native";
const THINKING_CONFIG_ID: &str = "thinking";
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
/// `prompt` responds once the prompt is accepted; image uploads can be slow.
const PROMPT_TIMEOUT: Duration = Duration::from_secs(60);
/// `abort` waits for Pi to go idle before responding.
const ABORT_GRACE: Duration = Duration::from_secs(10);

pub(super) struct PiProcess {
    pub(super) id: String,
    writer: Mutex<Writer>,
    pending: DashMap<String, oneshot::Sender<Value>>,
    next_id: AtomicU64,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    pub(super) alive: AtomicBool,
    terminated: Mutex<Option<oneshot::Receiver<()>>>,
    exit: tokio::sync::watch::Receiver<Option<String>>,
    stderr: Mutex<VecDeque<String>>,
}

impl PiProcess {
    pub(super) fn new(
        writer: Writer,
        shutdown: Option<oneshot::Sender<()>>,
        terminated: Option<oneshot::Receiver<()>>,
        exit: tokio::sync::watch::Receiver<Option<String>>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            writer: Mutex::new(writer),
            pending: DashMap::new(),
            next_id: AtomicU64::new(1),
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
            WRITE_TIMEOUT,
            "Pi stdin write timed out",
        )
        .await
    }

    /// Send one command and await its `response`. `data` is returned on
    /// success; `success:false` becomes an error carrying Pi's message.
    pub(super) async fn request(
        &self,
        mut command: Value,
        timeout: Option<Duration>,
    ) -> OpenBitFunResult<Value> {
        let id = format!("obf-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        command["id"] = Value::String(id.clone());
        let kind = command["type"].as_str().unwrap_or("command").to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id.clone(), tx);
        let result = async {
            self.write(&command).await?;
            let response = match timeout {
                Some(limit) => tokio::time::timeout(limit, rx)
                    .await
                    .map_err(|_| service_error(format!("Pi {kind} request timed out")))?,
                None => rx.await,
            }
            .map_err(|_| service_error(format!("Pi process exited during {kind}")))?;
            if response["success"] != true {
                return Err(service_error(
                    response["error"]
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("Pi {kind} failed")),
                ));
            }
            Ok(response.get("data").cloned().unwrap_or(Value::Null))
        }
        .await;
        self.pending.remove(&id);
        result
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

    pub(super) async fn kill(&self) {
        self.alive.store(false, Ordering::Release);
        self.pending.clear();
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

/// One approval dialog waiting for the user.
struct PendingApproval {
    process: Arc<PiProcess>,
    request: ApprovalRequest,
    turn_id: String,
}

/// A steering message sent with `steer` and not yet echoed by Pi.
#[derive(Debug, Clone)]
struct PendingSteer {
    id: String,
    text: String,
    display: Option<String>,
}

#[derive(Default)]
struct TurnState {
    projector: Option<PiTurnProjector>,
    index: usize,
    process_id: Option<String>,
    steers: VecDeque<PendingSteer>,
    /// `subagent` extension runs of the active turn, as child sessions.
    subagents: PiSubagentRuns,
}

pub struct PiNativeSession {
    session_id: String,
    workspace_path: String,
    remote_connection_id: Option<String>,
    /// Pi holds a transcript for `active_pi_session` (a prompt reached it).
    resume: AtomicBool,
    process: Mutex<Option<Arc<PiProcess>>>,
    /// Serializes launch, submission and cancellation; output never takes it.
    lifecycle: Mutex<()>,
    state: Mutex<TurnState>,
    settled: Notify,
    queue: Arc<EventQueue>,
    persistence: Option<(Arc<AcpSessionPersistence>, PathBuf)>,
    /// Remembered `provider/id`, applied after launch when Pi offers it.
    selected_model: std::sync::Mutex<Option<String>>,
    /// Remembered thinking level, applied after launch.
    selected_thinking: std::sync::Mutex<Option<String>>,
    /// Pi `get_available_models` rows and the active `Model` object.
    models: std::sync::Mutex<Vec<Value>>,
    current_model: std::sync::Mutex<Option<Value>>,
    thinking_levels: std::sync::Mutex<Vec<String>>,
    thinking: std::sync::Mutex<Option<String>>,
    commands: std::sync::Mutex<Vec<AcpAvailableCommand>>,
    context_usage: std::sync::Mutex<Option<AcpSessionContextUsage>>,
    notifier: Option<NativeAgentNotificationSink>,
    title_resolver: std::sync::Mutex<TitleResolver>,
    /// Pi session id the running process has open (changes on fork/rollback).
    active_pi_session: std::sync::Mutex<String>,
    permission_mode: std::sync::Mutex<String>,
    approvals: DashMap<String, PendingApproval>,
    /// Calls the user allowed for this session (see `ApprovalRequest::session_key`).
    session_grants: std::sync::Mutex<std::collections::HashSet<String>>,
    history: std::sync::Mutex<PiHistory>,
    /// The running process loaded the approval extension (seen in `get_commands`).
    bridge_loaded: AtomicBool,
    /// Serializes `get_entries` syncs so anchors are attributed in order.
    entry_sync: Mutex<()>,
    /// Every frame routed through this session (live protocol tests only).
    #[cfg(test)]
    frame_tap: std::sync::Mutex<Vec<Value>>,
    /// Extension path launched instead of the bridge (live tests only).
    #[cfg(test)]
    extension_override: std::sync::Mutex<Option<PathBuf>>,
}

fn lock<T>(slot: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl PiNativeSession {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::client) fn new(
        session_id: String,
        pi_session_id: String,
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
            active_pi_session: std::sync::Mutex::new(pi_session_id),
            workspace_path,
            remote_connection_id,
            resume: AtomicBool::new(resume),
            process: Mutex::new(None),
            lifecycle: Mutex::new(()),
            state: Mutex::new(TurnState {
                index: turn_index,
                ..Default::default()
            }),
            settled: Notify::new(),
            queue,
            persistence,
            selected_model: std::sync::Mutex::new(None),
            selected_thinking: std::sync::Mutex::new(None),
            models: std::sync::Mutex::new(Vec::new()),
            current_model: std::sync::Mutex::new(None),
            thinking_levels: std::sync::Mutex::new(Vec::new()),
            thinking: std::sync::Mutex::new(None),
            commands: std::sync::Mutex::new(Vec::new()),
            context_usage: std::sync::Mutex::new(None),
            notifier,
            title_resolver: std::sync::Mutex::new(title::default_resolver()),
            permission_mode: std::sync::Mutex::new(approval::MODE_ASK.to_string()),
            approvals: DashMap::new(),
            session_grants: std::sync::Mutex::new(Default::default()),
            history: std::sync::Mutex::new(PiHistory::default()),
            bridge_loaded: AtomicBool::new(false),
            entry_sync: Mutex::new(()),
            #[cfg(test)]
            frame_tap: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            extension_override: std::sync::Mutex::new(None),
        })
    }

    pub(crate) fn apply_preferences(&self, preferences: NativeAgentPreferences) {
        *lock(&self.selected_model) = preferences.model;
        *lock(&self.selected_thinking) = preferences.thinking;
        if let Some(mode) = preferences.permission_mode.filter(|mode| {
            approval::PERMISSION_MODES
                .iter()
                .any(|(id, _, _)| id == mode)
        }) {
            *lock(&self.permission_mode) = mode;
        }
    }

    pub(super) fn apply_history(&self, history: PiHistory) {
        *lock(&self.history) = history;
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

    fn notify(&self, update: NativeAgentNotification) {
        if let Some(notifier) = &self.notifier {
            notifier(update);
        }
    }

    async fn remember(&self, update: impl Fn(&mut NativeAgentPreferences)) {
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .save_native_preference(CLIENT_ID, storage, &self.session_id, update)
                .await
            {
                log::warn!("Failed to remember Pi native preference: {error}");
            }
        }
    }

    async fn enqueue(&self, events: Vec<AgenticEvent>) -> OpenBitFunResult<()> {
        super::super::events::enqueue(&self.queue, events, "Pi").await
    }

    fn live_process(slot: &Option<Arc<PiProcess>>) -> Option<Arc<PiProcess>> {
        slot.clone().filter(|p| p.alive.load(Ordering::Acquire))
    }

    /// `provider/id` of a Pi `Model` object.
    fn model_key(model: &Value) -> Option<String> {
        Some(format!(
            "{}/{}",
            model["provider"].as_str()?,
            model["id"].as_str()?
        ))
    }

    fn context_window(&self) -> Option<u64> {
        lock(&self.current_model)
            .as_ref()
            .and_then(|model| model["contextWindow"].as_u64())
    }

    fn model_name(&self) -> Option<String> {
        lock(&self.current_model)
            .as_ref()
            .and_then(|model| model["id"].as_str().map(str::to_string))
    }

    pub fn options(&self) -> AcpSessionOptions {
        let available_models: Vec<AcpSessionModelOption> = lock(&self.models)
            .iter()
            .filter_map(|model| {
                let id = Self::model_key(model)?;
                Some(AcpSessionModelOption {
                    name: model["name"].as_str().unwrap_or(&id).to_string(),
                    provider_name: model["provider"].as_str().map(str::to_string),
                    description: None,
                    id,
                })
            })
            .collect();
        let current_model_id = lock(&self.current_model)
            .as_ref()
            .and_then(Self::model_key)
            .or_else(|| lock(&self.selected_model).clone());
        let levels = lock(&self.thinking_levels).clone();
        let mut config_options = vec![AcpSessionConfigOption {
            id: approval::PERMISSION_MODE_CONFIG_ID.to_string(),
            name: "Permission mode".to_string(),
            description: None,
            category: Some("mode".to_string()),
            kind: AcpSessionConfigKind::Select {
                current_value: lock(&self.permission_mode).clone(),
                options: approval::PERMISSION_MODES
                    .iter()
                    .map(|(id, name, description)| AcpSessionConfigSelectOption {
                        value: (*id).to_string(),
                        name: (*name).to_string(),
                        description: Some((*description).to_string()),
                    })
                    .collect(),
            },
        }];
        if levels.len() > 1 {
            config_options.push(AcpSessionConfigOption {
                id: THINKING_CONFIG_ID.to_string(),
                name: "Thinking".to_string(),
                description: None,
                category: Some("thought_level".to_string()),
                kind: AcpSessionConfigKind::Select {
                    current_value: lock(&self.thinking)
                        .clone()
                        .unwrap_or_else(|| "off".to_string()),
                    options: levels
                        .iter()
                        .map(|level| AcpSessionConfigSelectOption {
                            value: level.clone(),
                            name: level.clone(),
                            description: None,
                        })
                        .collect(),
                },
            });
        }
        AcpSessionOptions {
            current_model_id,
            available_models,
            model_config_id: None,
            config_options,
            context_usage: lock(&self.context_usage).clone(),
            capabilities: Some(super::capabilities()),
            background_tasks: Vec::new(),
        }
    }

    pub(crate) fn commands(&self) -> Vec<AcpAvailableCommand> {
        let mut commands = vec![AcpAvailableCommand {
            name: "compact".into(),
            description: "Compact the conversation context".into(),
            input_hint: Some("optional instructions".into()),
        }];
        commands.extend(lock(&self.commands).iter().cloned());
        commands
    }

    pub(crate) fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        self.workspace_path == path && self.remote_connection_id.as_deref() == remote
    }

    /// Pi groups transcripts by cwd: once one exists the cwd is fixed.
    pub(crate) async fn workspace_bound(&self) -> bool {
        self.resume.load(Ordering::Acquire)
            || lock(&self.history).pending.is_some()
            || self.state.lock().await.projector.is_some()
    }

    fn active_pi_session(&self) -> String {
        lock(&self.active_pi_session).clone()
    }

    pub async fn warm_up(self: &Arc<Self>) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        if !lock(&self.models).is_empty() {
            return Ok(());
        }
        self.ensure_process().await.map(|_| ())
    }

    async fn ensure_process(self: &Arc<Self>) -> OpenBitFunResult<Arc<PiProcess>> {
        let mut slot = self.process.lock().await;
        if let Some(process) = Self::live_process(&slot) {
            return Ok(process);
        }
        if let Some(previous) = slot.as_ref() {
            let mut completion = previous.terminated.lock().await;
            if let Some(done) = completion.as_mut() {
                tokio::time::timeout(Duration::from_secs(5), done)
                    .await
                    .map_err(|_| {
                        service_error(
                            "Previous Pi process has not terminated; refusing concurrent resume",
                        )
                    })?
                    .map_err(|_| service_error("Pi process supervisor was lost"))?;
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
        let target = self.launch_target();
        let (args, remote_command) = if self.remote_connection_id.is_some() {
            let args = cli_args(&target, None);
            let command = remote_command(&self.workspace_path, &args);
            (args, command)
        } else {
            let extension = approval::install_local_extension().await?;
            #[cfg(test)]
            let extension = lock(&self.extension_override).clone().unwrap_or(extension);
            let args = cli_args(&target, Some(&extension.to_string_lossy()));
            (args, String::new())
        };
        let executable = local_executable();
        let io = super::super::process::spawn(super::super::process::ProcessConfig {
            client_id: CLIENT_ID,
            name: "Pi",
            executable: &executable,
            args: &args,
            workspace_path: &self.workspace_path,
            remote_connection_id: self.remote_connection_id.as_deref(),
            remote_command: &remote_command,
            env: &[],
        })
        .await?;
        let process = Arc::new(PiProcess::new(
            io.writer,
            Some(io.shutdown),
            Some(io.terminated),
            io.exit,
        ));
        *slot = Some(process.clone());
        drop(slot);
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
        if let Err(error) = self.initialize(&process).await {
            process.kill().await;
            let tail = process.stderr_tail().await;
            // Keep one "Service error:" prefix when re-wrapping with stderr.
            let error = match error {
                OpenBitFunError::Service(message) => message,
                other => other.to_string(),
            };
            return Err(service_error(format!("{error}\n{tail}")));
        }
        Ok(process)
    }

    /// Session to open: the pinned id, or a `--fork` copy of the source of a
    /// pending branch (the same session when a rollback is pending on it).
    fn launch_target(&self) -> LaunchTarget {
        let active = self.active_pi_session();
        match lock(&self.history).pending.as_ref() {
            Some(pending)
                if !(pending.session_id == active && self.resume.load(Ordering::Acquire)) =>
            {
                LaunchTarget::Fork(pending.session_id.clone())
            }
            _ => LaunchTarget::Open(active),
        }
    }

    /// Finish a pending branch on the opened session: cut it before the
    /// pending entry with `fork` (Pi copies the path before that user
    /// message into a new session), then bind this session to the result.
    async fn apply_pending(&self, process: &Arc<PiProcess>) -> OpenBitFunResult<()> {
        let Some(pending) = lock(&self.history).pending.clone() else {
            return Ok(());
        };
        if let Some(entry) = &pending.cut_before {
            let data = process
                .request(
                    json!({"type":"fork", "entryId": entry}),
                    Some(COMMAND_TIMEOUT),
                )
                .await?;
            if data["cancelled"] == true {
                return Err(service_error(
                    "A Pi extension cancelled the fork of this session",
                ));
            }
        }
        let state = process
            .request(json!({"type":"get_state"}), Some(COMMAND_TIMEOUT))
            .await?;
        let id = state["sessionId"]
            .as_str()
            .filter(|id| super::valid_pi_session_id(id))
            .ok_or_else(|| service_error("Pi did not report the forked session id"))?
            .to_string();
        self.bind_pi_session(&id).await?;
        {
            let mut history = lock(&self.history);
            history.pending = None;
            // The cursor may name an entry the branch left out.
            history.synced = false;
        }
        self.persist_history().await
    }

    /// The Pi transcript of this session is now `id`; later launches open it.
    async fn bind_pi_session(&self, id: &str) -> OpenBitFunResult<()> {
        *lock(&self.active_pi_session) = id.to_string();
        self.resume.store(true, Ordering::Release);
        if let Some((persistence, storage)) = &self.persistence {
            persistence
                .update_remote_session_state(
                    storage,
                    &self.session_id,
                    id,
                    super::RESUME_STRATEGY,
                    None,
                )
                .await?;
        }
        Ok(())
    }

    /// Record Pi entries appended since the cursor; the latest prompted turn
    /// that has no entry yet takes the first new user message.
    async fn sync_entries(&self, process: &Arc<PiProcess>) -> OpenBitFunResult<()> {
        let _guard = self.entry_sync.lock().await;
        let since = {
            let history = lock(&self.history);
            history.cursor.clone().filter(|_| history.synced)
        };
        let mut command = json!({"type":"get_entries"});
        if let Some(cursor) = &since {
            command["since"] = json!(cursor);
        }
        let data = match process.request(command, Some(COMMAND_TIMEOUT)).await {
            Ok(data) => data,
            // The cursor left the session (branch switch): start over.
            Err(_) if since.is_some() => {
                lock(&self.history).synced = false;
                process
                    .request(json!({"type":"get_entries"}), Some(COMMAND_TIMEOUT))
                    .await?
            }
            Err(error) => return Err(error),
        };
        let entries = data["entries"].as_array().cloned().unwrap_or_default();
        if lock(&self.history).absorb_entries(&entries, true) {
            self.persist_history().await?;
        }
        Ok(())
    }

    fn spawn_entry_sync(self: &Arc<Self>, process: Arc<PiProcess>) {
        let session = self.clone();
        tokio::spawn(async move {
            if let Err(error) = session.sync_entries(&process).await {
                log::debug!("Pi entry sync failed: {error}");
            }
        });
    }

    /// Load the model catalog, thinking levels and commands, and apply the
    /// remembered model/thinking when Pi still offers them.
    pub(super) async fn initialize(&self, process: &Arc<PiProcess>) -> OpenBitFunResult<()> {
        self.apply_pending(process).await?;
        let state = process
            .request(json!({"type":"get_state"}), Some(Duration::from_secs(30)))
            .await?;
        if let Some(id) = state["sessionId"].as_str() {
            *lock(&self.active_pi_session) = id.to_string();
        }
        *lock(&self.current_model) = state.get("model").cloned().filter(Value::is_object);
        *lock(&self.thinking) = state["thinkingLevel"].as_str().map(str::to_string);
        let models = process
            .request(
                json!({"type":"get_available_models"}),
                Some(COMMAND_TIMEOUT),
            )
            .await?;
        *lock(&self.models) = models["models"].as_array().cloned().unwrap_or_default();
        let remembered = lock(&self.selected_model).clone();
        let current = lock(&self.current_model).as_ref().and_then(Self::model_key);
        if let Some(model) = remembered.filter(|model| Some(model) != current.as_ref()) {
            if self.offers_model(&model) {
                if let Err(error) = self.apply_model(process, &model).await {
                    log::warn!("Remembered Pi model could not be applied: {error}");
                }
            } else {
                log::warn!("Remembered Pi model {model} is unavailable; using Pi's default");
            }
        }
        self.refresh_thinking_levels(process).await;
        let remembered = lock(&self.selected_thinking).clone();
        if let Some(level) = remembered {
            let offered = lock(&self.thinking_levels).contains(&level);
            if offered && lock(&self.thinking).as_deref() != Some(level.as_str()) {
                match process
                    .request(
                        json!({"type":"set_thinking_level", "level":level}),
                        Some(COMMAND_TIMEOUT),
                    )
                    .await
                {
                    Ok(_) => *lock(&self.thinking) = Some(level),
                    Err(error) => {
                        log::warn!("Remembered Pi thinking level could not be applied: {error}")
                    }
                }
            }
        }
        match process
            .request(json!({"type":"get_commands"}), Some(COMMAND_TIMEOUT))
            .await
        {
            Ok(data) => {
                let rows = data["commands"].as_array().cloned().unwrap_or_default();
                self.bridge_loaded.store(
                    rows.iter()
                        .any(|row| row["name"] == approval::BRIDGE_COMMAND),
                    Ordering::Release,
                );
                let commands = rows
                    .iter()
                    .filter(|row| row["name"] != approval::BRIDGE_COMMAND)
                    .filter_map(command_option)
                    .collect();
                *lock(&self.commands) = commands;
                self.notify(NativeAgentNotification::AvailableCommandsUpdated(
                    self.commands(),
                ));
            }
            Err(error) => {
                self.bridge_loaded.store(false, Ordering::Release);
                log::warn!("Pi get_commands failed; approvals cannot be verified: {error}");
            }
        }
        if let Err(error) = self.sync_entries(process).await {
            log::warn!("Pi entry sync failed at launch: {error}");
        }
        Ok(())
    }

    fn offers_model(&self, key: &str) -> bool {
        lock(&self.models)
            .iter()
            .any(|model| Self::model_key(model).as_deref() == Some(key))
    }

    async fn apply_model(&self, process: &Arc<PiProcess>, key: &str) -> OpenBitFunResult<()> {
        let (provider, model_id) = key
            .split_once('/')
            .ok_or_else(|| OpenBitFunError::Validation(format!("Invalid Pi model id: {key}")))?;
        let model = process
            .request(
                json!({"type":"set_model", "provider":provider, "modelId":model_id}),
                Some(COMMAND_TIMEOUT),
            )
            .await?;
        *lock(&self.current_model) = Some(model).filter(Value::is_object);
        Ok(())
    }

    async fn refresh_thinking_levels(&self, process: &Arc<PiProcess>) {
        match process
            .request(
                json!({"type":"get_available_thinking_levels"}),
                Some(COMMAND_TIMEOUT),
            )
            .await
        {
            Ok(data) => {
                *lock(&self.thinking_levels) = data["levels"]
                    .as_array()
                    .map(|levels| {
                        levels
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
            }
            Err(error) => log::debug!("Pi get_available_thinking_levels failed: {error}"),
        }
    }

    async fn read_loop(
        self: Arc<Self>,
        process: Arc<PiProcess>,
        reader: Reader,
        mut stderr_task: tokio::task::JoinHandle<()>,
    ) {
        // `lines()` splits on LF only (stripping CR); U+2028/2029 stay inside JSON.
        let mut lines = BufReader::new(reader).lines();
        let mut exit = process.exit.clone();
        let error = loop {
            let line = tokio::select! {
                biased;
                line = lines.next_line() => line,
                changed = exit.changed() => {
                    break if changed.is_ok() {
                        exit.borrow().clone().unwrap_or_else(|| "Pi process exited".into())
                    } else { "Pi process supervisor ended".into() };
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
                    // Stdout is protocol-only; a non-JSON line is a diagnostic.
                    Err(_) => log::debug!("Pi stdout non-JSON line ignored"),
                },
                Ok(None) => break "Pi output stream closed".to_string(),
                Err(error) => break format!("Pi output read failed: {error}"),
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
        // Hold the process fence while failing its turn: an old reader must
        // never fail a turn submitted after a restart.
        self.approvals
            .retain(|_, pending| pending.process.id != process.id);
        let slot = self.process.lock().await;
        let mut state = self.state.lock().await;
        state.steers.clear();
        let owns_turn = state.process_id.as_deref() == Some(process.id.as_str());
        drop(state);
        if slot.as_ref().is_some_and(|p| p.id == process.id) && owns_turn {
            self.fail_turn(&format!("{error}\n{tail}")).await;
        }
    }

    pub(super) async fn route_frame(
        self: &Arc<Self>,
        process: &Arc<PiProcess>,
        frame: Value,
    ) -> OpenBitFunResult<()> {
        #[cfg(test)]
        lock(&self.frame_tap).push(frame.clone());
        match frame["type"].as_str() {
            Some("response") => {
                if let Some(id) = frame["id"].as_str() {
                    if let Some((_, tx)) = process.pending.remove(id) {
                        let _ = tx.send(frame);
                    }
                } else {
                    log::warn!("Pi rejected a command: {}", frame["error"]);
                }
                return Ok(());
            }
            Some("extension_ui_request") => {
                if let Some(request) = ApprovalRequest::parse(&frame) {
                    return self.handle_approval(process, request).await;
                }
                // Other extension dialogs have no OpenBitFun surface yet.
                // Cancel instead of leaving the extension blocked.
                if matches!(
                    frame["method"].as_str(),
                    Some("select" | "confirm" | "input" | "editor")
                ) {
                    log::warn!(
                        "Pi extension dialog cancelled (unsupported): method={}",
                        frame["method"]
                    );
                    process
                        .write(&json!({"type":"extension_ui_response", "id":frame["id"], "cancelled":true}))
                        .await?;
                }
                return Ok(());
            }
            Some("extension_error") => {
                log::warn!(
                    "Pi extension error: event={} error={}",
                    frame["event"],
                    frame["error"]
                );
                return Ok(());
            }
            Some("thinking_level_changed") => {
                *lock(&self.thinking) = frame["level"].as_str().map(str::to_string);
                self.notify(NativeAgentNotification::OptionsChanged);
                return Ok(());
            }
            _ => {}
        }
        let slot = self.process.lock().await;
        if slot
            .as_ref()
            .is_some_and(|current| current.id != process.id)
            || !process.alive.load(Ordering::Acquire)
        {
            return Ok(());
        }
        drop(slot);
        let mut state = self.state.lock().await;
        let Some(projector) = state.projector.as_mut() else {
            // Pi only runs when prompted; nothing outside a turn is projected.
            return Ok(());
        };
        if projector.is_injected_user_message(&frame) {
            if let Some(steer) = state.steers.pop_front() {
                let projector = state.projector.as_ref().expect("active turn");
                let event = AgenticEvent::UserSteeringInjected {
                    session_id: self.session_id.clone(),
                    turn_id: projector.turn_id().to_string(),
                    round_index: projector.current_round_index(),
                    steering_id: steer.id,
                    display_content: steer.display.unwrap_or_else(|| steer.text.clone()),
                    content: steer.text,
                };
                self.enqueue(vec![event]).await?;
            }
        }
        if is_subagent_frame(&frame) {
            let turn_id = state
                .projector
                .as_ref()
                .expect("active turn")
                .turn_id()
                .to_string();
            let model = self.model_name();
            let update =
                state
                    .subagents
                    .consume(&self.session_id, &turn_id, model.as_deref(), &frame);
            for record in &update.records {
                self.persist_subagent_record(record).await;
            }
            self.enqueue(update.events).await?;
        }
        let projector = state.projector.as_mut().expect("active turn");
        let events = projector.consume(&frame);
        let usage = events.iter().find_map(|event| match event {
            AgenticEvent::TokenUsageUpdated {
                input_tokens,
                max_context_tokens: Some(max),
                ..
            } => Some((*input_tokens as u64, *max as u64)),
            _ => None,
        });
        self.enqueue(events).await?;
        if let Some((used, size)) = usage {
            self.publish_context_usage(Some(used), size, None);
        }
        if state
            .projector
            .as_ref()
            .is_some_and(PiTurnProjector::is_finished)
        {
            let turn_id = state.projector.take().map(|p| p.turn_id().to_string());
            state.steers.clear();
            let orphans = state.subagents.settle_all(frame["aborted"] == true);
            drop(state);
            self.enqueue(orphans).await?;
            if let Some(turn_id) = turn_id {
                self.clear_approvals(&turn_id).await;
            }
            self.settled.notify_waiters();
            self.spawn_stats_refresh(process.clone());
            self.spawn_entry_sync(process.clone());
        }
        Ok(())
    }

    fn publish_context_usage(&self, used: Option<u64>, size: u64, cost: Option<f64>) {
        if size == 0 {
            return;
        }
        let mut slot = lock(&self.context_usage);
        let used = used
            .or_else(|| slot.as_ref().map(|usage| usage.used))
            .unwrap_or(0);
        let cost = cost
            .map(|amount| agent_client_protocol::schema::Cost::new(amount, "USD"))
            .or_else(|| slot.as_ref().and_then(|usage| usage.cost.clone()));
        let usage = AcpSessionContextUsage { used, size, cost };
        *slot = Some(usage.clone());
        drop(slot);
        self.notify(NativeAgentNotification::ContextUsageUpdated(usage));
    }

    /// Pi's own accounting (`get_session_stats`): context occupancy, window and
    /// cumulative cost. Runs off the reader, which routes the response.
    fn spawn_stats_refresh(self: &Arc<Self>, process: Arc<PiProcess>) {
        let session = self.clone();
        tokio::spawn(async move {
            match process
                .request(json!({"type":"get_session_stats"}), Some(COMMAND_TIMEOUT))
                .await
            {
                Ok(stats) => {
                    let usage = &stats["contextUsage"];
                    let size = usage["contextWindow"]
                        .as_u64()
                        .or_else(|| session.context_window())
                        .unwrap_or(0);
                    session.publish_context_usage(
                        usage["tokens"].as_u64(),
                        size,
                        stats["cost"]
                            .as_f64()
                            .filter(|c| c.is_finite() && *c >= 0.0),
                    );
                }
                Err(error) => log::debug!("Pi get_session_stats failed: {error}"),
            }
        });
    }

    async fn begin_turn(
        &self,
        state: &mut TurnState,
        turn_id: String,
        input: String,
        original: Option<String>,
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
        lock(&self.history).turns.push(TurnAnchor {
            turn_id: turn_id.clone(),
            turn_index: state.index - 1,
            prompted: false,
            entry_id: None,
        });
        let mut projector = PiTurnProjector::new(
            &self.session_id,
            &turn_id,
            self.model_name().as_deref(),
            self.context_window(),
        );
        self.enqueue(projector.open_pending_round()).await?;
        state.projector = Some(projector);
        state.process_id = None;
        state.steers.clear();
        Ok(())
    }

    /// Persist the hidden child record before any child event, so the
    /// frontend can save and restore the child transcript.
    async fn persist_subagent_record(&self, record: &ChildRecord) {
        let Some((persistence, storage)) = &self.persistence else {
            return;
        };
        let result = async {
            let parent = persistence
                .load_metadata(storage, &self.session_id)
                .await?
                .ok_or_else(|| service_error("Pi parent session metadata is missing"))?;
            let relationship = openbitfun_core::service::session::SessionRelationship {
                kind: Some(openbitfun_core::service::session::SessionRelationshipKind::Subagent),
                parent_session_id: Some(self.session_id.clone()),
                parent_request_id: None,
                parent_dialog_turn_id: Some(record.parent_turn_id.clone()),
                parent_turn_index: None,
                parent_tool_call_id: Some(record.parent_tool_call_id.clone()),
                subagent_type: record.agent.clone(),
                continuation_policy: Some(
                    openbitfun_core::agentic::core::SessionContinuationPolicy::FreshOnly,
                ),
            };
            let label = record.agent.as_deref().unwrap_or("Subagent");
            let task: String = record.task.chars().take(60).collect();
            persistence
                .create_subagent_session_record(
                    storage,
                    &record.session_id,
                    &parent,
                    relationship,
                    format!("{label}: {task}"),
                    record.model.clone(),
                )
                .await
        }
        .await;
        if let Err(error) = result {
            log::warn!("Pi subagent record persistence failed: {error}");
        }
    }

    async fn fail_turn(&self, error: &str) {
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            if let Err(error) = self.enqueue(projector.fail(error)).await {
                log::error!("Pi failure event delivery failed: {error}");
            }
            if let Err(error) = self.enqueue(state.subagents.settle_all(false)).await {
                log::error!("Pi subagent failure event delivery failed: {error}");
            }
            state.steers.clear();
            drop(state);
            self.clear_approvals(projector.turn_id()).await;
            self.settled.notify_waiters();
        }
    }

    /// Answer approvals nobody will decide (turn ended, cancelled, failed):
    /// the extension blocks those calls.
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
                    .write(&approval::cancel_response(&pending.request.ui_id))
                    .await;
            }
        }
    }

    /// Route one approval dialog: answer from the permission mode or a
    /// session grant, otherwise show a permission card and wait.
    async fn handle_approval(
        &self,
        process: &Arc<PiProcess>,
        request: ApprovalRequest,
    ) -> OpenBitFunResult<()> {
        let mode = lock(&self.permission_mode).clone();
        let granted = lock(&self.session_grants).contains(&request.session_key());
        if let Some(answer) = approval::automatic_decision(&mode, granted) {
            return process
                .write(&approval::select_response(&request.ui_id, answer))
                .await;
        }
        let turn_id = {
            let mut state = self.state.lock().await;
            let current = state.process_id.as_deref() == Some(process.id.as_str());
            match state.projector.as_mut().filter(|_| current) {
                Some(projector) => {
                    let events = projector.permission_tool(
                        &request.tool_call_id,
                        &request.tool_name,
                        &request.input,
                    );
                    let turn_id = projector.turn_id().to_string();
                    self.enqueue(events).await?;
                    Some(turn_id)
                }
                None => None,
            }
        };
        let Some(turn_id) = turn_id else {
            // No turn to show it in: refuse rather than block Pi.
            return process
                .write(&approval::cancel_response(&request.ui_id))
                .await;
        };
        let permission_id = format!("pi_native_permission_{}", uuid::Uuid::new_v4());
        let payload = request.permission_payload(&permission_id, &self.session_id);
        self.approvals.insert(
            permission_id.clone(),
            PendingApproval {
                process: process.clone(),
                request,
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
                    .write(&approval::cancel_response(&pending.request.ui_id))
                    .await;
            }
            return Err(service_error(format!(
                "Pi approval delivery failed: {error}"
            )));
        }
        Ok(())
    }

    pub(crate) async fn submit_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> Option<OpenBitFunResult<()>> {
        let (_, pending) = self.approvals.remove(&request.permission_id)?;
        let answer = match approval::decision(request) {
            Ok(answer) => answer,
            Err(error) => {
                self.approvals
                    .insert(request.permission_id.clone(), pending);
                return Some(Err(error));
            }
        };
        if answer == approval::ALLOW_SESSION {
            lock(&self.session_grants).insert(pending.request.session_key());
        }
        Some(
            pending
                .process
                .write(&approval::select_response(&pending.request.ui_id, answer))
                .await,
        )
    }

    pub async fn start_turn(
        self: &Arc<Self>,
        turn_id: String,
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        let guard = self.lifecycle.lock().await;
        if self.state.lock().await.projector.is_some() {
            return Err(service_error("Pi session already has an active turn"));
        }
        let process = match self.ensure_process().await {
            Ok(process) => process,
            Err(error) => {
                let mut state = self.state.lock().await;
                self.begin_turn(&mut state, turn_id, input.text, original)
                    .await?;
                drop(state);
                self.fail_turn(&error.to_string()).await;
                return Err(error);
            }
        };
        let bridge = if *lock(&self.permission_mode) == approval::MODE_FULL_ACCESS {
            Ok(())
        } else {
            self.ensure_bridge()
        };
        if let Err(error) = bridge {
            let mut state = self.state.lock().await;
            self.begin_turn(&mut state, turn_id, input.text, original)
                .await?;
            drop(state);
            self.fail_turn(&error.to_string()).await;
            return Err(error);
        }
        let compact = compact_instructions(&input.text);
        let title_message = {
            let mut state = self.state.lock().await;
            let first = state.index == 0;
            self.begin_turn(
                &mut state,
                turn_id.clone(),
                input.text.clone(),
                original.clone(),
            )
            .await?;
            state.process_id = Some(process.id.clone());
            (first && compact.is_none())
                .then(|| title::title_source(&input.text, original.as_deref()))
                .flatten()
        };
        drop(guard);
        if let Some(instructions) = compact {
            // Manual compaction is a command, not a run: it emits
            // `compaction_*` events and ends with its response.
            let session = self.clone();
            tokio::spawn(async move {
                let mut command = json!({"type":"compact"});
                if let Some(text) = instructions {
                    command["customInstructions"] = Value::String(text);
                }
                let result = process.request(command, None).await;
                session.finish_command_turn(&turn_id, result).await;
                session.spawn_stats_refresh(process);
            });
            return Ok(());
        }
        // Everything Pi holds now predates this prompt, so the first user
        // message after the cursor is this turn's.
        if let Err(error) = self.sync_entries(&process).await {
            log::warn!("Pi entry sync before prompt failed: {error}");
            lock(&self.history).synced = false;
        }
        if let Some(anchor) = lock(&self.history).anchor_mut(&turn_id) {
            anchor.prompted = true;
        }
        let mut command = json!({"type":"prompt", "message":input.text});
        if !input.images.is_empty() {
            command["images"] = images_value(&input);
        }
        match process.request(command, Some(PROMPT_TIMEOUT)).await {
            Ok(data) => {
                self.mark_resumable().await;
                if data["disposition"] == "handled" {
                    // An extension command consumed it; no user message entry.
                    if let Some(anchor) = lock(&self.history).anchor_mut(&turn_id) {
                        anchor.prompted = false;
                    }
                }
                if let Err(error) = self.persist_history().await {
                    log::warn!("Failed to persist Pi native history anchors: {error}");
                }
                if data["disposition"] == "handled" {
                    // An extension command consumed the prompt; no run follows.
                    self.finish_command_turn(&turn_id, Ok(Value::Null)).await;
                }
                if let Some(message) = title_message {
                    self.spawn_title_generation(message);
                }
                Ok(())
            }
            Err(error) => {
                // A refused prompt left no user message; a timed-out one may
                // still land and is attributed by the next sync.
                if let Some(anchor) = lock(&self.history).anchor_mut(&turn_id) {
                    anchor.prompted = error.to_string().contains("timed out");
                }
                self.fail_turn(&error.to_string()).await;
                Err(error)
            }
        }
    }

    /// Close a turn whose work ended with a command response.
    async fn finish_command_turn(&self, turn_id: &str, result: OpenBitFunResult<Value>) {
        let mut state = self.state.lock().await;
        let Some(projector) = state
            .projector
            .as_mut()
            .filter(|projector| projector.turn_id() == turn_id)
        else {
            return;
        };
        let events = match result {
            Ok(data) => projector.settle_manual_compaction(&data),
            Err(error) => projector.fail(&error.to_string()),
        };
        state.projector = None;
        drop(state);
        if let Err(error) = self.enqueue(events).await {
            log::error!("Pi command turn event delivery failed: {error}");
        }
        self.settled.notify_waiters();
    }

    /// A prompt reached Pi: the transcript exists and later launches resume it.
    async fn mark_resumable(&self) {
        if self.resume.swap(true, Ordering::AcqRel) {
            return;
        }
        let active = self.active_pi_session();
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .update_remote_session_state(
                    storage,
                    &self.session_id,
                    &active,
                    super::RESUME_STRATEGY,
                    None,
                )
                .await
            {
                log::warn!("Failed to persist Pi native session state: {error}");
            }
        }
    }

    /// Title the session from its first user message in the background.
    fn spawn_title_generation(self: &Arc<Self>, message: String) {
        let session = self.clone();
        let resolver = lock(&self.title_resolver).clone();
        tokio::spawn(async move {
            let Some((persistence, storage)) = &session.persistence else {
                return;
            };
            let expected = match persistence
                .load_metadata(storage, &session.session_id)
                .await
            {
                Ok(Some(metadata)) => match title::expected_title(&metadata) {
                    Some(expected) => expected,
                    None => return,
                },
                Ok(None) => return,
                Err(error) => {
                    log::warn!("Pi session title skipped: {error}");
                    return;
                }
            };
            let resolved = resolver(message).await;
            match persistence
                .set_generated_title(storage, &session.session_id, &expected, &resolved.title)
                .await
            {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    log::warn!("Pi session title persistence failed: {error}");
                    return;
                }
            }
            let event = AgenticEvent::SessionTitleGenerated {
                session_id: session.session_id.clone(),
                title: resolved.title,
                method: resolved.method.as_str().to_string(),
            };
            if let Err(error) = session.enqueue(vec![event]).await {
                log::warn!("Pi session title event failed: {error}");
            }
        });
    }

    /// Queue a message for the running turn with `steer`; Pi delivers it
    /// after the current tool calls, before the next model call.
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
        let process = Self::live_process(&*self.process.lock().await)
            .ok_or_else(|| service_error("Pi process is not running"))?;
        let id = uuid::Uuid::new_v4().to_string();
        {
            let mut state = self.state.lock().await;
            if state.projector.as_ref().map(PiTurnProjector::turn_id) != Some(turn_id) {
                return Err(service_error("Pi turn is no longer running"));
            }
            state.steers.push_back(PendingSteer {
                id: id.clone(),
                text: input.text.clone(),
                display,
            });
        }
        let mut command = json!({"type":"steer", "message":input.text});
        if !input.images.is_empty() {
            command["images"] = images_value(&input);
        }
        match process.request(command, Some(PROMPT_TIMEOUT)).await {
            Ok(data) if data["disposition"] == "handled" => {
                self.state
                    .lock()
                    .await
                    .steers
                    .retain(|steer| steer.id != id);
                Ok(id)
            }
            Ok(_) => Ok(id),
            Err(error) => {
                self.state
                    .lock()
                    .await
                    .steers
                    .retain(|steer| steer.id != id);
                Err(error)
            }
        }
    }

    pub async fn cancel(&self) -> OpenBitFunResult<()> {
        let _guard = self.lifecycle.lock().await;
        {
            let mut state = self.state.lock().await;
            let Some(projector) = state.projector.as_mut() else {
                return Ok(());
            };
            projector.mark_cancel_requested();
        }
        let process = Self::live_process(&*self.process.lock().await);
        if let Some(process) = process {
            let settled = tokio::time::timeout(ABORT_GRACE, async {
                process.request(json!({"type":"abort"}), None).await?;
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
            if !matches!(settled, Ok(Ok(()))) {
                log::warn!("Pi abort did not settle in time; stopping the process");
                process.kill().await;
            }
        }
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            state.steers.clear();
            let children = state.subagents.settle_all(true);
            drop(state);
            self.clear_approvals(projector.turn_id()).await;
            self.enqueue(projector.cancel()).await?;
            self.enqueue(children).await?;
            self.settled.notify_waiters();
        }
        Ok(())
    }

    /// Drop the latest turn: the session continues on a Pi fork that ends
    /// before the turn's user message (or a fresh session when it was the
    /// first). The original Pi session and workspace files are untouched.
    pub async fn rollback_last_turn(
        self: &Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        let _guard = self.lifecycle.lock().await;
        if self.state.lock().await.projector.is_some() || !self.approvals.is_empty() {
            return Err(OpenBitFunError::Validation(
                "Wait until the Pi turn finishes before rolling back".into(),
            ));
        }
        let process = Self::live_process(&*self.process.lock().await);
        if let Some(process) = &process {
            // Attribute the latest prompt if its entry was not synced yet.
            if let Err(error) = self.sync_entries(process).await {
                log::warn!("Pi entry sync before rollback failed: {error}");
            }
        }
        let target = rollback_target(&lock(&self.history), turn_id)?;
        if let Some((persistence, storage)) = &self.persistence {
            persistence
                .ensure_latest_turn(storage, &self.session_id, turn_id)
                .await?;
        }
        if let Some(entry) = target.entry_id.clone() {
            let unlaunched = {
                let mut history = lock(&self.history);
                match history.pending.as_mut() {
                    // A fork that never launched: cut its source earlier.
                    Some(pending) => {
                        pending.cut_before = Some(entry.clone());
                        true
                    }
                    None => {
                        history.pending = Some(PendingFork {
                            session_id: self.active_pi_session(),
                            cut_before: Some(entry),
                        });
                        false
                    }
                }
            };
            self.persist_history().await?;
            match process.filter(|_| !unlaunched) {
                Some(process) => {
                    // Branch the running process now; on failure the
                    // pending fork is applied by the next launch.
                    if let Err(error) = self.apply_pending(&process).await {
                        log::warn!("Pi rollback deferred to next launch: {error}");
                        process.kill().await;
                    }
                }
                None if unlaunched => {}
                None => {
                    // The next launch opens the session and forks it.
                    self.resume.store(true, Ordering::Release);
                }
            }
        }
        {
            let mut history = lock(&self.history);
            history.turns.retain(|a| a.turn_index < target.turn_index);
        }
        self.state.lock().await.index = target.turn_index;
        if let Some((persistence, storage)) = &self.persistence {
            self.persist_history().await?;
            persistence
                .truncate_turns_from(storage, &self.session_id, target.turn_index)
                .await?;
        }
        Ok(NativeRollbackOutcome {
            files_changed: Vec::new(),
            files_rewound: false,
            note: Some(
                "Pi does not restore workspace files; only the conversation was rolled back."
                    .into(),
            ),
        })
    }

    pub async fn set_model(&self, model_id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        if !self.offers_model(model_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Unsupported Pi model: {model_id}"
            )));
        }
        if let Some(process) = Self::live_process(&*self.process.lock().await) {
            self.apply_model(&process, model_id).await?;
            self.refresh_thinking_levels(&process).await;
        }
        *lock(&self.selected_model) = Some(model_id.to_string());
        self.remember(|p| p.model = Some(model_id.to_string()))
            .await;
        Ok(self.options())
    }

    pub async fn set_config_option(
        &self,
        config_id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        let AcpSessionConfigValue::Select { value } = value else {
            return Err(OpenBitFunError::Validation(
                "Pi native options take a select value".into(),
            ));
        };
        if config_id == approval::PERMISSION_MODE_CONFIG_ID {
            if !approval::PERMISSION_MODES
                .iter()
                .any(|(id, _, _)| *id == value)
            {
                return Err(OpenBitFunError::Validation(format!(
                    "Unsupported Pi permission mode: {value}"
                )));
            }
            if value != approval::MODE_FULL_ACCESS
                && Self::live_process(&*self.process.lock().await).is_some()
            {
                // Fail loudly rather than let a running Pi skip approvals.
                self.ensure_bridge()?;
            }
            *lock(&self.permission_mode) = value.clone();
            self.remember(|p| p.permission_mode = Some(value.clone()))
                .await;
            return Ok(self.options());
        }
        if config_id != THINKING_CONFIG_ID {
            return Err(OpenBitFunError::Validation(format!(
                "Unknown Pi native option: {config_id}"
            )));
        }
        if !lock(&self.thinking_levels).contains(&value) {
            return Err(OpenBitFunError::Validation(format!(
                "Unsupported Pi thinking level: {value}"
            )));
        }
        if let Some(process) = Self::live_process(&*self.process.lock().await) {
            process
                .request(
                    json!({"type":"set_thinking_level", "level":value}),
                    Some(COMMAND_TIMEOUT),
                )
                .await?;
        }
        *lock(&self.thinking) = Some(value.clone());
        *lock(&self.selected_thinking) = Some(value.clone());
        self.remember(|p| p.thinking = Some(value.clone())).await;
        Ok(self.options())
    }

    /// Approvals need the bridge in the running process; without it Pi would
    /// run every tool unasked.
    fn ensure_bridge(&self) -> OpenBitFunResult<()> {
        if self.bridge_loaded.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(service_error(
            "The OpenBitFun approval extension did not load in Pi, so tool approvals cannot be enforced. Switch the permission mode to Full access to run without approvals, or check the Pi installation.",
        ))
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
impl super::super::NativeAgentSession for PiNativeSession {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn options(&self) -> AcpSessionOptions {
        PiNativeSession::options(self)
    }
    async fn warm_up(self: Arc<Self>) -> OpenBitFunResult<()> {
        PiNativeSession::warm_up(&self).await
    }
    async fn start_turn(
        self: Arc<Self>,
        turn_id: String,
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()> {
        PiNativeSession::start_turn(&self, turn_id, input, original).await
    }
    async fn steer(
        self: Arc<Self>,
        turn_id: &str,
        input: NativeTurnInput,
        display: Option<String>,
    ) -> OpenBitFunResult<String> {
        PiNativeSession::steer(&self, turn_id, input, display).await
    }
    async fn cancel(&self) -> OpenBitFunResult<()> {
        PiNativeSession::cancel(self).await
    }
    async fn set_model(&self, id: &str) -> OpenBitFunResult<AcpSessionOptions> {
        PiNativeSession::set_model(self, id).await
    }
    async fn set_config_option(
        &self,
        id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions> {
        PiNativeSession::set_config_option(self, id, value).await
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
    async fn rollback_last_turn(
        self: Arc<Self>,
        turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        PiNativeSession::rollback_last_turn(&self, turn_id).await
    }
    fn commands(&self) -> Vec<AcpAvailableCommand> {
        PiNativeSession::commands(self)
    }
    fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool {
        PiNativeSession::matches_scope(self, path, remote)
    }
    async fn workspace_bound(&self) -> bool {
        PiNativeSession::workspace_bound(self).await
    }
    async fn shutdown(&self) {
        PiNativeSession::shutdown(self).await
    }
}

/// `Some(instructions)` when the input is the `/compact` command.
fn compact_instructions(input: &str) -> Option<Option<String>> {
    let rest = input.trim().strip_prefix("/compact")?;
    if !(rest.is_empty() || rest.starts_with(char::is_whitespace)) {
        return None;
    }
    let rest = rest.trim();
    Some((!rest.is_empty()).then(|| rest.to_string()))
}

/// Pi `ImageContent` blocks for `prompt` / `steer`.
fn images_value(input: &NativeTurnInput) -> Value {
    Value::Array(
        input
            .images
            .iter()
            .map(|image| json!({"type":"image", "data":image.data, "mimeType":image.media_type}))
            .collect(),
    )
}

/// One `get_commands` row (extension command, prompt template or skill).
fn command_option(row: &Value) -> Option<AcpAvailableCommand> {
    let name = row["name"].as_str()?.trim();
    if name.is_empty() {
        return None;
    }
    Some(AcpAvailableCommand {
        name: name.to_string(),
        description: row["description"].as_str().unwrap_or_default().to_string(),
        input_hint: None,
    })
}

/// Pi session a launch opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LaunchTarget {
    /// `--session-id`: open the project session with this id or create it.
    Open(String),
    /// `--fork`: copy this session into a new one (Pi picks the new id).
    Fork(String),
}

/// Placeholder for the remote extension path; the launch script expands it.
const REMOTE_EXTENSION_ARG: &str = "\"$obf_pi_ext\"";

/// `extension` is the approval bridge path; `None` for remote launches,
/// whose command adds the path the install script wrote.
pub(super) fn cli_args(target: &LaunchTarget, extension: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = vec!["--mode".into(), "rpc".into()];
    match target {
        LaunchTarget::Open(id) => args.extend(["--session-id".into(), id.clone()]),
        LaunchTarget::Fork(source) => args.extend(["--fork".into(), source.clone()]),
    }
    if let Some(path) = extension {
        args.extend(["-e".into(), path.to_string()]);
    }
    args
}

/// Remote launch: write the approval extension on the host, then exec Pi
/// with it. If the write fails the chain stops and Pi does not start.
fn remote_command(path: &str, args: &[String]) -> String {
    let args = args
        .iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ");
    remote_user_shell_command(&format!(
        "{}\ncd {} && exec pi {args} -e {REMOTE_EXTENSION_ARG}",
        approval::remote_install_script(),
        shell_escape(path)
    ))
}

/// The latest anchor, when it is `turn_id` and can be rolled back.
fn rollback_target(history: &PiHistory, turn_id: &str) -> OpenBitFunResult<TurnAnchor> {
    let target = history
        .turns
        .last()
        .filter(|anchor| anchor.turn_id == turn_id)
        .cloned()
        .ok_or_else(|| {
            OpenBitFunError::Validation(
                "Only the latest Pi turn recorded with rollback support can be rolled back".into(),
            )
        })?;
    if target.prompted && target.entry_id.is_none() {
        return Err(OpenBitFunError::Validation(
            "Pi did not report a checkpoint for this turn, so it cannot be rolled back".into(),
        ));
    }
    Ok(target)
}

/// `pi` from PATH. On Windows npm installs a `pi.cmd` shim, which a bare
/// program name does not resolve to.
pub(crate) fn local_executable() -> PathBuf {
    crate::client::requirements::find_executable("pi").unwrap_or_else(|| PathBuf::from("pi"))
}

fn service_error(message: impl Into<String>) -> OpenBitFunError {
    OpenBitFunError::service(message.into())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;
