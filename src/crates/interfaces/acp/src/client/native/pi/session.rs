//! Long-lived `pi --mode rpc` stdio transport (JSONL commands, `response`
//! records correlated by `id`, and session events; see Pi `docs/rpc.md`).
//!
//! The process is session-scoped. A turn starts with `prompt`, streams
//! events, and ends at `agent_settled`; `steer` folds a message into the
//! running turn; `abort` cancels it. Pi runs tools without approval prompts.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::AgenticEvent;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{oneshot, Mutex, Notify};

use super::super::preferences::NativeAgentPreferences;
use super::super::process::{Reader, Writer};
use super::super::title::{self, TitleResolver};
use super::super::{NativeAgentNotification, NativeAgentNotificationSink, NativeTurnInput};
use super::projector::PiTurnProjector;
use crate::client::remote_shell::{remote_user_shell_command, shell_escape};
use crate::client::session_persistence::AcpSessionPersistence;
use crate::client::{
    AcpAvailableCommand, AcpSessionConfigKind, AcpSessionConfigOption,
    AcpSessionConfigSelectOption, AcpSessionConfigValue, AcpSessionContextUsage,
    AcpSessionModelOption, AcpSessionOptions,
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
}

pub struct PiNativeSession {
    session_id: String,
    pi_session_id: String,
    workspace_path: String,
    remote_connection_id: Option<String>,
    /// Pi holds a transcript for `pi_session_id` (a prompt reached it).
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
            pi_session_id,
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
        })
    }

    pub(crate) fn apply_preferences(&self, preferences: NativeAgentPreferences) {
        *lock(&self.selected_model) = preferences.model;
        *lock(&self.selected_thinking) = preferences.thinking;
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
        let mut config_options = Vec::new();
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
        self.resume.load(Ordering::Acquire) || self.state.lock().await.projector.is_some()
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
        let args = cli_args(&self.pi_session_id);
        let executable = local_executable();
        let remote_command = remote_command(&self.workspace_path, &args);
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
            return Err(service_error(format!("{error}\n{tail}")));
        }
        Ok(process)
    }

    /// Load the model catalog, thinking levels and commands, and apply the
    /// remembered model/thinking when Pi still offers them.
    pub(super) async fn initialize(&self, process: &Arc<PiProcess>) -> OpenBitFunResult<()> {
        let state = process
            .request(json!({"type":"get_state"}), Some(Duration::from_secs(30)))
            .await?;
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
                let commands = data["commands"]
                    .as_array()
                    .map(|rows| rows.iter().filter_map(command_option).collect())
                    .unwrap_or_default();
                *lock(&self.commands) = commands;
                self.notify(NativeAgentNotification::AvailableCommandsUpdated(
                    self.commands(),
                ));
            }
            Err(error) => log::debug!("Pi get_commands failed: {error}"),
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
                // Pi extensions can open dialogs; OpenBitFun has no surface for
                // them yet. Cancel instead of leaving the extension blocked.
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
            state.projector = None;
            state.steers.clear();
            drop(state);
            self.settled.notify_waiters();
            self.spawn_stats_refresh(process.clone());
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

    async fn fail_turn(&self, error: &str) {
        let mut state = self.state.lock().await;
        if let Some(mut projector) = state.projector.take() {
            if let Err(error) = self.enqueue(projector.fail(error)).await {
                log::error!("Pi failure event delivery failed: {error}");
            }
            state.steers.clear();
            self.settled.notify_waiters();
        }
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
        let mut command = json!({"type":"prompt", "message":input.text});
        if !input.images.is_empty() {
            command["images"] = images_value(&input);
        }
        match process.request(command, Some(PROMPT_TIMEOUT)).await {
            Ok(data) => {
                self.mark_resumable().await;
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
        if let Some((persistence, storage)) = &self.persistence {
            if let Err(error) = persistence
                .update_remote_session_state(
                    storage,
                    &self.session_id,
                    &self.pi_session_id,
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
            drop(state);
            self.enqueue(projector.cancel()).await?;
            self.settled.notify_waiters();
        }
        Ok(())
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
    /// Pi has no permission prompts; leave the request to other sessions.
    async fn respond_permission(
        &self,
        _request: &crate::client::SubmitAcpPermissionResponseRequest,
    ) -> OpenBitFunResult<bool> {
        Ok(false)
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

/// `--session-id` opens the project session with this id or creates it.
pub(super) fn cli_args(pi_session_id: &str) -> Vec<String> {
    vec![
        "--mode".into(),
        "rpc".into(),
        "--session-id".into(),
        pi_session_id.to_string(),
    ]
}

fn remote_command(path: &str, args: &[String]) -> String {
    let args = args
        .iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ");
    remote_user_shell_command(&format!("cd {} && exec pi {args}", shell_escape(path)))
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
