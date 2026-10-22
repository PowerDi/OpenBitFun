//! Native agent drivers share lifecycle dispatch, not an ACP connection.
pub mod capabilities;
pub mod claude;
mod events;
pub(crate) mod preferences;
mod process;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::util::errors::OpenBitFunResult;

use super::session_persistence::AcpSessionPersistence;
use super::{
    AcpAvailableCommand, AcpPlanEntry, AcpSessionConfigValue, AcpSessionContextUsage,
    AcpSessionOptions, SubmitAcpPermissionResponseRequest,
};
pub use capabilities::{NativeAgentCapabilities, NativeSubagentCapabilities};
pub(crate) use preferences::NativeAgentPreferencesStore;

/// Presentation updates scoped to one native session. Hosts choose their event transport.
#[derive(Debug, Clone)]
pub enum NativeAgentNotification {
    AvailableCommandsUpdated(Vec<AcpAvailableCommand>),
    PlanUpdated {
        turn_id: String,
        entries: Vec<AcpPlanEntry>,
    },
    /// Context occupancy (and cumulative cost when known) changed.
    ContextUsageUpdated(AcpSessionContextUsage),
    /// Session options (e.g. permission mode) changed outside a user request.
    OptionsChanged,
    /// The agent's background tasks changed; the payload is the full list.
    BackgroundTasksUpdated(Vec<NativeBackgroundTask>),
}

/// One background task run by the external agent (shell, subagent, ...).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeBackgroundTask {
    pub id: String,
    /// Agent-specific kind, e.g. Claude `local_bash` / `local_agent`.
    pub task_type: Option<String>,
    pub subagent_type: Option<String>,
    /// Command line or task description.
    pub description: String,
    pub tool_use_id: Option<String>,
    /// `running`, `completed`, `failed`, `killed` (or another agent status).
    pub status: String,
    /// Host clock (Unix ms) when the host first observed the task.
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub summary: Option<String>,
    pub output_file: Option<String>,
}

impl NativeBackgroundTask {
    pub fn is_running(&self) -> bool {
        matches!(self.status.as_str(), "running" | "pending" | "paused")
    }
}

/// Result of rolling back the latest turn of a native session.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeRollbackOutcome {
    /// Workspace files the agent's own checkpoint restored.
    pub files_changed: Vec<String>,
    pub files_rewound: bool,
    /// Why files were left untouched, when they were.
    pub note: Option<String>,
}

pub type NativeAgentNotificationSink = Arc<dyn Fn(NativeAgentNotification) + Send + Sync>;

/// Prepared host resources. Provider identity and resume policy belong to the driver.
pub struct NativeAgentOpenContext {
    pub session_id: String,
    pub workspace_path: String,
    pub remote_connection_id: Option<String>,
    pub session_storage_path: PathBuf,
    pub queue: Arc<EventQueue>,
    pub notifier: Option<NativeAgentNotificationSink>,
    pub(in crate::client) persistence: Arc<AcpSessionPersistence>,
}

#[async_trait]
pub trait NativeAgentDriver: Send + Sync {
    fn client_id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    /// Label used by the existing session validation errors.
    fn session_label(&self) -> &'static str;
    fn command(&self) -> &'static str;
    fn local_executable(&self) -> PathBuf;
    fn capabilities(&self) -> NativeAgentCapabilities;
    /// Custom-metadata keys for a session forked from `source` through
    /// `turn_id`; `copied_turn_ids` lists the copied turns in storage order.
    /// The host copies OpenBitFun turns; the driver owns how its agent state
    /// branches. Keys absent from the result keep the copied value.
    fn fork_metadata(
        &self,
        _source: &serde_json::Value,
        _turn_id: &str,
        _copied_turn_ids: &[String],
    ) -> OpenBitFunResult<serde_json::Map<String, serde_json::Value>> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            format!("{} sessions cannot be forked", self.session_label()),
        ))
    }
    /// Custom-metadata keys that turn a branch of an imported `ecosystem_id`
    /// transcript into a session of this agent, resuming the agent's own copy
    /// of `source_session_id` after the last of `turns` (storage order).
    fn import_metadata(
        &self,
        _ecosystem_id: &str,
        _source_session_id: &str,
        _turns: &[openbitfun_core::service::session::DialogTurnData],
    ) -> OpenBitFunResult<serde_json::Map<String, serde_json::Value>> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            format!(
                "{} cannot continue imported transcripts",
                self.session_label()
            ),
        ))
    }
    async fn open(
        &self,
        context: NativeAgentOpenContext,
    ) -> OpenBitFunResult<Arc<dyn NativeAgentSession>>;
}

#[async_trait]
pub trait NativeAgentSession: Send + Sync {
    fn client_id(&self) -> &'static str;
    fn options(&self) -> AcpSessionOptions;
    async fn warm_up(self: Arc<Self>) -> OpenBitFunResult<()>;
    async fn start_turn(
        self: Arc<Self>,
        turn_id: String,
        input: String,
        original: Option<String>,
    ) -> OpenBitFunResult<()>;
    async fn cancel(&self) -> OpenBitFunResult<()>;
    async fn set_model(&self, id: &str) -> OpenBitFunResult<AcpSessionOptions>;
    async fn set_config_option(
        &self,
        id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions>;
    /// False leaves the permission available to the other native/ACP sessions.
    async fn respond_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> OpenBitFunResult<bool>;
    fn commands(&self) -> Vec<AcpAvailableCommand>;
    fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool;
    /// Drop the latest turn (`turn_id`) from the agent and its persisted history.
    async fn rollback_last_turn(
        self: Arc<Self>,
        _turn_id: &str,
    ) -> OpenBitFunResult<NativeRollbackOutcome> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            "This native agent does not support rollback".into(),
        ))
    }
    /// Latest background task list (also published as `BackgroundTasksUpdated`).
    fn background_tasks(&self) -> Vec<NativeBackgroundTask> {
        Vec::new()
    }
    /// Stop one running background task.
    async fn stop_task(&self, _task_id: &str) -> OpenBitFunResult<()> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            "This native agent does not support background tasks".into(),
        ))
    }
    async fn shutdown(&self);
}

static CLAUDE: claude::ClaudeNativeDriver = claude::ClaudeNativeDriver;
static DRIVERS: [&'static dyn NativeAgentDriver; 1] = [&CLAUDE];

pub fn drivers() -> &'static [&'static dyn NativeAgentDriver] {
    &DRIVERS
}

pub fn native_driver(client_id: &str) -> Option<&'static dyn NativeAgentDriver> {
    drivers()
        .iter()
        .copied()
        .find(|driver| driver.client_id() == client_id)
}

pub fn is_native_client(client_id: &str) -> bool {
    native_driver(client_id).is_some()
}
