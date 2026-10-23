//! Native agent drivers share lifecycle dispatch, not an ACP connection.
pub mod capabilities;
pub mod claude;
mod events;
pub(crate) mod preferences;
mod process;
pub(crate) mod title;

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

/// Image formats native agents accept as inline prompt content.
pub const NATIVE_IMAGE_MEDIA_TYPES: [&str; 4] =
    ["image/png", "image/jpeg", "image/gif", "image/webp"];
/// Per-image cap on decoded bytes (the Anthropic API limit for base64 images).
pub const NATIVE_IMAGE_MAX_BYTES: usize = 5 * 1024 * 1024;

/// One inline image of a native turn or steering message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeImageInput {
    /// Normalized media type, one of [`NATIVE_IMAGE_MEDIA_TYPES`].
    pub media_type: String,
    /// Base64 payload without the `data:` prefix.
    pub data: String,
}

impl NativeImageInput {
    /// Build from a base64 `data:` URL; `mime_type` fills an empty declared
    /// type. The host resolves file attachments into data URLs first. The
    /// workspace may be remote: images travel through the agent's stdin.
    pub fn from_data_url(mime_type: &str, data_url: &str) -> OpenBitFunResult<Self> {
        let validation = openbitfun_core::util::errors::OpenBitFunError::Validation;
        let (header, data) = data_url
            .trim()
            .strip_prefix("data:")
            .and_then(|rest| rest.split_once(','))
            .filter(|(header, _)| header.ends_with(";base64"))
            .ok_or_else(|| validation("Image data URL is not base64 encoded".into()))?;
        let declared = header.trim_end_matches(";base64");
        let media_type = if declared.is_empty() {
            mime_type
        } else {
            declared
        };
        let media_type = match media_type.trim().to_ascii_lowercase().as_str() {
            "image/jpg" => "image/jpeg".to_string(),
            other => other.to_string(),
        };
        if !NATIVE_IMAGE_MEDIA_TYPES.contains(&media_type.as_str()) {
            return Err(validation(format!("Unsupported image type: {media_type}")));
        }
        let data = data.trim();
        let well_formed = !data.is_empty()
            && data.len() % 4 == 0
            && data
                .trim_end_matches('=')
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/');
        if !well_formed {
            return Err(validation("Invalid image data URL".into()));
        }
        let padding = data.len() - data.trim_end_matches('=').len();
        if data.len() / 4 * 3 - padding > NATIVE_IMAGE_MAX_BYTES {
            return Err(validation(format!(
                "Image is larger than {} MB",
                NATIVE_IMAGE_MAX_BYTES / (1024 * 1024)
            )));
        }
        Ok(Self {
            media_type,
            data: data.to_string(),
        })
    }
}

/// User input of a native turn or steering message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeTurnInput {
    pub text: String,
    pub images: Vec<NativeImageInput>,
}

impl From<String> for NativeTurnInput {
    fn from(text: String) -> Self {
        Self {
            text,
            images: Vec::new(),
        }
    }
}

impl From<&str> for NativeTurnInput {
    fn from(text: &str) -> Self {
        text.to_string().into()
    }
}

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
    /// Install/version/update facts derived from a requirement probe of
    /// `command()` on a host. `windows` is the probed host's OS.
    fn cli_status(
        &self,
        _tool: &super::AcpRequirementProbeItem,
        _windows: bool,
        _remote: bool,
    ) -> Option<super::config::NativeCliStatus> {
        None
    }
    /// Update the CLI in place on the local host; returns the CLI output.
    async fn update_cli(&self) -> OpenBitFunResult<String> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            format!("{} does not support updating its CLI", self.session_label()),
        ))
    }
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
        input: NativeTurnInput,
        original: Option<String>,
    ) -> OpenBitFunResult<()>;
    /// Deliver a user message to the running turn `turn_id` (`input.steering`).
    /// Returns the steering id the host renders the message under.
    async fn steer(
        self: Arc<Self>,
        _turn_id: &str,
        _input: NativeTurnInput,
        _display: Option<String>,
    ) -> OpenBitFunResult<String> {
        Err(openbitfun_core::util::errors::OpenBitFunError::Validation(
            "This native agent does not support steering".into(),
        ))
    }
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
    /// True once the agent's own transcript is keyed by this session's working
    /// directory (a turn was submitted, or it resumes/forks an existing
    /// transcript). A bound session must keep its cwd; an unbound one may be
    /// reopened under a new scope, e.g. after worktree isolation was applied.
    async fn workspace_bound(&self) -> bool {
        true
    }
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

/// A prepared session may move to a new cwd/host only while nothing is bound
/// to the old one; otherwise resume would silently lose the transcript.
pub(crate) fn scope_change_allowed(label: &str, bound: bool) -> OpenBitFunResult<()> {
    if bound {
        return Err(openbitfun_core::util::errors::OpenBitFunError::Validation(format!(
            "{label} sessions keep the working directory of their first turn; worktree isolation or the workspace cannot change now. Start a new session instead."
        )));
    }
    Ok(())
}

pub const MISSING_WORKSPACE_PREFIX: &str =
    "This session's working directory (worktree) no longer exists";

/// Message for a local native session whose cwd is gone (e.g. a reclaimed
/// worktree), or `None` while it exists. Checked before launch so the spawn
/// error is not misread as a missing CLI; session data is never touched.
pub fn missing_local_workspace(path: &str, resumes: bool) -> Option<String> {
    if std::path::Path::new(path).is_dir() {
        return None;
    }
    Some(if resumes {
        format!("{MISSING_WORKSPACE_PREFIX}: {path}. The agent resumes its transcript by working directory, so this session cannot continue; start a new session.")
    } else {
        format!("{MISSING_WORKSPACE_PREFIX}: {path}. Start a new session.")
    })
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
