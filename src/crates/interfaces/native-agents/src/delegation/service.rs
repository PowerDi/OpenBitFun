//! Child creation, submission, control and watch for delegated sessions.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use openbitfun_core::agentic::coordination::{
    get_global_coordinator, get_global_scheduler, DialogSubmissionPolicy, DialogTriggerSource,
};
use openbitfun_core::runtime_ports::{AgentDialogTurnPort, DialogSubmitOutcome};
use openbitfun_core::service::session::{SessionMetadata, TurnStatus};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use openbitfun_events::AgenticEvent;
use serde_json::{json, Value};

use super::snapshot::{self, SessionSnapshot};
use crate::session_persistence::AcpSessionPersistence;

/// Custom-metadata key on a delegated child: who delegated it and why.
pub(super) const CUSTOM_METADATA_DELEGATION_KEY: &str = "nativeDelegation";

/// Permission mode a delegated child runs in (owner decision: unattended).
fn unattended_permission_mode(client_id: &str) -> Option<&'static str> {
    match client_id {
        crate::claude::CLIENT_ID => Some("bypassPermissions"),
        crate::codex::CLIENT_ID | crate::pi::CLIENT_ID => Some("full-access"),
        _ => None,
    }
}

/// The session that called a delegation tool, resolved by the driver.
#[derive(Clone)]
pub struct DelegationCaller {
    pub session_id: String,
    pub client_id: String,
    /// Execution root (POSIX for remote workspaces).
    pub workspace_path: String,
    pub remote_connection_id: Option<String>,
    pub session_storage_path: PathBuf,
    pub persistence: Arc<AcpSessionPersistence>,
}

impl DelegationCaller {
    async fn metadata(&self) -> OpenBitFunResult<SessionMetadata> {
        self.persistence
            .load_metadata(&self.session_storage_path, &self.session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {}", self.session_id))
            })
    }

    pub(super) async fn child_metadata(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<SessionMetadata> {
        let metadata = self
            .persistence
            .load_metadata(&self.session_storage_path, session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!(
                    "Session {session_id} was not found in this workspace"
                ))
            })?;
        if metadata
            .external_client_id()
            .is_none_or(|id| !crate::is_native_client(id))
        {
            return Err(OpenBitFunError::Validation(format!(
                "Session {session_id} is not a native agent session"
            )));
        }
        Ok(metadata)
    }

    pub(super) async fn snapshot(
        &self,
        session_id: &str,
        messages: bool,
    ) -> OpenBitFunResult<SessionSnapshot> {
        let metadata = self.child_metadata(session_id).await?;
        let turns = self
            .persistence
            .load_turns(&self.session_storage_path, session_id)
            .await?;
        Ok(snapshot::build(
            &metadata,
            &turns,
            messages,
            is_running(session_id),
        ))
    }
}

pub(super) struct StartRequest {
    pub client_id: String,
    pub task: String,
    pub task_name: Option<String>,
    pub request_id: Option<String>,
    pub watch: bool,
}

pub(super) struct Started {
    pub session_id: String,
    pub client_id: String,
    pub queued: bool,
    pub reused: bool,
}

/// Same default as codex-host `thread watch` (29 minutes).
pub(super) const DEFAULT_WATCH_TIMEOUT: Duration = Duration::from_secs(29 * 60);

/// Idempotency: `(caller, request id)` -> child session id.
fn requests() -> &'static Mutex<std::collections::HashMap<(String, String), String>> {
    static REQUESTS: OnceLock<Mutex<std::collections::HashMap<(String, String), String>>> =
        OnceLock::new();
    REQUESTS.get_or_init(Default::default)
}

fn scheduler_unavailable() -> OpenBitFunError {
    OpenBitFunError::Validation(
        "Delegation needs the runtime scheduler with native session support; this host does not provide it"
            .into(),
    )
}

pub(super) fn is_running(session_id: &str) -> bool {
    get_global_scheduler().is_some_and(|scheduler| scheduler.is_session_busy_or_queued(session_id))
}

pub(super) async fn start(
    caller: &DelegationCaller,
    request: StartRequest,
) -> OpenBitFunResult<Started> {
    let driver = crate::native_driver(&request.client_id).ok_or_else(|| {
        let known = crate::drivers()
            .iter()
            .map(|driver| driver.client_id())
            .collect::<Vec<_>>()
            .join(", ");
        OpenBitFunError::Validation(format!(
            "Unknown agent '{}'. Available agents: {known}",
            request.client_id
        ))
    })?;
    if request.task.trim().is_empty() {
        return Err(OpenBitFunError::Validation("task must not be empty".into()));
    }
    // Refuse before creating anything on hosts that cannot run the child.
    if get_global_scheduler().is_none()
        || !openbitfun_core::agentic::coordination::external_sessions_supported(driver.client_id())
    {
        return Err(scheduler_unavailable());
    }
    if let Some(id) = request.request_id.as_deref() {
        let existing = requests()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(caller.session_id.clone(), id.to_string()))
            .cloned();
        if let Some(session_id) = existing {
            return Ok(Started {
                session_id,
                client_id: request.client_id,
                queued: false,
                reused: true,
            });
        }
    }
    let parent = caller.metadata().await?;
    let workspace_id = parent.workspace_id.clone().ok_or_else(|| {
        OpenBitFunError::Validation("The calling session has no workspace id".into())
    })?;
    let name = request
        .task_name
        .clone()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| first_line(&request.task, 40));
    let created = caller
        .persistence
        .create_flow_session_record(
            &caller.session_storage_path,
            &workspace_id,
            &caller.workspace_path,
            driver.client_id(),
            Some(name.clone()),
        )
        .await?;
    let child = created.session_id.clone();
    let mode = unattended_permission_mode(driver.client_id());
    if let Err(error) = caller
        .persistence
        .mark_delegated_child(
            &caller.session_storage_path,
            &child,
            driver.client_id(),
            mode,
            json!({
                "parentSessionId": caller.session_id,
                "parentClientId": caller.client_id,
                "requestId": request.request_id,
            }),
        )
        .await
    {
        let _ = caller
            .persistence
            .delete_flow_session_record(&caller.session_storage_path, &child)
            .await;
        return Err(error);
    }
    announce_created(caller, &parent, &created).await;
    let outcome = match submit(caller, &child, &created.agent_type, &request.task).await {
        Ok(outcome) => outcome,
        Err(error) => {
            // Never leave a published child that no turn will ever run in.
            let _ = caller
                .persistence
                .delete_flow_session_record(&caller.session_storage_path, &child)
                .await;
            emit(AgenticEvent::SessionDeleted {
                session_id: child.clone(),
            })
            .await;
            return Err(error);
        }
    };
    if let Some(id) = request.request_id {
        requests()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((caller.session_id.clone(), id), child.clone());
    }
    let (_, queued) = submitted(outcome);
    if request.watch {
        watch(caller, &child, DEFAULT_WATCH_TIMEOUT);
    }
    Ok(Started {
        session_id: child,
        client_id: request.client_id,
        queued,
        reused: false,
    })
}

fn submitted(outcome: DialogSubmitOutcome) -> (String, bool) {
    match outcome {
        DialogSubmitOutcome::Started { turn_id, .. } => (turn_id, false),
        DialogSubmitOutcome::Queued { turn_id, .. } => (turn_id, true),
    }
}

/// Submit `message` to `session_id` through the scheduler, which queues it
/// while the session is busy. `metadata` lands on the user message.
async fn submit_turn(
    caller: &DelegationCaller,
    session_id: &str,
    agent_type: &str,
    message: &str,
    metadata: serde_json::Map<String, Value>,
) -> OpenBitFunResult<DialogSubmitOutcome> {
    let scheduler = get_global_scheduler().ok_or_else(scheduler_unavailable)?;
    // Children live in the caller's workspace, so its id locates them too.
    let workspace_id = caller.metadata().await.ok().and_then(|m| m.workspace_id);
    AgentDialogTurnPort::submit_dialog_turn(
        scheduler.as_ref(),
        openbitfun_core::runtime_ports::AgentDialogTurnRequest {
            session_id: session_id.to_string(),
            message: message.to_string(),
            output_schema: None,
            original_message: None,
            turn_id: None,
            execution: Default::default(),
            agent_type: agent_type.to_string(),
            workspace_path: Some(caller.workspace_path.clone()),
            workspace_id,
            remote_connection_id: caller.remote_connection_id.clone(),
            remote_ssh_host: None,
            policy: DialogSubmissionPolicy::for_source(DialogTriggerSource::AgentSession),
            reply_route: None,
            prepended_reminders: Vec::new(),
            attachments: Vec::new(),
            metadata,
        },
    )
    .await
    .map_err(|error| OpenBitFunError::service(error.message))
}

/// Submit a task or follow-up from the caller to a child.
async fn submit(
    caller: &DelegationCaller,
    session_id: &str,
    agent_type: &str,
    message: &str,
) -> OpenBitFunResult<DialogSubmitOutcome> {
    let metadata = serde_json::Map::from_iter([(
        CUSTOM_METADATA_DELEGATION_KEY.to_string(),
        json!({"fromSessionId": caller.session_id, "fromClientId": caller.client_id}),
    )]);
    submit_turn(caller, session_id, agent_type, message, metadata).await
}

pub(super) async fn send(
    caller: &DelegationCaller,
    session_id: &str,
    message: &str,
    watch: bool,
) -> OpenBitFunResult<(String, bool)> {
    if session_id == caller.session_id {
        return Err(OpenBitFunError::Validation(
            "A session cannot send a message to itself".into(),
        ));
    }
    if message.trim().is_empty() {
        return Err(OpenBitFunError::Validation(
            "message must not be empty".into(),
        ));
    }
    let metadata = caller.child_metadata(session_id).await?;
    let outcome = submit(caller, session_id, &metadata.agent_type, message).await?;
    if watch {
        self::watch(caller, session_id, DEFAULT_WATCH_TIMEOUT);
    }
    Ok(submitted(outcome))
}

/// Cancel the running turn of `session_id`; returns the cancelled turn.
pub(super) async fn cancel(
    caller: &DelegationCaller,
    session_id: &str,
) -> OpenBitFunResult<Option<String>> {
    caller.child_metadata(session_id).await?;
    let scheduler = get_global_scheduler().ok_or_else(scheduler_unavailable)?;
    scheduler
        .cancel_active_turn_for_session(session_id, Duration::from_secs(10))
        .await
}

/// Wait until `session_id` is idle or `timeout` passes.
pub(super) async fn wait_idle(session_id: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !is_running(session_id) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Register a one-shot "notify me when it stops" watch: the next time the
/// child goes idle, the caller gets a new turn with the outcome.
pub(super) fn watch(caller: &DelegationCaller, session_id: &str, timeout: Duration) {
    let caller = caller.clone();
    let session_id = session_id.to_string();
    tokio::spawn(async move {
        let stopped = wait_idle(&session_id, timeout).await;
        let status = if stopped {
            match caller.snapshot(&session_id, false).await {
                Ok(snapshot) => snapshot.status,
                Err(_) => "unreadable".to_string(),
            }
        } else {
            "timedOut".to_string()
        };
        let message = format!(
            "Delegated session {session_id} stopped: {status}.\nThis is an automatic notice, not a user message. Use session_read to check the result."
        );
        let agent_type = caller
            .metadata()
            .await
            .map(|metadata| metadata.agent_type)
            .unwrap_or_else(|_| format!("acp:{}", caller.client_id));
        let caller_id = caller.session_id.clone();
        let metadata = serde_json::Map::from_iter([(
            "triggerSource".to_string(),
            json!(crate::NATIVE_WAKE_TRIGGER_SOURCE),
        )]);
        if let Err(error) = submit_turn(&caller, &caller_id, &agent_type, &message, metadata).await
        {
            log::warn!(
                "Delegation watch notice failed: session_id={caller_id}, child={session_id}, error={error}"
            );
        }
    });
}

/// Delegated children of the caller, newest first.
pub(super) async fn list_children(
    caller: &DelegationCaller,
    limit: usize,
) -> OpenBitFunResult<Vec<SessionSnapshot>> {
    let mut children = caller
        .persistence
        .list_metadata(&caller.session_storage_path)
        .await?
        .into_iter()
        .filter(|metadata| delegated_by(metadata) == Some(caller.session_id.as_str()))
        .collect::<Vec<_>>();
    children.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    children.truncate(limit);
    Ok(children
        .iter()
        .map(|metadata| {
            let mut snapshot =
                snapshot::build(metadata, &[], false, is_running(&metadata.session_id));
            if !snapshot.running {
                snapshot.status = metadata
                    .last_turn
                    .as_ref()
                    .map(|turn| status_label(&turn.status).to_string())
                    .unwrap_or_else(|| "idle".to_string());
            }
            snapshot
        })
        .collect())
}

pub(super) fn status_label(status: &TurnStatus) -> &'static str {
    match status {
        TurnStatus::InProgress => "running",
        TurnStatus::Completed => "completed",
        TurnStatus::Error => "failed",
        TurnStatus::Cancelled => "cancelled",
    }
}

pub(super) fn delegated_by(metadata: &SessionMetadata) -> Option<&str> {
    metadata
        .custom_metadata
        .as_ref()?
        .get(CUSTOM_METADATA_DELEGATION_KEY)?
        .get("parentSessionId")?
        .as_str()
}

async fn announce_created(
    caller: &DelegationCaller,
    parent: &SessionMetadata,
    created: &crate::CreateAcpFlowSessionRecordResponse,
) {
    emit(AgenticEvent::SessionCreated {
        session_id: created.session_id.clone(),
        session_name: created.session_name.clone(),
        agent_type: created.agent_type.clone(),
        workspace_path: Some(caller.workspace_path.clone()),
        project_workspace_path: None,
        execution_target: None,
        workspace_id: parent.workspace_id.clone(),
        remote_connection_id: caller.remote_connection_id.clone(),
        remote_ssh_host: parent
            .workspace_hostname
            .clone()
            .filter(|_| caller.remote_connection_id.is_some()),
    })
    .await;
}

async fn emit(event: AgenticEvent) {
    if let Some(coordinator) = get_global_coordinator() {
        if let Err(error) = coordinator.event_queue().enqueue(event, None).await {
            log::warn!("Delegation event enqueue failed: {error}");
        }
    }
}

fn first_line(text: &str, max_chars: usize) -> String {
    let line = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .trim();
    let mut name: String = line.chars().take(max_chars).collect();
    if line.chars().count() > max_chars {
        name.push('\u{2026}');
    }
    if name.is_empty() {
        "Delegated task".to_string()
    } else {
        name
    }
}

impl AcpSessionPersistence {
    pub(crate) async fn load_turns(
        &self,
        storage: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Vec<openbitfun_core::service::session::DialogTurnData>> {
        self.manager.load_session_turns(storage, session_id).await
    }

    pub(crate) async fn list_metadata(
        &self,
        storage: &Path,
    ) -> OpenBitFunResult<Vec<SessionMetadata>> {
        self.manager.list_session_metadata(storage).await
    }

    /// Mark a new child as delegated: parent link, unattended mode, and a
    /// fixed title (the task name), so title generation leaves it alone.
    pub(crate) async fn mark_delegated_child(
        &self,
        storage: &Path,
        session_id: &str,
        client_id: &str,
        permission_mode: Option<&str>,
        link: Value,
    ) -> OpenBitFunResult<()> {
        self.update_metadata(storage, session_id, |metadata| {
            mark_child(metadata, client_id, permission_mode, link);
            Ok(())
        })
        .await
    }
}

/// Parent link, a fixed title, and the session-scoped permission mode the
/// driver applies when it opens the child (session preferences win over the
/// client's global choice, which stays untouched).
fn mark_child(
    metadata: &mut SessionMetadata,
    client_id: &str,
    permission_mode: Option<&str>,
    link: Value,
) {
    let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
    if !custom.is_object() {
        custom = json!({});
    }
    custom[CUSTOM_METADATA_DELEGATION_KEY] = link;
    custom["titleUserEdited"] = json!(true);
    if let Some(mode) = permission_mode {
        let mut preferences =
            crate::preferences::NativeAgentPreferences::from_metadata(&custom, client_id);
        preferences.permission_mode = Some(mode.to_string());
        custom[crate::preferences::METADATA_KEY] =
            serde_json::to_value(preferences).unwrap_or(Value::Null);
    }
    metadata.custom_metadata = Some(custom);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delegated_child_is_linked_unattended_and_keeps_its_title() {
        for client in [
            crate::claude::CLIENT_ID,
            crate::codex::CLIENT_ID,
            crate::pi::CLIENT_ID,
        ] {
            let mut metadata = SessionMetadata::new(
                "child".into(),
                "Fix flaky test".into(),
                format!("acp:{client}"),
                "primary".into(),
            );
            metadata.custom_metadata = Some(json!({"provider": "acp", "acpClientId": client}));
            mark_child(
                &mut metadata,
                client,
                unattended_permission_mode(client),
                json!({"parentSessionId": "parent"}),
            );
            assert_eq!(delegated_by(&metadata), Some("parent"));
            let custom = metadata.custom_metadata.as_ref().unwrap();
            assert_eq!(custom["provider"], "acp", "existing keys are kept");
            assert_eq!(custom["titleUserEdited"], true);
            let preferences =
                crate::preferences::NativeAgentPreferences::from_metadata(custom, client);
            assert_eq!(
                preferences.permission_mode.as_deref(),
                unattended_permission_mode(client)
            );
            assert!(preferences.permission_mode.is_some(), "{client}");
        }
    }

    #[test]
    fn first_line_names_the_child() {
        assert_eq!(
            first_line("\n  Fix the build\nthen test", 40),
            "Fix the build"
        );
        assert_eq!(first_line("abcdef", 3), "abc\u{2026}");
        assert_eq!(first_line("   ", 10), "Delegated task");
    }
}
