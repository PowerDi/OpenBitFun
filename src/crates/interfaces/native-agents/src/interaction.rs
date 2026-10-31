//! Native approvals and questions through the runtime interaction owners.
//!
//! A native agent asks over its own wire protocol and waits for one answer.
//! The drivers keep that protocol state; this module publishes the ask to the
//! same owners local Agent turns use, so every surface (desktop composer band,
//! Peer controller, mobile mailbox, IM bots) sees and answers it:
//!
//! - approvals go to the runtime `PermissionRequestManager`
//!   (`source.kind = provider`, `source.identity` = agent name);
//! - questions go to the runtime user-question mailbox under the card id.
//!
//! A reply comes back as a [`SubmitAcpPermissionResponseRequest`], the
//! decision shape the drivers already validate. Native "always allow" is
//! remembered by the agent itself (Claude settings, Codex exec policy): the
//! request carries no `save_resources`, so OpenBitFun stores no grant for it.
use std::future::Future;
use std::sync::Arc;

use openbitfun_agent_runtime::permission::PermissionRequestManager;
pub use openbitfun_agent_runtime::permission::{PendingPermissionReceiver, PermissionWaitOutcome};
use openbitfun_agent_runtime::user_questions::{
    get_user_input_manager, wait_for_user_question_response_until, PendingUserQuestion,
    UserQuestionWaitOutcome,
};
use openbitfun_core::runtime_ports::{
    PermissionDelegationContext, PermissionReply, PermissionRequest, PermissionRequestSource,
    PermissionRequestSourceKind,
};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// One decision on a native approval or question.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitAcpPermissionResponseRequest {
    pub permission_id: String,
    pub approve: bool,
    #[serde(default)]
    pub option_id: Option<String>,
    /// Native question answers (question index or text -> answer value).
    #[serde(default)]
    pub answers: Option<serde_json::Value>,
    /// Optional free-text feedback sent with a rejection.
    #[serde(default)]
    pub feedback: Option<String>,
}

/// `displayMetadata` key: plan text of a plan review (Claude `ExitPlanMode`).
pub const PLAN_REVIEW_METADATA_KEY: &str = "planReview";
/// `displayMetadata` key: what the agent itself remembers on "always allow".
pub const AGENT_ALWAYS_METADATA_KEY: &str = "agentAlwaysScope";
/// `displayMetadata` key: native client id of the asking agent.
pub const NATIVE_CLIENT_METADATA_KEY: &str = "nativeClient";

/// The external agent session that asks and where it runs.
#[derive(Debug, Clone)]
pub struct InteractionScope {
    pub client_id: String,
    /// User-facing agent name, shown as the request owner.
    pub agent_name: String,
    pub workspace_path: String,
    pub remote_connection_id: Option<String>,
}

/// One approval to publish.
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalAsk {
    /// Driver key of the pending request; also the runtime request id.
    pub permission_id: String,
    /// Session whose card asks (a child session for subagents).
    pub session_id: String,
    pub turn_id: String,
    /// Native tool id of the card the request belongs to.
    pub tool_call_id: String,
    /// Permission action label (`bash`, `edit`, `read`, `mcp`, ...).
    pub action: String,
    /// Presentable summary: command, path or request text.
    pub resources: Vec<String>,
    /// What the agent remembers on "always allow"; `None` hides that choice.
    pub agent_always: Option<String>,
    /// Plan text when this is a plan review.
    pub plan: Option<String>,
    pub delegation: Option<PermissionDelegationContext>,
}

/// One question form to publish (the `AskUserQuestion` card input shape).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QuestionAsk {
    pub permission_id: String,
    /// Card id; the runtime mailbox and the card answer by this id.
    pub card_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub input: Value,
}

/// Option ids the drivers accept for a runtime reply.
pub(crate) const ALLOW_ONCE: &str = "allow_once";
pub(crate) const ALLOW_ALWAYS: &str = "allow_always";
pub(crate) const REJECT_ONCE: &str = "reject_once";

/// Driver decision for a runtime reply.
pub(crate) fn decision_for_reply(
    permission_id: &str,
    reply: &PermissionReply,
) -> SubmitAcpPermissionResponseRequest {
    let (approve, option, feedback) = match reply {
        PermissionReply::Once | PermissionReply::OnceWithInput { .. } => (true, ALLOW_ONCE, None),
        PermissionReply::Always => (true, ALLOW_ALWAYS, None),
        PermissionReply::Reject { feedback } => (
            false,
            REJECT_ONCE,
            feedback
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string),
        ),
    };
    SubmitAcpPermissionResponseRequest {
        permission_id: permission_id.to_string(),
        approve,
        option_id: Some(option.to_string()),
        answers: None,
        feedback,
    }
}

fn rejection(permission_id: &str, reason: Option<String>) -> SubmitAcpPermissionResponseRequest {
    decision_for_reply(permission_id, &PermissionReply::Reject { feedback: reason })
}

fn answered(permission_id: &str, answers: Value) -> SubmitAcpPermissionResponseRequest {
    SubmitAcpPermissionResponseRequest {
        permission_id: permission_id.to_string(),
        approve: true,
        option_id: Some(ALLOW_ONCE.to_string()),
        answers: Some(answers),
        feedback: None,
    }
}

/// Permission action label for a normalized card name.
pub(crate) fn action_for_card(card_name: &str) -> &'static str {
    match card_name {
        "ExecCommand" => "bash",
        "Edit" | "Write" | "Delete" => "edit",
        "Read" | "Grep" | "Glob" | "LS" => "read",
        "WebFetch" => "webfetch",
        "WebSearch" => "websearch",
        "Task" => "task",
        "Skill" => "skill",
        name if name.starts_with("mcp__") => "mcp",
        _ => "custom_tool",
    }
}

/// Presentable resources of a normalized card input: the command, the file,
/// or the compact input when nothing narrower identifies the call.
pub(crate) fn resources_for_card(card_name: &str, params: &Value) -> Vec<String> {
    let text = |key: &str| {
        params[key]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let narrow = match card_name {
        "ExecCommand" => text("cmd").or_else(|| text("command")),
        "Edit" | "Write" | "Delete" | "Read" => text("file_path").or_else(|| text("path")),
        "LS" => text("path"),
        "Grep" | "Glob" => text("pattern"),
        "WebFetch" => text("url"),
        "WebSearch" => text("query"),
        _ => None,
    };
    narrow.map(|value| vec![value]).unwrap_or_else(|| {
        let compact = serde_json::to_string(params).unwrap_or_default();
        if compact.is_empty() || compact == "{}" || compact == "null" {
            vec![card_name.to_string()]
        } else {
            vec![compact]
        }
    })
}

/// Permission manager the asks go to; tests install an in-memory one.
fn permission_manager() -> OpenBitFunResult<Arc<PermissionRequestManager>> {
    #[cfg(test)]
    if let Some(manager) = test_support::installed() {
        return Ok(manager);
    }
    openbitfun_core::product_runtime::core_permission_request_manager()
        .map_err(OpenBitFunError::service)
}

/// Permission project of the session's workspace (same identity local tools
/// use, so remote workspaces are keyed by host + POSIX root).
async fn project_identity(scope: &InteractionScope) -> (String, String) {
    let identity =
        openbitfun_core::service::remote_ssh::workspace_state::resolve_workspace_session_identity(
            &scope.workspace_path,
            scope.remote_connection_id.as_deref(),
            None,
        )
        .await;
    match identity {
        Some(identity) => {
            let project_path = identity.logical_workspace_path().to_string();
            match openbitfun_core::agentic::tools::pipeline::permission_project_id_for_workspace_identity(
                &identity,
                identity.is_remote(),
            ) {
                Ok(project_id) => (project_id, project_path),
                Err(error) => {
                    log::debug!("Native permission project id unavailable: {error}");
                    (format!("native_{}", scope.workspace_path), project_path)
                }
            }
        }
        None => (
            format!("native_{}", scope.workspace_path),
            scope.workspace_path.clone(),
        ),
    }
}

pub async fn permission_request(scope: &InteractionScope, ask: &ApprovalAsk) -> PermissionRequest {
    let (project_id, project_path) = project_identity(scope).await;
    let mut display_metadata = Map::new();
    display_metadata.insert(NATIVE_CLIENT_METADATA_KEY.into(), json!(scope.client_id));
    if let Some(plan) = &ask.plan {
        display_metadata.insert(PLAN_REVIEW_METADATA_KEY.into(), json!(plan));
    }
    if let Some(always) = &ask.agent_always {
        display_metadata.insert(AGENT_ALWAYS_METADATA_KEY.into(), json!(always));
    }
    PermissionRequest {
        request_id: ask.permission_id.clone(),
        round_id: ask.turn_id.clone(),
        order: 0,
        tool_call_id: Some(ask.tool_call_id.clone()),
        project_path: Some(project_path),
        project_id,
        session_id: ask.session_id.clone(),
        agent_id: scope.client_id.clone(),
        action: ask.action.clone(),
        resources: ask.resources.clone(),
        // The agent remembers "always" itself; OpenBitFun stores no grant.
        save_resources: Vec::new(),
        source: PermissionRequestSource {
            kind: PermissionRequestSourceKind::Provider,
            identity: scope.agent_name.clone(),
        },
        delegation: ask.delegation.clone(),
        display_metadata,
    }
}

/// Register an approval with the runtime owner; the receiver yields the
/// user's reply or the runtime's cancellation.
pub async fn register_approval(
    scope: &InteractionScope,
    ask: &ApprovalAsk,
) -> OpenBitFunResult<PendingPermissionReceiver> {
    let manager = permission_manager()?;
    let request = permission_request(scope, ask).await;
    manager
        .register_batch_for_turn(vec![request], ask.turn_id.clone())
        .await
        .map_err(|error| OpenBitFunError::service(error.to_string()))?
        .pop()
        .ok_or_else(|| OpenBitFunError::service("Permission request was not registered"))
}

/// Publish an approval and hand its decision to `answer`. A runtime
/// cancellation (session cancelled, request withdrawn) answers as a
/// rejection so the agent never waits on a request nobody can see; `answer`
/// returns `None` when the driver already settled the request itself.
pub(crate) async fn ask_approval<F, Fut>(
    scope: &InteractionScope,
    ask: ApprovalAsk,
    answer: F,
) -> OpenBitFunResult<()>
where
    F: Fn(SubmitAcpPermissionResponseRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<OpenBitFunResult<()>>> + Send,
{
    let permission_id = ask.permission_id.clone();
    let receiver = register_approval(scope, &ask).await?;
    tokio::spawn(async move {
        let decision = match receiver.wait().await {
            PermissionWaitOutcome::Replied(reply) => decision_for_reply(&permission_id, &reply),
            PermissionWaitOutcome::Cancelled { reason } => {
                log::debug!(
                    "Native permission cancelled: permission_id={permission_id}, reason={reason}"
                );
                rejection(&permission_id, None)
            }
        };
        if let Some(Err(error)) = answer(decision).await {
            // The runtime request is already resolved, so a refused answer
            // would leave the agent waiting with nothing on screen.
            log::warn!(
                "Native permission answer refused, rejecting instead: permission_id={permission_id}, error={error}"
            );
            if let Some(Err(error)) = answer(rejection(&permission_id, None)).await {
                log::error!(
                    "Native permission rejection failed: permission_id={permission_id}, error={error}"
                );
            }
        }
    });
    Ok(())
}

/// Publish a question form and hand its answers to `answer`. An answer the
/// driver refuses re-opens the question; a dropped question (turn ended,
/// cancelled) answers as skipped.
pub(crate) fn ask_question<F, Fut>(ask: QuestionAsk, answer: F)
where
    F: Fn(SubmitAcpPermissionResponseRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<OpenBitFunResult<()>>> + Send,
{
    let manager = get_user_input_manager();
    let register = |ask: &QuestionAsk| {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let registration = manager.register_question(
            PendingUserQuestion::new(
                ask.card_id.clone(),
                ask.session_id.clone(),
                Some(ask.turn_id.clone()),
                None,
                ask.input.clone(),
            ),
            sender,
        );
        (registration, receiver)
    };
    // Register before returning so the mailbox already lists the question.
    let first = register(&ask);
    tokio::spawn(async move {
        let mut pending = Some(first);
        loop {
            let (registration, receiver) = match pending.take() {
                Some(pending) => pending,
                None => register(&ask),
            };
            let outcome =
                wait_for_user_question_response_until(&registration, receiver, None).await;
            drop(registration);
            match outcome {
                UserQuestionWaitOutcome::Answered(response) => {
                    match answer(answered(&ask.permission_id, response.answers)).await {
                        Some(Err(error)) => {
                            log::warn!(
                                "Native question answer refused, asking again: card_id={}, error={error}",
                                ask.card_id
                            );
                            continue;
                        }
                        _ => return,
                    }
                }
                UserQuestionWaitOutcome::Cancelled | UserQuestionWaitOutcome::TimedOut => {
                    let _ = answer(rejection(&ask.permission_id, None)).await;
                    return;
                }
            }
        }
    });
}

/// Withdraw a published ask the driver settled itself (turn ended, process
/// gone). Unknown ids are ignored.
pub async fn withdraw(permission_id: &str, question_card: Option<&str>, reason: &str) {
    if let Some(card_id) = question_card {
        get_user_input_manager().cancel(card_id);
    }
    match permission_manager() {
        Ok(manager) => {
            if let Err(error) = manager.cancel_request(permission_id, reason).await {
                log::debug!("Native permission withdraw failed: {error}");
            }
        }
        Err(error) => log::debug!("Native permission manager unavailable: {error}"),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex, OnceLock};

    use openbitfun_agent_runtime::permission::PermissionRequestManager;
    use openbitfun_core::runtime_ports::{
        ClockPort, PermissionAuditRecord, PermissionAuditStorePort, PermissionGrant,
        PermissionReplyStorePort, PortResult, RuntimeServiceCapability, RuntimeServicePort,
    };

    #[derive(Debug, Default)]
    struct MemoryStore(Mutex<Vec<PermissionAuditRecord>>);

    impl RuntimeServicePort for MemoryStore {
        fn capability(&self) -> RuntimeServiceCapability {
            RuntimeServiceCapability::Permission
        }
    }

    #[async_trait::async_trait]
    impl PermissionAuditStorePort for MemoryStore {
        async fn append_permission_audit(&self, record: PermissionAuditRecord) -> PortResult<()> {
            self.0.lock().unwrap().push(record);
            Ok(())
        }
        async fn list_project_permission_audit(
            &self,
            _project_id: &str,
        ) -> PortResult<Vec<PermissionAuditRecord>> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl PermissionReplyStorePort for MemoryStore {
        async fn commit_permission_reply(
            &self,
            grants: Vec<PermissionGrant>,
            audit: Vec<PermissionAuditRecord>,
        ) -> PortResult<()> {
            assert!(grants.is_empty(), "native replies must not store grants");
            self.0.lock().unwrap().extend(audit);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct Clock;

    impl RuntimeServicePort for Clock {
        fn capability(&self) -> RuntimeServiceCapability {
            RuntimeServiceCapability::Clock
        }
    }

    impl ClockPort for Clock {
        fn now_unix_millis(&self) -> i64 {
            0
        }
    }

    static MANAGER: OnceLock<Arc<PermissionRequestManager>> = OnceLock::new();

    /// Process-wide in-memory manager for driver tests.
    pub(crate) fn manager() -> Arc<PermissionRequestManager> {
        MANAGER
            .get_or_init(|| {
                let store = Arc::new(MemoryStore::default());
                Arc::new(PermissionRequestManager::new(
                    store.clone(),
                    store,
                    Arc::new(Clock),
                ))
            })
            .clone()
    }

    pub(super) fn installed() -> Option<Arc<PermissionRequestManager>> {
        Some(manager())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openbitfun_core::runtime_ports::PermissionReplySource;

    fn scope() -> InteractionScope {
        InteractionScope {
            client_id: "claude-native".into(),
            agent_name: "Claude Code".into(),
            workspace_path: "/srv/repo".into(),
            remote_connection_id: Some("ssh-1".into()),
        }
    }

    fn ask(id: &str) -> ApprovalAsk {
        ApprovalAsk {
            permission_id: id.into(),
            session_id: "s1".into(),
            turn_id: "t1".into(),
            tool_call_id: "tool-1".into(),
            action: "bash".into(),
            resources: vec!["cargo test".into()],
            agent_always: Some("Bash(cargo test:*)".into()),
            plan: None,
            delegation: None,
        }
    }

    #[test]
    fn replies_map_to_driver_options() {
        let once = decision_for_reply("p", &PermissionReply::Once);
        assert_eq!(
            (once.approve, once.option_id.as_deref()),
            (true, Some(ALLOW_ONCE))
        );
        let always = decision_for_reply("p", &PermissionReply::Always);
        assert_eq!(always.option_id.as_deref(), Some(ALLOW_ALWAYS));
        let reject = decision_for_reply(
            "p",
            &PermissionReply::Reject {
                feedback: Some("  use a branch  ".into()),
            },
        );
        assert_eq!(
            (
                reject.approve,
                reject.option_id.as_deref(),
                reject.feedback.as_deref()
            ),
            (false, Some(REJECT_ONCE), Some("use a branch"))
        );
    }

    #[tokio::test]
    async fn remote_workspace_request_is_provider_owned_and_saves_no_grant() {
        let request = permission_request(&scope(), &ask("p")).await;
        assert_eq!(request.source.kind, PermissionRequestSourceKind::Provider);
        assert_eq!(request.source.identity, "Claude Code");
        assert!(request.save_resources.is_empty());
        assert_eq!(request.tool_call_id.as_deref(), Some("tool-1"));
        assert_eq!(request.round_id, "t1");
        // Remote paths stay POSIX on every client OS.
        assert_eq!(request.project_path.as_deref(), Some("/srv/repo"));
        assert_eq!(
            request.display_metadata[AGENT_ALWAYS_METADATA_KEY],
            "Bash(cargo test:*)"
        );
    }

    #[tokio::test]
    async fn approval_reply_and_cancellation_reach_the_driver() {
        let manager = test_support::manager();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for id in ["bridge-reply", "bridge-cancel"] {
            let tx = tx.clone();
            ask_approval(&scope(), ask(id), move |decision| {
                let tx = tx.clone();
                async move {
                    tx.send(decision).unwrap();
                    Some(Ok(()))
                }
            })
            .await
            .unwrap();
        }
        assert!(manager
            .interactive_pending_requests()
            .iter()
            .any(|request| request.request_id == "bridge-reply"));
        manager
            .reply(
                "bridge-reply",
                PermissionReply::Reject {
                    feedback: Some("no".into()),
                },
                PermissionReplySource::User,
            )
            .await
            .unwrap();
        let replied = rx.recv().await.unwrap();
        assert_eq!(replied.permission_id, "bridge-reply");
        assert_eq!(replied.feedback.as_deref(), Some("no"));
        withdraw("bridge-cancel", None, "turn ended").await;
        let cancelled = rx.recv().await.unwrap();
        assert_eq!(
            (cancelled.permission_id.as_str(), cancelled.approve),
            ("bridge-cancel", false)
        );
    }

    #[tokio::test]
    async fn refused_answers_reopen_the_question() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let refused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let question = QuestionAsk {
            permission_id: "q".into(),
            card_id: "bridge-question-card".into(),
            session_id: "bridge-question-session".into(),
            turn_id: "t1".into(),
            input: json!({"questions": []}),
        };
        ask_question(question, move |decision| {
            let tx = tx.clone();
            let refused = refused.clone();
            async move {
                tx.send(decision).unwrap();
                if refused.swap(true, std::sync::atomic::Ordering::AcqRel) {
                    Some(Ok(()))
                } else {
                    Some(Err(OpenBitFunError::Validation("unanswered".into())))
                }
            }
        });
        let manager = get_user_input_manager();
        assert_eq!(
            manager
                .pending_question_snapshot("bridge-question-session")
                .questions
                .len(),
            1
        );
        manager
            .send_answer("bridge-question-card", json!({"0": ""}))
            .unwrap();
        assert!(rx.recv().await.unwrap().answers.is_some());
        // Refused: the same card is answerable again.
        for _ in 0..100 {
            if manager.has_pending("bridge-question-card") {
                break;
            }
            tokio::task::yield_now().await;
        }
        manager
            .send_answer("bridge-question-card", json!({"0": "yes"}))
            .unwrap();
        assert_eq!(rx.recv().await.unwrap().answers, Some(json!({"0": "yes"})));
    }
}
