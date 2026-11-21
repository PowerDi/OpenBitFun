//! Session Manager
//!
//! Responsible for session CRUD, lifecycle management, and resource association

mod context_estimate;
pub(crate) use context_estimate::ContextEstimateInputs;

use crate::agentic::agents::get_agent_registry;
use crate::agentic::context_normalization::normalize_incomplete_tool_calls;
use crate::agentic::core::{
    new_turn_id, CompressionContract, CompressionState, InternalReminderKind, Message,
    MessageContent, MessageRole, MessageSemanticKind, ProcessingPhase, Session,
    SessionAgentRouteOwner, SessionConfig, SessionKind, SessionModelBindingPolicy, SessionState,
    SessionSummary, ToolPresentationMode, TurnStats,
};
use crate::agentic::image_analysis::ImageContextData;
use crate::agentic::keyed_lock::{KeyedAsyncLock, KeyedAsyncLockGuard};
use crate::agentic::memories::db::{MemoryDatabase, MEMORY_PHASE2_GLOBAL_JOB_KEY};
use crate::agentic::persistence::session_branch::TransientSessionBranchSource;
use crate::agentic::persistence::{MaterializedSessionReferenceTranscript, PersistenceManager};
use crate::agentic::session::revert::SessionRevertPhase;
use crate::agentic::session::session_store_port::CoreSessionStorePort;
use crate::agentic::session::{
    prompt_cache_persist_action, reconcile_prompt_cache_restore, CachedSystemPrompt,
    CachedUserContext, EvidenceLedgerCheckpoint, EvidenceLedgerEvent, EvidenceLedgerEventStatus,
    EvidenceLedgerSummary, EvidenceLedgerTargetKind, FileRevision, PromptCacheLookup,
    PromptCachePersistenceWriteAction, PromptCachePolicy, PromptCacheRestoreDecision,
    PromptCacheScope, ReviewReadCoverage, ReviewReadReceiptStore, SessionContextStore,
    SessionEvidenceLedger, SessionPromptCache, SessionPromptCacheStore, SystemPromptCacheIdentity,
    TokenAnchor, TokenAnchorSelection, TokenAnchorStore, TurnSkillAgentSnapshotStore,
    UserContextCacheIdentity,
};
use crate::agentic::skill_agent_snapshot::TurnSkillAgentSnapshot;
use crate::agentic::workspace::WorkspaceBinding;
use crate::agentic::ConversationCoordinator;
use crate::infrastructure::ai::get_global_ai_client_factory;
use crate::infrastructure::ai::reasoning_catalog::{
    load_models_dev_reasoning_catalog_without_refresh, normalize_reasoning_preset_for_model,
    project_model_reasoning_catalog, reasoning_preset_runtime_fingerprint,
    resolve_default_reasoning_preset, resolve_reasoning_preset,
};
use crate::service::config::types::{model_runtime_binding_fingerprint, AIConfig};
use crate::service::config::{
    get_app_language_code, get_global_config_service, short_model_user_language_instruction,
    subscribe_config_updates, ConfigUpdateEvent,
};
use crate::service::session::{
    DialogTurnData, DialogTurnKind, DialogTurnRecoveryData, DialogTurnRecoveryStatus,
    ModelRoundData, SessionContextUsage, SessionMemoryMode, SessionMetadata, SessionRelationship,
    SessionStatus, TextItemData, ThinkingItemData, ToolCallData, ToolItemData, ToolResultData,
    TranscriptLineRange, TurnStatus, UserMessageData,
};
use crate::service::snapshot::{
    ensure_snapshot_manager_for_workspace, get_or_create_snapshot_manager,
};
use crate::service::workspace::{get_global_workspace_service, WorkspaceInfo};
use crate::util::errors::{OpenBitFunError, OpenBitFunResult};
use crate::util::sanitize_plain_model_output;
use crate::util::timing::elapsed_ms_u64;
use dashmap::{mapref::entry::Entry, DashMap};
use log::{debug, error, info, warn};
use openbitfun_core_types::SessionExecutionTarget;
pub use openbitfun_runtime_ports::SessionViewRestoreTiming;
use openbitfun_runtime_ports::{
    AgentTurnSettlementResult, PermissionMode, SessionStoragePathRequest, SessionStorePort,
};
use openbitfun_services_core::session::{
    apply_session_lineage, collect_hidden_subagent_cascade as collect_hidden_subagent_cascade_ids,
    merge_session_custom_metadata as merge_session_custom_metadata_value,
    set_deep_review_run_manifest, set_review_target_evidence, set_session_relationship,
    SessionStorageLayout, SessionWriteLock,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::time::{Duration, SystemTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time;

const TOOL_PRESENTATION_MODE_CONFIG_PATH: &str = "ai.tool_presentation_mode";

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_MODEL_RESOLUTION_AI_CONFIG: crate::service::config::types::AIConfig;
}

/// Session manager configuration
#[derive(Debug, Clone)]
pub struct SessionManagerConfig {
    pub max_active_sessions: usize,
    pub session_idle_timeout: Duration,
    pub auto_save_interval: Duration,
    pub enable_persistence: bool,
    pub prompt_cache_policy: PromptCachePolicy,
}

/// Stable locator supplied by the UI for a session reference. The workspace
/// identity is required because session IDs are normally UUIDs but are not a
/// globally unique contract across all persisted workspaces.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionReferenceLocator {
    pub session_id: String,
    /// Owning workspace ID; authoritative when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    /// Upgrade-only pre-ID storage selector. New producers send workspace_id.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub workspace_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_ssh_host: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MaterializedSessionReference {
    pub session_id: String,
    pub session_name: String,
    pub transcript: MaterializedSessionReferenceTranscript,
}

pub(crate) const INTERRUPTED_TURN_PERMISSION_MODE_METADATA_KEY: &str = "resolved_permission_mode";
pub(crate) const INTERRUPTED_TURN_RESOLVED_MODEL_ID_METADATA_KEY: &str =
    "runtime_resolved_model_id";
pub(crate) const INTERRUPTED_TURN_MODEL_BINDING_FINGERPRINT_METADATA_KEY: &str =
    "runtime_model_binding_fingerprint";
pub(crate) const INTERRUPTED_TURN_REASONING_PRESET_METADATA_KEY: &str = "runtime_reasoning_preset";
pub(crate) const INTERRUPTED_TURN_REASONING_SELECTION_METADATA_KEY: &str =
    "runtime_reasoning_selection";
pub(crate) const INTERRUPTED_TURN_REASONING_FINGERPRINT_METADATA_KEY: &str =
    "runtime_reasoning_fingerprint";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnAdmissionSessionFacts {
    model_id: Option<String>,
    model_routing: Option<String>,
    model_pool_id: Option<String>,
    model_pool_selection_revision: u64,
    reasoning_preset: Option<String>,
    permission_mode: Option<PermissionMode>,
    max_context_tokens: usize,
    agent_type: String,
    agent_route_owner: SessionAgentRouteOwner,
    agent_route_key: Option<String>,
    enable_tools: bool,
    workspace_path: Option<String>,
    project_workspace_path: Option<String>,
    execution_target: Option<SessionExecutionTarget>,
    workspace_id: Option<String>,
    remote_connection_id: Option<String>,
    remote_ssh_host: Option<String>,
}

impl TurnAdmissionSessionFacts {
    pub(crate) fn from_session(session: &Session) -> Self {
        Self {
            model_id: session.config.model_id.clone(),
            model_routing: session.config.model_routing.clone(),
            model_pool_id: session.config.model_pool_id.clone(),
            model_pool_selection_revision: session.config.model_pool_selection_revision,
            reasoning_preset: session.config.reasoning_preset.clone(),
            permission_mode: session.config.permission_mode,
            max_context_tokens: session.config.max_context_tokens,
            agent_type: session.agent_type.clone(),
            agent_route_owner: session.config.agent_route_owner,
            agent_route_key: session.config.agent_route_key.clone(),
            enable_tools: session.config.enable_tools,
            workspace_path: session.config.workspace_path.clone(),
            project_workspace_path: session.config.project_workspace_path.clone(),
            execution_target: session.config.execution_target.clone(),
            workspace_id: session.config.workspace_id.clone(),
            remote_connection_id: session.config.remote_connection_id.clone(),
            remote_ssh_host: session.config.remote_ssh_host.clone(),
        }
    }

    pub(crate) fn with_reasoning_preset(mut self, reasoning_preset: Option<String>) -> Self {
        self.reasoning_preset = reasoning_preset;
        self
    }

    fn matches(&self, session: &Session) -> bool {
        self.model_routing == session.config.model_routing
            && self.model_id == session.config.model_id
            && self.model_pool_id == session.config.model_pool_id
            && self.model_pool_selection_revision == session.config.model_pool_selection_revision
            && self.reasoning_preset == session.config.reasoning_preset
            && self.permission_mode == session.config.permission_mode
            && self.max_context_tokens == session.config.max_context_tokens
            && self.agent_type == session.agent_type
            && self.agent_route_owner == session.config.agent_route_owner
            && self.agent_route_key == session.config.agent_route_key
            && self.enable_tools == session.config.enable_tools
            && self.workspace_path == session.config.workspace_path
            && self.project_workspace_path == session.config.project_workspace_path
            && self.execution_target == session.config.execution_target
            && self.workspace_id == session.config.workspace_id
            && self.remote_connection_id == session.config.remote_connection_id
            && self.remote_ssh_host == session.config.remote_ssh_host
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InterruptedTurnRecoveryPlan {
    pub session_id: String,
    pub turn_id: String,
    pub turn_index: usize,
    pub agent_type: String,
    pub execution_generation: u32,
    pub resume_count: u32,
    pub initial_round_index: usize,
    pub resolved_permission_mode: PermissionMode,
    pub resolved_model_id: String,
    pub model_binding_fingerprint: String,
    pub user_input: String,
    pub messages: Vec<Message>,
    pub user_message_metadata: Option<serde_json::Value>,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            max_active_sessions: 100,
            session_idle_timeout: Duration::from_secs(3600), // 1 hour
            auto_save_interval: Duration::from_secs(300),    // 5 minutes
            enable_persistence: true,
            prompt_cache_policy: PromptCachePolicy::default(),
        }
    }
}

fn should_apply_session_model_fallback(
    binding_policy: SessionModelBindingPolicy,
    current_model_id: &str,
    invalidated_model_ids: &HashSet<&str>,
) -> bool {
    session_model_allows_fallback(binding_policy)
        && invalidated_model_ids.contains(current_model_id)
}

fn session_model_allows_fallback(binding_policy: SessionModelBindingPolicy) -> bool {
    binding_policy == SessionModelBindingPolicy::Mutable
}

/// Project the same authoritative selection for runtime and persisted headers.
pub fn session_model_target(
    session: Session,
) -> openbitfun_core_types::model_pool::SessionModelTarget {
    use openbitfun_core_types::model_pool::{ModelSelection, SessionModelTarget};
    let selection = if let Some(pool_id) = session.config.model_pool_id {
        ModelSelection::Pool { pool_id }
    } else {
        ModelSelection::Model {
            model_ref: session.config.model_id.unwrap_or_else(|| "primary".into()),
        }
    };
    SessionModelTarget {
        session_id: session.session_id,
        selection,
        reasoning_preset: session.config.reasoning_preset,
        binding: session.config.model_pool_binding,
    }
}

fn effective_session_model_selector(
    ai_config: &crate::service::config::types::AIConfig,
    session: &Session,
) -> String {
    session
        .config
        .model_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if session.kind == SessionKind::Subagent {
                "primary".into()
            } else {
                ai_config.mode_model_reference().unwrap_or_default()
            }
        })
}

fn concrete_model_for_session_selection<'a>(
    ai_config: &'a crate::service::config::types::AIConfig,
    session: &Session,
) -> Option<&'a crate::service::config::types::AIModelConfig> {
    let configured_model_id = effective_session_model_selector(ai_config, session);

    let resolved_model_id = if matches!(
        session.config.model_binding_policy,
        SessionModelBindingPolicy::ApprovedImmutable
    ) {
        ai_config.resolve_model_reference(&configured_model_id)
    } else {
        ai_config.resolve_model_selection(&configured_model_id)
    }?;

    ai_config
        .models
        .iter()
        .find(|model| model.enabled && model.id == resolved_model_id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionTranscriptReference {
    pub uri: String,
    pub index_range: TranscriptLineRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionTitleMethod {
    Ai,
    Fallback,
}

impl SessionTitleMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ai => "ai",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSessionTitle {
    pub title: String,
    pub method: SessionTitleMethod,
}

/// Where an `Inherit` session-title task model takes its model from.
enum TitleModelSource<'a> {
    /// A Runtime session, looked up before the AI call.
    Session {
        id: &'a str,
        session: Option<Session>,
    },
    /// No Runtime session (e.g. a native external-agent session).
    Detached,
}

/// Whether first-turn titles may call the configured title model
/// (`app.ai_experience.enable_session_title_generation`, default on).
pub async fn is_ai_session_title_generation_enabled() -> bool {
    match get_global_config_service().await {
        Ok(service) => service
            .get_config::<bool>(Some("app.ai_experience.enable_session_title_generation"))
            .await
            .unwrap_or(true),
        Err(_) => true,
    }
}

// When a full skill/agent listing baseline is rebuilt at turn R, snapshots whose
// turn_index < R still contain now-redundant listing diff reminders. We do not
// eagerly rewrite all historical snapshots; instead restore/rollback sanitize those
// older snapshots lazily based on this persisted cutoff.
const LISTING_BASELINE_REBUILD_TURN_INDEX_METADATA_KEY: &str = "listingBaselineRebuildTurnIndex";

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

/// Where a session executes, as a single atomic rebind.
#[derive(Debug, Clone)]
pub struct SessionExecutionBindingUpdate {
    pub workspace_path: String,
    pub project_workspace_path: String,
    pub workspace_id: Option<String>,
    pub execution_target: SessionExecutionTarget,
}

/// Stable failure categories for atomically moving a session execution root.
///
/// Worktree lifecycle maps these categories to its public structured error
/// contract without having to inspect human-readable `OpenBitFunError` messages.
#[derive(Debug, thiserror::Error)]
pub enum SessionExecutionBindingError {
    #[error("{0}")]
    Busy(String),
    #[error("{0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] OpenBitFunError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionResourceCleanupPolicy {
    BestEffort,
    Required,
}

/// Session manager
pub struct SessionManager {
    /// Active sessions in memory
    sessions: Arc<DashMap<String, Session>>,

    /// Ephemeral permission override for the currently executing turn.
    ///
    /// This state is intentionally separate from `SessionConfig`: it may change
    /// between model rounds, and it must disappear when the owning turn ends.
    active_turn_permission_modes: Arc<DashMap<String, ActiveTurnPermissionMode>>,

    /// Process-local durability classification owned by the Session lifecycle.
    /// Entries are installed before a transient Session becomes visible and are
    /// removed with that Session; they are never serialized into public config.
    transient_session_ids: Arc<DashMap<String, ()>>,

    /// Authoritative temporary history survives context compression until explicit closure.
    transient_turns: Arc<DashMap<String, TransientSessionHistory>>,

    /// Recent authoritative terminal results for live Turn settlement callers.
    /// The bounded cache preserves the exact execution result; persisted Turns
    /// remain the fallback, while transient Sessions depend on this copy.
    turn_settlement_results: Arc<DashMap<(String, String), AgentTurnSettlementResult>>,
    turn_settlement_result_order: Arc<Mutex<VecDeque<(String, String)>>>,

    /// Exact admission accounting for loaded sessions. A permit is acquired
    /// before create/restore publishes runtime state and released on unload/delete/eviction.
    active_session_capacity: Arc<Semaphore>,
    active_session_permits: Arc<DashMap<String, OwnedSemaphorePermit>>,

    /// Runtime cache of session_id -> effective session storage path.
    /// Populated on session create/restore and used to restore evicted sessions
    /// or resolve workspace-bound operations that only receive a session_id.
    /// This cache is intentionally retained across memory eviction, but should
    /// be cleared when a session is explicitly deleted.
    session_storage_path_index: Arc<DashMap<String, SessionStoragePathBinding>>,

    /// Serializes create, restore, and delete mutations for one session ID.
    ///
    /// Storage-path claims prevent cross-workspace identity collisions, while
    /// this permit prevents a slower restore from replacing a session that a
    /// concurrent operation has already made active.
    session_mutation_locks: KeyedAsyncLock,

    /// Cross-process writers for durable Sessions currently loaded by this manager.
    /// The Session lifecycle remains the only owner of acquisition and release.
    session_write_locks: Arc<DashMap<String, SessionWriteLock>>,

    /// Sub-components
    context_store: Arc<SessionContextStore>,
    prompt_cache_store: Arc<SessionPromptCacheStore>,
    prompt_cache_operation_locks: KeyedAsyncLock,
    token_anchor_store: Arc<TokenAnchorStore>,
    turn_skill_agent_snapshot_store: Arc<TurnSkillAgentSnapshotStore>,
    skill_agent_baseline_override_snapshot_store: Arc<DashMap<String, TurnSkillAgentSnapshot>>,
    /// Session-scoped edit-constraint state. The in-memory copy serves the hot
    /// tool-validation path; the same state is persisted in session metadata so
    /// restore and fork paths preserve both constraints and extraction evidence.
    edit_constraints_store:
        Arc<DashMap<String, crate::agentic::execution::edit_constraint_guard::EditConstraintState>>,
    review_read_receipt_store: Arc<ReviewReadReceiptStore>,
    evidence_ledger: Arc<SessionEvidenceLedger>,
    evidence_ledger_operation_locks: Arc<KeyedAsyncLock>,
    persistence_manager: Arc<PersistenceManager>,
    memory_database: Arc<MemoryDatabase>,

    /// Configuration
    config: SessionManagerConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTurnPermissionMode {
    pub turn_id: String,
    pub mode: PermissionMode,
}

#[derive(Default)]
struct TransientSessionHistory {
    turns: Vec<DialogTurnData>,
    context_snapshots: BTreeMap<usize, Vec<Message>>,
}

fn clear_session_runtime_stores(
    session_id: &str,
    context_store: &SessionContextStore,
    transient_turns: &DashMap<String, TransientSessionHistory>,
    prompt_cache_store: &SessionPromptCacheStore,
    token_anchor_store: &TokenAnchorStore,
    turn_skill_agent_snapshot_store: &TurnSkillAgentSnapshotStore,
    skill_agent_baseline_override_snapshot_store: &DashMap<String, TurnSkillAgentSnapshot>,
    review_read_receipt_store: &ReviewReadReceiptStore,
    evidence_ledger: &SessionEvidenceLedger,
) {
    context_store.delete_session(session_id);
    transient_turns.remove(session_id);
    prompt_cache_store.delete_session(session_id);
    token_anchor_store.delete_session(session_id);
    turn_skill_agent_snapshot_store.delete_session(session_id);
    skill_agent_baseline_override_snapshot_store.remove(session_id);
    review_read_receipt_store.delete_session(session_id);
    evidence_ledger.delete_session(session_id);
}

#[derive(Clone)]
struct SessionAutoSaveSnapshot {
    session_id: String,
    updated_at: SystemTime,
    last_activity_at: SystemTime,
    session: Session,
}

#[derive(Clone)]
struct SessionCleanupCandidate {
    session_id: String,
    updated_at: SystemTime,
    last_activity_at: SystemTime,
}

#[derive(Clone, Debug)]
struct SessionStoragePathBinding {
    path: PathBuf,
    pending_claims: usize,
    committed: bool,
}

impl SessionManager {
    async fn lock_session_mutation(&self, session_id: &str) -> KeyedAsyncLockGuard {
        self.session_mutation_locks.lock(session_id).await
    }

    pub(crate) async fn acquire_session_mutation(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<KeyedAsyncLockGuard> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        Ok(self.lock_session_mutation(session_id).await)
    }

    fn reserve_active_session(&self) -> OpenBitFunResult<OwnedSemaphorePermit> {
        self.active_session_capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                OpenBitFunError::Validation(format!(
                    "Exceeded maximum session limit: {}",
                    self.config.max_active_sessions
                ))
            })
    }

    fn commit_active_session_reservation(&self, session_id: &str, permit: OwnedSemaphorePermit) {
        let previous = self
            .active_session_permits
            .insert(session_id.to_string(), permit);
        debug_assert!(previous.is_none(), "active session permit already existed");
    }

    fn release_active_session_reservation(&self, session_id: &str) {
        self.active_session_permits.remove(session_id);
    }

    fn try_acquire_session_write_lock(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<SessionWriteLock> {
        self.persistence_manager
            .lock_session_writes(session_storage_path, session_id)
    }

    fn commit_session_write_lock(&self, session_id: &str, write_lock: SessionWriteLock) {
        match self.session_write_locks.entry(session_id.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(write_lock);
            }
            Entry::Occupied(_) => {
                debug_assert!(false, "Session write lock already existed");
            }
        }
    }

    fn release_session_write_lock(&self, session_id: &str) {
        self.session_write_locks.remove(session_id);
    }

    #[cfg(test)]
    pub(crate) fn evict_loaded_session_for_test(&self, session_id: &str) {
        self.sessions.remove(session_id);
        self.transient_session_ids.remove(session_id);
        self.transient_turns.remove(session_id);
        self.clear_turn_settlement_results(session_id);
        self.release_active_session_reservation(session_id);
        self.release_session_write_lock(session_id);
    }

    #[cfg(test)]
    pub(crate) fn storage_path_binding_for_test(&self, session_id: &str) -> Option<PathBuf> {
        self.session_storage_path_index
            .get(session_id)
            .map(|binding| binding.path.clone())
    }

    fn normalize_session_storage_path(path: &Path) -> PathBuf {
        dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    }

    fn claim_session_storage_path(
        &self,
        session_id: &str,
        requested_path: &Path,
        allow_existing_same_path: bool,
    ) -> OpenBitFunResult<bool> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let requested_path = Self::normalize_session_storage_path(requested_path);
        match self
            .session_storage_path_index
            .entry(session_id.to_string())
        {
            Entry::Vacant(entry) => {
                entry.insert(SessionStoragePathBinding {
                    path: requested_path,
                    pending_claims: 1,
                    committed: false,
                });
                Ok(true)
            }
            Entry::Occupied(mut entry) => {
                let existing_path = Self::normalize_session_storage_path(&entry.get().path);
                if existing_path != requested_path {
                    return Err(OpenBitFunError::Validation(format!(
                        "Session ID is already bound to another workspace: session_id={}, existing_storage_path={}, requested_storage_path={}",
                        session_id,
                        existing_path.display(),
                        requested_path.display()
                    )));
                }
                if !allow_existing_same_path {
                    return Err(OpenBitFunError::Validation(format!(
                        "Session ID already exists: {session_id}"
                    )));
                }
                if entry.get().committed {
                    Ok(false)
                } else {
                    entry.get_mut().pending_claims += 1;
                    Ok(true)
                }
            }
        }
    }

    fn commit_session_storage_path_claim(
        &self,
        session_id: &str,
        requested_path: &Path,
        claimed: bool,
    ) {
        if !claimed {
            return;
        }
        let requested_path = Self::normalize_session_storage_path(requested_path);
        if let Entry::Occupied(mut entry) = self
            .session_storage_path_index
            .entry(session_id.to_string())
        {
            if Self::normalize_session_storage_path(&entry.get().path) == requested_path {
                let binding = entry.get_mut();
                binding.committed = true;
                binding.pending_claims = 0;
            }
        }
    }

    fn release_failed_session_storage_path_claim(
        &self,
        session_id: &str,
        requested_path: &Path,
        claimed: bool,
    ) {
        if !claimed {
            return;
        }
        let requested_path = Self::normalize_session_storage_path(requested_path);
        let session_exists = self.sessions.contains_key(session_id);
        if let Entry::Occupied(mut entry) = self
            .session_storage_path_index
            .entry(session_id.to_string())
        {
            if Self::normalize_session_storage_path(&entry.get().path) != requested_path {
                return;
            }
            if session_exists {
                let binding = entry.get_mut();
                binding.committed = true;
                binding.pending_claims = 0;
                return;
            }
            let binding = entry.get_mut();
            binding.pending_claims = binding.pending_claims.saturating_sub(1);
            if binding.pending_claims == 0 && !binding.committed {
                entry.remove();
            }
        }
    }

    fn bind_session_storage_path_committed(&self, session_id: &str, path: PathBuf) {
        self.session_storage_path_index.insert(
            session_id.to_string(),
            SessionStoragePathBinding {
                path,
                pending_claims: 0,
                committed: true,
            },
        );
    }

    pub(crate) fn ensure_session_storage_path(
        &self,
        session_id: &str,
        requested_path: &Path,
    ) -> OpenBitFunResult<()> {
        let claimed = self.claim_session_storage_path(session_id, requested_path, true)?;
        self.commit_session_storage_path_claim(session_id, requested_path, claimed);
        Ok(())
    }

    pub(crate) fn validate_session_storage_path_binding(
        &self,
        session_id: &str,
        requested_path: &Path,
    ) -> OpenBitFunResult<()> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let requested_path = Self::normalize_session_storage_path(requested_path);
        let Some(binding) = self.session_storage_path_index.get(session_id) else {
            return Ok(());
        };
        let existing_path = Self::normalize_session_storage_path(&binding.path);
        if existing_path != requested_path {
            return Err(OpenBitFunError::Validation(format!(
                "Session ID is already bound to another workspace: session_id={}, existing_storage_path={}, requested_storage_path={}",
                session_id,
                existing_path.display(),
                requested_path.display()
            )));
        }
        Ok(())
    }

    pub(crate) fn is_session_loaded_from_storage_path(
        &self,
        storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<bool> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        if !self.sessions.contains_key(session_id) {
            return Ok(false);
        }
        self.ensure_session_storage_path(session_id, storage_path)?;
        Ok(true)
    }

    pub(crate) async fn active_turn_id_in_storage_path(
        &self,
        storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Option<String>> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        self.validate_session_storage_path_binding(session_id, storage_path)?;
        Ok(self
            .get_session(session_id)
            .and_then(|session| match session.state {
                SessionState::Processing {
                    current_turn_id, ..
                } => Some(current_turn_id),
                _ => None,
            }))
    }

    pub(crate) async fn load_ai_config_for_model_resolution() -> Option<AIConfig> {
        #[cfg(test)]
        if let Ok(ai_config) = TEST_MODEL_RESOLUTION_AI_CONFIG.try_with(Clone::clone) {
            return Some(ai_config);
        }

        let config_service = get_global_config_service().await.ok()?;
        Self::load_effective_ai_config_from_service(config_service.as_ref()).await
    }

    pub(crate) async fn load_ai_config_for_session(
        config: &SessionConfig,
    ) -> OpenBitFunResult<Option<AIConfig>> {
        #[cfg(test)]
        if let Ok(ai_config) = TEST_MODEL_RESOLUTION_AI_CONFIG.try_with(Clone::clone) {
            return Ok(Some(ai_config));
        }
        let Ok(service) = get_global_config_service().await else {
            return Ok(None);
        };
        service
            .get_project_ai_config(
                config
                    .project_workspace_id
                    .as_deref()
                    .or(config.workspace_id.as_deref()),
            )
            .await
            .map(Some)
    }

    async fn load_effective_ai_config_from_service(
        config_service: &crate::service::config::ConfigService,
    ) -> Option<AIConfig> {
        config_service.get_effective_ai_config().await.ok()
    }

    pub(crate) async fn resolve_effective_reasoning_preset_for_turn(
        resolved_model_id: &str,
        selected_preset: Option<&str>,
    ) -> OpenBitFunResult<(Option<String>, String)> {
        let ai_config = Self::load_ai_config_for_model_resolution()
            .await
            .ok_or_else(|| {
                OpenBitFunError::AIClient(
                    "AI configuration is unavailable for reasoning preset resolution".to_string(),
                )
            })?;
        Self::resolve_effective_reasoning_preset_from_config(
            &ai_config,
            resolved_model_id,
            selected_preset,
        )
        .await
    }

    async fn resolve_effective_reasoning_preset_from_config(
        ai_config: &AIConfig,
        resolved_model_id: &str,
        selected_preset: Option<&str>,
    ) -> OpenBitFunResult<(Option<String>, String)> {
        let canonical_model_id = ai_config
            .resolve_model_reference(resolved_model_id)
            .ok_or_else(|| {
                OpenBitFunError::AIClient(format!(
                    "Dialog turn model is unavailable for reasoning preset resolution: {resolved_model_id}"
                ))
            })?;
        let model = ai_config
            .models
            .iter()
            .find(|model| model.enabled && model.id == canonical_model_id)
            .ok_or_else(|| {
                OpenBitFunError::AIClient(format!(
                    "Dialog turn model configuration is unavailable: {canonical_model_id}"
                ))
            })?;
        let models_dev = load_models_dev_reasoning_catalog_without_refresh().await;
        let projection = project_model_reasoning_catalog(model, models_dev.catalog.as_deref());
        let preset = match selected_preset
            .map(str::trim)
            .filter(|preset| !preset.is_empty())
        {
            Some(selected_preset) => resolve_reasoning_preset(&projection, selected_preset)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(format!(
                        "Reasoning preset is unavailable for the dialog turn model: {selected_preset}"
                    ))
                })
                .map(Some)?,
            None => resolve_default_reasoning_preset(&projection),
        };
        Ok((
            preset.map(|preset| preset.id.clone()),
            reasoning_preset_runtime_fingerprint(preset),
        ))
    }

    fn context_window_for_model_selection(
        ai_config: &crate::service::config::types::AIConfig,
        model_id: &str,
    ) -> Option<usize> {
        let trimmed = model_id.trim();
        let resolved_model_id = ai_config.resolve_model_selection(trimmed)?;
        ai_config
            .models
            .iter()
            .find(|model| model.id == resolved_model_id)
            .and_then(|model| model.context_window)
            .map(|tokens| tokens as usize)
    }

    fn session_context_window_from_ai_config(
        session: &Session,
        ai_config: &crate::service::config::types::AIConfig,
    ) -> Option<usize> {
        Self::context_window_for_model_selection(
            ai_config,
            &effective_session_model_selector(ai_config, session),
        )
    }

    fn sync_session_context_window_from_ai_config(
        session: &mut Session,
        ai_config: &crate::service::config::types::AIConfig,
    ) -> Option<usize> {
        if session.config.model_binding_policy == SessionModelBindingPolicy::Pool {
            return None;
        }
        let context_window = Self::session_context_window_from_ai_config(session, ai_config)?;
        session.config.max_context_tokens = context_window;
        Some(context_window)
    }

    async fn normalize_session_reasoning_preset(
        session: &Session,
        ai_config: &crate::service::config::types::AIConfig,
    ) -> Option<String> {
        if session.config.model_binding_policy == SessionModelBindingPolicy::Pool {
            return None;
        }
        let Some(preset_id) = session
            .config
            .reasoning_preset
            .as_deref()
            .map(str::trim)
            .filter(|preset_id| !preset_id.is_empty() && !preset_id.eq_ignore_ascii_case("auto"))
        else {
            return None;
        };
        let Some(model) = concrete_model_for_session_selection(ai_config, session) else {
            // Model reconciliation owns unavailable model selectors. Do not
            // erase the preset while the concrete model is unresolved.
            return Some(preset_id.to_string());
        };
        let models_dev = load_models_dev_reasoning_catalog_without_refresh().await;
        normalize_reasoning_preset_for_model(model, models_dev.catalog.as_deref(), Some(preset_id))
    }

    fn normalize_session_title_input(title: &str) -> OpenBitFunResult<String> {
        let trimmed = title.trim();
        if trimmed.is_empty() {
            return Err(OpenBitFunError::validation(
                "Session title must not be empty".to_string(),
            ));
        }

        Ok(trimmed.to_string())
    }

    fn normalize_whitespace(value: &str) -> String {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn truncate_chars(value: &str, max_length: usize) -> String {
        value.chars().take(max_length).collect()
    }

    fn fallback_session_title(user_message: &str, max_length: usize) -> String {
        let max_length = max_length.max(1);
        let normalized = Self::normalize_whitespace(user_message);

        if normalized.is_empty() {
            return Self::truncate_chars("New Session", max_length);
        }

        let truncated_chars: Vec<char> = normalized.chars().take(max_length).collect();
        if normalized.chars().count() <= max_length {
            return truncated_chars.iter().collect();
        }

        let sentence_break_chars = ['。', '！', '？', '；', '.', '!', '?'];
        let break_chars = ['。', '！', '？', '；', '.', '!', '?', '，', ',', ' '];
        let min_break_index = max_length / 2;
        let mut best_break_index: Option<usize> = None;

        for (idx, ch) in truncated_chars.iter().enumerate() {
            if break_chars.contains(ch) && idx > min_break_index {
                best_break_index = Some(idx);
            }
        }

        if let Some(idx) = best_break_index {
            let candidate: String = truncated_chars[..=idx].iter().collect();
            if candidate
                .chars()
                .last()
                .map(|ch| sentence_break_chars.contains(&ch))
                .unwrap_or(false)
            {
                return candidate;
            }

            return format!("{}...", candidate.trim_end());
        }

        let truncated: String = truncated_chars.iter().collect();
        format!("{truncated}...")
    }

    fn paginate_messages(
        messages: &[Message],
        limit: usize,
        before_message_id: Option<&str>,
    ) -> (Vec<Message>, bool) {
        if messages.is_empty() {
            return (vec![], false);
        }

        let end_idx = if let Some(before_id) = before_message_id {
            messages.iter().position(|m| m.id == before_id).unwrap_or(0)
        } else {
            messages.len()
        };

        if end_idx == 0 {
            return (vec![], false);
        }

        let start_idx = end_idx.saturating_sub(limit);
        let has_more = start_idx > 0;

        (messages[start_idx..end_idx].to_vec(), has_more)
    }

    fn session_workspace_from_config(config: &SessionConfig) -> Option<PathBuf> {
        config.workspace_path.as_ref().map(PathBuf::from)
    }

    fn should_persist_session_kind(kind: SessionKind) -> bool {
        match kind {
            SessionKind::Standard | SessionKind::Subagent => true,
            SessionKind::EphemeralChild => false,
        }
    }

    fn should_persist_session_with_transient_ids(
        session: &Session,
        transient_session_ids: &DashMap<String, ()>,
    ) -> bool {
        !transient_session_ids.contains_key(&session.session_id)
            && Self::should_persist_session_kind(session.kind)
    }

    fn should_persist_session(&self, session: &Session) -> bool {
        Self::should_persist_session_with_transient_ids(session, &self.transient_session_ids)
    }

    fn same_session_version(
        session: &Session,
        updated_at: SystemTime,
        last_activity_at: SystemTime,
    ) -> bool {
        session.updated_at == updated_at && session.last_activity_at == last_activity_at
    }

    fn collect_auto_save_snapshots(
        sessions: &DashMap<String, Session>,
        transient_session_ids: &DashMap<String, ()>,
    ) -> Vec<SessionAutoSaveSnapshot> {
        sessions
            .iter()
            .filter_map(|entry| {
                let session = entry.value();
                if !Self::should_persist_session_with_transient_ids(session, transient_session_ids)
                {
                    return None;
                }
                Some(SessionAutoSaveSnapshot {
                    session_id: session.session_id.clone(),
                    updated_at: session.updated_at,
                    last_activity_at: session.last_activity_at,
                    session: session.clone(),
                })
            })
            .collect()
    }

    fn auto_save_snapshot_is_current(
        sessions: &DashMap<String, Session>,
        snapshot: &SessionAutoSaveSnapshot,
    ) -> bool {
        sessions
            .get(&snapshot.session_id)
            .map(|session| {
                Self::same_session_version(&session, snapshot.updated_at, snapshot.last_activity_at)
            })
            .unwrap_or(false)
    }

    fn auto_save_interval(interval: Duration) -> time::Interval {
        time::interval_at(time::Instant::now() + interval, interval)
    }

    fn is_session_expired(session: &Session, now: SystemTime, timeout: Duration) -> bool {
        now.duration_since(session.last_activity_at)
            .map(|idle_duration| idle_duration > timeout)
            .unwrap_or(false)
    }

    fn collect_expired_session_candidates(
        sessions: &DashMap<String, Session>,
        transient_session_ids: &DashMap<String, ()>,
        now: SystemTime,
        timeout: Duration,
    ) -> Vec<SessionCleanupCandidate> {
        sessions
            .iter()
            .filter_map(|entry| {
                let session = entry.value();
                // Idle eviction is a restore optimization for durable Sessions.
                // Non-persistent Sessions have an explicit lifecycle owner and
                // no on-disk state from which they could be restored.
                if !matches!(session.state, SessionState::Idle)
                    || !Self::should_persist_session_with_transient_ids(
                        session,
                        transient_session_ids,
                    )
                    || !Self::is_session_expired(session, now, timeout)
                {
                    return None;
                }
                Some(SessionCleanupCandidate {
                    session_id: session.session_id.clone(),
                    updated_at: session.updated_at,
                    last_activity_at: session.last_activity_at,
                })
            })
            .collect()
    }

    fn cleanup_candidate_matches_session(
        session: &Session,
        candidate: &SessionCleanupCandidate,
        now: SystemTime,
        timeout: Duration,
    ) -> bool {
        matches!(session.state, SessionState::Idle)
            && Self::same_session_version(session, candidate.updated_at, candidate.last_activity_at)
            && Self::is_session_expired(session, now, timeout)
    }

    fn cleanup_snapshot_for_candidate(
        sessions: &DashMap<String, Session>,
        candidate: &SessionCleanupCandidate,
        now: SystemTime,
        timeout: Duration,
    ) -> Option<Session> {
        sessions.get(&candidate.session_id).and_then(|session| {
            Self::cleanup_candidate_matches_session(&session, candidate, now, timeout)
                .then(|| session.clone())
        })
    }

    pub fn should_persist_session_id(&self, session_id: &str) -> bool {
        self.config.enable_persistence
            && !self.transient_session_ids.contains_key(session_id)
            && self
                .sessions
                .get(session_id)
                .map(|session| self.should_persist_session(&session))
                .unwrap_or(true)
    }

    pub(crate) fn is_transient_session(&self, session_id: &str) -> bool {
        self.transient_session_ids.contains_key(session_id)
    }

    async fn effective_storage_path_for_config_with_persistence(
        persistence_manager: &PersistenceManager,
        config: &SessionConfig,
    ) -> Option<PathBuf> {
        CoreSessionStorePort::with_path_manager(persistence_manager.path_manager().clone())
            .resolve_storage_for_reference(
                config.workspace_id.as_deref(),
                config
                    .workspace_path
                    .as_deref()
                    .or(config.project_workspace_path.as_deref())
                    .unwrap_or_default(),
                config.remote_connection_id.clone(),
                config.remote_ssh_host.clone(),
            )
            .await
            .ok()
            .map(|resolution| resolution.effective_storage_path)
    }

    async fn effective_storage_path_for_config(&self, config: &SessionConfig) -> Option<PathBuf> {
        Self::effective_storage_path_for_config_with_persistence(
            self.persistence_manager.as_ref(),
            config,
        )
        .await
    }

    pub(crate) async fn resolve_storage_path_for_workspace_path(
        &self,
        workspace_path: &Path,
    ) -> OpenBitFunResult<PathBuf> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self
            .resolve_storage_path_for_request(SessionStoragePathRequest {
                workspace_path: workspace_path.to_path_buf(),
                remote_connection_id: None,
                remote_ssh_host: None,
            })
            .await?;
        debug!(
            "Session storage path resolved from workspace: workspace_path={}, session_storage_path={}, duration_ms={}",
            workspace_path.display(),
            session_storage_path.display(),
            elapsed_ms_u64(storage_path_started_at)
        );
        Ok(session_storage_path)
    }

    async fn resolve_storage_path_for_restore_workspace_path(
        &self,
        workspace_path: &Path,
    ) -> OpenBitFunResult<PathBuf> {
        if self
            .persistence_manager
            .is_resolved_sessions_dir(workspace_path)
        {
            return Err(OpenBitFunError::Validation(format!(
                "Expected a workspace path, received a resolved sessions directory: {}",
                workspace_path.display()
            )));
        }
        self.resolve_storage_path_for_workspace_path(workspace_path)
            .await
    }

    async fn resolve_storage_path_for_request(
        &self,
        request: SessionStoragePathRequest,
    ) -> OpenBitFunResult<PathBuf> {
        let storage_path_started_at = Instant::now();
        let requested_workspace_path = request.workspace_path.clone();
        let session_storage_path = CoreSessionStorePort::with_path_manager(
            self.persistence_manager.path_manager().clone(),
        )
        .resolve_session_storage_path(request)
        .await
        .map(|resolution| resolution.effective_storage_path)
        .map_err(|error| OpenBitFunError::Session(error.to_string()))?;
        debug!(
            "Session storage path resolved from workspace request: workspace_path={}, session_storage_path={}, duration_ms={}",
            requested_workspace_path.display(),
            session_storage_path.display(),
            elapsed_ms_u64(storage_path_started_at)
        );
        Ok(session_storage_path)
    }

    #[allow(dead_code)]
    fn session_workspace_path(&self, session_id: &str) -> Option<PathBuf> {
        self.sessions
            .get(session_id)
            .and_then(|session| Self::session_workspace_from_config(&session.config))
    }

    fn committed_session_storage_path(
        index: &DashMap<String, SessionStoragePathBinding>,
        session_id: &str,
    ) -> Option<PathBuf> {
        index
            .get(session_id)
            .filter(|binding| binding.committed)
            .map(|binding| binding.path.clone())
    }

    /// Storage binding committed by an earlier create/restore admission, kept
    /// after the session is unloaded from memory.
    pub(crate) fn committed_session_storage_path_for(&self, session_id: &str) -> Option<PathBuf> {
        Self::committed_session_storage_path(&self.session_storage_path_index, session_id)
    }

    /// Reuse the storage binding admitted when this session was created/restored.
    /// A workspace path is an execution projection, never a replacement for this
    /// binding. In particular, a later same-path SSH record or runtime-layout
    /// change must not redirect history, persistence, or queued continuations.
    ///
    /// Without a committed binding the path is resolved from the session config
    /// on every call. Only create/restore admission commits a binding; a read
    /// must not pin whatever the current workspace catalog happens to resolve.
    pub(crate) async fn require_session_storage_path(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<PathBuf> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        if let Some(path) =
            Self::committed_session_storage_path(&self.session_storage_path_index, session_id)
        {
            return Ok(path);
        }
        let config = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?
            .config;
        let resolution = CoreSessionStorePort::with_path_manager(self.path_manager())
            .resolve_storage_for_reference(
                config.workspace_id.as_deref(),
                config
                    .workspace_path
                    .as_deref()
                    .or(config.project_workspace_path.as_deref())
                    .unwrap_or_default(),
                config.remote_connection_id,
                config.remote_ssh_host,
            )
            .await
            .map_err(|error| OpenBitFunError::Session(error.to_string()))?;
        self.validate_session_storage_path_binding(session_id, &resolution.effective_storage_path)?;
        Ok(resolution.effective_storage_path)
    }

    pub(crate) async fn effective_session_storage_path(&self, session_id: &str) -> Option<PathBuf> {
        self.require_session_storage_path(session_id).await.ok()
    }

    pub(crate) fn path_manager(&self) -> Arc<crate::infrastructure::PathManager> {
        self.persistence_manager.path_manager().clone()
    }

    pub(crate) async fn load_related_dialog_turn(
        &self,
        parent_session_id: &str,
        related_session_id: &str,
        dialog_turn_id: &str,
    ) -> OpenBitFunResult<Option<DialogTurnData>> {
        let storage_path = self
            .effective_session_storage_path(parent_session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!(
                    "Session storage path not found: {parent_session_id}"
                ))
            })?;
        let _related_history_read = self.acquire_session_mutation(related_session_id).await?;
        Ok(self
            .persistence_manager
            .load_visible_session_turns(&storage_path, related_session_id)
            .await?
            .into_iter()
            .find(|turn| turn.turn_id == dialog_turn_id))
    }

    pub async fn create_compression_transcript_reference(
        &self,
        session_id: &str,
        boundary_turn_index: usize,
        compression_id: &str,
        trigger: &str,
    ) -> OpenBitFunResult<Option<CompressionTranscriptReference>> {
        if !self.should_persist_session_id(session_id) {
            return Ok(None);
        }
        let storage_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session storage path is unavailable: {}",
                    session_id
                ))
            })?;
        let artifact = self
            .persistence_manager
            .create_compression_transcript(
                &storage_path,
                session_id,
                boundary_turn_index,
                compression_id,
                trigger,
            )
            .await?;
        if let Some(artifact) = artifact {
            debug!(
                "Created compression transcript: session_id={}, boundary_turn_index={}, transcript_path={}, meta_path={}",
                session_id,
                boundary_turn_index,
                artifact.transcript_path.display(),
                artifact.meta_path.display()
            );
            Ok(Some(CompressionTranscriptReference {
                uri: artifact.uri,
                index_range: artifact.index_range,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn persistent_model_exchange_trace_dir(&self, session_id: &str) -> Option<PathBuf> {
        self.persistent_session_layout(session_id)
            .await
            .map(|layout| layout.request_traces_dir(session_id))
    }

    /// Runtime-owned checkpoints share the session snapshot directory, not diagnostic traces.
    pub(crate) async fn persistent_context_snapshot_dir(
        &self,
        session_id: &str,
    ) -> Option<PathBuf> {
        self.persistent_session_layout(session_id)
            .await
            .map(|layout| layout.snapshots_dir(session_id))
    }

    async fn persistent_session_layout(&self, session_id: &str) -> Option<SessionStorageLayout> {
        if !self.should_persist_session_id(session_id) {
            return None;
        }

        let storage_path = self.effective_session_storage_path(session_id).await?;

        Some(SessionStorageLayout::new(storage_path))
    }

    /// Materialize a bounded transcript copy for a user-selected reference.
    /// The referenced session is only read by the backend; the generated file
    /// is written beneath the current session's artifacts so normal Read/Grep
    /// tools cannot traverse into another session's storage.
    pub async fn materialize_session_reference_transcript(
        &self,
        source_session_id: &str,
        reference: &SessionReferenceLocator,
        reference_artifact_stem: &str,
    ) -> OpenBitFunResult<MaterializedSessionReference> {
        openbitfun_core_types::validate_session_id(source_session_id)
            .map_err(OpenBitFunError::Validation)?;
        openbitfun_core_types::validate_session_id(&reference.session_id)
            .map_err(OpenBitFunError::Validation)?;
        openbitfun_core_types::validate_session_id(reference_artifact_stem)
            .map_err(OpenBitFunError::Validation)?;
        let workspace_id = reference
            .workspace_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty());
        let workspace_path = reference.workspace_path.trim();
        if workspace_id.is_none() && workspace_path.is_empty() {
            return Err(OpenBitFunError::Validation(
                "Referenced session workspace_id is required".to_string(),
            ));
        }

        let source_storage_path = self
            .effective_session_storage_path(source_session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!(
                    "Current session storage path is unavailable: {}",
                    source_session_id
                ))
            })?;
        let reference_storage_path = CoreSessionStorePort::with_path_manager(
            self.persistence_manager.path_manager().clone(),
        )
        .resolve_storage_for_reference(
            workspace_id,
            workspace_path,
            reference.remote_connection_id.clone(),
            reference.remote_ssh_host.clone(),
        )
        .await
        .map(|resolution| resolution.effective_storage_path)
        .map_err(|error| OpenBitFunError::Session(error.to_string()))?;

        if source_session_id == reference.session_id
            && source_storage_path == reference_storage_path
        {
            return Err(OpenBitFunError::Validation(
                "A session cannot reference itself".to_string(),
            ));
        }

        let metadata = self
            .persistence_manager
            .load_session_metadata(&reference_storage_path, &reference.session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!(
                    "Referenced session not found: {}",
                    reference.session_id
                ))
            })?;
        if metadata.status == SessionStatus::Archived {
            return Err(OpenBitFunError::Validation(format!(
                "Referenced session is archived: {}",
                reference.session_id
            )));
        }
        if !matches!(metadata.session_kind, SessionKind::Standard) {
            return Err(OpenBitFunError::Validation(format!(
                "Referenced session is not a visible top-level session: {}",
                reference.session_id
            )));
        }
        if self
            .get_session(&reference.session_id)
            .is_some_and(|session| matches!(session.state, SessionState::Processing { .. }))
        {
            return Err(OpenBitFunError::Validation(format!(
                "Referenced session is busy: {}",
                reference.session_id
            )));
        }

        let _reference_history_read = self.acquire_session_mutation(&reference.session_id).await?;

        let transcript = self
            .persistence_manager
            .materialize_session_reference_transcript(
                &source_storage_path,
                source_session_id,
                &reference_storage_path,
                &reference.session_id,
                reference_artifact_stem,
            )
            .await?;
        Ok(MaterializedSessionReference {
            session_id: reference.session_id.clone(),
            session_name: metadata.session_name,
            transcript,
        })
    }

    pub async fn resolve_session_workspace_binding(
        &self,
        session_id: &str,
    ) -> Option<WorkspaceBinding> {
        if let Some(config) = self
            .get_session(session_id)
            .map(|session| session.config.clone())
        {
            if let Some(binding) = ConversationCoordinator::build_workspace_binding(&config).await {
                return Some(binding);
            }
        }

        let indexed_storage_path =
            Self::committed_session_storage_path(&self.session_storage_path_index, session_id);
        if let Some(session_storage_path) = indexed_storage_path {
            if let Some(binding) = self
                .resolve_persisted_session_workspace_binding(
                    session_id,
                    &session_storage_path,
                    None,
                )
                .await
            {
                return Some(binding);
            }
        }

        for workspace in self.tracked_workspace_candidates().await? {
            let Some(session_storage_path) =
                Self::session_storage_path_for_workspace_info(&workspace).await
            else {
                continue;
            };

            if let Some(binding) = self
                .resolve_persisted_session_workspace_binding(
                    session_id,
                    &session_storage_path,
                    Some(&workspace),
                )
                .await
            {
                if let Err(error) =
                    self.ensure_session_storage_path(session_id, &session_storage_path)
                {
                    debug!(
                        "Ignoring conflicting persisted session workspace binding: session_id={}, storage_path={}, error={}",
                        session_id,
                        session_storage_path.display(),
                        error
                    );
                    continue;
                }
                return Some(binding);
            }
        }

        None
    }

    async fn resolve_persisted_session_workspace_binding(
        &self,
        session_id: &str,
        session_storage_path: &Path,
        workspace_hint: Option<&WorkspaceInfo>,
    ) -> Option<WorkspaceBinding> {
        let metadata = match self
            .persistence_manager
            .load_session_metadata(session_storage_path, session_id)
            .await
        {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return None,
            Err(err) => {
                debug!(
                    "Failed to load session metadata while resolving workspace binding: session_id={} storage_path={} error={}",
                    session_id,
                    session_storage_path.display(),
                    err
                );
                return None;
            }
        };

        let config = self
            .session_config_from_persisted_metadata(&metadata, workspace_hint)
            .await?;

        ConversationCoordinator::build_workspace_binding(&config).await
    }

    async fn session_config_from_persisted_metadata(
        &self,
        metadata: &SessionMetadata,
        workspace_hint: Option<&WorkspaceInfo>,
    ) -> Option<SessionConfig> {
        let mut config = SessionConfig {
            tool_presentation_mode: Default::default(),
            workspace_id: metadata
                .workspace_id
                .clone()
                .or_else(|| workspace_hint.map(|record| record.id.clone())),
            project_workspace_id: metadata.project_workspace_id.clone(),
            workspace_path: metadata.workspace_path.clone().or_else(|| {
                workspace_hint.map(|record| record.root_path.to_string_lossy().into_owned())
            }),
            project_workspace_path: metadata.project_workspace_path.clone(),
            execution_target: metadata.execution_target.clone(),
            remote_ssh_host: metadata.workspace_hostname.clone(),
            ..SessionConfig::default()
        };
        crate::agentic::workspace::normalize_session_workspace(&mut config)
            .await
            .ok()?;
        Some(config)
    }

    /// Storage of a persisted Session that was never loaded or restored on
    /// this host, found in the tracked workspaces and bound for later calls.
    /// Externally driven Sessions are never loaded, so a freshly created one
    /// has no committed binding until this finds it.
    pub(crate) async fn locate_persisted_session_storage_path(
        &self,
        session_id: &str,
    ) -> Option<PathBuf> {
        openbitfun_core_types::validate_session_id(session_id).ok()?;
        if let Some(path) =
            Self::committed_session_storage_path(&self.session_storage_path_index, session_id)
        {
            return Some(path);
        }
        for workspace in self.tracked_workspace_candidates().await? {
            let Some(storage) = Self::session_storage_path_for_workspace_info(&workspace).await
            else {
                continue;
            };
            if !matches!(
                self.persistence_manager
                    .load_session_metadata(&storage, session_id)
                    .await,
                Ok(Some(_))
            ) {
                continue;
            }
            match self.ensure_session_storage_path(session_id, &storage) {
                Ok(()) => return Some(storage),
                Err(error) => debug!(
                    "Ignoring conflicting persisted session storage: session_id={}, storage_path={}, error={}",
                    session_id,
                    storage.display(),
                    error
                ),
            }
        }
        None
    }

    async fn tracked_workspace_candidates(&self) -> Option<Vec<WorkspaceInfo>> {
        let workspace_service = get_global_workspace_service()?;
        let mut workspaces = workspace_service.list_workspace_infos().await;
        workspaces.sort_by_key(|workspace| std::cmp::Reverse(workspace.last_accessed));
        Some(workspaces)
    }

    async fn session_storage_path_for_workspace_info(workspace: &WorkspaceInfo) -> Option<PathBuf> {
        CoreSessionStorePort::default()
            .resolve_workspace_storage(&workspace.id)
            .await
            .ok()
            .map(|resolution| resolution.effective_storage_path)
    }

    fn build_messages_from_turns(turns: &[DialogTurnData]) -> Vec<Message> {
        let mut messages = Vec::new();

        for turn in turns {
            if !turn.kind.is_model_visible() {
                continue;
            }

            let user_message = if let Some(metadata) = &turn.user_message.metadata {
                let images = metadata
                    .get("images")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .map(|value| ImageContextData {
                                id: value
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default()
                                    .to_string(),
                                image_path: value
                                    .get("image_path")
                                    .and_then(|v| v.as_str())
                                    .map(str::to_string),
                                data_url: value
                                    .get("data_url")
                                    .and_then(|v| v.as_str())
                                    .map(str::to_string),
                                mime_type: value
                                    .get("mime_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("image/png")
                                    .to_string(),
                                metadata: Some(value.clone()),
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                if images.is_empty() {
                    Message::user(turn.user_message.content.clone())
                } else {
                    Message::user_multimodal(turn.user_message.content.clone(), images)
                }
            } else {
                Message::user(turn.user_message.content.clone())
            };
            messages.push(
                user_message
                    .with_turn_id(turn.turn_id.clone())
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput),
            );

            let assistant_text = turn
                .model_rounds
                .iter()
                .flat_map(|round| round.text_items.iter())
                .map(|item| item.content.clone())
                .filter(|value| !value.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");

            let assistant_thinking = turn
                .model_rounds
                .iter()
                .flat_map(|round| round.thinking_items.iter())
                .map(|item| item.content.clone())
                .filter(|value| !value.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");

            let has_text = !assistant_text.trim().is_empty();
            let has_thinking = !assistant_thinking.trim().is_empty();

            if has_text || has_thinking {
                let reasoning_content = if has_thinking {
                    Some(assistant_thinking)
                } else {
                    None
                };
                messages.push(
                    Message::assistant_with_reasoning(
                        reasoning_content,
                        assistant_text,
                        Vec::new(),
                    )
                    .with_turn_id(turn.turn_id.clone()),
                );
            }
        }

        messages
    }

    async fn rebuild_messages_from_turns(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Vec<Message>> {
        let turns = self
            .persistence_manager
            .load_visible_session_turns(workspace_path, session_id)
            .await?;
        Ok(Self::build_messages_from_turns(&turns))
    }

    /// Persist the current runtime context by overwriting `snapshots/context-{turn_index}.json`.
    ///
    /// Save timing is intentionally tied to semantic context changes rather than token chunks:
    /// - after a turn starts and the user message enters runtime context
    /// - after assistant/tool messages are appended to runtime context
    /// - after compression replaces runtime context
    /// - once more when a turn completes or fails
    ///
    /// This is still a best-effort multi-file persistence flow, not a transactional commit.
    /// `session.json`, `turns/turn-*.json`, and `snapshots/context-*.json` may be briefly out of
    /// sync if the process crashes between writes, so restore logic must tolerate partial updates.
    async fn persist_context_snapshot_for_turn_best_effort(
        &self,
        session_id: &str,
        turn_index: usize,
        reason: &str,
    ) {
        if !self.should_persist_session_id(session_id) {
            if let Some(mut history) = self.transient_turns.get_mut(session_id) {
                history.context_snapshots.insert(
                    turn_index,
                    self.context_store.get_context_messages(session_id),
                );
            }
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping context snapshot persistence because workspace path is unavailable: session_id={}, turn_index={}, reason={}",
                session_id, turn_index, reason
            );
            return;
        };

        let context_messages = self.context_store.get_context_messages(session_id);
        if let Err(err) = self
            .persistence_manager
            .save_turn_context_snapshot(&workspace_path, session_id, turn_index, &context_messages)
            .await
        {
            warn!(
                "failed to persist context snapshot: session_id={}, turn_index={}, reason={}, err={}",
                session_id, turn_index, reason, err
            );
        }
    }

    async fn persist_current_turn_context_snapshot_best_effort(
        &self,
        session_id: &str,
        reason: &str,
    ) {
        let Some(turn_index) = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.len().checked_sub(1))
        else {
            debug!(
                "Skipping current-turn context snapshot because no turn is active: session_id={}, reason={}",
                session_id, reason
            );
            return;
        };

        self.persist_context_snapshot_for_turn_best_effort(session_id, turn_index, reason)
            .await;
    }

    async fn ensure_prompt_cache_loaded(&self, session_id: &str) {
        if self.prompt_cache_store.has_session(session_id) {
            return;
        }
        let _operation_guard = self.prompt_cache_operation_locks.lock(session_id).await;
        if self.prompt_cache_store.has_session(session_id) {
            return;
        }

        let cache = if self.should_persist_session_id(session_id) {
            match self.effective_session_storage_path(session_id).await {
                Some(workspace_path) => {
                    match self
                        .load_prompt_cache_from_persistence(&workspace_path, session_id)
                        .await
                    {
                        Ok(Some(cache)) => cache,
                        Ok(None) => SessionPromptCache::default(),
                        Err(error) => {
                            warn!(
                                "Failed to load prompt cache: session_id={}, workspace_path={}, error={}",
                                session_id,
                                workspace_path.display(),
                                error
                            );
                            SessionPromptCache::default()
                        }
                    }
                }
                None => SessionPromptCache::default(),
            }
        } else {
            SessionPromptCache::default()
        };

        self.prompt_cache_store.replace_cache(session_id, cache);
    }

    async fn load_turn_skill_agent_snapshot_from_persistence(
        &self,
        workspace_path: &Path,
        session_id: &str,
        turn_index: usize,
    ) -> OpenBitFunResult<Option<TurnSkillAgentSnapshot>> {
        self.persistence_manager
            .load_turn_skill_agent_snapshot(workspace_path, session_id, turn_index)
            .await
    }

    async fn load_prompt_cache_from_persistence(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Option<SessionPromptCache>> {
        let cache = match self
            .persistence_manager
            .load_prompt_cache(workspace_path, session_id)
            .await?
        {
            Some(cache) => cache,
            None => return Ok(None),
        };

        let decision =
            reconcile_prompt_cache_restore(cache, self.config.prompt_cache_policy.persistence_ttl);
        match &decision {
            PromptCacheRestoreDecision::DeleteExpired => {
                self.persistence_manager
                    .delete_prompt_cache(workspace_path, session_id)
                    .await?;
            }
            PromptCacheRestoreDecision::SavePruned(cache) => {
                self.persistence_manager
                    .save_prompt_cache(workspace_path, session_id, cache)
                    .await?;
            }
            PromptCacheRestoreDecision::Keep(_) => {}
        }
        Ok(decision.into_cache())
    }

    async fn persist_prompt_cache_best_effort(&self, session_id: &str, reason: &str) {
        if !self.should_persist_session_id(session_id) {
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping prompt cache persistence because workspace path is unavailable: session_id={}, reason={}",
                session_id, reason
            );
            return;
        };
        let _operation_guard = self.prompt_cache_operation_locks.lock(session_id).await;

        let cache = self
            .prompt_cache_store
            .get_cache(session_id)
            .unwrap_or_default();

        let persist_result = match prompt_cache_persist_action(&cache) {
            PromptCachePersistenceWriteAction::Delete => {
                self.persistence_manager
                    .delete_prompt_cache(&workspace_path, session_id)
                    .await
            }
            PromptCachePersistenceWriteAction::Save => {
                self.persistence_manager
                    .save_prompt_cache(&workspace_path, session_id, &cache)
                    .await
            }
        };

        if let Err(error) = persist_result {
            warn!(
                "Failed to persist prompt cache: session_id={}, workspace_path={}, reason={}, error={}",
                session_id,
                workspace_path.display(),
                reason,
                error
            );
        }
    }

    async fn ensure_token_anchors_loaded(&self, session_id: &str) {
        if self.token_anchor_store.has_session(session_id) {
            return;
        }

        let anchors = if self.should_persist_session_id(session_id) {
            match self.effective_session_storage_path(session_id).await {
                Some(workspace_path) => match self
                    .persistence_manager
                    .load_token_anchors(&workspace_path, session_id)
                    .await
                {
                    Ok(Some(anchors)) => anchors,
                    Ok(None) => Vec::new(),
                    Err(error) => {
                        warn!(
                            "Failed to load token anchors: session_id={}, workspace_path={}, error={}",
                            session_id,
                            workspace_path.display(),
                            error
                        );
                        Vec::new()
                    }
                },
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        if let Some(stats) = self.token_anchor_store.replace_session(session_id, anchors) {
            debug!(
                "Token anchor retention pruned loaded anchors: session_id={}, before={}, after={}, removed={}, recent_limit={}, retained_recent={}, retained_turn_boundaries={}",
                session_id,
                stats.before,
                stats.after,
                stats.removed,
                stats.recent_limit,
                stats.retained_recent,
                stats.retained_turn_boundaries
            );
        }
    }

    async fn persist_token_anchors_best_effort(&self, session_id: &str, reason: &str) {
        if !self.should_persist_session_id(session_id) {
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping token anchor persistence because workspace path is unavailable: session_id={}, reason={}",
                session_id, reason
            );
            return;
        };

        let anchors = self.token_anchor_store.anchors(session_id);
        let persist_result = if anchors.is_empty() {
            self.persistence_manager
                .delete_token_anchors(&workspace_path, session_id)
                .await
        } else {
            self.persistence_manager
                .save_token_anchors(&workspace_path, session_id, &anchors)
                .await
        };

        if let Err(error) = persist_result {
            warn!(
                "Failed to persist token anchors: session_id={}, workspace_path={}, reason={}, error={}",
                session_id,
                workspace_path.display(),
                reason,
                error
            );
        }
    }

    pub async fn remember_token_anchor(&self, anchor: TokenAnchor) {
        let session_id = anchor.session_id.clone();
        self.ensure_token_anchors_loaded(&session_id).await;
        if let Some(stats) = self.token_anchor_store.append(anchor) {
            debug!(
                "Token anchor retention pruned anchors: session_id={}, before={}, after={}, removed={}, recent_limit={}, retained_recent={}, retained_turn_boundaries={}",
                session_id,
                stats.before,
                stats.after,
                stats.removed,
                stats.recent_limit,
                stats.retained_recent,
                stats.retained_turn_boundaries
            );
        }
        self.persist_token_anchors_best_effort(&session_id, "token_anchor_recorded")
            .await;
    }

    pub async fn latest_matching_token_anchor(
        &self,
        session_id: &str,
        messages: &[Message],
    ) -> Option<TokenAnchor> {
        self.select_latest_matching_token_anchor(session_id, messages)
            .await
            .selected
    }

    pub async fn select_latest_matching_token_anchor(
        &self,
        session_id: &str,
        messages: &[Message],
    ) -> TokenAnchorSelection {
        self.ensure_token_anchors_loaded(session_id).await;
        self.token_anchor_store
            .select_latest_matching(session_id, messages)
    }

    pub async fn select_latest_matching_token_anchor_for_model(
        &self,
        session_id: &str,
        messages: &[Message],
        model_id: &str,
    ) -> TokenAnchorSelection {
        self.ensure_token_anchors_loaded(session_id).await;
        self.token_anchor_store.select_latest_matching_for_model(
            session_id,
            messages,
            Some(model_id),
        )
    }

    pub async fn prune_token_anchors_to_messages(&self, session_id: &str, messages: &[Message]) {
        self.ensure_token_anchors_loaded(session_id).await;
        self.token_anchor_store
            .remove_non_matching(session_id, messages);
        self.persist_token_anchors_best_effort(session_id, "token_anchor_pruned")
            .await;
    }

    pub fn new(
        context_store: Arc<SessionContextStore>,
        persistence_manager: Arc<PersistenceManager>,
        config: SessionManagerConfig,
    ) -> Self {
        let enable_persistence = config.enable_persistence;
        let memory_database = Arc::new(MemoryDatabase::new(
            persistence_manager.path_manager().clone(),
        ));

        let manager = Self {
            sessions: Arc::new(DashMap::new()),
            active_turn_permission_modes: Arc::new(DashMap::new()),
            transient_session_ids: Arc::new(DashMap::new()),
            transient_turns: Arc::new(DashMap::new()),
            turn_settlement_results: Arc::new(DashMap::new()),
            turn_settlement_result_order: Arc::new(Mutex::new(VecDeque::new())),
            active_session_capacity: Arc::new(Semaphore::new(config.max_active_sessions)),
            active_session_permits: Arc::new(DashMap::new()),
            session_storage_path_index: Arc::new(DashMap::new()),
            session_mutation_locks: KeyedAsyncLock::default(),
            session_write_locks: Arc::new(DashMap::new()),
            context_store,
            prompt_cache_store: Arc::new(SessionPromptCacheStore::new()),
            prompt_cache_operation_locks: KeyedAsyncLock::default(),
            token_anchor_store: Arc::new(TokenAnchorStore::new()),
            turn_skill_agent_snapshot_store: Arc::new(TurnSkillAgentSnapshotStore::new()),
            skill_agent_baseline_override_snapshot_store: Arc::new(DashMap::new()),
            edit_constraints_store: Arc::new(DashMap::new()),
            review_read_receipt_store: Arc::new(ReviewReadReceiptStore::new()),
            evidence_ledger: Arc::new(SessionEvidenceLedger::new()),
            evidence_ledger_operation_locks: Arc::new(KeyedAsyncLock::default()),
            persistence_manager,
            memory_database,
            config,
        };

        // Start background tasks
        if enable_persistence {
            manager.spawn_auto_save_task();
        }
        manager.spawn_cleanup_task();
        manager.spawn_model_reconciliation_listener();

        manager
    }

    pub(crate) fn persistence_manager(&self) -> Arc<PersistenceManager> {
        self.persistence_manager.clone()
    }

    pub(crate) fn record_turn_settlement_result(
        &self,
        session_id: &str,
        turn_id: &str,
        result: AgentTurnSettlementResult,
    ) {
        const MAX_RECENT_TURN_SETTLEMENT_RESULTS: usize = 1_024;
        let key = (session_id.to_string(), turn_id.to_string());
        let mut order = self
            .turn_settlement_result_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        order.retain(|existing| existing != &key);
        self.turn_settlement_results.insert(key.clone(), result);
        order.push_back(key);
        while order.len() > MAX_RECENT_TURN_SETTLEMENT_RESULTS {
            if let Some(oldest) = order.pop_front() {
                self.turn_settlement_results.remove(&oldest);
            }
        }
    }

    pub(crate) fn turn_settlement_result(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Option<AgentTurnSettlementResult> {
        self.turn_settlement_results
            .get(&(session_id.to_string(), turn_id.to_string()))
            .map(|entry| entry.value().clone())
    }

    fn clear_turn_settlement_results(&self, session_id: &str) {
        let mut order = self
            .turn_settlement_result_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        order.retain(|key| {
            if key.0 != session_id {
                return true;
            }
            self.turn_settlement_results.remove(key);
            false
        });
    }

    fn clear_turn_settlement_result(&self, session_id: &str, turn_id: &str) {
        let key = (session_id.to_string(), turn_id.to_string());
        let mut order = self
            .turn_settlement_result_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        order.retain(|existing| existing != &key);
        self.turn_settlement_results.remove(&key);
    }

    pub async fn append_evidence_event(
        &self,
        event: EvidenceLedgerEvent,
    ) -> OpenBitFunResult<EvidenceLedgerEvent> {
        let _mutation_guard = self.lock_session_mutation(&event.session_id).await;
        let _operation_guard = self
            .evidence_ledger_operation_locks
            .lock(&event.session_id)
            .await;
        let should_persist = self.config.enable_persistence
            && self
                .sessions
                .get(&event.session_id)
                .is_some_and(|session| self.should_persist_session(&session));
        if !should_persist {
            return Ok(self.evidence_ledger.append(event));
        }

        let storage_path = self
            .effective_session_storage_path(&event.session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::session(format!(
                    "Session storage path unavailable while persisting evidence: {}",
                    event.session_id
                ))
            })?;
        let persisted_events = self
            .persistence_manager
            .append_evidence_ledger_event(&storage_path, &event)
            .await?;
        // Project the persisted events to the session's currently visible
        // turns before publishing to memory. This prevents stale evidence
        // (from a sidecar that has not yet been converged, e.g. after an
        // older build rewrote session history) from re-entering the runtime.
        let visible_events = {
            let visible_turn_ids = self
                .sessions
                .get(&event.session_id)
                .map(|session| {
                    session
                        .dialog_turn_ids
                        .iter()
                        .cloned()
                        .collect::<std::collections::HashSet<String>>()
                })
                .unwrap_or_default();
            if visible_turn_ids.is_empty() {
                persisted_events
            } else {
                persisted_events
                    .into_iter()
                    .filter(|e| visible_turn_ids.contains(&e.turn_id))
                    .collect::<Vec<_>>()
            }
        };
        self.evidence_ledger
            .replace_session(&event.session_id, visible_events)
            .map_err(|error| OpenBitFunError::parse(error.to_string()))?;
        Ok(event)
    }

    /// Callers must hold the Session mutation boundary. The evidence operation
    /// lock serializes this retention with evidence appends and restores.
    ///
    /// `prune_persisted_sidecar` must only be true for permanent history
    /// truncations. Staged undo/redo keeps the sidecar complete so a later
    /// redo can restore hidden evidence; committing the revert or performing a
    /// legacy rollback permanently discards the hidden suffix, so those paths
    /// prune the sidecar as well.
    async fn retain_evidence_events_locked(
        &self,
        session_storage_path: Option<&Path>,
        session_id: &str,
        surviving_turn_ids: &HashSet<String>,
        prune_persisted_sidecar: bool,
    ) -> OpenBitFunResult<()> {
        let _operation_guard = self.evidence_ledger_operation_locks.lock(session_id).await;
        let storage_path = session_storage_path.ok_or_else(|| {
            OpenBitFunError::session(format!(
                "Session storage path unavailable while retaining evidence: {}",
                session_id
            ))
        })?;
        if prune_persisted_sidecar {
            let mut retained = Vec::new();
            if let Some(events) = self
                .persistence_manager
                .retain_evidence_ledger_events(storage_path, session_id, surviving_turn_ids)
                .await?
            {
                retained = events;
            }
            self.evidence_ledger
                .replace_session(session_id, retained)
                .map_err(|error| OpenBitFunError::parse(error.to_string()))?;
            return Ok(());
        }
        // Staged undo/redo only changes what this runtime can see. Rebuild
        // memory from the untouched sidecar so redo can reveal hidden evidence
        // without losing it from disk.
        let sidecar_events = self
            .persistence_manager
            .load_evidence_ledger_events(storage_path, session_id)
            .await?;
        let retained = sidecar_events
            .into_iter()
            .filter(|event| surviving_turn_ids.contains(&event.turn_id))
            .collect::<Vec<_>>();
        self.evidence_ledger
            .replace_session(session_id, retained)
            .map_err(|error| OpenBitFunError::parse(error.to_string()))?;
        Ok(())
    }

    pub async fn record_checkpoint_created(
        &self,
        session_id: &str,
        turn_id: &str,
        tool_name: &str,
        target: &str,
        checkpoint: EvidenceLedgerCheckpoint,
    ) -> OpenBitFunResult<EvidenceLedgerEvent> {
        self.append_evidence_event(EvidenceLedgerEvent::checkpoint_created(
            session_id, turn_id, tool_name, target, checkpoint,
        ))
        .await
    }

    pub fn evidence_events_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Vec<EvidenceLedgerEvent> {
        self.evidence_ledger.events_for_turn(session_id, turn_id)
    }

    pub fn evidence_summary_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> EvidenceLedgerSummary {
        self.evidence_ledger.summary_for_session(session_id, limit)
    }

    pub fn compression_contract_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Option<CompressionContract> {
        let contract: CompressionContract =
            self.evidence_summary_for_session(session_id, limit).into();
        (!contract.is_empty()).then_some(contract)
    }

    pub async fn record_subagent_partial_timeout(
        &self,
        session_id: &str,
        turn_id: &str,
        subagent_type: &str,
        partial_output: &str,
        error_kind: Option<&str>,
    ) -> OpenBitFunResult<EvidenceLedgerEvent> {
        let summary = format!(
            "Subagent {} timed out after producing partial output.",
            subagent_type
        );
        let event = EvidenceLedgerEvent::new(
            session_id,
            turn_id,
            "AgentSpawn",
            EvidenceLedgerTargetKind::Subagent,
            subagent_type,
            EvidenceLedgerEventStatus::PartialTimeout,
            summary,
        )
        .with_error_kind(error_kind.unwrap_or("timeout"))
        .with_partial_output(partial_output);

        self.append_evidence_event(event).await
    }

    /// Decide whether the given session model id is still usable.
    ///
    /// `model_id` is treated as "usable" when:
    /// - it is a special selector (`primary` / `fast` /
    ///   empty) — these are evaluated again at request time against
    ///   `default_models`, so their long-term validity is governed elsewhere;
    /// - it resolves to a model that exists AND is enabled.
    fn is_session_model_id_usable(
        ai_config: &crate::service::config::types::AIConfig,
        model_id: &str,
    ) -> bool {
        let trimmed = model_id.trim();
        if trimmed.is_empty() || trimmed == "primary" || trimmed == "fast" {
            return true;
        }
        ai_config.is_model_reference_active(trimmed)
    }

    /// Reset every active mutable session whose bound model was invalidated to
    /// the configured primary selector. Persists the change and emits a model
    /// fallback event so surfaces can refresh their session-owned selection.
    async fn apply_fallback_to_invalidated_session_models(
        &self,
        invalidated_model_ids: &[String],
        reason: &'static str,
    ) {
        if invalidated_model_ids.is_empty() {
            return;
        }
        let invalid: HashSet<&str> = invalidated_model_ids.iter().map(String::as_str).collect();

        // Snapshot affected sessions first to avoid holding DashMap iterators
        // across async writes.
        let affected: Vec<(String, String)> = self
            .sessions
            .iter()
            .filter_map(|entry| {
                let session = entry.value();
                // Reconciliation observes provider configuration; it is not an
                // explicit manual selection that may retire a saved strategy.
                if session.config.model_routing.as_deref() == Some("smart") {
                    return None;
                }
                let current = session.config.model_id.as_deref()?.trim().to_string();
                // External generations pin the model that the user approved.
                // If that model disappears, execution must fail closed instead
                // of silently changing the approved behavior.
                if should_apply_session_model_fallback(
                    session.config.model_binding_policy,
                    current.as_str(),
                    &invalid,
                ) {
                    Some((session.session_id.clone(), current))
                } else {
                    None
                }
            })
            .collect();

        if affected.is_empty() {
            return;
        }

        for (session_id, previous_model_id) in affected {
            if let Err(e) = self.update_session_model_id(&session_id, "primary").await {
                warn!(
                    "Failed to apply session model fallback after reconcile: session_id={}, previous={}, error={}",
                    session_id, previous_model_id, e
                );
                continue;
            }
            info!(
                "Session model fell back to primary: session_id={}, previous_model_id={}, reason={}",
                session_id, previous_model_id, reason
            );

            if let Some(coordinator) = crate::agentic::coordination::get_global_coordinator() {
                coordinator
                    .emit_session_model_fallback_applied(
                        &session_id,
                        &previous_model_id,
                        "primary",
                        reason,
                    )
                    .await;
            }
        }
    }

    async fn reconcile_session_reasoning_preset_locked(
        &self,
        session_id: &str,
        ai_config: &crate::service::config::types::AIConfig,
        reason: &'static str,
    ) -> OpenBitFunResult<Option<String>> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let Some(original_session) = self.sessions.get(session_id).map(|value| value.clone())
        else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        };
        let Some(previous_preset_id) = original_session.config.reasoning_preset.clone() else {
            return Ok(None);
        };
        let normalized =
            Self::normalize_session_reasoning_preset(&original_session, ai_config).await;
        if normalized.is_some() {
            return Ok(normalized);
        }

        let mut updated_session = original_session.clone();
        updated_session.config.reasoning_preset = None;
        let now = SystemTime::now();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;
        if self.config.enable_persistence && self.should_persist_session(&original_session) {
            let workspace_path = self
                .effective_session_storage_path(session_id)
                .await
                .ok_or_else(|| {
                    OpenBitFunError::session(format!(
                        "Session storage path unavailable while clearing reasoning preset: {session_id}"
                    ))
                })?;
            if let Err(error) = self
                .persistence_manager
                .save_session(&workspace_path, &updated_session)
                .await
            {
                if let Err(rollback_error) = self
                    .persistence_manager
                    .save_session(&workspace_path, &original_session)
                    .await
                {
                    return Err(OpenBitFunError::session(format!(
                        "Reasoning preset persistence failed and rollback did not complete: session_id={session_id}, error={error}, rollback_error={rollback_error}"
                    )));
                }
                return Err(error);
            }
        }

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.config.reasoning_preset = None;
            session.updated_at = now;
            session.last_activity_at = now;
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        }
        drop(_mutation_guard);

        warn!(
            "Session reasoning preset became unavailable; normalized to Auto: session_id={}, preset_id={}, reason={}",
            session_id, previous_preset_id, reason
        );
        if let Some(coordinator) = crate::agentic::coordination::get_global_coordinator() {
            coordinator
                .emit_session_reasoning_preset_auto_cleared(session_id, &previous_preset_id, reason)
                .await;
        }
        Ok(None)
    }

    /// Last-resort turn-time canonicalization. Normal write/restore/config
    /// reconciliation paths should have already converged the Session.
    pub(crate) async fn reconcile_session_reasoning_preset_for_turn(
        &self,
        session_id: &str,
        reason: &'static str,
    ) -> OpenBitFunResult<Option<String>> {
        let config = self
            .get_session(session_id)
            .map(|session| session.config.clone());
        let Some(ai_config) = (match config {
            Some(config) => Self::load_ai_config_for_session(&config).await?,
            None => None,
        }) else {
            return Ok(self
                .sessions
                .get(session_id)
                .and_then(|session| session.config.reasoning_preset.clone()));
        };
        self.reconcile_session_reasoning_preset_locked(session_id, &ai_config, reason)
            .await
    }

    async fn reconcile_loaded_session_reasoning_presets(&self, reason: &'static str) {
        if Self::load_ai_config_for_model_resolution().await.is_none() {
            debug!(
                "Skipping session reasoning preset reconciliation because AI config is unavailable: reason={}",
                reason
            );
            return;
        }
        let candidates = self
            .sessions
            .iter()
            .filter(|entry| entry.config.reasoning_preset.is_some())
            .map(|entry| entry.session_id.clone())
            .collect::<Vec<_>>();

        for session_id in candidates {
            if let Err(error) = self
                .reconcile_session_reasoning_preset_for_turn(&session_id, reason)
                .await
            {
                warn!(
                    "Failed to reconcile session reasoning preset: session_id={}, reason={}, error={}",
                    session_id, reason, error
                );
            }
        }
    }

    /// Best-effort: drop cached AI clients for invalidated models so the next
    /// request rebuilds against the reconciled config.
    async fn invalidate_ai_clients_for_models(invalidated_model_ids: &[String]) {
        if invalidated_model_ids.is_empty() {
            return;
        }
        if let Ok(factory) = get_global_ai_client_factory().await {
            for model_id in invalidated_model_ids {
                factory.invalidate_model(model_id);
            }
        }
    }

    fn spawn_model_reconciliation_listener(&self) {
        let sessions = self.sessions.clone();
        let active_turn_permission_modes = self.active_turn_permission_modes.clone();
        let transient_session_ids = self.transient_session_ids.clone();
        let transient_turns = self.transient_turns.clone();
        let turn_settlement_results = self.turn_settlement_results.clone();
        let turn_settlement_result_order = self.turn_settlement_result_order.clone();
        let active_session_capacity = self.active_session_capacity.clone();
        let active_session_permits = self.active_session_permits.clone();
        let session_storage_path_index = self.session_storage_path_index.clone();
        let session_mutation_locks = self.session_mutation_locks.clone();
        let session_write_locks = self.session_write_locks.clone();
        let context_store = self.context_store.clone();
        let prompt_cache_store = self.prompt_cache_store.clone();
        let prompt_cache_operation_locks = self.prompt_cache_operation_locks.clone();
        let token_anchor_store = self.token_anchor_store.clone();
        let turn_skill_agent_snapshot_store = self.turn_skill_agent_snapshot_store.clone();
        let skill_agent_baseline_override_snapshot_store =
            self.skill_agent_baseline_override_snapshot_store.clone();
        let edit_constraints_store = self.edit_constraints_store.clone();
        let review_read_receipt_store = self.review_read_receipt_store.clone();
        let evidence_ledger = self.evidence_ledger.clone();
        let evidence_ledger_operation_locks = self.evidence_ledger_operation_locks.clone();
        let persistence_manager = self.persistence_manager.clone();
        let memory_database = self.memory_database.clone();
        let manager_config = self.config.clone();

        tokio::spawn(async move {
            let Some(mut receiver) = subscribe_config_updates() else {
                debug!(
                    "SessionManager: config update subscription unavailable; skipping model reconciliation listener"
                );
                return;
            };

            // Re-build a thin handle that mirrors `self` for the listener loop.
            // We can't move `self` into a 'static task, so we recreate the
            // surface area we need from the cloned shared fields above.
            let manager = Self {
                sessions,
                active_turn_permission_modes,
                transient_session_ids,
                transient_turns,
                turn_settlement_results,
                turn_settlement_result_order,
                active_session_capacity,
                active_session_permits,
                session_storage_path_index,
                session_mutation_locks,
                session_write_locks,
                context_store,
                prompt_cache_store,
                prompt_cache_operation_locks,
                token_anchor_store,
                turn_skill_agent_snapshot_store,
                skill_agent_baseline_override_snapshot_store,
                edit_constraints_store,
                review_read_receipt_store,
                evidence_ledger,
                evidence_ledger_operation_locks,
                persistence_manager,
                memory_database,
                config: manager_config,
            };

            let mut model_inputs = Self::load_ai_config_for_model_resolution()
                .await
                .and_then(|ai| serde_json::to_value((&ai.models, &ai.default_models)).ok());
            loop {
                match receiver.recv().await {
                    Ok(ConfigUpdateEvent::ModelsReconciled {
                        invalidated_model_ids,
                        ..
                    }) => {
                        Self::invalidate_ai_clients_for_models(&invalidated_model_ids).await;
                        manager
                            .apply_fallback_to_invalidated_session_models(
                                &invalidated_model_ids,
                                "model_reconciled",
                            )
                            .await;
                    }
                    Ok(
                        ConfigUpdateEvent::ModelConfigurationUpdated
                        | ConfigUpdateEvent::AIModelUpdated { .. }
                        | ConfigUpdateEvent::DefaultAIModelUpdated { .. }
                        | ConfigUpdateEvent::ConfigReloaded,
                    ) => {
                        let next_inputs = Self::load_ai_config_for_model_resolution()
                            .await
                            .and_then(|ai| {
                                serde_json::to_value((&ai.models, &ai.default_models)).ok()
                            });
                        if next_inputs.is_some() && next_inputs == model_inputs {
                            continue;
                        }
                        model_inputs = next_inputs;
                        manager
                            .reconcile_loaded_session_reasoning_presets(
                                "model_configuration_updated",
                            )
                            .await;
                    }
                    Ok(ConfigUpdateEvent::ReasoningCatalogUpdated) => {
                        manager
                            .reconcile_loaded_session_reasoning_presets("reasoning_catalog_updated")
                            .await;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        debug!("SessionManager model reconciliation listener: channel closed");
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            "SessionManager model reconciliation listener lagged by {} events; continuing",
                            n
                        );
                    }
                }
            }
        });
    }

    // ============ Session CRUD ============

    /// Create a new session
    pub async fn create_session(
        &self,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
    ) -> OpenBitFunResult<Session> {
        self.create_session_with_id_and_details(
            None,
            session_name,
            agent_type,
            config,
            None,
            SessionKind::Standard,
        )
        .await
    }

    /// Create a new session (supports specifying session ID)
    pub async fn create_session_with_id(
        &self,
        session_id: Option<String>,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
    ) -> OpenBitFunResult<Session> {
        self.create_session_with_id_and_details(
            session_id,
            session_name,
            agent_type,
            config,
            None,
            SessionKind::Standard,
        )
        .await
    }

    /// Create a new session (supports specifying session ID and creator identity)
    pub async fn create_session_with_id_and_creator(
        &self,
        session_id: Option<String>,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
        created_by: Option<String>,
    ) -> OpenBitFunResult<Session> {
        self.create_session_with_id_and_details(
            session_id,
            session_name,
            agent_type,
            config,
            created_by,
            SessionKind::Standard,
        )
        .await
    }

    /// Create a new session with explicit kind.
    pub async fn create_session_with_id_and_details(
        &self,
        session_id: Option<String>,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
        created_by: Option<String>,
        kind: SessionKind,
    ) -> OpenBitFunResult<Session> {
        self.create_session_with_id_and_details_internal(
            session_id,
            session_name,
            agent_type,
            config,
            created_by,
            kind,
            false,
        )
        .await
    }

    pub(crate) async fn create_transient_session_with_id_and_details(
        &self,
        session_id: Option<String>,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
        created_by: Option<String>,
        kind: SessionKind,
    ) -> OpenBitFunResult<Session> {
        self.create_session_with_id_and_details_internal(
            session_id,
            session_name,
            agent_type,
            config,
            created_by,
            kind,
            true,
        )
        .await
    }

    async fn create_session_with_id_and_details_internal(
        &self,
        session_id: Option<String>,
        session_name: String,
        agent_type: String,
        config: SessionConfig,
        created_by: Option<String>,
        kind: SessionKind,
        transient: bool,
    ) -> OpenBitFunResult<Session> {
        let mut config = config;
        // The global setting is sampled exactly once while creating a session.
        // A caller may provide an explicit PTC value (used by delegated/runtime
        // paths); legacy/default native values follow the current global
        // default. Persisting the resolved value makes existing sessions
        // independent from later global changes.
        if config.tool_presentation_mode.is_native() {
            if let Ok(config_service) = get_global_config_service().await {
                if let Ok(mode) = config_service
                    .get_config::<ToolPresentationMode>(Some(TOOL_PRESENTATION_MODE_CONFIG_PATH))
                    .await
                {
                    config.tool_presentation_mode = mode;
                }
            }
        }
        crate::agentic::workspace::normalize_session_workspace(&mut config).await?;
        let _workspace_path = Self::session_workspace_from_config(&config).ok_or_else(|| {
            OpenBitFunError::Validation("Session workspace_path is required".to_string())
        })?;

        let session_storage_path = self
            .effective_storage_path_for_config(&config)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation("Session workspace_path is required".to_string())
            })?;

        let mut session = if let Some(id) = session_id {
            Session::new_with_id(id, session_name, agent_type.clone(), config)
        } else {
            Session::new(session_name, agent_type.clone(), config)
        };
        session.created_by = created_by;
        session.kind = kind;
        let ai_config = Self::load_ai_config_for_session(&session.config).await?;
        if session.config.model_routing.is_none()
            && kind != SessionKind::Subagent
            && session.config.model_binding_policy == SessionModelBindingPolicy::Mutable
            && matches!(
                session.config.agent_route_owner,
                crate::agentic::core::SessionAgentRouteOwner::Local
            )
        {
            if !crate::agentic::execution::round_model_routing::configuration::available() {
                if ai_config.as_ref().is_some_and(|config| {
                    config.model_routing.default_mode.as_deref() == Some("smart")
                }) {
                    info!("Smart mode is unavailable in this release; creating a manual session: session_id={}", session.session_id);
                }
                session.config.model_routing = Some("manual".into());
            } else if let Some(mode) = ai_config
                .as_ref()
                .and_then(|config| config.model_routing.default_mode.as_ref())
            {
                session.config.model_routing = Some(mode.clone());
            } else if !crate::agentic::execution::round_model_routing::configuration::legacy_enabled(
            ) {
                session.config.model_routing = Some("manual".into());
            }
        }
        if let Some(ai_config) = ai_config.as_ref() {
            let previous_reasoning_preset = session.config.reasoning_preset.clone();
            session.config.reasoning_preset =
                Self::normalize_session_reasoning_preset(&session, ai_config).await;
            if previous_reasoning_preset.is_some() && session.config.reasoning_preset.is_none() {
                warn!(
                    "Session creation received an unavailable reasoning preset; normalizing to Auto: session_id={}, preset_id={}",
                    session.session_id,
                    previous_reasoning_preset.as_deref().unwrap_or_default()
                );
            }
            let previous_context_window = session.config.max_context_tokens;
            if let Some(resolved_context_window) =
                Self::sync_session_context_window_from_ai_config(&mut session, ai_config)
            {
                if resolved_context_window != previous_context_window {
                    debug!(
                        "Resolved session context window before creation: session_id={}, previous={}, resolved={}",
                        session.session_id, previous_context_window, resolved_context_window
                    );
                }
            }
        } else if let Some(preset_id) = session.config.reasoning_preset.as_deref() {
            session.config.reasoning_preset =
                Some(preset_id.trim().to_string()).filter(|preset_id| {
                    !preset_id.is_empty() && !preset_id.eq_ignore_ascii_case("auto")
                });
        }
        let persist = self.config.enable_persistence
            && !transient
            && Self::should_persist_session_kind(session.kind);
        let session_id = session.session_id.clone();
        let _mutation_guard = self.lock_session_mutation(&session_id).await;
        if self.sessions.contains_key(&session_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Session ID already exists: {session_id}"
            )));
        }
        let session_write_lock = if persist {
            Some(self.try_acquire_session_write_lock(&session_storage_path, &session_id)?)
        } else {
            None
        };

        // Claim both the runtime session ID and its workspace storage identity before
        // exposing the session. Persistent sessions must never reuse an on-disk ID:
        // overwriting the header would retain old turns and silently mix histories.
        if self.sessions.contains_key(&session_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Session ID already exists: {session_id}"
            )));
        }
        if self.config.enable_persistence
            && self
                .persistence_manager
                .session_storage_exists(&session_storage_path, &session_id)?
        {
            return Err(OpenBitFunError::Validation(format!(
                "Persisted session ID already exists: {session_id}"
            )));
        }
        let active_session_permit = self.reserve_active_session()?;
        let storage_claim =
            self.claim_session_storage_path(&session_id, &session_storage_path, true)?;

        // Persist before publishing runtime state. Cancellation or timeout while
        // this await is in progress cannot leave a writable in-memory Session.
        if persist {
            if let Err(error) = self
                .persistence_manager
                .create_session_if_absent(&session_storage_path, &session)
                .await
            {
                self.release_failed_session_storage_path_claim(
                    &session_id,
                    &session_storage_path,
                    storage_claim,
                );
                return Err(error);
            }
        }

        // Publication is synchronous after all fallible persistence work.
        match self.sessions.entry(session_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(session.clone());
            }
            Entry::Occupied(_) => {
                self.release_failed_session_storage_path_claim(
                    &session_id,
                    &session_storage_path,
                    storage_claim,
                );
                return Err(OpenBitFunError::Validation(format!(
                    "Session ID already exists: {session_id}"
                )));
            }
        }
        if transient {
            self.transient_session_ids.insert(session_id.clone(), ());
        }
        self.context_store.create_session(&session_id);
        self.token_anchor_store.create_session(&session_id);
        self.turn_skill_agent_snapshot_store
            .create_session(&session_id);
        self.review_read_receipt_store.create_session(&session_id);
        self.commit_session_storage_path_claim(&session_id, &session_storage_path, storage_claim);
        self.commit_active_session_reservation(&session_id, active_session_permit);
        if let Some(write_lock) = session_write_lock {
            self.commit_session_write_lock(&session_id, write_lock);
        }

        info!("Session created: session_name={}", session.session_name);

        Ok(session)
    }

    /// Get session
    pub fn get_session(&self, session_id: &str) -> Option<Session> {
        self.sessions.get(session_id).map(|s| s.clone())
    }

    /// Owning workspace facts for Turn lifecycle events, read without cloning
    /// the session. `None` when this host has not loaded the session.
    pub fn session_workspace_binding(
        &self,
        session_id: &str,
    ) -> Option<openbitfun_events::SessionWorkspaceBinding> {
        self.sessions.get(session_id).map(|session| {
            let config = &session.config;
            openbitfun_events::SessionWorkspaceBinding {
                workspace_path: config.workspace_path.clone(),
                project_workspace_path: config.project_workspace_path.clone(),
                execution_target: config.execution_target.clone(),
                workspace_id: config.workspace_id.clone(),
                remote_connection_id: config.remote_connection_id.clone(),
                remote_ssh_host: config.remote_ssh_host.clone(),
            }
        })
    }

    /// Read selector facts without restoring history, reserving runtime capacity,
    /// or waiting for an executing Turn's mutation permit. An omitted locator
    /// supports legacy callers only when this host already owns the session.
    pub(crate) async fn read_session_header(
        &self,
        storage_path: Option<&Path>,
        session_id: &str,
        include_internal: bool,
    ) -> OpenBitFunResult<Session> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        if let Some(path) = storage_path {
            self.validate_session_storage_path_binding(session_id, path)?;
        }
        let session = match self.get_session(session_id) {
            Some(session) => session,
            None => {
                let path = storage_path.ok_or_else(|| {
                    OpenBitFunError::NotFound(format!("Session not found: {session_id}"))
                })?;
                let persisted = self
                    .persistence_manager
                    .load_session_header(path, session_id)
                    .await;
                // A real restore or selector update may have completed during
                // disk IO. Its live selection takes precedence over disk.
                self.validate_session_storage_path_binding(session_id, path)?;
                self.get_session(session_id).map(Ok).unwrap_or(persisted)?
            }
        };
        if let Some(path) = storage_path {
            self.validate_session_storage_path_binding(session_id, path)?;
        }
        if !include_internal
            && (session.kind != SessionKind::Standard
                || SessionMetadata::is_legacy_leaked_subagent_identity(
                    session.created_by.as_deref(),
                    &session.session_name,
                ))
        {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        }
        Ok(session)
    }

    /// Read only the execution fact; navigation must not clone Session content.
    pub fn get_session_state(&self, session_id: &str) -> Option<SessionState> {
        self.sessions
            .get(session_id)
            .map(|session| session.state.clone())
    }

    pub async fn cached_system_prompt(
        &self,
        session_id: &str,
        identity: &SystemPromptCacheIdentity,
    ) -> Option<String> {
        self.ensure_prompt_cache_loaded(session_id).await;
        match self.prompt_cache_store.lookup_system_prompt(
            session_id,
            identity,
            self.config.prompt_cache_policy.cache_ttl,
        ) {
            PromptCacheLookup::Hit(prompt) => Some(prompt),
            PromptCacheLookup::Miss => None,
            PromptCacheLookup::Expired => {
                self.persist_prompt_cache_best_effort(
                    session_id,
                    "system_prompt_cache_expired_cleanup",
                )
                .await;
                None
            }
        }
    }

    pub async fn remember_system_prompt(
        &self,
        session_id: &str,
        identity: SystemPromptCacheIdentity,
        prompt: String,
    ) {
        self.ensure_prompt_cache_loaded(session_id).await;
        self.prompt_cache_store
            .set_system_prompt(session_id, CachedSystemPrompt::new(identity, prompt));
        self.persist_prompt_cache_best_effort(session_id, "system_prompt_cached")
            .await;
    }

    pub async fn cached_user_context(
        &self,
        session_id: &str,
        identity: &UserContextCacheIdentity,
    ) -> Option<String> {
        self.ensure_prompt_cache_loaded(session_id).await;
        match self.prompt_cache_store.lookup_user_context(
            session_id,
            identity,
            self.config.prompt_cache_policy.cache_ttl,
        ) {
            PromptCacheLookup::Hit(user_context) => Some(user_context),
            PromptCacheLookup::Miss => None,
            PromptCacheLookup::Expired => {
                self.persist_prompt_cache_best_effort(
                    session_id,
                    "user_context_cache_expired_cleanup",
                )
                .await;
                None
            }
        }
    }

    pub async fn remember_user_context(
        &self,
        session_id: &str,
        identity: UserContextCacheIdentity,
        user_context: String,
    ) {
        self.ensure_prompt_cache_loaded(session_id).await;
        self.prompt_cache_store
            .set_user_context(session_id, CachedUserContext::new(identity, user_context));
        self.persist_prompt_cache_best_effort(session_id, "user_context_cached")
            .await;
    }

    pub async fn user_context_cache_generation(&self, session_id: &str) -> u64 {
        self.ensure_prompt_cache_loaded(session_id).await;
        self.prompt_cache_store.user_context_generation(session_id)
    }

    pub async fn remember_user_context_if_generation(
        &self,
        session_id: &str,
        generation: u64,
        identity: UserContextCacheIdentity,
        user_context: String,
    ) -> bool {
        self.ensure_prompt_cache_loaded(session_id).await;
        let stored = self.prompt_cache_store.set_user_context_if_generation(
            session_id,
            generation,
            CachedUserContext::new(identity, user_context),
        );
        if stored {
            self.persist_prompt_cache_best_effort(session_id, "user_context_cached")
                .await;
        }
        stored
    }

    pub async fn clone_prompt_cache(
        &self,
        source_session_id: &str,
        target_session_id: &str,
    ) -> bool {
        self.ensure_prompt_cache_loaded(source_session_id).await;
        let Some(cache) = self.prompt_cache_store.get_cache(source_session_id) else {
            return false;
        };
        if cache.is_empty() {
            return false;
        }

        self.prompt_cache_store
            .replace_cache(target_session_id, cache);
        self.persist_prompt_cache_best_effort(target_session_id, "prompt_cache_cloned")
            .await;
        true
    }

    pub async fn turn_skill_agent_snapshot(
        &self,
        session_id: &str,
        turn_index: usize,
    ) -> Option<TurnSkillAgentSnapshot> {
        if let Some(snapshot) = self
            .turn_skill_agent_snapshot_store
            .get_snapshot(session_id, turn_index)
        {
            return Some(snapshot);
        }

        if !self.should_persist_session_id(session_id) {
            return None;
        }

        let workspace_path = self.effective_session_storage_path(session_id).await?;
        match self
            .load_turn_skill_agent_snapshot_from_persistence(
                &workspace_path,
                session_id,
                turn_index,
            )
            .await
        {
            Ok(Some(snapshot)) => {
                self.turn_skill_agent_snapshot_store.set_snapshot(
                    session_id,
                    turn_index,
                    snapshot.clone(),
                );
                Some(snapshot)
            }
            Ok(None) => None,
            Err(error) => {
                warn!(
                    "Failed to load turn skill-agent snapshot: session_id={}, turn_index={}, workspace_path={}, error={}",
                    session_id,
                    turn_index,
                    workspace_path.display(),
                    error
                );
                None
            }
        }
    }

    pub async fn latest_turn_skill_agent_snapshot_at_or_before(
        &self,
        session_id: &str,
        turn_index: usize,
    ) -> Option<(usize, TurnSkillAgentSnapshot)> {
        let cached_snapshot = self
            .turn_skill_agent_snapshot_store
            .latest_snapshot_at_or_before(session_id, turn_index);
        if let Some(snapshot) = cached_snapshot.as_ref() {
            if snapshot.0 == turn_index || !self.should_persist_session_id(session_id) {
                return cached_snapshot;
            }
        }

        if !self.should_persist_session_id(session_id) {
            return cached_snapshot;
        }

        let workspace_path = self.effective_session_storage_path(session_id).await?;
        let scan_floor_exclusive = cached_snapshot.as_ref().map(|snapshot| snapshot.0);
        for index in (0..=turn_index).rev() {
            if scan_floor_exclusive.is_some_and(|floor| index <= floor) {
                break;
            }
            match self
                .load_turn_skill_agent_snapshot_from_persistence(&workspace_path, session_id, index)
                .await
            {
                Ok(Some(snapshot)) => {
                    self.turn_skill_agent_snapshot_store.set_snapshot(
                        session_id,
                        index,
                        snapshot.clone(),
                    );
                    return Some((index, snapshot));
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        "Failed to load turn skill-agent snapshot while scanning backwards: session_id={}, turn_index={}, workspace_path={}, error={}",
                        session_id,
                        index,
                        workspace_path.display(),
                        error
                    );
                }
            }
        }

        cached_snapshot
    }

    pub async fn remember_turn_skill_agent_snapshot(
        &self,
        session_id: &str,
        turn_index: usize,
        snapshot: TurnSkillAgentSnapshot,
    ) {
        self.turn_skill_agent_snapshot_store
            .set_snapshot(session_id, turn_index, snapshot.clone());

        if !self.should_persist_session_id(session_id) {
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping turn skill-agent snapshot persistence because workspace path is unavailable: session_id={}, turn_index={}",
                session_id, turn_index
            );
            return;
        };

        if let Err(error) = self
            .persistence_manager
            .save_turn_skill_agent_snapshot(&workspace_path, session_id, turn_index, &snapshot)
            .await
        {
            warn!(
                "Failed to persist turn skill-agent snapshot: session_id={}, turn_index={}, workspace_path={}, error={}",
                session_id,
                turn_index,
                workspace_path.display(),
                error
            );
        }
    }

    pub async fn recover_first_turn_skill_agent_snapshot(
        &self,
        session_id: &str,
        snapshot: TurnSkillAgentSnapshot,
    ) {
        self.turn_skill_agent_snapshot_store
            .remove_from(session_id, 1);
        self.turn_skill_agent_snapshot_store
            .set_snapshot(session_id, 0, snapshot.clone());

        if !self.should_persist_session_id(session_id) {
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping first-turn skill-agent baseline recovery persistence because workspace path is unavailable: session_id={}",
                session_id
            );
            return;
        };

        if let Err(error) = self
            .persistence_manager
            .delete_turn_skill_agent_snapshots_from(&workspace_path, session_id, 1)
            .await
        {
            warn!(
                "Failed to prune turn skill-agent snapshots during baseline recovery: session_id={}, workspace_path={}, error={}",
                session_id,
                workspace_path.display(),
                error
            );
        }

        if let Err(error) = self
            .persistence_manager
            .save_turn_skill_agent_snapshot(&workspace_path, session_id, 0, &snapshot)
            .await
        {
            warn!(
                "Failed to persist recovered first-turn skill-agent snapshot: session_id={}, workspace_path={}, error={}",
                session_id,
                workspace_path.display(),
                error
            );
        }
    }

    pub async fn remember_skill_agent_baseline_override_snapshot(
        &self,
        session_id: &str,
        snapshot: TurnSkillAgentSnapshot,
    ) {
        self.skill_agent_baseline_override_snapshot_store
            .insert(session_id.to_string(), snapshot.clone());

        if !self.should_persist_session_id(session_id) {
            return;
        }

        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            debug!(
                "Skipping listing reminder baseline override persistence because workspace path is unavailable: session_id={}",
                session_id
            );
            return;
        };

        if let Err(error) = self
            .persistence_manager
            .save_skill_agent_baseline_override_snapshot(&workspace_path, session_id, &snapshot)
            .await
        {
            warn!(
                "Failed to persist listing reminder baseline override snapshot: session_id={}, workspace_path={}, error={}",
                session_id,
                workspace_path.display(),
                error
            );
        }
    }

    pub async fn skill_agent_baseline_override_snapshot(
        &self,
        session_id: &str,
    ) -> Option<TurnSkillAgentSnapshot> {
        if let Some(snapshot) = self
            .skill_agent_baseline_override_snapshot_store
            .get(session_id)
            .map(|value| value.clone())
        {
            return Some(snapshot);
        }

        if !self.should_persist_session_id(session_id) {
            return None;
        }

        let workspace_path = self.effective_session_storage_path(session_id).await?;
        let snapshot = match self
            .persistence_manager
            .load_skill_agent_baseline_override_snapshot(&workspace_path, session_id)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(
                    "Failed to load listing reminder baseline override snapshot: session_id={}, workspace_path={}, error={}",
                    session_id,
                    workspace_path.display(),
                    error
                );
                return None;
            }
        };
        let snapshot = snapshot?;
        self.skill_agent_baseline_override_snapshot_store
            .insert(session_id.to_string(), snapshot.clone());
        Some(snapshot)
    }

    /// The snapshot that the request-head listing reminders are rendered from:
    /// a fork's baseline override when present, otherwise the turn-0 snapshot.
    pub async fn skill_agent_listing_baseline_snapshot(
        &self,
        session_id: &str,
    ) -> Option<TurnSkillAgentSnapshot> {
        match self
            .skill_agent_baseline_override_snapshot(session_id)
            .await
        {
            Some(snapshot) => Some(snapshot),
            None => self.turn_skill_agent_snapshot(session_id, 0).await,
        }
    }

    pub async fn seed_forked_skill_agent_listing_baselines(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) {
        // Forked children need two different baselines at the same time:
        // - the parent's turn-0 snapshot stays as the prompt/listing baseline so the child's
        //   first request can reuse the same full skill/agent listing prefix
        // - the parent's latest snapshot becomes the child's own turn-0 snapshot so later child
        //   turns diff against the fork-time surface instead of diffing forever against the
        //   parent's original turn-0 baseline
        let prompt_listing_baseline = self.turn_skill_agent_snapshot(parent_session_id, 0).await;
        if let Some(snapshot) = prompt_listing_baseline.clone() {
            self.remember_skill_agent_baseline_override_snapshot(child_session_id, snapshot)
                .await;
        }

        let latest_parent_snapshot = match self.get_turn_count(parent_session_id).checked_sub(1) {
            Some(turn_index) => self
                .latest_turn_skill_agent_snapshot_at_or_before(parent_session_id, turn_index)
                .await
                .map(|(_, snapshot)| snapshot),
            None => None,
        };

        if let Some(snapshot) = latest_parent_snapshot.or(prompt_listing_baseline) {
            self.remember_turn_skill_agent_snapshot(child_session_id, 0, snapshot)
                .await;
        }
    }

    /// Merges one extraction record into the active session state and persists
    /// the resulting constraints plus extraction evidence.
    pub async fn remember_edit_constraint_extraction(
        &self,
        session_id: &str,
        extraction: crate::agentic::execution::edit_constraint_guard::ConstraintExtractionRecord,
    ) {
        let mut state = self.edit_constraint_state(session_id).unwrap_or_default();
        state.merge_extraction(extraction);
        self.edit_constraints_store
            .insert(session_id.to_string(), state.clone());

        if self.should_persist_session_id(session_id) {
            if let Err(error) = self
                .merge_session_custom_metadata(
                    session_id,
                    json!({
                        crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY: state,
                    }),
                )
                .await
            {
                warn!(
                    "Failed to persist edit constraint state: session_id={}, error={}",
                    session_id, error
                );
            }
        }
    }

    /// Records paths first created through direct agent file tools. This is
    /// session-persistent provenance used to distinguish temporary agent
    /// helpers from repository files protected by edit constraints.
    pub async fn remember_edit_constraint_agent_created_paths(
        &self,
        session_id: &str,
        paths: Vec<String>,
        dialog_turn_id: &str,
    ) {
        let mut state = self.edit_constraint_state(session_id).unwrap_or_default();
        state.remember_agent_created_paths(paths, dialog_turn_id);
        self.edit_constraints_store
            .insert(session_id.to_string(), state.clone());

        if self.should_persist_session_id(session_id) {
            if let Err(error) = self
                .merge_session_custom_metadata(
                    session_id,
                    json!({
                        crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY: state,
                    }),
                )
                .await
            {
                warn!(
                    "Failed to persist agent-created file provenance: session_id={}, error={}",
                    session_id, error
                );
            }
        }
    }

    /// Removes direct-agent provenance after a successful delete. Descendants
    /// are removed as well so recursive cleanup cannot leave stale records.
    pub async fn forget_edit_constraint_agent_created_paths_under(
        &self,
        session_id: &str,
        paths: Vec<String>,
    ) {
        let Some(mut state) = self.edit_constraint_state(session_id) else {
            return;
        };
        state.forget_agent_created_paths_under(&paths);
        self.edit_constraints_store
            .insert(session_id.to_string(), state.clone());

        if self.should_persist_session_id(session_id) {
            if let Err(error) = self
                .merge_session_custom_metadata(
                    session_id,
                    json!({
                        crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY: state,
                    }),
                )
                .await
            {
                warn!(
                    "Failed to persist removed agent-created file provenance: session_id={}, error={}",
                    session_id, error
                );
            }
        }
    }

    /// Rewinds edit constraints and direct-file provenance to the turns that
    /// remain after a session rollback. This prevents a restriction, explicit
    /// relaxation, or temporary helper created in discarded future context
    /// from leaking into the resumed branch.
    pub async fn rollback_edit_constraint_state_to_turns(
        &self,
        session_id: &str,
        surviving_turn_ids: &std::collections::HashSet<String>,
    ) {
        let Some(mut state) = self.edit_constraint_state(session_id) else {
            return;
        };
        state.rollback_to_surviving_turns(surviving_turn_ids);
        self.edit_constraints_store
            .insert(session_id.to_string(), state.clone());

        if self.should_persist_session_id(session_id) {
            if let Err(error) = self
                .merge_session_custom_metadata(
                    session_id,
                    json!({
                        crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY: state,
                    }),
                )
                .await
            {
                warn!(
                    "Failed to persist rolled-back edit constraint state: session_id={}, error={}",
                    session_id, error
                );
            }
        }
    }

    pub fn edit_constraint_state(
        &self,
        session_id: &str,
    ) -> Option<crate::agentic::execution::edit_constraint_guard::EditConstraintState> {
        self.edit_constraints_store
            .get(session_id)
            .map(|value| value.clone())
    }

    fn edit_constraint_state_from_metadata(
        metadata: Option<&SessionMetadata>,
    ) -> Option<crate::agentic::execution::edit_constraint_guard::EditConstraintState> {
        let value = metadata?
            .custom_metadata
            .as_ref()?
            .get(crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY)?;
        match serde_json::from_value(value.clone()) {
            Ok(state) => Some(state),
            Err(error) => {
                warn!("Failed to restore edit constraint state from session metadata: {error}");
                None
            }
        }
    }

    pub fn edit_constraints(
        &self,
        session_id: &str,
    ) -> Option<Vec<crate::agentic::execution::edit_constraint_guard::ExtractedConstraint>> {
        self.edit_constraint_state(session_id)
            .map(|state| state.constraints)
    }

    /// Subagents inherit both active constraints and extraction evidence.
    pub async fn seed_forked_edit_constraints(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) {
        if let Some(mut state) = self.edit_constraint_state(parent_session_id) {
            state.mark_current_state_as_fork_baseline();
            self.edit_constraints_store
                .insert(child_session_id.to_string(), state.clone());
            if self.should_persist_session_id(child_session_id) {
                if let Err(error) = self
                    .merge_session_custom_metadata(
                        child_session_id,
                        json!({
                            crate::agentic::execution::edit_constraint_guard::EDIT_CONSTRAINT_METADATA_KEY: state,
                        }),
                    )
                    .await
                {
                    warn!(
                        "Failed to persist inherited edit constraint state: session_id={}, error={}",
                        child_session_id, error
                    );
                }
            }
        }
    }

    pub async fn rebuild_skill_agent_listing_baseline_to_latest(&self, session_id: &str) -> bool {
        let Some(turn_index) = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.len().checked_sub(1))
        else {
            return false;
        };

        let Some((_, latest_snapshot)) = self
            .latest_turn_skill_agent_snapshot_at_or_before(session_id, turn_index)
            .await
        else {
            return false;
        };

        if self
            .skill_agent_baseline_override_snapshot(session_id)
            .await
            .is_some()
        {
            self.remember_skill_agent_baseline_override_snapshot(
                session_id,
                latest_snapshot.clone(),
            )
            .await;
        }

        self.recover_first_turn_skill_agent_snapshot(session_id, latest_snapshot)
            .await;
        self.persist_listing_baseline_rebuild_turn_index_best_effort(session_id, turn_index)
            .await;

        let _ = self
            .remove_listing_diff_internal_reminders(session_id)
            .await;
        true
    }

    pub async fn remove_listing_diff_internal_reminders(&self, session_id: &str) -> bool {
        let changed = self
            .context_store
            .try_transform_context(session_id, |messages| {
                let (filtered, changed) =
                    Self::strip_listing_diff_internal_reminders(messages.to_vec());
                Ok((changed.then_some(filtered), changed))
            })
            .unwrap_or(false);
        if !changed {
            return false;
        }
        self.persist_current_turn_context_snapshot_best_effort(
            session_id,
            "listing_diff_internal_reminders_removed",
        )
        .await;
        true
    }

    fn strip_listing_diff_internal_reminders(messages: Vec<Message>) -> (Vec<Message>, bool) {
        let original_len = messages.len();
        let filtered_messages = messages
            .into_iter()
            .filter(|message| {
                !message
                    .internal_reminder_kind()
                    .is_some_and(InternalReminderKind::is_listing_diff)
            })
            .collect::<Vec<_>>();

        let changed = filtered_messages.len() != original_len;
        (filtered_messages, changed)
    }

    fn listing_baseline_rebuild_turn_index_from_custom_metadata(
        custom_metadata: Option<&serde_json::Value>,
    ) -> Option<usize> {
        custom_metadata?
            .get(LISTING_BASELINE_REBUILD_TURN_INDEX_METADATA_KEY)?
            .as_u64()?
            .try_into()
            .ok()
    }

    fn listing_baseline_rebuild_turn_index_from_metadata(
        metadata: Option<&SessionMetadata>,
    ) -> Option<usize> {
        Self::listing_baseline_rebuild_turn_index_from_custom_metadata(
            metadata.and_then(|metadata| metadata.custom_metadata.as_ref()),
        )
    }

    async fn persist_context_snapshot_messages_best_effort(
        &self,
        workspace_path: &Path,
        session_id: &str,
        turn_index: usize,
        messages: &[Message],
        reason: &str,
    ) {
        if !self.should_persist_session_id(session_id) {
            return;
        }

        if let Err(err) = self
            .persistence_manager
            .save_turn_context_snapshot(workspace_path, session_id, turn_index, messages)
            .await
        {
            warn!(
                "failed to persist explicit context snapshot: session_id={}, turn_index={}, reason={}, err={}",
                session_id, turn_index, reason, err
            );
        }
    }

    async fn sanitize_listing_diff_context_snapshot_if_needed(
        &self,
        workspace_path: &Path,
        session_id: &str,
        turn_index: usize,
        messages: Vec<Message>,
        cutoff_turn_index: Option<usize>,
        reason: &str,
    ) -> Vec<Message> {
        let Some(cutoff_turn_index) = cutoff_turn_index else {
            return messages;
        };
        // The rebuild performed at turn R already persisted snapshots on and after R against
        // the new baseline. Only snapshots strictly before that rebuilt turn need diff-reminder
        // cleanup, so the predicate is `< cutoff`, not `<= cutoff`.
        if turn_index >= cutoff_turn_index {
            return messages;
        }

        let (sanitized_messages, changed) = Self::strip_listing_diff_internal_reminders(messages);
        if !changed {
            return sanitized_messages;
        }

        debug!(
            "Sanitized listing diff reminders from pre-rebuild context snapshot: session_id={}, turn_index={}, cutoff_turn_index={}, reason={}",
            session_id, turn_index, cutoff_turn_index, reason
        );
        self.persist_context_snapshot_messages_best_effort(
            workspace_path,
            session_id,
            turn_index,
            &sanitized_messages,
            reason,
        )
        .await;
        sanitized_messages
    }

    async fn persist_listing_baseline_rebuild_turn_index_best_effort(
        &self,
        session_id: &str,
        turn_index: usize,
    ) {
        if let Err(err) = self
            .merge_session_custom_metadata(
                session_id,
                json!({
                    LISTING_BASELINE_REBUILD_TURN_INDEX_METADATA_KEY: turn_index,
                }),
            )
            .await
        {
            warn!(
                "failed to persist listing baseline rebuild turn index: session_id={}, turn_index={}, err={}",
                session_id, turn_index, err
            );
        }
    }

    async fn truncate_listing_baseline_rebuild_turn_index_after_rollback(
        &self,
        workspace_path: &Path,
        session_id: &str,
        target_turn: usize,
    ) -> OpenBitFunResult<()> {
        let metadata = self
            .persistence_manager
            .load_session_metadata(workspace_path, session_id)
            .await?;
        let Some(existing_cutoff) =
            Self::listing_baseline_rebuild_turn_index_from_metadata(metadata.as_ref())
        else {
            return Ok(());
        };

        if existing_cutoff <= target_turn {
            return Ok(());
        }

        // After rollback, the session branches again from `target_turn`. Keeping a cutoff newer
        // than that branch point would cause future snapshots on the new branch to be mistaken
        // for "pre-rebuild" history during the next restore, so clamp the cutoff down.
        self.merge_session_custom_metadata(
            session_id,
            json!({
                LISTING_BASELINE_REBUILD_TURN_INDEX_METADATA_KEY: target_turn,
            }),
        )
        .await
    }

    pub async fn invalidate_prompt_cache(
        &self,
        session_id: &str,
        scope: PromptCacheScope,
        reason: &str,
    ) {
        self.ensure_prompt_cache_loaded(session_id).await;
        let changed = self.prompt_cache_store.invalidate(session_id, scope);

        if changed {
            debug!(
                "Invalidated session prompt cache: session_id={}, scope={:?}, reason={}",
                session_id, scope, reason
            );
            self.persist_prompt_cache_best_effort(session_id, reason)
                .await;
        }
    }

    /// Synchronously reset session state to Idle if it is currently Processing
    /// the expected turn.
    ///
    /// This is an in-memory-only operation intended for RAII-style cleanup in
    /// spawn tasks.  Because `Drop::drop` is synchronous we cannot do async
    /// file I/O here, but that is acceptable: the in-memory state is the
    /// source of truth at runtime, and `restore_session` already resets any
    /// non-Idle persisted state to Idle on application restart.
    pub fn reset_session_state_if_processing(&self, session_id: &str, expected_turn_id: &str) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if matches!(
                &session.state,
                SessionState::Processing {
                    current_turn_id,
                    ..
                } if current_turn_id == expected_turn_id
            ) {
                debug!(
                    "RAII guard resetting stuck Processing state to Idle: session_id={}, turn_id={}",
                    session_id, expected_turn_id
                );
                session.state = SessionState::Idle;
                session.updated_at = SystemTime::now();
                session.last_activity_at = SystemTime::now();
            }
        }
    }

    /// Update session state
    pub async fn update_session_state(
        &self,
        session_id: &str,
        new_state: SessionState,
    ) -> OpenBitFunResult<()> {
        let effective_path = self.effective_session_storage_path(session_id).await;

        // IMPORTANT: keep the DashMap guard scope short -- do NOT hold it across .await.
        // Collect the data needed for persistence, then release the guard before doing I/O.
        let should_persist = if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.state = new_state.clone();
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();

            self.config.enable_persistence && self.should_persist_session(&session)
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        };
        // RefMut guard released here -- DashMap shard lock is free.

        // Persist state changes outside the guard scope.
        if should_persist {
            if let Some(ref workspace_path) = effective_path {
                self.persistence_manager
                    .save_session_state(workspace_path, session_id, &new_state)
                    .await?;
            }
        }

        debug!(
            "Updated session state: session_id={}, state={:?}",
            session_id, new_state
        );

        Ok(())
    }

    /// Update session state only when the session is still processing the
    /// expected turn. Returns `true` when the state was updated.
    pub async fn update_session_state_for_turn_if_processing(
        &self,
        session_id: &str,
        expected_turn_id: &str,
        new_state: SessionState,
    ) -> OpenBitFunResult<bool> {
        let effective_path = self.effective_session_storage_path(session_id).await;

        let should_persist = if let Some(mut session) = self.sessions.get_mut(session_id) {
            let owns_processing_turn = matches!(
                &session.state,
                SessionState::Processing {
                    current_turn_id,
                    ..
                } if current_turn_id == expected_turn_id
            );

            if !owns_processing_turn {
                debug!(
                    "Skipped session state update for stale turn: session_id={}, expected_turn_id={}, current_state={:?}",
                    session_id, expected_turn_id, session.state
                );
                return Ok(false);
            }

            session.state = new_state.clone();
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();

            self.config.enable_persistence && self.should_persist_session(&session)
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        };

        if should_persist {
            if let Some(ref workspace_path) = effective_path {
                self.persistence_manager
                    .save_session_state(workspace_path, session_id, &new_state)
                    .await?;
            }
        }

        debug!(
            "Updated session state for turn: session_id={}, turn_id={}, state={:?}",
            session_id, expected_turn_id, new_state
        );

        Ok(true)
    }

    /// Update session title (in-memory + persistence)
    pub async fn update_session_title(
        &self,
        session_id: &str,
        title: &str,
    ) -> OpenBitFunResult<()> {
        let normalized_title = Self::normalize_session_title_input(title)?;
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        self.update_session_title_locked(session_id, normalized_title, true)
            .await
    }

    async fn update_session_title_locked(
        &self,
        session_id: &str,
        normalized_title: String,
        user_edited: bool,
    ) -> OpenBitFunResult<()> {
        let workspace_path = self.effective_session_storage_path(session_id).await;
        let mut updated_session = self
            .sessions
            .get(session_id)
            .map(|session| session.clone())
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        let now = SystemTime::now();
        updated_session.session_name = normalized_title.clone();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;

        if self.should_persist_session_id(session_id) {
            let Some(workspace_path) = workspace_path.as_ref() else {
                return Err(OpenBitFunError::Session(format!(
                    "Workspace path is unavailable for session {}",
                    session_id
                )));
            };
            let last_active_at = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.persistence_manager
                .update_session_title_metadata(
                    workspace_path,
                    session_id,
                    &updated_session.session_name,
                    last_active_at,
                    user_edited,
                )
                .await?;
        }

        let Some(mut session) = self.sessions.get_mut(session_id) else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        };
        session.session_name = updated_session.session_name;
        session.updated_at = now;
        session.last_activity_at = now;

        info!(
            "Session title updated: session_id={}, title={}",
            session_id, normalized_title
        );

        Ok(())
    }

    pub async fn update_session_title_if_current(
        &self,
        session_id: &str,
        expected_current_title: &str,
        title: &str,
    ) -> OpenBitFunResult<bool> {
        let normalized_title = Self::normalize_session_title_input(title)?;
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if let Some(path) = self.effective_session_storage_path(session_id).await {
            if self
                .persistence_manager
                .load_session_metadata(&path, session_id)
                .await?
                .and_then(|m| m.custom_metadata)
                .is_some_and(|m| m.get("titleUserEdited").and_then(|v| v.as_bool()) == Some(true))
            {
                return Ok(false);
            }
        }
        let Some(session) = self.sessions.get(session_id) else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        };

        if session.session_name != expected_current_title {
            debug!(
                "Skipping auto-generated title because current title changed: session_id={}, expected_title={}, current_title={}",
                session_id, expected_current_title, session.session_name
            );
            return Ok(false);
        }
        drop(session);

        self.update_session_title_locked(session_id, normalized_title, false)
            .await?;
        Ok(true)
    }

    /// Legacy mutation helper retained only for persistence-focused unit tests.
    /// Production callers must update the logical id and route owner atomically
    /// through `update_session_agent_binding`.
    #[cfg(test)]
    async fn update_session_agent_type(
        &self,
        session_id: &str,
        agent_type: &str,
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let mut session = self
            .sessions
            .get(session_id)
            .map(|session| session.clone())
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {}", session_id))
            })?;

        if session.agent_type == agent_type {
            return Ok(());
        }

        let now = SystemTime::now();
        session.agent_type = agent_type.to_string();
        session.updated_at = now;
        session.last_activity_at = now;

        if self.should_persist_session_id(session_id) {
            let last_active_at = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.update_persisted_session_metadata(session_id, |metadata| {
                metadata.agent_type = agent_type.to_string();
                metadata.last_active_at = last_active_at;
            })
            .await?;
        }

        if let Some(mut active_session) = self.sessions.get_mut(session_id) {
            active_session.agent_type = session.agent_type.clone();
            active_session.updated_at = now;
            active_session.last_activity_at = now;
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }
        drop(_mutation_guard);

        debug!(
            "Session agent type updated: session_id={}, agent_type={}",
            session_id, agent_type
        );

        Ok(())
    }

    /// Update the logical main-agent id and its durable route owner together.
    ///
    /// The owner is part of the execution binding: an externally owned Session
    /// must remain fail-closed after restart instead of resolving a same-name
    /// local mode. Persist the complete Session so metadata and state sidecar
    /// cannot disagree about this pair.
    pub async fn update_session_agent_binding(
        &self,
        session_id: &str,
        agent_type: &str,
        route_owner: SessionAgentRouteOwner,
        route_key: Option<String>,
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let original_session = self
            .sessions
            .get(session_id)
            .map(|session| session.clone())
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        if original_session.agent_type == agent_type
            && original_session.config.agent_route_owner == route_owner
            && original_session.config.agent_route_key == route_key
        {
            return Ok(());
        }

        let mut updated_session = original_session.clone();
        let now = SystemTime::now();
        updated_session.agent_type = agent_type.to_string();
        updated_session.config.agent_route_owner = route_owner;
        updated_session.config.agent_route_key = route_key.clone();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;

        if self.should_persist_session_id(session_id) {
            if let Some(workspace_path) = self.effective_session_storage_path(session_id).await {
                if let Err(error) = self
                    .persistence_manager
                    .save_session(&workspace_path, &updated_session)
                    .await
                {
                    if let Err(rollback_error) = self
                        .persistence_manager
                        .save_session(&workspace_path, &original_session)
                        .await
                    {
                        return Err(OpenBitFunError::session(format!(
                            "Session agent binding persistence failed and rollback did not complete: session_id={session_id}, error={error}, rollback_error={rollback_error}"
                        )));
                    }
                    return Err(error);
                }
            }
        }

        let Some(mut active_session) = self.sessions.get_mut(session_id) else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        };
        active_session.agent_type = updated_session.agent_type;
        active_session.config.agent_route_owner = route_owner;
        active_session.config.agent_route_key = route_key.clone();
        active_session.updated_at = now;
        active_session.last_activity_at = now;
        debug!(
            "Session agent binding updated: session_id={}, agent_type={}, route_owner={:?}, route_key={:?}",
            session_id, agent_type, route_owner, route_key
        );

        Ok(())
    }

    /// Update the most recent scheduler-accepted user submission mode.
    ///
    /// This state is intentionally independent from rollback-sensitive history
    /// semantics. Prompt-cache guards should read this instead of deriving from
    /// surviving dialog turns.
    pub async fn update_last_submitted_agent_type(
        &self,
        session_id: &str,
        agent_type: &str,
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.last_submitted_agent_type = Some(agent_type.to_string());
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }

        if self.should_persist_session_id(session_id) {
            let effective_path = self.effective_session_storage_path(session_id).await;
            let session_snapshot = self.sessions.get(session_id).map(|s| s.clone());
            if let (Some(workspace_path), Some(session)) = (effective_path, session_snapshot) {
                self.persistence_manager
                    .save_session(&workspace_path, &session)
                    .await?;
            }
        }

        debug!(
            "Session last submitted agent type updated: session_id={}, agent_type={}",
            session_id, agent_type
        );

        Ok(())
    }

    /// Update whether a session runs its tool loop.
    ///
    /// `enable_tools` is persisted per session, so a session created while the
    /// caller disabled tools stays tool-less forever on reuse. Hosts that later
    /// change their mind (for example MiniApp runs that moved from a frontend
    /// switch to a backend allowlist) call this to repair existing sessions.
    pub async fn update_session_tool_enablement(
        &self,
        session_id: &str,
        enable_tools: bool,
    ) -> OpenBitFunResult<bool> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if session.config.enable_tools == enable_tools {
                return Ok(false);
            }
            session.config.enable_tools = enable_tools;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }

        if self.should_persist_session_id(session_id) {
            let effective_path = self.effective_session_storage_path(session_id).await;
            let session_snapshot = self.sessions.get(session_id).map(|s| s.clone());
            if let (Some(workspace_path), Some(session)) = (effective_path, session_snapshot) {
                self.persistence_manager
                    .save_session(&workspace_path, &session)
                    .await?;
            }
        }

        debug!(
            "Session tool enablement updated: session_id={}, enable_tools={}",
            session_id, enable_tools
        );

        Ok(true)
    }

    /// Inherit parent dialog mode state when creating forked child sessions.
    ///
    /// `last_user_dialog_agent_type` drives first-entry mode reminders, while
    /// `last_submitted_agent_type` preserves scheduler prompt-cache state.
    pub async fn inherit_session_agent_type_state(
        &self,
        session_id: &str,
        last_user_dialog_agent_type: Option<String>,
        last_submitted_agent_type: Option<String>,
    ) -> OpenBitFunResult<()> {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.last_user_dialog_agent_type = last_user_dialog_agent_type;
            session.last_submitted_agent_type = last_submitted_agent_type;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }

        if self.should_persist_session_id(session_id) {
            let effective_path = self.effective_session_storage_path(session_id).await;
            let session_snapshot = self.sessions.get(session_id).map(|s| s.clone());
            if let (Some(workspace_path), Some(session)) = (effective_path, session_snapshot) {
                self.persistence_manager
                    .save_session(&workspace_path, &session)
                    .await?;
            }
        }

        debug!(
            "Session agent type state inherited: session_id={}",
            session_id
        );

        Ok(())
    }

    fn derive_last_user_dialog_agent_type_from_turns(
        turns: &[DialogTurnData],
        fallback_agent_type: Option<&str>,
    ) -> Option<String> {
        // New turns persist their mode on the turn itself. For older persisted
        // sessions that predate this field, fall back to the session default
        // only when at least one surviving user dialog turn exists.
        turns
            .iter()
            .rev()
            .find(|turn| turn.kind == DialogTurnKind::UserDialog)
            .and_then(|turn| {
                turn.agent_type
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                if turns
                    .iter()
                    .any(|turn| turn.kind == DialogTurnKind::UserDialog)
                {
                    fallback_agent_type
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                } else {
                    None
                }
            })
    }

    /// Update session model id (in-memory + persistence)
    pub async fn update_session_model_id(
        &self,
        session_id: &str,
        model_id: &str,
    ) -> OpenBitFunResult<()> {
        self.update_session_model_selection(session_id, model_id, None)
            .await
    }

    /// Atomically updates the model and optional reasoning preset.
    pub async fn update_session_model_selection(
        &self,
        session_id: &str,
        model_id: &str,
        reasoning_preset: Option<&str>,
    ) -> OpenBitFunResult<()> {
        self.update_session_model_selection_with_routing(
            session_id,
            model_id,
            reasoning_preset,
            None,
        )
        .await
    }

    pub async fn update_session_model_selection_with_routing(
        &self,
        session_id: &str,
        model_id: &str,
        reasoning_preset: Option<&str>,
        routing_mode: Option<&str>,
    ) -> OpenBitFunResult<()> {
        self.update_session_model_selection_in_storage_with_routing(
            None,
            session_id,
            model_id,
            reasoning_preset,
            routing_mode,
            true,
            |_| Ok(()),
        )
        .await
        .map(|_| ())
    }

    #[cfg(test)]
    pub(crate) async fn update_session_model_selection_in_storage(
        &self,
        storage_path: Option<&Path>,
        session_id: &str,
        model_id: &str,
        reasoning_preset: Option<&str>,
        include_internal: bool,
        validate_binding: impl FnOnce(&Session) -> OpenBitFunResult<()>,
    ) -> OpenBitFunResult<Session> {
        self.update_session_model_selection_in_storage_with_routing(
            storage_path,
            session_id,
            model_id,
            reasoning_preset,
            None,
            include_internal,
            validate_binding,
        )
        .await
    }

    pub(crate) async fn update_session_model_selection_in_storage_with_routing(
        &self,
        storage_path: Option<&Path>,
        session_id: &str,
        model_id: &str,
        reasoning_preset: Option<&str>,
        routing_mode: Option<&str>,
        include_internal: bool,
        validate_binding: impl FnOnce(&Session) -> OpenBitFunResult<()>,
    ) -> OpenBitFunResult<Session> {
        self.update_session_selection_in_storage(
            storage_path,
            session_id,
            model_id,
            reasoning_preset,
            routing_mode,
            include_internal,
            None,
            validate_binding,
        )
        .await
    }

    pub async fn update_session_model_target(
        &self,
        session_id: &str,
        selection: &openbitfun_core_types::model_pool::ModelSelection,
    ) -> OpenBitFunResult<Session> {
        self.update_session_model_target_with_reasoning(session_id, selection, None)
            .await
    }

    pub(crate) async fn update_session_model_target_with_reasoning(
        &self,
        session_id: &str,
        selection: &openbitfun_core_types::model_pool::ModelSelection,
        reasoning_preset: Option<&str>,
    ) -> OpenBitFunResult<Session> {
        use openbitfun_core_types::model_pool::ModelSelection;
        let model_id = match selection {
            ModelSelection::Model { model_ref } => model_ref.as_str(),
            ModelSelection::Pool { .. } => "",
        };
        self.update_session_selection_in_storage(
            None,
            session_id,
            model_id,
            reasoning_preset,
            None,
            true,
            Some(selection),
            |_| Ok(()),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_session_selection_in_storage(
        &self,
        storage_path: Option<&Path>,
        session_id: &str,
        model_id: &str,
        reasoning_preset: Option<&str>,
        routing_mode: Option<&str>,
        include_internal: bool,
        selection: Option<&openbitfun_core_types::model_pool::ModelSelection>,
        validate_binding: impl FnOnce(&Session) -> OpenBitFunResult<()>,
    ) -> OpenBitFunResult<Session> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let storage_path = storage_path.map(Path::to_path_buf).or_else(|| {
            Self::committed_session_storage_path(&self.session_storage_path_index, session_id)
        });
        if let Some(path) = storage_path.as_deref() {
            self.validate_session_storage_path_binding(session_id, path)?;
        }
        let loaded = self.sessions.get(session_id).map(|session| session.clone());
        // Keep the cross-process lease until the complete read/modify/write has
        // finished. A cold selector update never reserves active-session capacity.
        let _writer = if loaded.is_none() {
            storage_path
                .as_deref()
                .map(|path| {
                    self.persistence_manager
                        .lock_session_writes(path, session_id)
                })
                .transpose()?
        } else {
            None
        };
        let was_loaded = loaded.is_some();
        let original_session = match loaded {
            Some(session) => session,
            None => {
                let path = storage_path.as_deref().ok_or_else(|| {
                    OpenBitFunError::NotFound(format!("Session not found: {session_id}"))
                })?;
                let session = self
                    .persistence_manager
                    .load_session_header(path, session_id)
                    .await?;
                if !include_internal && session.kind != SessionKind::Standard {
                    return Err(OpenBitFunError::NotFound(format!(
                        "Session not found: {session_id}"
                    )));
                }
                session
            }
        };
        validate_binding(&original_session)?;
        if routing_mode == Some("smart") {
            crate::agentic::execution::round_model_routing::configuration::require_available()?;
        }
        if original_session.config.model_pool_id.is_some() && selection.is_none() {
            return Err(OpenBitFunError::validation("This session uses a model pool; explicit model_pool_selection_v1 selection is required"));
        }
        if selection.is_some()
            && original_session.config.model_binding_policy
                == SessionModelBindingPolicy::ApprovedImmutable
        {
            return Err(OpenBitFunError::validation(
                "An approved immutable model binding cannot be replaced by model pool selection",
            ));
        }
        let ai_config = Self::load_ai_config_for_session(&original_session.config).await?;
        let mut resolved_context_window = None;
        let mut auto_cleared_reasoning_preset = None;
        let mut updated_session = original_session.clone();
        if routing_mode.is_none()
            && selection.is_none()
            && original_session.config.model_routing.as_deref() == Some("smart")
            && original_session.config.model_id.as_deref() != Some(model_id)
        {
            if crate::agentic::execution::round_model_routing::configuration::available() {
                return Err(OpenBitFunError::Validation(
                    "This session uses smart routing; update its model strategy with a compatible GUI"
                        .into(),
                ));
            }
            // A legacy client explicitly choosing another ordinary model is a
            // manual selection. Merely reading or updating reasoning preserves
            // the retained smart-mode record.
            updated_session.config.model_routing = Some("manual".into());
        }
        if let Some(mode) = routing_mode {
            if !matches!(mode, "manual" | "smart") {
                return Err(OpenBitFunError::Validation(
                    "Unsupported model routing strategy".into(),
                ));
            }
            if mode == "smart" {
                let eligible = original_session.kind != SessionKind::Subagent
                    && original_session.config.model_binding_policy
                        != SessionModelBindingPolicy::ApprovedImmutable
                    && matches!(
                        original_session.config.agent_route_owner,
                        crate::agentic::core::SessionAgentRouteOwner::Local
                    );
                if !eligible {
                    return Err(OpenBitFunError::Validation(
                        "Smart mode is unavailable for this session".into(),
                    ));
                }
                crate::agentic::execution::round_model_routing::configuration::prepare_turn(
                    Some(mode),
                    true,
                )
                .await?;
            }
            updated_session.config.model_routing = Some(mode.into());
        }
        updated_session.config.model_id = Some(model_id.to_string());
        if let Some(selection) = selection {
            updated_session.config.model_routing = Some("manual".into());
            updated_session.config.model_pool_selection_revision = original_session
                .config
                .model_pool_selection_revision
                .saturating_add(1);
            updated_session.config.model_pool_binding = None;
            use openbitfun_core_types::model_pool::ModelSelection;
            match selection {
                ModelSelection::Pool { pool_id } => {
                    let config_service =
                        crate::service::config::get_global_config_service().await?;
                    let pool = config_service.model_pool_store().await.get(pool_id).await?;
                    if pool.member_model_ids.is_empty() {
                        return Err(OpenBitFunError::validation(
                            "Cannot select an empty model pool",
                        ));
                    }
                    updated_session.config.model_pool_id = Some(pool_id.clone());
                    updated_session.config.model_binding_policy = SessionModelBindingPolicy::Pool;
                    updated_session.config.model_id = None;
                }
                ModelSelection::Model { model_ref } => {
                    let config = ai_config.as_ref().ok_or_else(|| {
                        OpenBitFunError::config("AI configuration is unavailable")
                    })?;
                    if config.resolve_model_selection(model_ref).is_none() {
                        return Err(OpenBitFunError::validation(
                            "Selected model is missing or disabled",
                        ));
                    }
                    updated_session.config.model_pool_id = None;
                    updated_session.config.model_binding_policy =
                        SessionModelBindingPolicy::Mutable;
                }
            }
        }
        updated_session.config.reasoning_preset = reasoning_preset
            .map(str::trim)
            .filter(|preset| !preset.is_empty() && !preset.eq_ignore_ascii_case("auto"))
            .map(ToOwned::to_owned);
        if let Some(ai_config) = ai_config.as_ref() {
            let requested_reasoning_preset = updated_session.config.reasoning_preset.clone();
            updated_session.config.reasoning_preset =
                Self::normalize_session_reasoning_preset(&updated_session, ai_config).await;
            if requested_reasoning_preset.is_some()
                && updated_session.config.reasoning_preset.is_none()
            {
                auto_cleared_reasoning_preset = requested_reasoning_preset.clone();
                warn!(
                    "Session reasoning preset is not available for the selected model; normalizing to Auto: session_id={}, model_id={}, preset_id={}",
                    session_id,
                    model_id,
                    requested_reasoning_preset.as_deref().unwrap_or_default()
                );
            }
            resolved_context_window =
                Self::sync_session_context_window_from_ai_config(&mut updated_session, ai_config);
        }
        if original_session.config.model_routing == updated_session.config.model_routing
            && original_session.config.model_id == updated_session.config.model_id
            && original_session.config.model_pool_id == updated_session.config.model_pool_id
            && original_session.config.model_binding_policy
                == updated_session.config.model_binding_policy
            && original_session.config.reasoning_preset == updated_session.config.reasoning_preset
            && original_session.config.max_context_tokens
                == updated_session.config.max_context_tokens
        {
            if !was_loaded && self.should_persist_session_id(session_id) {
                if let Some(path) = storage_path.as_deref() {
                    // Repair a metadata projection left behind by an interrupted
                    // two-file commit; an aligned snapshot performs no write.
                    self.persistence_manager
                        .save_session_model_selection(path, &original_session)
                        .await?;
                }
            }
            return Ok(original_session);
        }
        let now = SystemTime::now();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;

        if self.should_persist_session_id(session_id) {
            let effective_path = match storage_path {
                Some(path) => Some(path),
                None => self.effective_session_storage_path(session_id).await,
            };
            if let Some(path) = effective_path {
                self.persistence_manager
                    .save_session_model_selection(&path, &updated_session)
                    .await?;
            }
        }

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.config.model_id = updated_session.config.model_id.clone();
            session.config.model_routing = updated_session.config.model_routing.clone();
            session.config.model_pool_selection_revision =
                updated_session.config.model_pool_selection_revision;
            session.config.model_pool_id = updated_session.config.model_pool_id.clone();
            session.config.model_pool_binding = updated_session.config.model_pool_binding.clone();
            session.config.model_binding_policy = updated_session.config.model_binding_policy;
            session.config.reasoning_preset = updated_session.config.reasoning_preset.clone();
            session.config.max_context_tokens = updated_session.config.max_context_tokens;
            session.updated_at = now;
            session.last_activity_at = now;
        }

        debug!(
            "Session model selection updated: session_id={}, model_id={}, reasoning_preset={:?}, max_context_tokens={:?}",
            session_id,
            model_id,
            updated_session.config.reasoning_preset,
            resolved_context_window
        );

        if let Some(previous_preset_id) = auto_cleared_reasoning_preset {
            if let Some(coordinator) = crate::agentic::coordination::get_global_coordinator() {
                coordinator
                    .emit_session_reasoning_preset_auto_cleared(
                        session_id,
                        &previous_preset_id,
                        "selection_unavailable",
                    )
                    .await;
            }
        }

        Ok(updated_session)
    }

    /// Persist affinity without replacing the user's selection or a newer selection.
    pub async fn remember_model_pool_binding(
        &self,
        session_id: &str,
        selection_revision: u64,
        binding: openbitfun_core_types::model_pool::ModelPoolBinding,
    ) -> OpenBitFunResult<()> {
        let _guard = self.acquire_session_mutation(session_id).await?;
        let Some(mut session) = self.get_session(session_id) else {
            return Ok(());
        };
        if session.config.model_pool_id.as_deref() != Some(&binding.pool_id)
            || session.config.model_pool_selection_revision != selection_revision
        {
            return Ok(());
        }
        session.config.model_pool_binding = Some(binding.clone());
        if self.should_persist_session_id(session_id) {
            if let Some(path) = self.effective_session_storage_path(session_id).await {
                self.persistence_manager
                    .save_session_model_selection(&path, &session)
                    .await?;
            }
        }
        if let Some(mut current) = self.sessions.get_mut(session_id) {
            current.config.model_pool_binding = Some(binding);
        }
        Ok(())
    }

    /// Sets the session's own tool permission mode (in-memory + persistence).
    ///
    /// `None` clears the override so the session follows the user-level default
    /// again, including later changes to that default. The value only takes
    /// effect from the next model round. A running round keeps the policy it
    /// resolved at its boundary, while later rounds read the updated session.
    pub async fn update_session_permission_mode(
        &self,
        session_id: &str,
        permission_mode: Option<PermissionMode>,
    ) -> OpenBitFunResult<()> {
        // Permission updates still restore an evicted session before
        // the mutation permit is taken, because restore owns the same keyed lock.
        if !self.sessions.contains_key(session_id) && self.config.enable_persistence {
            let session_storage_path =
                Self::committed_session_storage_path(&self.session_storage_path_index, session_id);
            if let Some(session_storage_path) = session_storage_path {
                debug!(
                    "Session evicted from memory, restoring for permission mode update: session_id={}",
                    session_id
                );
                let _ = self
                    .restore_session_from_storage_path(&session_storage_path, session_id)
                    .await;
            }
        }

        let _mutation_guard = self.acquire_session_mutation(session_id).await?;

        let original_session = self
            .sessions
            .get(session_id)
            .map(|session| session.clone())
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {}", session_id))
            })?;
        if original_session.config.permission_mode == permission_mode {
            return Ok(());
        }

        let mut updated_session = original_session.clone();
        updated_session.config.permission_mode = permission_mode;
        let now = SystemTime::now();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;

        if self.should_persist_session_id(session_id) {
            let effective_path = self.effective_session_storage_path(session_id).await;
            if let Some(workspace_path) = effective_path {
                if let Err(error) = self
                    .persistence_manager
                    .save_session(&workspace_path, &updated_session)
                    .await
                {
                    // A selection that is not durable must not stay applied in
                    // memory, so restore the previous value before failing.
                    if let Err(rollback_error) = self
                        .persistence_manager
                        .save_session(&workspace_path, &original_session)
                        .await
                    {
                        return Err(OpenBitFunError::session(format!(
                            "Session permission mode persistence failed and rollback did not complete: session_id={session_id}, error={error}, rollback_error={rollback_error}"
                        )));
                    }
                    return Err(error);
                }
            }
        }

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.config.permission_mode = permission_mode;
            session.updated_at = now;
            session.last_activity_at = now;
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }

        debug!(
            "Session permission mode updated: session_id={}, permission_mode={:?}",
            session_id, permission_mode
        );

        Ok(())
    }

    /// Reads the session's own permission mode without falling back to the
    /// user-level default. `None` means the session never chose one.
    pub fn session_permission_mode(&self, session_id: &str) -> Option<PermissionMode> {
        self.sessions
            .get(session_id)
            .and_then(|session| session.config.permission_mode)
    }

    /// Installs or replaces the ephemeral permission mode for one exact active
    /// turn. A stale update can never leak into a newer turn in the session.
    pub fn set_active_turn_permission_mode(
        &self,
        session_id: &str,
        turn_id: &str,
        permission_mode: PermissionMode,
    ) -> bool {
        let Some(session) = self.sessions.get(session_id) else {
            return false;
        };
        if !matches!(
            &session.state,
            SessionState::Processing { current_turn_id, .. } if current_turn_id == turn_id
        ) {
            return false;
        }

        // Keep the session shard read-locked through the write. A concurrent
        // turn-completion state update must happen after this insert, so its
        // cleanup cannot run just before a stale override is published.
        self.active_turn_permission_modes.insert(
            session_id.to_string(),
            ActiveTurnPermissionMode {
                turn_id: turn_id.to_string(),
                mode: permission_mode,
            },
        );
        true
    }

    pub fn active_turn_permission_mode(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Option<PermissionMode> {
        let session = self.sessions.get(session_id)?;
        if !matches!(
            &session.state,
            SessionState::Processing { current_turn_id, .. } if current_turn_id == turn_id
        ) {
            return None;
        }
        self.active_turn_permission_modes
            .get(session_id)
            .filter(|entry| entry.turn_id == turn_id)
            .map(|entry| entry.mode)
    }

    /// Clears an ephemeral override only when it belongs to the expected turn.
    pub fn clear_active_turn_permission_mode(&self, session_id: &str, turn_id: &str) -> bool {
        match self
            .active_turn_permission_modes
            .entry(session_id.to_string())
        {
            dashmap::mapref::entry::Entry::Occupied(entry) if entry.get().turn_id == turn_id => {
                entry.remove();
                true
            }
            _ => false,
        }
    }

    /// Rebind where a session executes (in-memory + persistence).
    ///
    /// Only the workspace roots and the resolved execution target move; session
    /// storage stays keyed on `project_workspace_path`, so the transcript keeps
    /// its identity when a session is moved into or out of a managed worktree.
    pub async fn update_session_execution_binding(
        &self,
        session_id: &str,
        binding: SessionExecutionBindingUpdate,
    ) -> Result<(), SessionExecutionBindingError> {
        // Mirrors update_session_model_id: an evicted session must be restored
        // before the mutation permit is taken. View-only historical restores do
        // not populate the storage-path index, so use the owning project path as
        // the stable fallback locator.
        if !self.sessions.contains_key(session_id) && self.config.enable_persistence {
            let session_storage_path =
                Self::committed_session_storage_path(&self.session_storage_path_index, session_id);
            let restore_result = if let Some(session_storage_path) = session_storage_path {
                self.restore_session_from_storage_path(&session_storage_path, session_id)
                    .await
            } else {
                self.restore_session(Path::new(&binding.project_workspace_path), session_id)
                    .await
            };
            if let Err(restore_error) = restore_result {
                return match restore_error {
                    OpenBitFunError::NotFound(message) => {
                        Err(SessionExecutionBindingError::NotFound(message))
                    }
                    other => Err(SessionExecutionBindingError::Internal(other)),
                };
            }
        }

        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let mut has_persisted_turns = false;
        if self.should_persist_session_id(session_id) {
            if let Some(storage_path) = self.effective_session_storage_path(session_id).await {
                if self
                    .persistence_manager
                    .load_session_revert_state(&storage_path, session_id)
                    .await?
                    .is_some()
                {
                    return Err(SessionExecutionBindingError::Busy(
                        "Worktree isolation cannot change while Session undo or redo is pending"
                            .to_string(),
                    ));
                }
                has_persisted_turns = !self
                    .persistence_manager
                    .load_session_turns(&storage_path, session_id)
                    .await?
                    .is_empty();
            }
        }

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if has_persisted_turns
                || !session.dialog_turn_ids.is_empty()
                || !matches!(session.state, SessionState::Idle)
            {
                return Err(SessionExecutionBindingError::Busy(
                    "Worktree isolation can only be changed before the session's first message"
                        .to_string(),
                ));
            }
            session.config.workspace_path = Some(binding.workspace_path.clone());
            session.config.project_workspace_path = Some(binding.project_workspace_path.clone());
            session.config.execution_target = Some(binding.execution_target.clone());
            session.config.workspace_id = binding.workspace_id.clone();
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        } else {
            return Err(SessionExecutionBindingError::NotFound(format!(
                "Session not found: {session_id}"
            )));
        }

        if self.should_persist_session_id(session_id) {
            let effective_path = self.effective_session_storage_path(session_id).await;
            let session_snapshot = self.sessions.get(session_id).map(|s| s.clone());
            if let (Some(workspace_path), Some(session)) = (effective_path, session_snapshot) {
                self.persistence_manager
                    .save_session(&workspace_path, &session)
                    .await?;
            }
        }

        debug!(
            "Session execution binding updated: session_id={}, workspace_path={}",
            session_id, binding.workspace_path
        );

        Ok(())
    }

    /// Refresh the legacy session window snapshot for existing session consumers.
    /// Request budgets are resolved independently from the active model or pool.
    pub async fn refresh_session_context_window(&self, session_id: &str) -> OpenBitFunResult<()> {
        let config = self
            .get_session(session_id)
            .map(|session| session.config.clone());
        if let Some(ai_config) = match config {
            Some(config) => Self::load_ai_config_for_session(&config).await?,
            None => None,
        } {
            if let Some(mut session) = self.sessions.get_mut(session_id) {
                let previous = session.config.max_context_tokens;
                Self::sync_session_context_window_from_ai_config(&mut session, &ai_config);
                let updated = session.config.max_context_tokens;
                if updated != previous {
                    debug!(
                        "Refreshed session context window: session_id={}, previous={}, updated={}",
                        session_id, previous, updated
                    );
                }
            }
        }
        Ok(())
    }

    /// Update session activity time
    pub fn touch_session(&self, session_id: &str) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.last_activity_at = SystemTime::now();
        }
    }

    async fn resolve_session_cleanup_workspace_path(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        fallback: &Path,
    ) -> PathBuf {
        if let Some(workspace_path) = self
            .sessions
            .get(session_id)
            .and_then(|session| session.config.workspace_path.as_deref().map(PathBuf::from))
        {
            return workspace_path;
        }

        if self.config.enable_persistence {
            if let Ok(Some(metadata)) = self
                .persistence_manager
                .load_session_metadata(session_storage_path, session_id)
                .await
            {
                if let Some(workspace_path) = metadata.workspace_path {
                    return PathBuf::from(workspace_path);
                }
            }
        }

        fallback.to_path_buf()
    }

    /// Delete session (cascade delete all resources)
    pub async fn delete_session(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        self.delete_session_locked(workspace_path, session_id).await
    }

    pub(crate) async fn delete_session_locked(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        let session_storage_path = self
            .resolve_storage_path_for_workspace_path(workspace_path)
            .await?;
        self.validate_session_storage_path_binding(session_id, &session_storage_path)?;
        let cleanup_workspace_path = self
            .resolve_session_cleanup_workspace_path(
                &session_storage_path,
                session_id,
                workspace_path,
            )
            .await;
        self.delete_session_from_paths_locked(
            &cleanup_workspace_path,
            &session_storage_path,
            session_id,
        )
        .await
    }

    pub(crate) async fn delete_session_by_id(&self, session_id: &str) -> OpenBitFunResult<()> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        let session_storage_path = self.require_session_storage_path(session_id).await?;
        self.validate_session_storage_path_binding(session_id, &session_storage_path)?;
        let cleanup_workspace_path = self
            .resolve_session_cleanup_workspace_path(
                &session_storage_path,
                session_id,
                &session_storage_path,
            )
            .await;
        self.delete_session_from_paths_locked(
            &cleanup_workspace_path,
            &session_storage_path,
            session_id,
        )
        .await
    }

    /// Discards one loaded non-durable Session without touching persisted
    /// Session storage. Missing Sessions are an idempotent success.
    pub(crate) async fn discard_transient_session(
        &self,
        workspace_path: &Path,
        remote_connection_id: Option<&str>,
        remote_ssh_host: Option<&str>,
        session_id: &str,
    ) -> OpenBitFunResult<bool> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let Some(root) = self.get_session(session_id) else {
            return Ok(false);
        };
        self.validate_transient_session_binding(
            &root,
            workspace_path,
            remote_connection_id,
            remote_ssh_host,
        )?;

        for descendant in self.transient_descendants_postorder(session_id) {
            let workspace_path = descendant
                .config
                .workspace_path
                .as_deref()
                .map(Path::new)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(format!(
                        "Transient session workspace binding is missing: {}",
                        descendant.session_id
                    ))
                })?;
            self.discard_one_transient_session(
                workspace_path,
                descendant.config.remote_connection_id.as_deref(),
                descendant.config.remote_ssh_host.as_deref(),
                &descendant.session_id,
            )
            .await?;
        }

        self.discard_one_transient_session(
            workspace_path,
            remote_connection_id,
            remote_ssh_host,
            session_id,
        )
        .await
    }

    pub(crate) fn transient_session_family_postorder(
        &self,
        workspace_path: &Path,
        remote_connection_id: Option<&str>,
        remote_ssh_host: Option<&str>,
        session_id: &str,
    ) -> OpenBitFunResult<Vec<String>> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let Some(root) = self.get_session(session_id) else {
            return Ok(Vec::new());
        };
        self.validate_transient_session_binding(
            &root,
            workspace_path,
            remote_connection_id,
            remote_ssh_host,
        )?;
        let mut family = self
            .transient_descendants_postorder(session_id)
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        family.push(session_id.to_string());
        Ok(family)
    }

    fn transient_descendants_postorder(&self, root_session_id: &str) -> Vec<Session> {
        fn visit(
            parent_session_id: &str,
            sessions: &[Session],
            transient_session_ids: &DashMap<String, ()>,
            visited: &mut HashSet<String>,
            ordered: &mut Vec<Session>,
        ) {
            let marker = format!("session-{parent_session_id}");
            for child in sessions.iter().filter(|session| {
                transient_session_ids.contains_key(&session.session_id)
                    && session.created_by.as_deref() == Some(marker.as_str())
            }) {
                if !visited.insert(child.session_id.clone()) {
                    continue;
                }
                visit(
                    &child.session_id,
                    sessions,
                    transient_session_ids,
                    visited,
                    ordered,
                );
                ordered.push(child.clone());
            }
        }

        let sessions = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        let mut ordered = Vec::new();
        let mut visited = HashSet::from([root_session_id.to_string()]);
        visit(
            root_session_id,
            &sessions,
            &self.transient_session_ids,
            &mut visited,
            &mut ordered,
        );
        ordered
    }

    fn validate_transient_session_binding(
        &self,
        session: &Session,
        workspace_path: &Path,
        remote_connection_id: Option<&str>,
        remote_ssh_host: Option<&str>,
    ) -> OpenBitFunResult<()> {
        if !self.is_transient_session(&session.session_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Cannot discard a durable session as transient: {}",
                session.session_id
            )));
        }
        let expected_workspace = Self::normalize_session_storage_path(workspace_path);
        let actual_workspace = session
            .config
            .workspace_path
            .as_deref()
            .map(Path::new)
            .map(Self::normalize_session_storage_path)
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Transient session workspace binding is missing: {}",
                    session.session_id
                ))
            })?;
        if actual_workspace != expected_workspace
            || session.config.remote_connection_id.as_deref() != remote_connection_id
            || session.config.remote_ssh_host.as_deref() != remote_ssh_host
        {
            return Err(OpenBitFunError::Validation(format!(
                "Transient session ownership binding does not match: {}",
                session.session_id
            )));
        }
        Ok(())
    }

    async fn discard_one_transient_session(
        &self,
        workspace_path: &Path,
        remote_connection_id: Option<&str>,
        remote_ssh_host: Option<&str>,
        session_id: &str,
    ) -> OpenBitFunResult<bool> {
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        let Some(session) = self.get_session(session_id) else {
            return Ok(false);
        };
        self.validate_transient_session_binding(
            &session,
            workspace_path,
            remote_connection_id,
            remote_ssh_host,
        )?;
        if matches!(session.state, SessionState::Processing { .. }) {
            return Err(OpenBitFunError::Validation(format!(
                "Cannot discard a processing transient session: {session_id}"
            )));
        }
        self.cleanup_session_owned_resources(
            session.config.workspace_id.as_deref(),
            session_id,
            SessionResourceCleanupPolicy::Required,
        )
        .await?;
        self.sessions.remove(session_id);
        self.transient_session_ids.remove(session_id);
        self.clear_turn_settlement_results(session_id);
        self.release_active_session_reservation(session_id);
        self.session_storage_path_index.remove(session_id);
        Ok(true)
    }

    /// Release one loaded session and its transient runtime stores while keeping
    /// persisted history and the storage-path binding available for a later restore.
    ///
    /// Callers must quiesce scheduler execution before unloading. A processing
    /// session is rejected so close/failure compensation cannot detach live work.
    pub(crate) async fn unload_session_from_memory(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<bool> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        let Some(session) = self.get_session(session_id) else {
            return Ok(false);
        };
        if self.is_transient_session(session_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Cannot unload a transient session; use the owned discard path: {session_id}"
            )));
        }
        if matches!(session.state, SessionState::Processing { .. }) {
            return Err(OpenBitFunError::Validation(format!(
                "Cannot unload a processing session: {session_id}"
            )));
        }

        if self.config.enable_persistence && self.should_persist_session(&session) {
            let storage_path = self
                .effective_session_storage_path(session_id)
                .await
                .ok_or_else(|| {
                    OpenBitFunError::NotFound(format!(
                        "Session storage path is unavailable: {session_id}"
                    ))
                })?;
            self.persistence_manager
                .save_session(&storage_path, &session)
                .await?;
        }

        if self.sessions.remove(session_id).is_none() {
            return Ok(false);
        }
        self.clear_turn_settlement_results(session_id);
        self.release_active_session_reservation(session_id);
        self.active_turn_permission_modes.remove(session_id);
        clear_session_runtime_stores(
            session_id,
            self.context_store.as_ref(),
            self.transient_turns.as_ref(),
            self.prompt_cache_store.as_ref(),
            self.token_anchor_store.as_ref(),
            self.turn_skill_agent_snapshot_store.as_ref(),
            self.skill_agent_baseline_override_snapshot_store.as_ref(),
            self.review_read_receipt_store.as_ref(),
            self.evidence_ledger.as_ref(),
        );
        self.release_session_write_lock(session_id);
        Ok(true)
    }

    async fn cleanup_session_owned_resources(
        &self,
        workspace_id: Option<&str>,
        session_id: &str,
        policy: SessionResourceCleanupPolicy,
    ) -> OpenBitFunResult<()> {
        let mut required_error = None;
        let mut record_error = |stage: &'static str, error: String| {
            warn!(
                "Session resource cleanup failed: session_id={}, stage={}, error={}",
                session_id, stage, error
            );
            if policy == SessionResourceCleanupPolicy::Required && required_error.is_none() {
                required_error = Some(OpenBitFunError::Session(format!(
                    "Session resource cleanup is incomplete: session_id={session_id}, stage={stage}, error={error}"
                )));
            }
        };

        if let Some(snapshot_manager) =
            workspace_id.and_then(|id| ensure_snapshot_manager_for_workspace(id).ok())
        {
            let snapshot_service = snapshot_manager.get_snapshot_service();
            let snapshot_service = snapshot_service.read().await;
            if let Err(error) = snapshot_service.accept_session(session_id).await {
                record_error("snapshot", error.to_string());
            }
        }

        crate::native_hooks::clear_session_hook_state(session_id);
        #[cfg(feature = "tools-browser-web")]
        crate::agentic::tools::implementations::control_hub_tool::release_browser_selection(
            session_id,
        )
        .await;
        self.active_turn_permission_modes.remove(session_id);
        clear_session_runtime_stores(
            session_id,
            self.context_store.as_ref(),
            self.transient_turns.as_ref(),
            self.prompt_cache_store.as_ref(),
            self.token_anchor_store.as_ref(),
            self.turn_skill_agent_snapshot_store.as_ref(),
            self.skill_agent_baseline_override_snapshot_store.as_ref(),
            self.review_read_receipt_store.as_ref(),
            self.evidence_ledger.as_ref(),
        );

        if let Some(cron) = crate::service::cron::get_global_cron_service() {
            match cron.delete_jobs_for_session(session_id).await {
                Ok(removed) if removed > 0 => info!(
                    "Removed {} scheduled job(s) for session_id={}",
                    removed, session_id
                ),
                Ok(_) => {}
                Err(error) => record_error("cron", error.to_string()),
            }
        }

        use crate::service::terminal::TerminalApi;
        if let Ok(terminal_api) = TerminalApi::from_singleton() {
            let binding = terminal_api.session_manager().binding();
            if let Err(error) = binding.remove(session_id).await {
                record_error("terminal", error.to_string());
            }
        }

        if let Some(error) = required_error {
            return Err(error);
        }
        Ok(())
    }

    async fn delete_session_from_paths_locked(
        &self,
        cleanup_workspace_path: &Path,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        let delete_started_at = Instant::now();
        let cleanup_workspace_id = self
            .resolve_session_workspace_binding(session_id)
            .await
            .and_then(|binding| binding.workspace_id);
        let _temporary_write_lock = if self.config.enable_persistence
            && !self.is_transient_session(session_id)
            && !self.session_write_locks.contains_key(session_id)
        {
            Some(self.try_acquire_session_write_lock(session_storage_path, session_id)?)
        } else {
            None
        };
        debug!(
            "Session deletion started: session_id={}, cleanup_workspace_path={}, session_storage_path={}, persistence_enabled={}",
            session_id,
            cleanup_workspace_path.display(),
            session_storage_path.display(),
            self.config.enable_persistence
        );

        // Persisted deletion is the only fallible required stage. Complete it
        // before mutating loaded runtime state so a storage failure leaves the
        // active session usable and retryable.
        if self.config.enable_persistence {
            let revert_state = self
                .persistence_manager
                .load_session_revert_state(session_storage_path, session_id)
                .await?;
            if revert_state
                .as_ref()
                .is_some_and(|state| state.phase != SessionRevertPhase::Staged)
            {
                return Err(OpenBitFunError::OutcomeUnknown(format!(
                    "Session deletion cannot discard an unfinished revert transition: session_id={session_id}"
                )));
            }
            let persistence_stage_started_at = Instant::now();
            debug!(
                "Session deletion stage starting: session_id={}, stage=persistence_delete",
                session_id
            );
            self.persistence_manager
                .delete_session(session_storage_path, session_id)
                .await?;
            debug!(
                "Session deletion stage completed: session_id={}, stage=persistence_delete, duration_ms={}",
                session_id,
                elapsed_ms_u64(persistence_stage_started_at)
            );
            if let (Some(revert_state), Some(workspace_id)) =
                (revert_state, cleanup_workspace_id.as_deref())
            {
                match get_or_create_snapshot_manager(workspace_id, None)
                .await
                {
                    Ok(snapshot_manager) => {
                        if let Err(error) = snapshot_manager
                            .delete_workspace_revert_checkpoint(&revert_state)
                            .await
                        {
                            warn!(
                                "Failed to delete Session revert checkpoint after Session deletion: session_id={}, error={}",
                                session_id, error
                            );
                        }
                    }
                    Err(error) => warn!(
                        "Failed to initialize snapshot cleanup for deleted Session revert checkpoint: session_id={}, error={}",
                        session_id, error
                    ),
                }
            }
        }

        self.cleanup_session_owned_resources(
            cleanup_workspace_id.as_deref(),
            session_id,
            SessionResourceCleanupPolicy::BestEffort,
        )
        .await?;

        // 4. Remove from memory
        let memory_stage_started_at = Instant::now();
        debug!(
            "Session deletion stage starting: session_id={}, stage=in_memory_remove",
            session_id
        );
        self.sessions.remove(session_id);
        self.transient_session_ids.remove(session_id);
        self.clear_turn_settlement_results(session_id);
        self.release_active_session_reservation(session_id);
        debug!(
            "Session deletion stage completed: session_id={}, stage=in_memory_remove, duration_ms={}",
            session_id,
            elapsed_ms_u64(memory_stage_started_at)
        );
        self.session_storage_path_index.remove(session_id);
        self.release_session_write_lock(session_id);

        info!(
            "Session deletion completed: session_id={}, cleanup_workspace_path={}, session_storage_path={}, duration_ms={}",
            session_id,
            cleanup_workspace_path.display(),
            session_storage_path.display(),
            elapsed_ms_u64(delete_started_at)
        );

        Ok(())
    }

    /// Restore session from a local or legacy workspace path.
    ///
    /// Callers that know remote identity must use [`Self::restore_session_for_workspace`].
    /// Callers that already resolved a `sessions` directory must use
    /// [`Self::restore_session_from_storage_path`].
    pub async fn restore_session(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        self.restore_session_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_session_for_workspace(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        self.restore_session_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_internal_session(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        self.restore_internal_session_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_internal_session_for_workspace(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        self.restore_internal_session_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_session_from_storage_path(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        self.restore_session_from_storage_path_internal(session_storage_path, session_id, false)
            .await
    }

    pub async fn restore_internal_session_from_storage_path(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Session> {
        self.restore_session_from_storage_path_internal(session_storage_path, session_id, true)
            .await
    }

    async fn restore_session_from_storage_path_internal(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        include_internal: bool,
    ) -> OpenBitFunResult<Session> {
        let (session, _) = self
            .restore_session_with_turns_from_storage_path_internal(
                session_storage_path,
                session_id,
                include_internal,
            )
            .await?;
        Ok(session)
    }

    /// Restore the persisted session header and turns needed by the UI view
    /// without loading runtime context snapshots or inserting the session into
    /// the in-memory coordinator state.
    ///
    /// This workspace-path overload is for local or legacy callers. Remote
    /// callers must use [`Self::restore_session_view_for_workspace_timed`] or a
    /// storage-path restore method so remote identity is preserved.
    pub async fn restore_session_view(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        self.restore_session_view_timed(workspace_path, session_id)
            .await
            .map(|(session, turns, _)| (session, turns))
    }

    pub async fn restore_session_view_timed(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, mut timing) = self
            .restore_session_view_from_storage_path_timed(&session_storage_path, session_id)
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, timing))
    }

    pub async fn restore_session_view_for_workspace_timed(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, mut timing) = self
            .restore_session_view_from_storage_path_timed(&session_storage_path, session_id)
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, timing))
    }

    pub async fn restore_internal_session_view(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        self.restore_internal_session_view_timed(workspace_path, session_id)
            .await
            .map(|(session, turns, _)| (session, turns))
    }

    pub async fn restore_internal_session_view_timed(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, mut timing) = self
            .restore_internal_session_view_from_storage_path_timed(
                &session_storage_path,
                session_id,
            )
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, timing))
    }

    pub async fn restore_internal_session_view_for_workspace_timed(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, mut timing) = self
            .restore_internal_session_view_from_storage_path_timed(
                &session_storage_path,
                session_id,
            )
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, timing))
    }

    pub async fn restore_session_view_tail(
        &self,
        workspace_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, usize)> {
        self.restore_session_view_tail_timed(workspace_path, session_id, tail_turn_count)
            .await
            .map(|(session, turns, total_turn_count, _)| (session, turns, total_turn_count))
    }

    pub async fn restore_session_view_tail_timed(
        &self,
        workspace_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(
        Session,
        Vec<DialogTurnData>,
        usize,
        SessionViewRestoreTiming,
    )> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, total_turn_count, mut timing) = self
            .restore_session_view_from_storage_path_tail_timed(
                &session_storage_path,
                session_id,
                tail_turn_count,
            )
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, total_turn_count, timing))
    }

    pub async fn restore_internal_session_view_tail(
        &self,
        workspace_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, usize)> {
        self.restore_internal_session_view_tail_timed(workspace_path, session_id, tail_turn_count)
            .await
            .map(|(session, turns, total_turn_count, _)| (session, turns, total_turn_count))
    }

    pub async fn restore_internal_session_view_tail_timed(
        &self,
        workspace_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(
        Session,
        Vec<DialogTurnData>,
        usize,
        SessionViewRestoreTiming,
    )> {
        let storage_path_started_at = Instant::now();
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        let resolve_storage_path_duration_ms = elapsed_ms_u64(storage_path_started_at);
        let (session, turns, total_turn_count, mut timing) = self
            .restore_internal_session_view_from_storage_path_tail_timed(
                &session_storage_path,
                session_id,
                tail_turn_count,
            )
            .await?;
        timing.resolve_storage_path_duration_ms = resolve_storage_path_duration_ms;
        Ok((session, turns, total_turn_count, timing))
    }

    pub async fn restore_session_view_from_storage_path_timed(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        self.restore_session_view_from_storage_path_internal(
            session_storage_path,
            session_id,
            false,
            None,
        )
        .await
        .map(|(session, turns, _, timing)| (session, turns, timing))
    }

    pub async fn restore_internal_session_view_from_storage_path_timed(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>, SessionViewRestoreTiming)> {
        self.restore_session_view_from_storage_path_internal(
            session_storage_path,
            session_id,
            true,
            None,
        )
        .await
        .map(|(session, turns, _, timing)| (session, turns, timing))
    }

    pub async fn restore_session_view_from_storage_path_tail_timed(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(
        Session,
        Vec<DialogTurnData>,
        usize,
        SessionViewRestoreTiming,
    )> {
        self.restore_session_view_from_storage_path_internal(
            session_storage_path,
            session_id,
            false,
            Some(tail_turn_count),
        )
        .await
    }

    pub async fn restore_internal_session_view_from_storage_path_tail_timed(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        tail_turn_count: usize,
    ) -> OpenBitFunResult<(
        Session,
        Vec<DialogTurnData>,
        usize,
        SessionViewRestoreTiming,
    )> {
        self.restore_session_view_from_storage_path_internal(
            session_storage_path,
            session_id,
            true,
            Some(tail_turn_count),
        )
        .await
    }

    async fn restore_session_view_from_storage_path_internal(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        include_internal: bool,
        tail_turn_count: Option<usize>,
    ) -> OpenBitFunResult<(
        Session,
        Vec<DialogTurnData>,
        usize,
        SessionViewRestoreTiming,
    )> {
        openbitfun_core_types::validate_session_id(session_id)
            .map_err(OpenBitFunError::Validation)?;
        let restore_started_at = Instant::now();
        let resolve_storage_path_duration_ms = 0;
        debug!(
            "Session view restore phase completed: session_id={}, phase=use_storage_path, duration_ms={}",
            session_id, resolve_storage_path_duration_ms
        );

        let metadata_started_at = Instant::now();
        if self
            .persistence_manager
            .load_session_metadata(session_storage_path, session_id)
            .await?
            .is_some_and(|metadata| !include_internal && metadata.should_hide_from_user_lists())
        {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }
        let visibility_metadata_duration_ms = elapsed_ms_u64(metadata_started_at);
        debug!(
            "Session view restore phase completed: session_id={}, phase=load_metadata, duration_ms={}",
            session_id, visibility_metadata_duration_ms
        );

        let staged_revert = self
            .persistence_manager
            .load_session_revert_state(session_storage_path, session_id)
            .await?;
        let session_started_at = Instant::now();
        let (mut session, mut persisted_turns, mut total_turn_count, turn_load) =
            if let Some(tail_turn_count) = tail_turn_count {
                self.persistence_manager
                    .load_session_with_tail_turns_before_timed(
                        session_storage_path,
                        session_id,
                        tail_turn_count,
                        staged_revert.as_ref().map(|revert| revert.boundary_turn),
                    )
                    .await?
            } else {
                let (session, turns, timing) = self
                    .persistence_manager
                    .load_session_with_turns_timed(session_storage_path, session_id)
                    .await?;
                let total_turn_count = turns.len();
                (session, turns, total_turn_count, timing)
            };
        if let Some(revert) = staged_revert.as_ref() {
            persisted_turns.retain(|turn| turn.turn_index < revert.boundary_turn);
            if tail_turn_count.is_none() {
                total_turn_count = persisted_turns.len();
            }
        }
        let load_session_with_turns_duration_ms = elapsed_ms_u64(session_started_at);
        debug!(
            "Session view restore phase completed: session_id={}, phase=load_session_with_turns, turn_count={}, total_turn_count={}, tail_turn_count={:?}, duration_ms={}",
            session_id,
            persisted_turns.len(),
            total_turn_count,
            tail_turn_count,
            load_session_with_turns_duration_ms
        );

        if !matches!(session.state, SessionState::Idle) {
            let old_state = session.state.clone();
            session.state = SessionState::Idle;
            debug!(
                "Resetting session state during view restore: session_id={}, state={:?} -> Idle",
                session_id, old_state
            );
        }

        let normalize_started_at = Instant::now();
        let persisted_turn_ids: Vec<String> = persisted_turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect();
        if session.dialog_turn_ids != persisted_turn_ids {
            debug!(
                "Session view restore normalized turn ids: session_id={}, session_turn_count={}, persisted_turn_count={}",
                session_id,
                session.dialog_turn_ids.len(),
                persisted_turn_ids.len()
            );
            session.dialog_turn_ids = persisted_turn_ids;
        }
        let normalize_turn_ids_duration_ms = elapsed_ms_u64(normalize_started_at);

        let total_duration_ms = elapsed_ms_u64(restore_started_at);
        debug!(
            "Session view restored: session_id={}, session_name={}, turn_count={}, total_duration_ms={}",
            session_id,
            session.session_name,
            persisted_turns.len(),
            total_duration_ms
        );

        let timing = SessionViewRestoreTiming {
            resolve_storage_path_duration_ms,
            visibility_metadata_duration_ms,
            load_session_with_turns_duration_ms,
            normalize_turn_ids_duration_ms,
            turn_catalog_duration_ms: 0,
            total_duration_ms,
            turn_load,
        };

        Ok((session, persisted_turns, total_turn_count, timing))
    }

    /// Restore session and return the persisted turns read during restore.
    ///
    /// This workspace-path overload is for local or legacy callers. Remote
    /// callers must use [`Self::restore_session_with_turns_for_workspace`] or a
    /// storage-path restore method so remote identity is preserved.
    pub async fn restore_session_with_turns(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        self.restore_session_with_turns_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_session_with_turns_for_workspace(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        self.restore_session_with_turns_from_storage_path(&session_storage_path, session_id)
            .await
    }

    pub async fn restore_internal_session_with_turns(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        self.restore_internal_session_with_turns_from_storage_path(
            &session_storage_path,
            session_id,
        )
        .await
    }

    pub async fn restore_internal_session_with_turns_for_workspace(
        &self,
        request: SessionStoragePathRequest,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let session_storage_path = self.resolve_storage_path_for_request(request).await?;
        self.restore_internal_session_with_turns_from_storage_path(
            &session_storage_path,
            session_id,
        )
        .await
    }

    pub async fn restore_session_with_turns_from_storage_path(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        self.restore_session_with_turns_from_storage_path_internal(
            session_storage_path,
            session_id,
            false,
        )
        .await
    }

    pub async fn restore_internal_session_with_turns_from_storage_path(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        self.restore_session_with_turns_from_storage_path_internal(
            session_storage_path,
            session_id,
            true,
        )
        .await
    }

    async fn restore_session_with_turns_from_storage_path_internal(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        include_internal: bool,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        let _evidence_ledger_guard = self.evidence_ledger_operation_locks.lock(session_id).await;

        if self.is_session_loaded_from_storage_path(session_storage_path, session_id)? {
            let session = self.get_session(session_id).ok_or_else(|| {
                OpenBitFunError::NotFound(format!(
                    "Session not found after identity check: {session_id}"
                ))
            })?;
            let (_, turns, _) = if include_internal {
                self.restore_internal_session_view_from_storage_path_timed(
                    session_storage_path,
                    session_id,
                )
                .await?
            } else {
                self.restore_session_view_from_storage_path_timed(session_storage_path, session_id)
                    .await?
            };
            return Ok((session, turns));
        }

        let session_write_lock = if self.config.enable_persistence {
            Some(self.try_acquire_session_write_lock(session_storage_path, session_id)?)
        } else {
            None
        };
        let claimed = self.claim_session_storage_path(session_id, session_storage_path, true)?;
        // Runtime restore includes history decoding and context reconstruction.
        // Keep that state on the heap across the nested fork/restore facades.
        let result = Box::pin(
            self.restore_session_with_turns_from_claimed_storage_path_internal(
                session_storage_path,
                session_id,
                include_internal,
            ),
        )
        .await;
        if result.is_err() {
            self.release_failed_session_storage_path_claim(
                session_id,
                session_storage_path,
                claimed,
            );
        } else if let Some(write_lock) = session_write_lock {
            self.commit_session_write_lock(session_id, write_lock);
        }
        result
    }

    async fn restore_session_with_turns_from_claimed_storage_path_internal(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        include_internal: bool,
    ) -> OpenBitFunResult<(Session, Vec<DialogTurnData>)> {
        let restore_started_at = Instant::now();
        // Check if session is already in memory
        let session_already_in_memory = self.sessions.contains_key(session_id);
        let active_session_permit = if session_already_in_memory {
            None
        } else {
            Some(self.reserve_active_session()?)
        };

        debug!(
            "Session restore phase completed: session_id={}, phase=use_storage_path, duration_ms=0",
            session_id
        );

        let metadata_started_at = Instant::now();
        let session_metadata = self
            .persistence_manager
            .load_session_metadata(session_storage_path, session_id)
            .await?;
        if session_metadata
            .as_ref()
            .is_some_and(|metadata| !include_internal && metadata.should_hide_from_user_lists())
        {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        }
        let listing_baseline_rebuild_turn_index =
            Self::listing_baseline_rebuild_turn_index_from_metadata(session_metadata.as_ref());
        let restored_edit_constraint_state =
            Self::edit_constraint_state_from_metadata(session_metadata.as_ref());
        debug!(
            "Session restore phase completed: session_id={}, phase=load_metadata, duration_ms={}",
            session_id,
            elapsed_ms_u64(metadata_started_at)
        );

        // 1. Load session and turns from storage in one pass
        let session_started_at = Instant::now();
        let (mut session, mut persisted_turns) = self
            .persistence_manager
            .load_session_with_turns(session_storage_path, session_id)
            .await?;
        let staged_revert = self
            .persistence_manager
            .load_session_revert_state(session_storage_path, session_id)
            .await?;
        if let Some(revert) = staged_revert.as_ref() {
            persisted_turns.retain(|turn| turn.turn_index < revert.boundary_turn);
        }
        let surviving_turn_ids: HashSet<String> = persisted_turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect();
        let all_evidence_events = self
            .persistence_manager
            .load_evidence_ledger_events(session_storage_path, session_id)
            .await?;
        let restored_evidence_events = all_evidence_events
            .iter()
            .filter(|event| surviving_turn_ids.contains(&event.turn_id))
            .cloned()
            .collect::<Vec<_>>();
        // Converge the sidecar to surviving turns when there is no staged undo
        // marker. A staged undo keeps the full sidecar on disk so a later redo
        // can restore hidden evidence. Without this convergence, a stale event
        // left by an older build (or a previous rollback) would re-enter memory
        // on the next evidence append.
        if staged_revert.is_none() && self.config.enable_persistence {
            let should_converge = self
                .sessions
                .get(session_id)
                .is_some_and(|session| self.should_persist_session(&session))
                || !session_already_in_memory;
            if should_converge {
                let converged_events = restored_evidence_events.clone();
                self.persistence_manager
                    .save_evidence_ledger_events(session_storage_path, session_id, converged_events)
                    .await?;
            }
        }
        debug!(
            "Session restore phase completed: session_id={}, phase=load_session_with_turns, turn_count={}, duration_ms={}",
            session_id,
            persisted_turns.len(),
            elapsed_ms_u64(session_started_at)
        );

        let ai_config_for_restore = Self::load_ai_config_for_session(&session.config).await?;
        let mut should_persist_restored_session = false;
        let mut fallback_previous_model_id = None;
        let mut auto_cleared_reasoning_preset = None;
        // An external agent (`provider: "acp"`) owns this conversation. Its
        // `acp:<client>` mode is not a local agent and must never be replaced
        // by a local fallback mode or model.
        let externally_driven = session_metadata
            .as_ref()
            .is_some_and(SessionMetadata::is_externally_driven);

        if !include_internal && !externally_driven {
            let external_sources_supported =
                cfg!(feature = "external-sources") && !session.config.is_remote_workspace();
            #[cfg(feature = "external-sources")]
            if external_sources_supported {
                if let Err(error) =
                    crate::external_sources::ensure_external_source_workspace_snapshot(
                        session.config.workspace_id.as_deref(),
                    )
                    .await
                {
                    warn!(
                        "External agent source discovery failed during session restore: session_id={}, error_category={}",
                        session.session_id,
                        crate::external_sources::external_integration_error_code(&error),
                    );
                }
            }
            let agent_registry = get_agent_registry();
            agent_registry
                .load_custom_agents(session.config.workspace_id.as_deref())
                .await;
            let available_modes = agent_registry
                .get_modes_info_for_workspace(
                    session.config.workspace_id.as_deref(),
                    external_sources_supported,
                )
                .await;
            let persisted_binding = agent_registry.resolve_primary_agent_for_turn_with_route(
                &session.agent_type,
                session.config.workspace_id.as_deref(),
                external_sources_supported,
                Some(session.config.agent_route_owner),
                session.config.agent_route_key.as_deref(),
            );
            if let Some(binding) = persisted_binding {
                // A missing local route key is a valid legacy binding. Local
                // resolution is already constrained by the persisted owner, so
                // avoid rewriting the runtime state solely to backfill it.
                // External bindings still persist their exact provider route
                // key to prevent a same-name provider from taking over.
                let external_route_key_changed = session.config.agent_route_owner
                    == SessionAgentRouteOwner::External
                    && session.config.agent_route_key != binding.route_key;
                if session.config.agent_route_owner != binding.route_owner
                    || external_route_key_changed
                {
                    session.config.agent_route_owner = binding.route_owner;
                    session.config.agent_route_key = binding.route_key;
                    should_persist_restored_session = true;
                }
            } else if session.config.agent_route_owner == SessionAgentRouteOwner::External {
                warn!(
                    "Persisted external main agent is currently unavailable; preserving fail-closed session binding: session_id={}, persisted_mode={}",
                    session.session_id, session.agent_type
                );
            } else {
                let fallback_mode = available_modes
                    .iter()
                    .find(|mode| mode.id == "Standard")
                    .or_else(|| available_modes.first())
                    .map(|mode| mode.id.clone())
                    .ok_or_else(|| {
                        OpenBitFunError::Validation(
                            "No executable main agent mode is available for session restore"
                                .to_string(),
                        )
                    })?;
                warn!(
                    "Persisted session mode is unavailable; applying executable fallback: session_id={}, persisted_mode={}, fallback_mode={}",
                    session.session_id, session.agent_type, fallback_mode
                );
                session.agent_type = fallback_mode;
                session.config.agent_route_owner = SessionAgentRouteOwner::Local;
                session.config.agent_route_key = None;
                should_persist_restored_session = true;
            }
        }

        // Restore fallback: if the persisted model_id is no longer usable
        // (model deleted, disabled, or from a retired selector), repoint it to
        // the primary selector before the session re-enters memory.
        if let Some(persisted_model_id) = session
            .config
            .model_id
            .as_deref()
            .filter(|_| !externally_driven)
        {
            let trimmed = persisted_model_id.trim();
            let needs_fallback = if trimmed.is_empty()
                || !session_model_allows_fallback(session.config.model_binding_policy)
                || (!crate::agentic::execution::round_model_routing::configuration::available()
                    && session.config.model_routing.as_deref() == Some("smart"))
            {
                false
            } else if let Some(ai_config) = ai_config_for_restore.as_ref() {
                !Self::is_session_model_id_usable(ai_config, trimmed)
            } else {
                false
            };

            if needs_fallback {
                warn!(
                    "Session restore detected stale model_id; falling back to primary: session_id={}, previous_model_id={}",
                    session_id, trimmed
                );
                let previous_model_id = trimmed.to_string();
                session.config.model_id = Some("primary".to_string());
                should_persist_restored_session = true;
                fallback_previous_model_id = Some(previous_model_id);
            }
        }

        if let Some(ai_config) = ai_config_for_restore
            .as_ref()
            .filter(|_| !externally_driven)
        {
            let previous_reasoning_preset = session.config.reasoning_preset.clone();
            session.config.reasoning_preset =
                Self::normalize_session_reasoning_preset(&session, ai_config).await;
            if previous_reasoning_preset.is_some() && session.config.reasoning_preset.is_none() {
                auto_cleared_reasoning_preset = previous_reasoning_preset.clone();
                warn!(
                    "Session restore detected stale reasoning preset; normalizing to Auto: session_id={}, preset_id={}",
                    session_id,
                    previous_reasoning_preset.as_deref().unwrap_or_default()
                );
                should_persist_restored_session = true;
            }

            let previous_max_context_tokens = session.config.max_context_tokens;
            if let Some(context_window) =
                Self::sync_session_context_window_from_ai_config(&mut session, ai_config)
            {
                if context_window != previous_max_context_tokens {
                    should_persist_restored_session = true;
                    debug!(
                        "Session context window refreshed during restore: session_id={}, previous={}, resolved={}",
                        session_id, previous_max_context_tokens, context_window
                    );
                }
            }
        }

        // Reset session state to Idle
        // After application restart, previous Processing state is invalid and must be reset
        let previous_state_was_not_idle = !matches!(session.state, SessionState::Idle);
        let mut interrupted_recovery_restored_turn = None;
        if previous_state_was_not_idle {
            let old_state = session.state.clone();
            session.state = SessionState::Idle;
            should_persist_restored_session = true;
            debug!(
                "Resetting session state during restore: session_id={}, state={:?} -> Idle",
                session_id, old_state
            );
        }

        // A process exit can happen after the recovering Turn write but before
        // the Processing Session write, or after both. No execution survives
        // restart, so normalize from the durable Turn fact independently of
        // the stale Session state.
        if let Some(turn) = persisted_turns.last_mut().filter(|turn| {
            turn.status == TurnStatus::InProgress
                && turn
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.status == DialogTurnRecoveryStatus::Recovering)
        }) {
            let now = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            turn.status = TurnStatus::Cancelled;
            turn.finish_reason = Some("interrupted".to_string());
            turn.end_time = Some(now);
            turn.duration_ms = Some(now.saturating_sub(turn.start_time));
            if let Some(recovery) = turn.recovery.as_mut() {
                recovery.status = DialogTurnRecoveryStatus::Interrupted;
                recovery.interrupted_at = Some(now);
                turn.recovery_epoch = Some(recovery.execution_generation);
            }
            interrupted_recovery_restored_turn = Some(turn.clone());
            should_persist_restored_session = true;
            info!(
                "Restored abandoned recovery generation as interrupted: session_id={}, turn_id={}",
                session_id, turn.turn_id
            );
        }

        // 2. Restore runtime context with snapshot-first semantics.
        // If the latest snapshot lags behind turn persistence, append the missing turn delta
        // instead of truncating session history.
        //
        // This compensates for the fact that persistence is not transactional across
        // `session.json`, `turns/*.json`, and `snapshots/context-*.json`.
        let persisted_turn_ids: Vec<String> = persisted_turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect();
        session.last_user_dialog_agent_type = Self::derive_last_user_dialog_agent_type_from_turns(
            &persisted_turns,
            Some(session.agent_type.as_str()),
        );
        let mut latest_turn_index: Option<usize> = None;
        let context_snapshot_started_at = Instant::now();
        let latest_context_snapshot = match staged_revert.as_ref() {
            Some(revert) => {
                self.persistence_manager
                    .load_latest_turn_context_snapshot_before(
                        session_storage_path,
                        session_id,
                        revert.boundary_turn,
                    )
                    .await?
            }
            None => {
                self.persistence_manager
                    .load_latest_turn_context_snapshot(session_storage_path, session_id)
                    .await?
            }
        };
        let mut messages = match latest_context_snapshot {
            Some((turn_index, msgs)) => {
                latest_turn_index = Some(turn_index);
                self.sanitize_listing_diff_context_snapshot_if_needed(
                    session_storage_path,
                    session_id,
                    turn_index,
                    msgs,
                    listing_baseline_rebuild_turn_index,
                    "restore_pre_listing_baseline_rebuild_snapshot",
                )
                .await
            }
            None => Self::build_messages_from_turns(&persisted_turns),
        };
        debug!(
            "Session restore phase completed: session_id={}, phase=load_context_snapshot, snapshot_turn_index={:?}, message_count={}, duration_ms={}",
            session_id,
            latest_turn_index,
            messages.len(),
            elapsed_ms_u64(context_snapshot_started_at)
        );

        if let Some(snapshot_turn_index) = latest_turn_index {
            let delta_turns = persisted_turns
                .iter()
                .filter(|turn| turn.turn_index > snapshot_turn_index)
                .cloned()
                .collect::<Vec<_>>();
            if !delta_turns.is_empty() {
                warn!(
                    "Context snapshot is behind persisted turns, rebuilding delta: session_id={}, snapshot_turn_index={}, persisted_turn_count={}",
                    session_id,
                    snapshot_turn_index,
                    persisted_turns.len()
                );
                messages.extend(Self::build_messages_from_turns(&delta_turns));
            }
        };

        // A process can exit after the assistant tool-call message is persisted
        // and before the tool result is appended. Providers reject that history
        // shape, so close the final active round during ordinary restore. The
        // repaired snapshot becomes the durable recovery boundary for the next
        // process as well; no interrupted tool is re-executed here.
        let repaired_incomplete_tool_calls = normalize_incomplete_tool_calls(
            &mut messages,
            "Tool execution was still in progress when the application closed; the result is unknown. Verify any external side effects before continuing.",
        );
        if repaired_incomplete_tool_calls {
            let repaired_snapshot_turn_index = latest_turn_index
                .into_iter()
                .chain(persisted_turns.last().map(|turn| turn.turn_index))
                .max();
            if let Some(turn_index) = repaired_snapshot_turn_index {
                self.persistence_manager
                    .save_turn_context_snapshot(
                        session_storage_path,
                        session_id,
                        turn_index,
                        &messages,
                    )
                    .await?;
                info!(
                    "Normalized incomplete tool calls during session restore: session_id={}, turn_index={}",
                    session_id, turn_index
                );
            }
        }

        if messages.is_empty() {
            debug!(
                "Session {} has empty persisted messages (may be new session)",
                session_id
            );
        }

        let recoverable_turn_count = if staged_revert.is_some() {
            persisted_turns.len()
        } else {
            latest_turn_index
                .map(|turn_index| turn_index + 1)
                .unwrap_or(0)
                .max(persisted_turns.len())
        };

        if session.dialog_turn_ids.len() < persisted_turns.len() {
            warn!(
                "Session metadata is behind persisted turns, rebuilding dialog_turn_ids: session_id={}, session_turn_count={}, persisted_turn_count={}",
                session_id,
                session.dialog_turn_ids.len(),
                persisted_turns.len()
            );
            session.dialog_turn_ids = persisted_turn_ids;
            should_persist_restored_session = true;
        } else if session.dialog_turn_ids.len() > recoverable_turn_count {
            warn!(
                "Session metadata exceeds recoverable history, truncating: session_id={}, session_turn_count={}, recoverable_turn_count={}",
                session_id,
                session.dialog_turn_ids.len(),
                recoverable_turn_count
            );
            session.dialog_turn_ids.truncate(recoverable_turn_count);
            should_persist_restored_session = true;
        } else if persisted_turns.len() == session.dialog_turn_ids.len()
            && session.dialog_turn_ids != persisted_turn_ids
        {
            warn!(
                "Session metadata turn ids diverge from persisted turns, normalizing order: session_id={}",
                session_id
            );
            session.dialog_turn_ids = persisted_turn_ids;
            should_persist_restored_session = true;
        }

        if recoverable_turn_count == 0 && !session.dialog_turn_ids.is_empty() && messages.is_empty()
        {
            warn!(
                "Session has no available context snapshot and messages are empty, clearing turns: session_id={}",
                session_id
            );
            session.dialog_turn_ids.clear();
            should_persist_restored_session = true;
        }

        // Complete all fallible restore migrations before publishing any runtime state.
        // A failed write keeps the session unloaded; restore-time recovery handles any
        // partial metadata/state update left by the existing multi-file persistence format.
        if let Some(turn) = interrupted_recovery_restored_turn.as_ref() {
            self.persistence_manager
                .save_dialog_turn(session_storage_path, turn)
                .await?;
        }
        if should_persist_restored_session && self.should_persist_session_id(session_id) {
            self.persistence_manager
                .save_session(session_storage_path, &session)
                .await?;
        }

        // Finish async notifications before publishing runtime state. If restore is
        // cancelled or times out before publication, the temporary write lock drops
        // together with this future and no writable in-memory Session remains.
        if let Some(previous_model_id) = fallback_previous_model_id {
            if let Some(coordinator) = crate::agentic::coordination::get_global_coordinator() {
                coordinator
                    .emit_session_model_fallback_applied(
                        session_id,
                        &previous_model_id,
                        "primary",
                        "model_unavailable_on_restore",
                    )
                    .await;
            }
        }
        if let Some(previous_preset_id) = auto_cleared_reasoning_preset {
            if let Some(coordinator) = crate::agentic::coordination::get_global_coordinator() {
                coordinator
                    .emit_session_reasoning_preset_auto_cleared(
                        session_id,
                        &previous_preset_id,
                        "preset_unavailable_on_restore",
                    )
                    .await;
            }
        }

        // 3. Publish the recovered runtime context only after migrations are durable.
        if session_already_in_memory {
            clear_session_runtime_stores(
                session_id,
                self.context_store.as_ref(),
                self.transient_turns.as_ref(),
                self.prompt_cache_store.as_ref(),
                self.token_anchor_store.as_ref(),
                self.turn_skill_agent_snapshot_store.as_ref(),
                self.skill_agent_baseline_override_snapshot_store.as_ref(),
                self.review_read_receipt_store.as_ref(),
                self.evidence_ledger.as_ref(),
            );
        }
        self.evidence_ledger
            .replace_session(session_id, restored_evidence_events)
            .map_err(|error| OpenBitFunError::parse(error.to_string()))?;

        let context_replace_started_at = Instant::now();
        self.context_store
            .replace_context(session_id, messages.clone());
        debug!(
            "Session restore phase completed: session_id={}, phase=replace_context, message_count={}, duration_ms={}",
            session_id,
            messages.len(),
            elapsed_ms_u64(context_replace_started_at)
        );
        let context_msg_count = self.context_store.get_context_messages(session_id).len();

        debug!(
            "Session restored: session_id={}, session_name={}, messages={}, context_messages={}, turn_count={}, total_duration_ms={}",
            session_id,
            session.session_name,
            messages.len(),
            context_msg_count,
            persisted_turns.len(),
            elapsed_ms_u64(restore_started_at)
        );

        // Do not infer unread completion from persisted runtime state during restore.
        // Older IDE versions could leave sessions in non-idle states on disk; treating those
        // as completed would surface misleading unread indicators after an upgrade.
        // Unread completion is now written only by runtime completion/persist paths.

        // 4. Add to memory (will overwrite if already exists)
        self.sessions
            .insert(session_id.to_string(), session.clone());
        if let Some(permit) = active_session_permit {
            self.commit_active_session_reservation(session_id, permit);
        }
        self.bind_session_storage_path_committed(session_id, session_storage_path.to_path_buf());

        if let Some(state) = restored_edit_constraint_state {
            self.edit_constraints_store
                .insert(session_id.to_string(), state);
        }

        Ok((session, persisted_turns))
    }

    /// Rollback "model context" to before the start of specified turn (i.e., keep 0..target_turn-1)
    pub async fn rollback_context_to_turn_start(
        &self,
        workspace_path: &Path,
        session_id: &str,
        target_turn: usize,
    ) -> OpenBitFunResult<()> {
        let session_storage_path = self
            .resolve_storage_path_for_restore_workspace_path(workspace_path)
            .await?;
        if !self.sessions.contains_key(session_id) && self.config.enable_persistence {
            self.restore_session_from_storage_path(&session_storage_path, session_id)
                .await?;
        }
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        self.validate_session_storage_path_binding(session_id, &session_storage_path)?;
        self.rollback_context_to_turn_start_locked(&session_storage_path, session_id, target_turn)
            .await
    }

    /// Move the loaded Session to a persisted staged-revert boundary without
    /// deleting any turn, context snapshot, compression artifact, or evidence
    /// sidecar. The durable `session-revert.json` remains the authoritative
    /// visibility fact, and the complete sidecar lets a later redo restore the
    /// hidden evidence.
    pub(crate) async fn apply_staged_revert_context_locked(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        boundary_turn: usize,
    ) -> OpenBitFunResult<()> {
        let turns = self
            .persistence_manager
            .load_session_turns(session_storage_path, session_id)
            .await?;
        let visible_turns = turns
            .iter()
            .filter(|turn| turn.turn_index < boundary_turn)
            .cloned()
            .collect::<Vec<_>>();
        let listing_baseline_rebuild_turn_index = self
            .persistence_manager
            .load_session_metadata(session_storage_path, session_id)
            .await?
            .as_ref()
            .and_then(|metadata| {
                Self::listing_baseline_rebuild_turn_index_from_metadata(Some(metadata))
            });

        let messages = if boundary_turn == 0 {
            Vec::new()
        } else if let Some((snapshot_turn_index, snapshot_messages)) = self
            .persistence_manager
            .load_latest_turn_context_snapshot_before(
                session_storage_path,
                session_id,
                boundary_turn,
            )
            .await?
        {
            let mut messages = self
                .sanitize_listing_diff_context_snapshot_if_needed(
                    session_storage_path,
                    session_id,
                    snapshot_turn_index,
                    snapshot_messages,
                    listing_baseline_rebuild_turn_index,
                    "staged_revert_context_snapshot",
                )
                .await;
            let delta = visible_turns
                .iter()
                .filter(|turn| turn.turn_index > snapshot_turn_index)
                .cloned()
                .collect::<Vec<_>>();
            messages.extend(Self::build_messages_from_turns(&delta));
            messages
        } else {
            Self::build_messages_from_turns(&visible_turns)
        };

        self.context_store.replace_context(session_id, messages);
        self.review_read_receipt_store.clear_session(session_id);
        let fallback_agent_type = self
            .sessions
            .get(session_id)
            .map(|session| session.agent_type.clone());
        let last_user_dialog_agent_type = Self::derive_last_user_dialog_agent_type_from_turns(
            &visible_turns,
            fallback_agent_type.as_deref(),
        );
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.dialog_turn_ids = visible_turns
                .iter()
                .map(|turn| turn.turn_id.clone())
                .collect();
            session.last_user_dialog_agent_type = last_user_dialog_agent_type;
            session.state = SessionState::Idle;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        }
        let surviving_turn_ids = visible_turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect::<HashSet<_>>();
        self.retain_evidence_events_locked(
            Some(session_storage_path),
            session_id,
            &surviving_turn_ids,
            false,
        )
        .await?;
        Ok(())
    }

    /// Permanently discard the hidden suffix after a staged revert. Callers
    /// persist the `Committing` phase before entering this idempotent cleanup.
    pub(crate) async fn commit_staged_revert_context_locked(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        boundary_turn: usize,
    ) -> OpenBitFunResult<()> {
        self.apply_staged_revert_context_locked(session_storage_path, session_id, boundary_turn)
            .await?;

        let session_snapshot = self.sessions.get(session_id).and_then(|session| {
            (self.should_persist_session(&session) && self.config.enable_persistence)
                .then(|| session.clone())
        });
        if let Some(session) = session_snapshot {
            self.persistence_manager
                .save_session(session_storage_path, &session)
                .await?;
        }
        // Committing the marker permanently discards the hidden suffix, so the
        // evidence sidecar must be pruned to the surviving turns as well.
        self.persistence_manager
            .delete_dialog_turns_from(session_storage_path, session_id, boundary_turn)
            .await?;
        self.persistence_manager
            .delete_turn_context_snapshots_from(session_storage_path, session_id, boundary_turn)
            .await?;
        self.persistence_manager
            .delete_compression_transcripts_from(session_storage_path, session_id, boundary_turn)
            .await?;
        self.truncate_listing_baseline_rebuild_turn_index_after_rollback(
            session_storage_path,
            session_id,
            boundary_turn,
        )
        .await?;
        self.turn_skill_agent_snapshot_store
            .remove_from(session_id, boundary_turn);
        let surviving_turn_ids = self
            .sessions
            .get(session_id)
            .map(|session| {
                session
                    .dialog_turn_ids
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        self.rollback_edit_constraint_state_to_turns(session_id, &surviving_turn_ids)
            .await;
        self.retain_evidence_events_locked(
            Some(session_storage_path),
            session_id,
            &surviving_turn_ids,
            true,
        )
        .await?;
        let messages = self.context_store.get_context_messages(session_id);
        self.prune_token_anchors_to_messages(session_id, &messages)
            .await;
        Ok(())
    }

    pub(crate) async fn rollback_context_to_turn_start_locked(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        target_turn: usize,
    ) -> OpenBitFunResult<()> {
        let workspace_path = session_storage_path;

        self.validate_rollback_context_to_turn_start_locked(
            session_storage_path,
            session_id,
            target_turn,
        )
        .await?;
        let surviving_turns = if target_turn == 0 {
            Vec::new()
        } else {
            self.persistence_manager
                .load_session_turns(workspace_path, session_id)
                .await?
        };

        // Rollback may load a historical snapshot from before the latest rebuilt baseline. In
        // that case we must strip all listing diff reminders before the snapshot re-enters
        // runtime context, otherwise old diffs reappear after rollback/reopen.
        let listing_baseline_rebuild_turn_index = if self.config.enable_persistence {
            let metadata = self
                .persistence_manager
                .load_session_metadata(workspace_path, session_id)
                .await?;
            Self::listing_baseline_rebuild_turn_index_from_metadata(metadata.as_ref())
        } else {
            None
        };

        // 1) Load target context (target_turn == 0 => empty context)
        let messages = if target_turn == 0 {
            Vec::new()
        } else {
            let messages = self
                .persistence_manager
                .load_turn_context_snapshot(workspace_path, session_id, target_turn - 1)
                .await?
                .ok_or_else(|| {
                    OpenBitFunError::NotFound(format!(
                        "turn context snapshot not found: session_id={} turn={}",
                        session_id,
                        target_turn - 1
                    ))
                })?;
            self.sanitize_listing_diff_context_snapshot_if_needed(
                workspace_path,
                session_id,
                target_turn - 1,
                messages,
                listing_baseline_rebuild_turn_index,
                "rollback_restore_pre_listing_baseline_rebuild_snapshot",
            )
            .await
        };

        // 2) Restore the in-memory context cache.
        self.context_store
            .replace_context(session_id, messages.clone());
        self.review_read_receipt_store.clear_session(session_id);
        self.prune_token_anchors_to_messages(session_id, &messages)
            .await;

        let (last_user_dialog_agent_type, surviving_dialog_turn_ids) = if target_turn == 0 {
            (None, std::collections::HashSet::new())
        } else {
            let kept_turns = surviving_turns
                .into_iter()
                .take(target_turn)
                .collect::<Vec<_>>();
            let fallback_agent_type = self
                .sessions
                .get(session_id)
                .map(|session| session.agent_type.clone());
            let last_agent_type = Self::derive_last_user_dialog_agent_type_from_turns(
                &kept_turns,
                fallback_agent_type.as_deref(),
            );
            let turn_ids = kept_turns
                .iter()
                .map(|turn| turn.turn_id.clone())
                .collect::<std::collections::HashSet<_>>();
            (last_agent_type, turn_ids)
        };

        // 3) Truncate session turn list & persist
        // IMPORTANT: keep the DashMap guard scope short -- do NOT hold it across .await.
        let session_snapshot = if let Some(mut session) = self.sessions.get_mut(session_id) {
            if session.dialog_turn_ids.len() > target_turn {
                session.dialog_turn_ids.truncate(target_turn);
            }
            session.last_user_dialog_agent_type = last_user_dialog_agent_type;
            session.state = SessionState::Idle;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();

            let should_persist =
                self.should_persist_session(&session) && self.config.enable_persistence;
            if should_persist {
                Some(session.clone())
            } else {
                None
            }
        } else {
            None
        };
        // RefMut guard released here -- DashMap shard lock is free.
        self.clear_turn_settlement_results(session_id);

        if let Some(session) = session_snapshot {
            self.persistence_manager
                .save_session(workspace_path, &session)
                .await?;
        }

        // 4) Delete persisted turns and snapshots from target_turn (inclusive) onwards.
        // Runtime restore rebuilds history from persisted turn files, so removing only
        // context snapshots would make rolled-back prompts reappear after reload.
        if self.config.enable_persistence {
            self.persistence_manager
                .delete_dialog_turns_from(workspace_path, session_id, target_turn)
                .await?;
            self.persistence_manager
                .delete_turn_context_snapshots_from(workspace_path, session_id, target_turn)
                .await?;
            self.persistence_manager
                .delete_compression_transcripts_from(workspace_path, session_id, target_turn)
                .await?;
            self.truncate_listing_baseline_rebuild_turn_index_after_rollback(
                workspace_path,
                session_id,
                target_turn,
            )
            .await?;
        }
        self.turn_skill_agent_snapshot_store
            .remove_from(session_id, target_turn);
        self.rollback_edit_constraint_state_to_turns(session_id, &surviving_dialog_turn_ids)
            .await;
        self.retain_evidence_events_locked(
            Some(workspace_path),
            session_id,
            &surviving_dialog_turn_ids,
            true,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn validate_rollback_context_to_turn_start_locked(
        &self,
        session_storage_path: &Path,
        session_id: &str,
        target_turn: usize,
    ) -> OpenBitFunResult<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }
        if self
            .persistence_manager
            .load_session_revert_state(session_storage_path, session_id)
            .await?
            .is_some()
        {
            return Err(OpenBitFunError::OutcomeUnknown(format!(
                "Legacy context rollback cannot overlap a staged Session undo: session_id={session_id}"
            )));
        }
        self.persistence_manager
            .load_session_metadata(session_storage_path, session_id)
            .await?;
        self.persistence_manager
            .load_session_turns(session_storage_path, session_id)
            .await?;
        if target_turn > 0
            && self
                .persistence_manager
                .load_turn_context_snapshot(session_storage_path, session_id, target_turn - 1)
                .await?
                .is_none()
        {
            return Err(OpenBitFunError::NotFound(format!(
                "turn context snapshot not found: session_id={} turn={}",
                session_id,
                target_turn - 1
            )));
        }
        Ok(())
    }

    /// List all sessions
    pub async fn list_sessions(
        &self,
        workspace_path: &Path,
    ) -> OpenBitFunResult<Vec<SessionSummary>> {
        if self.config.enable_persistence {
            self.persistence_manager.list_sessions(workspace_path).await
        } else {
            let summaries: Vec<_> = self
                .sessions
                .iter()
                .map(|entry| {
                    let session = entry.value();
                    SessionSummary {
                        session_id: session.session_id.clone(),
                        session_name: session.session_name.clone(),
                        agent_type: session.agent_type.clone(),
                        model_id: session.config.model_id.clone(),
                        reasoning_preset: session.config.reasoning_preset.clone(),
                        last_user_dialog_agent_type: session.last_user_dialog_agent_type.clone(),
                        last_submitted_agent_type: session.last_submitted_agent_type.clone(),
                        created_by: session.created_by.clone(),
                        kind: session.kind,
                        turn_count: session.dialog_turn_ids.len(),
                        created_at: session.created_at,
                        last_activity_at: session.last_activity_at,
                        state: session.state.clone(),
                    }
                })
                .filter(|summary| {
                    !matches!(
                        summary.kind,
                        SessionKind::Subagent | SessionKind::EphemeralChild
                    )
                })
                .collect();
            Ok(summaries)
        }
    }

    pub async fn load_session_metadata(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Option<SessionMetadata>> {
        self.persistence_manager
            .load_session_metadata(workspace_path, session_id)
            .await
    }

    /// Read the compact latest user-turn outcome without restoring a Session or
    /// reading its transcript. Legacy metadata may have no summary yet.
    pub(crate) async fn latest_turn_for_agent_listing(
        &self,
        session_id: &str,
        parent_storage_path: Option<&Path>,
    ) -> OpenBitFunResult<Option<(String, TurnStatus)>> {
        if !self.should_persist_session_id(session_id) {
            return Ok(self.transient_turns.get(session_id).and_then(|history| {
                history
                    .turns
                    .iter()
                    .rev()
                    .find(|turn| turn.kind == DialogTurnKind::UserDialog)
                    .map(|turn| (turn.turn_id.clone(), turn.status.clone()))
            }));
        }
        let storage_path = match self.effective_session_storage_path(session_id).await {
            Some(path) => path,
            None => parent_storage_path.map(Path::to_path_buf).ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Agent session storage not found: {session_id}"))
            })?,
        };
        Ok(self
            .load_session_metadata(&storage_path, session_id)
            .await?
            .and_then(|metadata| metadata.last_turn)
            .map(|turn| (turn.turn_id, turn.status)))
    }

    pub async fn update_session_metadata(
        &self,
        workspace_path: &Path,
        session_id: &str,
        update: impl FnOnce(&mut SessionMetadata),
    ) -> OpenBitFunResult<()> {
        self.persistence_manager
            .update_session_metadata(workspace_path, session_id, update)
            .await
    }

    #[cfg(test)]
    pub async fn save_session_metadata(
        &self,
        workspace_path: &Path,
        metadata: &SessionMetadata,
    ) -> OpenBitFunResult<()> {
        self.persistence_manager
            .save_session_metadata(workspace_path, metadata)
            .await
    }

    pub async fn set_session_memory_mode(
        &self,
        workspace_path: &Path,
        session_id: &str,
        mode: SessionMemoryMode,
    ) -> OpenBitFunResult<()> {
        self.update_session_metadata_at_workspace(workspace_path, session_id, |metadata| {
            metadata.memory_mode = mode;
        })
        .await
    }

    pub async fn set_persisted_session_memory_mode(
        &self,
        session_id: &str,
        mode: SessionMemoryMode,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            metadata.memory_mode = mode;
        })
        .await
    }

    pub async fn mark_session_memory_mode_polluted(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        let mut should_enqueue_phase2 = false;
        self.update_session_metadata_at_workspace(workspace_path, session_id, |metadata| {
            should_enqueue_phase2 = matches!(
                metadata.memory_mode,
                SessionMemoryMode::Enabled | SessionMemoryMode::Polluted
            );
            if metadata.memory_mode == SessionMemoryMode::Enabled {
                metadata.memory_mode = SessionMemoryMode::Polluted;
            }
        })
        .await?;
        if should_enqueue_phase2 {
            self.enqueue_phase2_if_session_selected(session_id).await?;
        }
        Ok(())
    }

    async fn enqueue_phase2_if_session_selected(&self, session_id: &str) -> OpenBitFunResult<()> {
        if self
            .memory_database
            .phase2_selected_for_session(session_id)
            .await?
        {
            self.memory_database
                .enqueue_phase2_job(MEMORY_PHASE2_GLOBAL_JOB_KEY, current_unix_secs())
                .await?;
        }
        Ok(())
    }

    async fn metadata_workspace_path_for_update(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<PathBuf> {
        if !self.should_persist_session_id(session_id) {
            return Err(OpenBitFunError::Validation(format!(
                "Session persistence is disabled: {}",
                session_id
            )));
        }

        self.effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })
    }

    async fn ensure_session_metadata_persisted(
        &self,
        workspace_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        if self
            .persistence_manager
            .load_session_metadata(workspace_path, session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }

        let session = self
            .sessions
            .get(session_id)
            .map(|value| value.clone())
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {}", session_id))
            })?;
        self.persistence_manager
            .save_session(workspace_path, &session)
            .await
    }

    async fn update_session_metadata_at_workspace(
        &self,
        workspace_path: &Path,
        session_id: &str,
        update: impl FnOnce(&mut SessionMetadata),
    ) -> OpenBitFunResult<()> {
        self.ensure_session_metadata_persisted(workspace_path, session_id)
            .await?;
        self.persistence_manager
            .update_session_metadata(workspace_path, session_id, update)
            .await
    }

    async fn update_persisted_session_metadata(
        &self,
        session_id: &str,
        update: impl FnOnce(&mut SessionMetadata),
    ) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(session_id) {
            return Ok(());
        }

        let workspace_path = self.metadata_workspace_path_for_update(session_id).await?;
        self.update_session_metadata_at_workspace(&workspace_path, session_id, update)
            .await
    }

    pub async fn merge_session_custom_metadata(
        &self,
        session_id: &str,
        patch: serde_json::Value,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            merge_session_custom_metadata_value(metadata, patch)
        })
        .await
    }

    pub(crate) async fn persist_current_context_usage(
        &self,
        session_id: &str,
        usage: SessionContextUsage,
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let should_persist_usage = self.sessions.get(session_id).is_some_and(|session| {
            !session.agent_type.starts_with("acp:")
                && session
                    .dialog_turn_ids
                    .iter()
                    .any(|turn_id| turn_id == &usage.turn_id)
                && self.should_persist_session(&session)
        });
        if !should_persist_usage || !self.config.enable_persistence {
            return Ok(());
        }

        self.update_persisted_session_metadata(session_id, |metadata| {
            metadata.current_context_usage = Some(usage);
        })
        .await
    }

    pub async fn merge_session_relationship(
        &self,
        session_id: &str,
        relationship: SessionRelationship,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            set_session_relationship(metadata, relationship)
        })
        .await
    }

    pub async fn persist_session_lineage(
        &self,
        session_id: &str,
        relationship: SessionRelationship,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            apply_session_lineage(metadata, relationship)
        })
        .await
    }

    pub async fn collect_hidden_subagent_cascade_for_parent_turns(
        &self,
        workspace_path: &Path,
        parent_session_id: &str,
        parent_dialog_turn_ids: &HashSet<String>,
    ) -> OpenBitFunResult<Vec<String>> {
        if parent_session_id.trim().is_empty() || parent_dialog_turn_ids.is_empty() {
            return Ok(Vec::new());
        }

        let metadata_list = self
            .persistence_manager
            .list_session_metadata_including_internal(workspace_path)
            .await?;
        Ok(collect_hidden_subagent_cascade_ids(
            metadata_list,
            parent_session_id,
            parent_dialog_turn_ids,
        ))
    }

    pub async fn set_session_deep_review_run_manifest(
        &self,
        session_id: &str,
        deep_review_run_manifest: Option<serde_json::Value>,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            set_deep_review_run_manifest(metadata, deep_review_run_manifest)
        })
        .await
    }

    pub async fn set_session_review_target_evidence(
        &self,
        session_id: &str,
        review_target_evidence: Option<serde_json::Value>,
    ) -> OpenBitFunResult<()> {
        self.update_persisted_session_metadata(session_id, |metadata| {
            set_review_target_evidence(metadata, review_target_evidence)
        })
        .await
    }

    // ============ Dialog Turn Management ============

    async fn ensure_persisted_turn_append_allowed(&self, session_id: &str) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(session_id) {
            return Ok(());
        }
        let Some(storage_path) = self.effective_session_storage_path(session_id).await else {
            return Ok(());
        };
        if let Some(revert) = self
            .persistence_manager
            .load_session_revert_state(&storage_path, session_id)
            .await?
        {
            return Err(OpenBitFunError::Validation(format!(
                "Cannot append a persisted Turn while a Session revert is {:?}: session_id={}, boundary_turn={}",
                revert.phase, session_id, revert.boundary_turn
            )));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_persisted_turn(
        &self,
        session_id: &str,
        kind: DialogTurnKind,
        agent_type: Option<String>,
        user_input: String,
        turn_id: Option<String>,
        context_messages: Vec<Message>,
        processing_phase: ProcessingPhase,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        self.start_persisted_turn_locked(
            session_id,
            kind,
            agent_type,
            user_input,
            turn_id,
            context_messages,
            processing_phase,
            user_message_metadata,
        )
        .await
    }

    /// Start a persisted Turn while the caller owns the Session mutation lock.
    #[allow(clippy::too_many_arguments)]
    async fn start_persisted_turn_locked(
        &self,
        session_id: &str,
        kind: DialogTurnKind,
        agent_type: Option<String>,
        user_input: String,
        turn_id: Option<String>,
        context_messages: Vec<Message>,
        processing_phase: ProcessingPhase,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        self.ensure_persisted_turn_append_allowed(session_id)
            .await?;
        let session = self.get_session(session_id).ok_or_else(|| {
            OpenBitFunError::NotFound(format!("Session not found: {}", session_id))
        })?;
        match &session.state {
            SessionState::Idle => {}
            SessionState::Processing {
                current_turn_id,
                phase,
            } => {
                return Err(OpenBitFunError::Validation(format!(
                    "Session is still processing: current_turn_id={}, phase={:?}",
                    current_turn_id, phase
                )));
            }
            SessionState::Error { .. } if kind == DialogTurnKind::UserDialog => {}
            SessionState::Error { error, .. } => {
                return Err(OpenBitFunError::Validation(format!(
                    "Session must be idle before starting a turn: {}",
                    error
                )));
            }
        }
        let workspace_path = self.require_session_storage_path(session_id).await?;

        let turn_index = session.dialog_turn_ids.len();
        let turn_id = new_turn_id(turn_id);
        if session
            .dialog_turn_ids
            .iter()
            .any(|existing| existing == &turn_id)
        {
            return Err(OpenBitFunError::Validation(format!(
                "Dialog turn already exists: {turn_id}"
            )));
        }

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.dialog_turn_ids.push(turn_id.clone());
            if kind == DialogTurnKind::UserDialog {
                session.last_user_dialog_agent_type = agent_type.clone();
            }
            session.state = SessionState::Processing {
                current_turn_id: turn_id.clone(),
                phase: processing_phase,
            };
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
        }

        for message in context_messages {
            self.context_store
                .add_message(session_id, message.with_turn_id(turn_id.clone()));
        }

        let turn_data = DialogTurnData::new_with_kind(
            kind,
            turn_id.clone(),
            turn_index,
            session_id.to_string(),
            if kind == DialogTurnKind::UserDialog {
                agent_type.clone()
            } else {
                None
            },
            UserMessageData {
                id: format!("{}-user", turn_id),
                content: user_input,
                timestamp: SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                metadata: user_message_metadata,
            },
        );
        if self.should_persist_session_id(session_id) {
            // Clone the session data out of the DashMap guard before awaiting I/O.
            let session_snapshot = self.sessions.get(session_id).map(|s| s.clone());
            // Ref guard released -- DashMap shard lock is free.
            if let Some(session) = session_snapshot {
                self.persistence_manager
                    .save_session(&workspace_path, &session)
                    .await?;
            }
            self.persistence_manager
                .save_dialog_turn(&workspace_path, &turn_data)
                .await?;
        } else {
            self.transient_turns
                .entry(session_id.to_string())
                .or_default()
                .turns
                .push(turn_data);
        }

        self.persist_context_snapshot_for_turn_best_effort(session_id, turn_index, "turn_started")
            .await;

        Ok(turn_id)
    }

    /// Start a new dialog turn
    /// turn_id: Optional frontend-specified ID, if None then backend generates
    /// Returns: turn_id
    pub async fn start_dialog_turn(
        &self,
        session_id: &str,
        agent_type: String,
        user_input: String,
        turn_id: Option<String>,
        image_contexts: Option<Vec<ImageContextData>>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let user_message =
            if let Some(images) = image_contexts.as_ref().filter(|v| !v.is_empty()).cloned() {
                Message::user_multimodal(user_input.clone(), images)
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            } else {
                Message::user(user_input.clone())
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            };

        let turn_id = self
            .start_persisted_turn(
                session_id,
                DialogTurnKind::UserDialog,
                Some(agent_type),
                user_input,
                turn_id,
                vec![user_message],
                ProcessingPhase::Starting,
                user_message_metadata,
            )
            .await?;

        debug!("Starting dialog turn: turn_id={}", turn_id);

        Ok(turn_id)
    }

    /// Starts a normal user dialog while the caller owns the Session mutation
    /// lock. This keeps staged-revert commit and new-turn admission atomic for
    /// non-model Runtime operations that still produce standard dialog turns.
    pub(crate) async fn start_dialog_turn_locked(
        &self,
        session_id: &str,
        agent_type: String,
        user_input: String,
        turn_id: Option<String>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let user_message = Message::user(user_input.clone())
            .with_semantic_kind(MessageSemanticKind::ActualUserInput);
        self.start_persisted_turn_locked(
            session_id,
            DialogTurnKind::UserDialog,
            Some(agent_type),
            user_input,
            turn_id,
            vec![user_message],
            ProcessingPhase::Starting,
            user_message_metadata,
        )
        .await
    }

    pub async fn start_dialog_turn_with_prepended_messages(
        &self,
        session_id: &str,
        agent_type: String,
        user_input: String,
        turn_id: Option<String>,
        image_contexts: Option<Vec<ImageContextData>>,
        prepended_messages: Vec<Message>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let user_message =
            if let Some(images) = image_contexts.as_ref().filter(|v| !v.is_empty()).cloned() {
                Message::user_multimodal(user_input.clone(), images)
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            } else {
                Message::user(user_input.clone())
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            };

        let mut context_messages = prepended_messages;
        context_messages.push(user_message);

        let turn_id = self
            .start_persisted_turn(
                session_id,
                DialogTurnKind::UserDialog,
                Some(agent_type),
                user_input,
                turn_id,
                context_messages,
                ProcessingPhase::Starting,
                user_message_metadata,
            )
            .await?;

        debug!(
            "Starting dialog turn with prepended messages: turn_id={}",
            turn_id
        );

        Ok(turn_id)
    }

    /// Persist a Turn only if the execution-affecting Session settings still
    /// match the snapshot used to resolve its model, permission, agent route,
    /// workspace, prompt, and reasoning metadata. The validation and Turn
    /// append share one Session mutation lock, so a concurrent settings write
    /// must retry admission.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_dialog_turn_with_prepended_messages_if_session_matches(
        &self,
        session_id: &str,
        agent_type: String,
        user_input: String,
        turn_id: Option<String>,
        image_contexts: Option<Vec<ImageContextData>>,
        prepended_messages: Vec<Message>,
        user_message_metadata: Option<serde_json::Value>,
        expected: &TurnAdmissionSessionFacts,
    ) -> OpenBitFunResult<String> {
        let user_message =
            if let Some(images) = image_contexts.as_ref().filter(|v| !v.is_empty()).cloned() {
                Message::user_multimodal(user_input.clone(), images)
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            } else {
                Message::user(user_input.clone())
                    .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            };
        let mut context_messages = prepended_messages;
        context_messages.push(user_message);

        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let current = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        if !expected.matches(&current) {
            return Err(OpenBitFunError::Validation(
                "Session execution settings changed during turn admission; retry submission"
                    .to_string(),
            ));
        }

        self.start_persisted_turn_locked(
            session_id,
            DialogTurnKind::UserDialog,
            Some(agent_type),
            user_input,
            turn_id,
            context_messages,
            ProcessingPhase::Starting,
            user_message_metadata,
        )
        .await
    }

    /// Start a new dialog turn when the model-visible user message has already
    /// been inserted into runtime context by the caller.
    ///
    /// This is used by forked/hidden subagent flows that seed inherited context
    /// before they acquire a concrete dialog turn id. The turn still needs the
    /// normal persisted lifecycle (turn record, active turn bookkeeping, and
    /// context snapshot), but must not append a duplicate user message into the
    /// runtime context cache.
    pub async fn start_dialog_turn_with_existing_context(
        &self,
        session_id: &str,
        agent_type: String,
        user_input: String,
        turn_id: Option<String>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let turn_id = self
            .start_persisted_turn(
                session_id,
                DialogTurnKind::UserDialog,
                Some(agent_type),
                user_input,
                turn_id,
                Vec::new(),
                ProcessingPhase::Starting,
                user_message_metadata,
            )
            .await?;

        debug!(
            "Starting dialog turn with existing context: turn_id={}",
            turn_id
        );

        Ok(turn_id)
    }

    /// Start a persisted maintenance turn that should not enter model-visible context.
    pub async fn start_maintenance_turn(
        &self,
        session_id: &str,
        display_message: String,
        turn_id: Option<String>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let turn_id = self
            .start_persisted_turn(
                session_id,
                DialogTurnKind::ManualCompaction,
                None,
                display_message,
                turn_id,
                Vec::new(),
                ProcessingPhase::Compacting,
                user_message_metadata,
            )
            .await?;

        debug!("Starting maintenance turn: turn_id={}", turn_id);

        Ok(turn_id)
    }

    pub(crate) async fn start_maintenance_turn_locked(
        &self,
        session_id: &str,
        display_message: String,
        turn_id: Option<String>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<String> {
        let turn_id = self
            .start_persisted_turn_locked(
                session_id,
                DialogTurnKind::ManualCompaction,
                None,
                display_message,
                turn_id,
                Vec::new(),
                ProcessingPhase::Compacting,
                user_message_metadata,
            )
            .await?;
        debug!("Starting maintenance turn: turn_id={}", turn_id);
        Ok(turn_id)
    }

    /// Record a provider-native voice exchange in the same model-visible ledger.
    /// Stable exchange IDs make transport retries harmless. Admission and persistence
    /// share the ordinary Session mutation lock; a running Agent is never overwritten.
    pub async fn append_voice_exchange(
        &self,
        session_id: &str,
        exchange_id: &str,
        user_text: String,
        assistant_text: String,
    ) -> OpenBitFunResult<()> {
        if exchange_id.trim().is_empty() || user_text.trim().is_empty() {
            return Err(OpenBitFunError::Validation(
                "Voice exchange requires an id and final user transcript".into(),
            ));
        }
        let _guard = self.acquire_session_mutation(session_id).await?;
        let mut session = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        if session.dialog_turn_ids.iter().any(|id| id == exchange_id) {
            return Ok(());
        }
        if matches!(session.state, SessionState::Processing { .. }) {
            return Err(OpenBitFunError::Validation(
                "Voice exchange is waiting for the active Agent turn".into(),
            ));
        }
        self.ensure_persisted_turn_append_allowed(session_id)
            .await?;
        let storage = self.require_session_storage_path(session_id).await?;
        let index = session.dialog_turn_ids.len();
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let user = Message::user(user_text.clone())
            .with_semantic_kind(MessageSemanticKind::ActualUserInput)
            .with_turn_id(exchange_id.to_string());
        let assistant =
            Message::assistant(assistant_text.clone()).with_turn_id(exchange_id.to_string());
        let mut turn = DialogTurnData::new_with_kind(
            DialogTurnKind::UserDialog,
            exchange_id.to_string(),
            index,
            session_id.to_string(),
            Some(session.agent_type.clone()),
            UserMessageData {
                id: format!("{exchange_id}-user"),
                content: user_text,
                timestamp,
                metadata: Some(json!({ "source": "realtime_voice", "nativeExchange": true })),
            },
        );
        turn.timestamp = timestamp;
        turn.start_time = timestamp;
        turn.end_time = Some(timestamp);
        turn.duration_ms = Some(0);
        turn.status = TurnStatus::Completed;
        turn.model_rounds =
            Self::build_model_rounds_from_messages(&[assistant.clone()], exchange_id, timestamp);
        let persist = self.config.enable_persistence && self.should_persist_session(&session);
        // A failed write leaves the in-memory ledger untouched and can be retried.
        if persist {
            self.persistence_manager
                .save_dialog_turn(&storage, &turn)
                .await?;
        }
        session.dialog_turn_ids.push(exchange_id.to_string());
        session.updated_at = SystemTime::now();
        session.last_activity_at = session.updated_at;
        if persist {
            self.persistence_manager
                .save_session(&storage, &session)
                .await?;
        }
        self.sessions.insert(session_id.to_string(), session);
        self.context_store.add_message(session_id, user);
        if !assistant_text.is_empty() {
            self.context_store.add_message(session_id, assistant);
        }
        self.persist_context_snapshot_for_turn_best_effort(
            session_id,
            index,
            "voice_exchange_recorded",
        )
        .await;
        Ok(())
    }

    /// Append a completed local command turn that should be persisted in user-facing
    /// history without entering model-visible runtime context.
    pub async fn append_completed_local_command_turn(
        &self,
        session_id: &str,
        content: String,
        turn_id: Option<String>,
        timestamp_ms: Option<u64>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<DialogTurnData> {
        let _mutation_guard = self.lock_session_mutation(session_id).await;
        self.append_completed_local_command_turn_locked(
            session_id,
            content,
            turn_id,
            timestamp_ms,
            user_message_metadata,
        )
        .await
    }

    pub(crate) async fn append_completed_local_command_turn_locked(
        &self,
        session_id: &str,
        content: String,
        turn_id: Option<String>,
        timestamp_ms: Option<u64>,
        user_message_metadata: Option<serde_json::Value>,
    ) -> OpenBitFunResult<DialogTurnData> {
        self.ensure_persisted_turn_append_allowed(session_id)
            .await?;
        let session = self.get_session(session_id).ok_or_else(|| {
            OpenBitFunError::NotFound(format!("Session not found: {}", session_id))
        })?;
        let workspace_path = self.require_session_storage_path(session_id).await?;

        let turn_id = new_turn_id(turn_id);
        let turn_index = session
            .dialog_turn_ids
            .iter()
            .position(|existing| existing == &turn_id)
            .unwrap_or(session.dialog_turn_ids.len());
        let timestamp = timestamp_ms.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        let mut turn = DialogTurnData::new_with_kind(
            DialogTurnKind::LocalCommand,
            turn_id.clone(),
            turn_index,
            session_id.to_string(),
            None,
            UserMessageData {
                id: format!("{}-user", turn_id),
                content,
                timestamp,
                metadata: user_message_metadata,
            },
        );
        turn.timestamp = timestamp;
        turn.start_time = timestamp;
        turn.end_time = Some(timestamp);
        turn.duration_ms = Some(0);
        turn.status = TurnStatus::Completed;

        if self.config.enable_persistence && self.should_persist_session(&session) {
            self.persistence_manager
                .save_dialog_turn(&workspace_path, &turn)
                .await?;
        } else {
            let mut history = self
                .transient_turns
                .entry(session_id.to_string())
                .or_default();
            if let Some(current) = history.turns.get_mut(turn_index) {
                *current = turn.clone();
            } else {
                history.turns.push(turn.clone());
            }
        }

        let session_snapshot = if let Some(mut session) = self.sessions.get_mut(session_id) {
            if !session
                .dialog_turn_ids
                .iter()
                .any(|existing| existing == &turn_id)
            {
                session.dialog_turn_ids.push(turn_id);
            }
            session.state = SessionState::Idle;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();

            if self.config.enable_persistence && self.should_persist_session(&session) {
                Some(session.clone())
            } else {
                None
            }
        } else {
            None
        };

        if let Some(session) = session_snapshot {
            self.persistence_manager
                .save_session(&workspace_path, &session)
                .await?;
        }

        self.persist_context_snapshot_for_turn_best_effort(
            session_id,
            turn_index,
            "local_command_turn_persisted",
        )
        .await;

        Ok(turn)
    }

    /// Build model rounds from execution messages.
    ///
    /// Used by `complete_dialog_turn` to populate `model_rounds` when the
    /// host surface (e.g. CLI) does not persist rounds itself. This ensures
    /// turn files contain rich conversation data (text, tools, thinking) that
    /// other surfaces (e.g. Desktop) can render.
    pub(crate) fn build_model_rounds_from_messages(
        messages: &[Message],
        turn_id: &str,
        timestamp: u64,
    ) -> Vec<ModelRoundData> {
        let mut rounds: Vec<ModelRoundData> = Vec::new();

        for msg in messages {
            match msg.role {
                MessageRole::Assistant => {
                    let round_index = rounds.len();
                    let round_id = msg
                        .metadata
                        .round_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|round_id| !round_id.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("{}-round-{}", turn_id, round_index));

                    let mut text_items = Vec::new();
                    let mut thinking_items = Vec::new();
                    let mut tool_items = Vec::new();
                    let mut order_index = 0usize;

                    match &msg.content {
                        MessageContent::Text(text) => {
                            if !text.trim().is_empty() {
                                text_items.push(Self::make_text_item(
                                    &format!("{}-text-{}", round_id, order_index),
                                    text,
                                    timestamp,
                                    order_index,
                                ));
                            }
                        }
                        MessageContent::Mixed {
                            reasoning_content,
                            text,
                            tool_calls,
                        } => {
                            // Thinking / reasoning content
                            if let Some(reasoning) = reasoning_content {
                                if !reasoning.trim().is_empty() {
                                    thinking_items.push(ThinkingItemData {
                                        id: format!("{}-think-{}", round_id, order_index),
                                        content: reasoning.clone(),
                                        reasoning_kind: msg.metadata.reasoning_content_kind,
                                        is_streaming: false,
                                        is_collapsed: true,
                                        timestamp,
                                        order_index: Some(order_index),
                                        status: Some("completed".to_string()),
                                        is_subagent_item: None,
                                        parent_task_tool_id: None,
                                        subagent_session_id: None,
                                        attempt_id: None,
                                        attempt_index: None,
                                    });
                                    order_index += 1;
                                }
                            }
                            // Text content
                            if !text.trim().is_empty() {
                                text_items.push(Self::make_text_item(
                                    &format!("{}-text-{}", round_id, order_index),
                                    text,
                                    timestamp,
                                    order_index,
                                ));
                                order_index += 1;
                            }
                            // Tool calls
                            for tc in tool_calls {
                                tool_items.push(ToolItemData {
                                    id: tc.tool_id.clone(),
                                    tool_name: tc.tool_name.clone(),
                                    tool_call: ToolCallData {
                                        input: tc.arguments.clone(),
                                        id: tc.tool_id.clone(),
                                    },
                                    tool_result: None,
                                    ai_intent: None,
                                    start_time: timestamp,
                                    end_time: None,
                                    duration_ms: None,
                                    queue_wait_ms: None,
                                    preflight_ms: None,
                                    confirmation_wait_ms: None,
                                    execution_ms: None,
                                    order_index: Some(order_index),
                                    is_subagent_item: None,
                                    parent_task_tool_id: None,
                                    subagent_session_id: None,
                                    subagent_dialog_turn_id: None,
                                    attempt_id: None,
                                    attempt_index: None,
                                    subagent_model_id: None,
                                    subagent_model_display_name: None,
                                    status: Some("completed".to_string()),
                                    interruption_reason: None,
                                });
                                order_index += 1;
                            }
                        }
                        MessageContent::Multimodal { text, .. } if !text.trim().is_empty() => {
                            text_items.push(Self::make_text_item(
                                &format!("{}-text-{}", round_id, order_index),
                                text,
                                timestamp,
                                order_index,
                            ));
                        }
                        _ => {}
                    }

                    // Only add the round if it has any content
                    if !text_items.is_empty()
                        || !tool_items.is_empty()
                        || !thinking_items.is_empty()
                    {
                        // Messages written before the Runtime recorded round
                        // timing leave these unknown rather than claiming 0 ms.
                        let timing = msg
                            .metadata
                            .model_execution
                            .as_ref()
                            .and_then(|facts| facts.timing)
                            .unwrap_or_default();
                        rounds.push(ModelRoundData {
                            id: round_id,
                            turn_id: turn_id.to_string(),
                            round_index,
                            round_group_id: None,
                            timestamp,
                            text_items,
                            tool_items,
                            thinking_items,
                            start_time: timestamp,
                            end_time: Some(timestamp),
                            duration_ms: timing.duration_ms,
                            provider_id: None,
                            model_config_id: msg
                                .metadata
                                .model_execution
                                .as_ref()
                                .map(|facts| facts.model_config_id.clone()),
                            effective_model_name: msg
                                .metadata
                                .model_execution
                                .as_ref()
                                .map(|facts| facts.effective_model_name.clone()),
                            routing: msg
                                .metadata
                                .model_execution
                                .as_ref()
                                .and_then(|facts| facts.routing.clone()),
                            first_chunk_ms: timing.first_chunk_ms,
                            first_visible_output_ms: timing.first_visible_output_ms,
                            stream_duration_ms: timing.stream_duration_ms,
                            ttft_ms: timing.ttft_ms,
                            attempt_count: timing.attempt_count,
                            attempt_diagnostics: vec![],
                            failure_category: None,
                            token_details: None,
                            status: "completed".to_string(),
                        });
                    }
                }
                MessageRole::Tool => {
                    // Attach tool result to the matching tool item in the last round
                    if let MessageContent::ToolResult {
                        tool_id,
                        result,
                        result_for_assistant,
                        image_attachments,
                        is_error,
                        ..
                    } = &msg.content
                    {
                        if let Some(last_round) = rounds.last_mut() {
                            for tool_item in &mut last_round.tool_items {
                                if tool_item.id == *tool_id {
                                    let assistant_text = result_for_assistant
                                        .clone()
                                        .or_else(|| serde_json::to_string(result).ok());
                                    tool_item.tool_result = Some(ToolResultData {
                                        result: result.clone(),
                                        success: !is_error,
                                        result_for_assistant: assistant_text,
                                        image_attachments: image_attachments.clone(),
                                        error: if *is_error {
                                            result
                                                .get("error")
                                                .and_then(serde_json::Value::as_str)
                                                .map(str::to_owned)
                                                .or_else(|| serde_json::to_string(result).ok())
                                        } else {
                                            None
                                        },
                                        duration_ms: None,
                                    });
                                    tool_item.status = Some(
                                        if *is_error { "error" } else { "completed" }.to_string(),
                                    );
                                    tool_item.end_time = Some(timestamp);
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        rounds
    }

    /// Helper to create a `TextItemData` with common defaults.
    fn make_text_item(id: &str, content: &str, timestamp: u64, order_index: usize) -> TextItemData {
        TextItemData {
            id: id.to_string(),
            content: content.to_string(),
            is_streaming: false,
            timestamp,
            is_markdown: true,
            order_index: Some(order_index),
            is_subagent_item: None,
            parent_task_tool_id: None,
            subagent_session_id: None,
            status: Some("completed".to_string()),
            attempt_id: None,
            attempt_index: None,
        }
    }

    /// Read the canonical turn from its durable or temporary owner.
    async fn load_runtime_dialog_turn(
        &self,
        workspace_path: &Path,
        session_id: &str,
        turn_index: usize,
    ) -> OpenBitFunResult<Option<DialogTurnData>> {
        if self.should_persist_session_id(session_id) {
            self.persistence_manager
                .load_dialog_turn(workspace_path, session_id, turn_index)
                .await
        } else {
            Ok(self
                .transient_turns
                .get(session_id)
                .and_then(|history| history.turns.get(turn_index).cloned()))
        }
    }

    /// Read the current temporary history without cloning unrelated turns or
    /// holding its map guard across an await. None denotes a disk-backed read.
    pub(crate) fn with_transient_session_turns<T>(
        &self,
        session_id: &str,
        read: impl FnOnce(&[DialogTurnData]) -> T,
    ) -> Option<T> {
        let session = self.sessions.get(session_id)?;
        if self.config.enable_persistence && self.should_persist_session(&session) {
            return None;
        }
        drop(session);
        let history = self.transient_turns.get(session_id);
        Some(read(history.as_deref().map_or(&[], |h| h.turns.as_slice())))
    }

    async fn save_runtime_dialog_turn(
        &self,
        workspace_path: &Path,
        turn: &DialogTurnData,
    ) -> OpenBitFunResult<()> {
        if self.should_persist_session_id(&turn.session_id) {
            self.persistence_manager
                .save_dialog_turn(workspace_path, turn)
                .await
        } else {
            let mut history = self
                .transient_turns
                .get_mut(&turn.session_id)
                .ok_or_else(|| {
                    OpenBitFunError::NotFound(format!(
                        "Temporary session history not found: {}",
                        turn.session_id
                    ))
                })?;
            let current = history.turns.get_mut(turn.turn_index).ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn.turn_id))
            })?;
            *current = turn.clone();
            Ok(())
        }
    }

    /// Called under the source Session mutation lock after execution has settled.
    pub(crate) fn transient_branch_source_locked(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<TransientSessionBranchSource> {
        let session = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        if !matches!(
            session.state,
            SessionState::Idle | SessionState::Error { .. }
        ) {
            return Err(OpenBitFunError::Validation(
                "Stop the temporary conversation before saving it".to_string(),
            ));
        }
        let history = self.transient_turns.get(session_id).ok_or_else(|| {
            OpenBitFunError::Validation(
                "Temporary conversation has no submitted questions".to_string(),
            )
        })?;
        let turns = history.turns.clone();
        let context_snapshots = history
            .context_snapshots
            .iter()
            .map(|(index, messages)| (*index, messages.clone()))
            .collect();
        drop(history);
        if turns.len() != session.dialog_turn_ids.len()
            || turns
                .iter()
                .any(|turn| turn.status == TurnStatus::InProgress)
        {
            return Err(OpenBitFunError::Validation(
                "Temporary conversation history has not settled".to_string(),
            ));
        }
        let skill_agent_snapshots = turns
            .iter()
            .filter_map(|turn| {
                self.turn_skill_agent_snapshot_store
                    .get_snapshot(session_id, turn.turn_index)
                    .map(|snapshot| (turn.turn_index, snapshot))
            })
            .collect();
        let evidence_events = turns
            .iter()
            .flat_map(|turn| self.evidence_events_for_turn(session_id, &turn.turn_id))
            .collect();
        Ok(TransientSessionBranchSource {
            session,
            metadata: None,
            turns,
            context_snapshots,
            prompt_cache: self.prompt_cache_store.get_cache(session_id),
            skill_agent_snapshots,
            baseline_override: self
                .skill_agent_baseline_override_snapshot_store
                .get(session_id)
                .map(|snapshot| snapshot.clone()),
            evidence_events,
        })
    }

    pub(crate) async fn background_delivery_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<Option<openbitfun_core_types::background_delivery::BackgroundDelivery>>
    {
        let path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| OpenBitFunError::validation("Session storage is unavailable"))?;
        let index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| OpenBitFunError::validation("Background run is not loaded"))?;
        let turn = self
            .load_runtime_dialog_turn(&path, session_id, index)
            .await?;
        Ok(turn.and_then(|turn| {
            let mut delivery = openbitfun_core_types::background_delivery::BackgroundDelivery::from_metadata(turn.user_message.metadata.as_ref(), turn_id)?;
            let generation = turn.recovery.as_ref().map(|recovery| recovery.execution_generation).or(turn.recovery_epoch).unwrap_or(0);
            if generation > delivery.execution_generation {
                delivery.execution_generation = generation;
                delivery.disposition = openbitfun_core_types::background_delivery::BackgroundReportDisposition::Pending;
                delivery.proposal = None;
                delivery.resolved_at_ms = None;
                delivery.decision_usage = None;
                delivery.decision_source = "pending_recovery".to_owned();
                delivery.reason.clear();
            }
            Some(delivery)
        }))
    }

    pub(crate) async fn background_task_instructions(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<String> {
        let path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| OpenBitFunError::validation("Session storage is unavailable"))?;
        let index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| OpenBitFunError::validation("Background run is not loaded"))?;
        Ok(self
            .load_runtime_dialog_turn(&path, session_id, index)
            .await?
            .map(|turn| turn.user_message.content)
            .unwrap_or_default())
    }

    pub(crate) async fn set_background_delivery_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        delivery: &openbitfun_core_types::background_delivery::BackgroundDelivery,
    ) -> OpenBitFunResult<()> {
        use openbitfun_core_types::background_delivery::{
            BackgroundDelivery, BACKGROUND_DELIVERY_METADATA_KEY,
        };
        let path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| OpenBitFunError::validation("Session storage is unavailable"))?;
        let index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| OpenBitFunError::validation("Background run is not loaded"))?;
        let _guard = self.acquire_session_mutation(session_id).await?;
        let mut turn = self
            .load_runtime_dialog_turn(&path, session_id, index)
            .await?
            .ok_or_else(|| OpenBitFunError::validation("Background run record is unavailable"))?;
        let current =
            BackgroundDelivery::from_metadata(turn.user_message.metadata.as_ref(), turn_id)
                .ok_or_else(|| {
                    OpenBitFunError::validation("This turn is not an admitted background run")
                })?;
        let generation = turn
            .recovery
            .as_ref()
            .map(|recovery| recovery.execution_generation)
            .or(turn.recovery_epoch)
            .unwrap_or(0);
        if current.task_id != delivery.task_id
            || current.run_id != delivery.run_id
            || current.policy != delivery.policy
            || delivery.execution_generation != generation
        {
            return Err(OpenBitFunError::validation(
                "Background report identity or policy changed",
            ));
        }
        if current.resolved_at_ms.is_some()
            && current.execution_generation >= delivery.execution_generation
            && current != *delivery
        {
            return Err(OpenBitFunError::validation(
                "Background report is already resolved",
            ));
        }
        let metadata = turn
            .user_message
            .metadata
            .get_or_insert_with(|| serde_json::json!({}));
        if delivery.execution_generation > current.execution_generation {
            if let Some(object) = metadata.as_object_mut() {
                let history = object
                    .entry("backgroundDeliveryHistory")
                    .or_insert_with(|| serde_json::json!([]));
                if let Some(history) = history.as_array_mut() {
                    history.push(
                        serde_json::to_value(&current)
                            .map_err(|error| OpenBitFunError::validation(error.to_string()))?,
                    );
                }
            }
        }
        metadata[BACKGROUND_DELIVERY_METADATA_KEY] = serde_json::to_value(delivery)
            .map_err(|error| OpenBitFunError::validation(error.to_string()))?;
        self.save_runtime_dialog_turn(&path, &turn).await
    }

    pub(crate) async fn propose_background_report(
        &self,
        session_id: &str,
        turn_id: &str,
        proposal: openbitfun_core_types::background_delivery::BackgroundReport,
    ) -> OpenBitFunResult<()> {
        use openbitfun_core_types::background_delivery::{
            BackgroundReportDisposition as Disposition, BackgroundReportPolicy as Policy,
        };
        if proposal.reason.trim().is_empty()
            || !matches!(
                proposal.disposition,
                Disposition::Quiet | Disposition::Report | Disposition::Attention
            )
        {
            return Err(OpenBitFunError::validation(
                "A final report disposition and reason are required",
            ));
        }
        let mut delivery = self
            .background_delivery_for_turn(session_id, turn_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::validation(
                    "Report proposals are only available inside a background run",
                )
            })?;
        if delivery.resolved_at_ms.is_some()
            || (delivery.policy == Policy::Always && proposal.disposition == Disposition::Quiet)
        {
            return Err(OpenBitFunError::validation(
                "The run policy does not permit this report proposal",
            ));
        }
        delivery.proposal = Some(proposal);
        self.set_background_delivery_for_turn(session_id, turn_id, &delivery)
            .await
    }

    async fn load_turn_token_usage(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<Option<crate::service::session::DialogTurnTokenUsageData>> {
        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            return Ok(None);
        };
        let Some(turn_index) = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
        else {
            return Ok(None);
        };
        let Some(turn) = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .filter(|turn| turn.turn_id == turn_id)
        else {
            return Ok(None);
        };
        Ok(super::turn_usage::load_turn_token_usage(
            self.persistence_manager.path_manager(),
            session_id,
            &turn.turn_id,
            turn.start_time,
        )
        .await)
    }

    /// Complete a dialog turn in its history owner.
    pub async fn complete_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        final_response: String,
        new_messages: &[Message],
        stats: TurnStats,
        finish_reason: Option<String>,
        has_final_response: Option<bool>,
    ) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(session_id)
            && !self.transient_turns.contains_key(session_id)
        {
            debug!(
                "Skipping dialog turn persistence for transient session completion: session_id={}, turn_id={}, response_len={}, rounds={}",
                session_id,
                turn_id,
                final_response.len(),
                stats.total_rounds
            );
            return Ok(());
        }

        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;

        // The context snapshot is a session-level artifact built from the
        // in-memory context store; it does not participate in the projected
        // checkpoint race below. Persist it before taking the mutation lock so
        // slow storage (for example a remote SSH workspace) cannot extend the
        // critical section and stall the next turn's start behind completion.
        self.persist_context_snapshot_for_turn_best_effort(
            session_id,
            turn_index,
            "turn_completed",
        )
        .await;

        let token_usage = self.load_turn_token_usage(session_id, turn_id).await?;

        // Serialize Runtime completion against projected UI checkpoints. If a
        // checkpoint wins first, the generation journal below repairs it; if
        // completion wins first, the projected-save guard sees the terminal
        // authoritative record and cannot replace it with an older prefix.
        // Keep this critical section to the turn-record load, merge, and save
        // only: the same keyed lock also serializes dialog-turn starts, so any
        // extra work held here directly delays the next user message.
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;

        let mut turn = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;

        // Update state
        let completion_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        // The Runtime generation journal is authoritative at completion. A
        // frontend checkpoint may already contain assistant text, but that
        // text can be only a prefix when the final stream chunks were dropped.
        // Merge by stable round identity instead of treating any non-empty
        // projected text as proof that the Turn is complete.
        let turn_identity = turn.turn_id.clone();
        let generated_count = Self::append_generation_rounds(
            &mut turn,
            &turn_identity,
            new_messages,
            completion_timestamp,
        );
        if generated_count == 0 && !final_response.trim().is_empty() {
            let mut reconciled_existing_text = false;
            for round in turn.model_rounds.iter_mut().rev() {
                let Some(item) = round
                    .text_items
                    .iter_mut()
                    .rev()
                    .find(|item| !item.content.trim().is_empty())
                else {
                    continue;
                };
                if final_response == item.content || final_response.starts_with(&item.content) {
                    item.content = final_response.clone();
                    item.is_streaming = false;
                    item.status = Some("completed".to_string());
                    round.status = "completed".to_string();
                    round.end_time = Some(completion_timestamp);
                    reconciled_existing_text = true;
                }
                break;
            }

            let has_assistant_text = turn.model_rounds.iter().any(|round| {
                round
                    .text_items
                    .iter()
                    .any(|item| !item.content.trim().is_empty())
            });
            if !reconciled_existing_text && !has_assistant_text {
                // Fallback: append a single text-only round
                let round_index = turn.model_rounds.len();
                turn.model_rounds.push(ModelRoundData {
                    id: format!("{}-final-round", turn.turn_id),
                    turn_id: turn.turn_id.clone(),
                    round_index,
                    round_group_id: None,
                    timestamp: completion_timestamp,
                    text_items: vec![TextItemData {
                        id: format!("{}-final-text", turn.turn_id),
                        content: final_response.clone(),
                        is_streaming: false,
                        timestamp: completion_timestamp,
                        is_markdown: true,
                        order_index: Some(0),
                        is_subagent_item: None,
                        parent_task_tool_id: None,
                        subagent_session_id: None,
                        status: Some("completed".to_string()),
                        attempt_id: None,
                        attempt_index: None,
                    }],
                    tool_items: Vec::new(),
                    thinking_items: Vec::new(),
                    start_time: completion_timestamp,
                    end_time: Some(completion_timestamp),
                    duration_ms: Some(0),
                    provider_id: None,
                    model_config_id: None,
                    effective_model_name: None,
                    routing: None,
                    first_chunk_ms: None,
                    first_visible_output_ms: None,
                    stream_duration_ms: None,
                    ttft_ms: None,
                    attempt_count: None,
                    attempt_diagnostics: vec![],
                    failure_category: None,
                    token_details: None,
                    status: "completed".to_string(),
                });
            }
        }
        self.persistence_manager
            .seal_completed_figures(&workspace_path, &mut turn)
            .await;
        turn.status = TurnStatus::Completed;
        turn.recovery = None;
        turn.finish_reason = finish_reason;
        turn.has_final_response = has_final_response;
        turn.duration_ms = Some(stats.duration_ms);
        turn.end_time = Some(completion_timestamp);
        super::turn_usage::reconcile_turn_token_usage(&mut turn, token_usage);

        // Persist
        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;

        debug!(
            "Dialog turn completed: turn_id={}, rounds={}, tools={}",
            turn_id, stats.total_rounds, stats.total_tools
        );

        Ok(())
    }

    pub(crate) fn append_generation_rounds(
        turn: &mut DialogTurnData,
        turn_id: &str,
        new_messages: &[Message],
        timestamp: u64,
    ) -> usize {
        let generated_rounds =
            Self::build_model_rounds_from_messages(new_messages, turn_id, timestamp);
        let generated_count = generated_rounds.len();
        if generated_count == 0 {
            return 0;
        }
        let mut generation_order = BTreeMap::new();
        for (index, round) in generated_rounds.iter().enumerate() {
            generation_order.entry(round.id.clone()).or_insert(index);
        }
        for mut round in generated_rounds {
            if let Some(existing_index) = turn
                .model_rounds
                .iter()
                .position(|existing| existing.id == round.id)
            {
                let existing = &turn.model_rounds[existing_index];
                for text_item in &mut round.text_items {
                    if let Some(previous) = existing
                        .text_items
                        .iter()
                        .find(|item| item.id == text_item.id)
                    {
                        text_item.timestamp = previous.timestamp;
                        text_item.is_markdown = previous.is_markdown;
                        text_item.order_index = previous.order_index;
                        text_item.is_subagent_item = previous.is_subagent_item;
                        text_item.parent_task_tool_id = previous.parent_task_tool_id.clone();
                        text_item.subagent_session_id = previous.subagent_session_id.clone();
                        text_item.status = previous.status.clone().or(text_item.status.clone());
                        text_item.attempt_id = previous.attempt_id.clone();
                        text_item.attempt_index = previous.attempt_index;
                    }
                }
                for thinking_item in &mut round.thinking_items {
                    if let Some(previous) = existing
                        .thinking_items
                        .iter()
                        .find(|item| item.id == thinking_item.id)
                    {
                        thinking_item.timestamp = previous.timestamp;
                        thinking_item.order_index = previous.order_index;
                        thinking_item.status =
                            previous.status.clone().or(thinking_item.status.clone());
                        thinking_item.is_subagent_item = previous.is_subagent_item;
                        thinking_item.parent_task_tool_id = previous.parent_task_tool_id.clone();
                        thinking_item.subagent_session_id = previous.subagent_session_id.clone();
                        thinking_item.attempt_id = previous.attempt_id.clone();
                        thinking_item.attempt_index = previous.attempt_index;
                    }
                }
                for tool_item in &mut round.tool_items {
                    if let Some(previous) = existing
                        .tool_items
                        .iter()
                        .find(|item| item.id == tool_item.id)
                    {
                        tool_item.ai_intent = previous.ai_intent.clone();
                        tool_item.start_time = previous.start_time;
                        tool_item.end_time = previous.end_time.or(tool_item.end_time);
                        tool_item.duration_ms = previous.duration_ms.or(tool_item.duration_ms);
                        tool_item.queue_wait_ms = previous.queue_wait_ms;
                        tool_item.preflight_ms = previous.preflight_ms;
                        tool_item.confirmation_wait_ms = previous.confirmation_wait_ms;
                        tool_item.execution_ms = previous.execution_ms;
                        tool_item.order_index = previous.order_index;
                        tool_item.is_subagent_item = previous.is_subagent_item;
                        tool_item.parent_task_tool_id = previous.parent_task_tool_id.clone();
                        tool_item.subagent_session_id = previous.subagent_session_id.clone();
                        tool_item.subagent_dialog_turn_id =
                            previous.subagent_dialog_turn_id.clone();
                        tool_item.attempt_id = previous.attempt_id.clone();
                        tool_item.attempt_index = previous.attempt_index;
                        tool_item.subagent_model_id = previous.subagent_model_id.clone();
                        tool_item.subagent_model_display_name =
                            previous.subagent_model_display_name.clone();
                        // A runtime result settles the request immediately. A persisted
                        // waiting/running checkpoint must not overwrite that newer fact.
                        if tool_item.tool_result.is_none() {
                            tool_item.status = previous.status.clone().or(tool_item.status.clone());
                            tool_item.interruption_reason = previous.interruption_reason.clone();
                        }
                    }
                }
                // Client-derived display cards are not part of the model's
                // generation journal. Preserve the recognized additive card
                // while replacing Runtime text/tool content authoritatively.
                let generated_tool_ids = round
                    .tool_items
                    .iter()
                    .map(|item| item.id.clone())
                    .collect::<std::collections::HashSet<_>>();
                let derived_tool_items = existing
                    .tool_items
                    .iter()
                    .filter(|item| {
                        item.id.starts_with("plan-display-")
                            && !generated_tool_ids.contains(&item.id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                round.tool_items.extend(derived_tool_items);
                round.round_group_id = existing.round_group_id.clone();
                round.timestamp = existing.timestamp;
                round.start_time = existing.start_time;
                round.end_time = existing.end_time.or(round.end_time);
                // Round timing is a Runtime fact carried by the generation
                // journal. The client checkpoint only mirrors
                // `ModelRoundCompleted`, and for the last round it routinely
                // loses the race with Turn completion, so it is a fallback for
                // journals written before the Runtime recorded timing. Older
                // completion writers persisted `0` for unknown durations.
                round.duration_ms = round
                    .duration_ms
                    .or(existing.duration_ms.filter(|duration_ms| *duration_ms > 0));
                round.provider_id = existing.provider_id.clone();
                round.model_config_id = round
                    .model_config_id
                    .or_else(|| existing.model_config_id.clone());
                round.effective_model_name = round
                    .effective_model_name
                    .or_else(|| existing.effective_model_name.clone());
                round.routing = round.routing.or_else(|| existing.routing.clone());
                round.first_chunk_ms = round.first_chunk_ms.or(existing.first_chunk_ms);
                round.first_visible_output_ms = round
                    .first_visible_output_ms
                    .or(existing.first_visible_output_ms);
                round.stream_duration_ms = round.stream_duration_ms.or(existing.stream_duration_ms);
                round.ttft_ms = round.ttft_ms.or(existing.ttft_ms);
                round.attempt_count = round.attempt_count.or(existing.attempt_count);
                round.attempt_diagnostics = existing.attempt_diagnostics.clone();
                round.failure_category = existing.failure_category.clone();
                round.token_details = existing.token_details.clone();
                if existing.status != "streaming" {
                    round.status = existing.status.clone();
                }
                turn.model_rounds[existing_index] = round;
            } else {
                turn.model_rounds.push(round);
            }
        }

        // A client can checkpoint only a suffix or scattered rounds. Appending
        // missing earlier rounds behind that checkpoint puts the final answer
        // in the middle. The generation journal owns their relative order.
        // Keep older generations before the first shared round; other rounds
        // absent from this journal stay beside their preceding shared round.
        // Nothing is discarded, including interrupted or client-only records.
        let mut preceding_generation_index = None;
        let mut ordered = std::mem::take(&mut turn.model_rounds)
            .into_iter()
            .map(|round| {
                let generation_index = generation_order.get(&round.id).copied();
                if generation_index.is_some() {
                    preceding_generation_index = generation_index;
                }
                (
                    (preceding_generation_index, generation_index.is_none()),
                    round,
                )
            })
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(order, _)| *order);
        turn.model_rounds = ordered
            .into_iter()
            .enumerate()
            .map(|(index, (_, mut round))| {
                round.round_index = index;
                round
            })
            .collect();
        generated_count
    }

    /// Record a spawned subagent's child Session on the parent Turn's tool item.
    ///
    /// A synchronous delegated command is stamped in memory before its synthetic
    /// Turn is written. An asynchronous spawn returns to the parent Turn long
    /// before the child Turn finishes, so the link has to become durable on the
    /// already persisted parent Turn for hosts that only read persisted Turns.
    /// Rounds are materialized from the live generation exactly like a relay
    /// read, so the stamped item keeps the round and item identity that the
    /// later completion merge preserves.
    ///
    /// This is a best-effort annotation of an in-progress Turn: an unknown Turn,
    /// an already terminal Turn, or a missing tool item leaves the Turn as is.
    pub(crate) async fn link_subagent_session_on_parent_tool_item(
        &self,
        parent_session_id: &str,
        parent_dialog_turn_id: &str,
        parent_tool_call_id: &str,
        subagent_session_id: &str,
        subagent_dialog_turn_id: &str,
        subagent_model_id: Option<String>,
    ) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(parent_session_id)
            && !self.transient_turns.contains_key(parent_session_id)
        {
            return Ok(());
        }
        let Some(workspace_path) = self.effective_session_storage_path(parent_session_id).await
        else {
            return Ok(());
        };
        let _mutation_guard = self.acquire_session_mutation(parent_session_id).await?;
        let Some(turn_index) = self.sessions.get(parent_session_id).and_then(|session| {
            session
                .dialog_turn_ids
                .iter()
                .position(|turn_id| turn_id == parent_dialog_turn_id)
        }) else {
            return Ok(());
        };
        let Some(mut turn) = self
            .load_runtime_dialog_turn(&workspace_path, parent_session_id, turn_index)
            .await?
        else {
            return Ok(());
        };
        // A terminal Turn is owned by its completion writer, which merges the
        // same link from the in-memory rounds instead.
        if turn.status != TurnStatus::InProgress {
            return Ok(());
        }
        // Rounds materialize on the Turn's own start time, so a mid-Turn
        // checkpoint does not reorder the running generation.
        let materialized_at = turn.start_time;
        let messages: Vec<Message> = self
            .context_store
            .get_context_messages(parent_session_id)
            .into_iter()
            .filter(|message| message.metadata.turn_id.as_deref() == Some(parent_dialog_turn_id))
            .collect();
        Self::append_generation_rounds(
            &mut turn,
            parent_dialog_turn_id,
            &messages,
            materialized_at,
        );
        let Some(tool_item) = turn
            .model_rounds
            .iter_mut()
            .flat_map(|round| round.tool_items.iter_mut())
            .find(|item| {
                item.id == parent_tool_call_id || item.tool_call.id == parent_tool_call_id
            })
        else {
            return Ok(());
        };
        tool_item.subagent_session_id = Some(subagent_session_id.to_string());
        tool_item.subagent_dialog_turn_id = Some(subagent_dialog_turn_id.to_string());
        if subagent_model_id.is_some() {
            tool_item.subagent_model_id = subagent_model_id;
        }
        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;
        debug!(
            "Linked spawned subagent Session on the parent tool item: parent_session_id={}, parent_turn_id={}, parent_tool_call_id={}, subagent_session_id={}",
            parent_session_id, parent_dialog_turn_id, parent_tool_call_id, subagent_session_id
        );
        Ok(())
    }

    /// Complete a reopened interrupted turn by appending only the messages
    /// produced by the current execution generation.
    pub async fn complete_recovered_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_generation: u32,
        final_response: String,
        new_messages: &[Message],
        stats: TurnStats,
        finish_reason: Option<String>,
        has_final_response: Option<bool>,
    ) -> OpenBitFunResult<()> {
        let token_usage = self.load_turn_token_usage(session_id, turn_id).await?;
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {session_id}"
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {turn_id}"))
            })?;
        let mut turn = self
            .persistence_manager
            .load_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {turn_id}"))
            })?;

        if turn.status == TurnStatus::Completed && turn.recovery.is_none() {
            return Ok(());
        }
        let recovery = turn.recovery.as_ref().ok_or_else(|| {
            OpenBitFunError::Validation(format!(
                "Recovered dialog turn has no recovery metadata: {turn_id}"
            ))
        })?;
        if recovery.status != DialogTurnRecoveryStatus::Recovering
            || recovery.execution_generation != execution_generation
        {
            return Err(OpenBitFunError::Validation(format!(
                "Recovered turn generation mismatch: expected={execution_generation}, actual={}",
                recovery.execution_generation
            )));
        }

        let completion_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let first_round_index = turn
            .model_rounds
            .iter()
            .map(|round| round.round_index)
            .max()
            .map_or(0, |index| index.saturating_add(1));
        let appended_count =
            Self::append_generation_rounds(&mut turn, turn_id, new_messages, completion_timestamp);
        if appended_count == 0 && !final_response.trim().is_empty() {
            turn.model_rounds.push(ModelRoundData {
                id: format!("{turn_id}-round-{first_round_index}"),
                turn_id: turn_id.to_string(),
                round_index: first_round_index,
                round_group_id: None,
                timestamp: completion_timestamp,
                text_items: vec![Self::make_text_item(
                    &format!("{turn_id}-round-{first_round_index}-text-0"),
                    &final_response,
                    completion_timestamp,
                    0,
                )],
                tool_items: Vec::new(),
                thinking_items: Vec::new(),
                start_time: completion_timestamp,
                end_time: Some(completion_timestamp),
                duration_ms: Some(0),
                provider_id: None,
                model_config_id: None,
                effective_model_name: None,
                routing: None,
                first_chunk_ms: None,
                first_visible_output_ms: None,
                stream_duration_ms: None,
                ttft_ms: None,
                attempt_count: None,
                attempt_diagnostics: vec![],
                failure_category: None,
                token_details: None,
                status: "completed".to_string(),
            });
        }
        turn.status = TurnStatus::Completed;
        turn.recovery_epoch = Some(execution_generation);
        turn.recovery = None;
        turn.finish_reason = finish_reason;
        turn.has_final_response = has_final_response;
        turn.duration_ms = Some(stats.duration_ms);
        turn.end_time = Some(completion_timestamp);
        super::turn_usage::reconcile_turn_token_usage(&mut turn, token_usage);

        // A recovered generation is Runtime-owned, so its context snapshot is
        // part of the completion commit. Persist it before the Completed Turn:
        // a crash after the snapshot leaves a Recovering Turn that restore can
        // safely normalize, while a Completed Turn never points at an older
        // same-index snapshot that omits this generation's tool/results tail.
        let context_messages = self.context_store.get_context_messages(session_id);
        self.persistence_manager
            .save_turn_context_snapshot(
                &workspace_path,
                session_id,
                turn.turn_index,
                &context_messages,
            )
            .await?;
        self.persistence_manager
            .save_dialog_turn(&workspace_path, &turn)
            .await?;
        Ok(())
    }

    /// Mark a dialog turn as failed and persist it.
    /// Unlike `complete_dialog_turn`, this sets the state to `Failed` with an error message.
    pub async fn fail_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        error: String,
    ) -> OpenBitFunResult<()> {
        self.fail_dialog_turn_with_messages(session_id, turn_id, error, &[])
            .await
    }

    pub(crate) async fn fail_dialog_turn_with_messages(
        &self,
        session_id: &str,
        turn_id: &str,
        error: String,
        generation_messages: &[Message],
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if !self.should_persist_session_id(session_id)
            && !self.transient_turns.contains_key(session_id)
        {
            debug!(
                "Skipping dialog turn persistence for transient session failure: session_id={}, turn_id={}, error={}",
                session_id, turn_id, error
            );
            return Ok(());
        }

        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let mut turn = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let recovered_generation = turn.recovery.is_some();
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self::append_generation_rounds(&mut turn, turn_id, generation_messages, now);
        turn.status = TurnStatus::Error;
        turn.recovery = None;
        turn.finish_reason = Some("failed".to_string());
        turn.has_final_response = Some(false);
        turn.end_time = Some(now);

        if recovered_generation {
            let context_messages = self.context_store.get_context_messages(session_id);
            self.persistence_manager
                .save_turn_context_snapshot(
                    &workspace_path,
                    session_id,
                    turn.turn_index,
                    &context_messages,
                )
                .await?;
        } else {
            self.persist_context_snapshot_for_turn_best_effort(
                session_id,
                turn.turn_index,
                "turn_failed",
            )
            .await;
        }
        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;

        debug!(
            "Dialog turn marked as failed: turn_id={}, turn_index={}, error={}",
            turn_id, turn.turn_index, error
        );

        Ok(())
    }

    /// Mark a dialog turn as cancelled and persist it. Unlike
    /// `complete_dialog_turn`, this writes `TurnStatus::Cancelled` so the
    /// frontend / persistence layer can distinguish a user-cancelled turn
    /// from a fully-completed one. Any partial assistant content that was
    /// already streamed is preserved in `model_rounds`.
    pub async fn cancel_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<()> {
        self.cancel_dialog_turn_with_messages(session_id, turn_id, &[])
            .await
    }

    pub(crate) async fn cancel_dialog_turn_with_messages(
        &self,
        session_id: &str,
        turn_id: &str,
        generation_messages: &[Message],
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if !self.should_persist_session_id(session_id)
            && !self.transient_turns.contains_key(session_id)
        {
            debug!(
                "Skipping dialog turn persistence for transient session cancellation: session_id={}, turn_id={}",
                session_id, turn_id
            );
            return Ok(());
        }

        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let mut turn = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let recovered_generation = turn.recovery.is_some();
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self::append_generation_rounds(&mut turn, turn_id, generation_messages, now);
        turn.status = TurnStatus::Cancelled;
        turn.recovery = None;
        turn.finish_reason = Some("cancelled".to_string());
        turn.end_time = Some(now);

        if recovered_generation {
            let context_messages = self.context_store.get_context_messages(session_id);
            self.persistence_manager
                .save_turn_context_snapshot(
                    &workspace_path,
                    session_id,
                    turn.turn_index,
                    &context_messages,
                )
                .await?;
        } else {
            self.persist_context_snapshot_for_turn_best_effort(
                session_id,
                turn.turn_index,
                "turn_cancelled",
            )
            .await;
        }

        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;

        debug!(
            "Dialog turn marked as cancelled: turn_id={}, turn_index={}",
            turn_id, turn.turn_index
        );

        Ok(())
    }

    /// Persist a settled, intentionally interrupted user turn without adding a
    /// new persisted enum variant that older builds cannot deserialize.
    pub async fn mark_dialog_turn_interrupted(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<DialogTurnRecoveryData> {
        self.mark_dialog_turn_interrupted_with_messages(session_id, turn_id, &[])
            .await
    }

    pub(crate) async fn mark_dialog_turn_interrupted_with_messages(
        &self,
        session_id: &str,
        turn_id: &str,
        generation_messages: &[Message],
    ) -> OpenBitFunResult<DialogTurnRecoveryData> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        if !self.should_persist_session_id(session_id) {
            return Err(OpenBitFunError::Validation(
                "Recoverable interruption is unavailable for transient sessions".to_string(),
            ));
        }
        let session = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        let turn_index = session
            .dialog_turn_ids
            .len()
            .checked_sub(1)
            .filter(|index| session.dialog_turn_ids[*index] == turn_id)
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Only the latest dialog turn can be interrupted: {turn_id}"
                ))
            })?;
        let workspace_path = self.require_session_storage_path(session_id).await?;
        let mut turn = self
            .persistence_manager
            .load_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {turn_id}"))
            })?;
        if turn.kind != DialogTurnKind::UserDialog {
            return Err(OpenBitFunError::Validation(
                "Only a user dialog turn can be interrupted".to_string(),
            ));
        }
        if turn
            .user_message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("acp_transport"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(OpenBitFunError::Validation(
                "Recoverable interruption is unavailable for ACP turns".to_string(),
            ));
        }
        if let Some(recovery) = turn
            .recovery
            .as_ref()
            .filter(|recovery| recovery.status == DialogTurnRecoveryStatus::Interrupted)
            .cloned()
        {
            if turn.recovery_epoch != Some(recovery.execution_generation) {
                turn.recovery_epoch = Some(recovery.execution_generation);
                self.persistence_manager
                    .save_dialog_turn(&workspace_path, &turn)
                    .await?;
            }
            return Ok(recovery);
        }
        if turn.status != TurnStatus::InProgress {
            return Err(OpenBitFunError::Validation(format!(
                "Dialog turn is not running: {turn_id}"
            )));
        }

        // Recovery is offered only when the exact settled model context is
        // durable. If this write fails, the coordinator falls back to an
        // ordinary non-recoverable cancellation rather than exposing a broken
        // continue button.
        let context_messages = self.context_store.get_context_messages(session_id);
        self.persistence_manager
            .save_turn_context_snapshot(&workspace_path, session_id, turn_index, &context_messages)
            .await?;
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let recovery = match turn.recovery.take() {
            Some(mut recovery) => {
                recovery.status = DialogTurnRecoveryStatus::Interrupted;
                recovery.interrupted_at = Some(now);
                recovery
            }
            None => DialogTurnRecoveryData {
                status: DialogTurnRecoveryStatus::Interrupted,
                execution_generation: 0,
                resume_count: 0,
                interrupted_at: Some(now),
                model_id: session.config.model_id.clone(),
            },
        };
        Self::append_generation_rounds(&mut turn, turn_id, generation_messages, now);
        turn.status = TurnStatus::Cancelled;
        turn.finish_reason = Some("interrupted".to_string());
        turn.end_time = Some(now);
        turn.duration_ms = Some(now.saturating_sub(turn.start_time));
        turn.recovery = Some(recovery.clone());
        turn.recovery_epoch = Some(recovery.execution_generation);
        self.persistence_manager
            .save_dialog_turn(&workspace_path, &turn)
            .await?;
        Ok(recovery)
    }

    /// Reopen the latest settled interrupted user turn from its exact context
    /// snapshot. This does not run prompt-submit hooks or append a user Turn.
    pub(crate) async fn reopen_interrupted_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        expected_execution_generation: u32,
    ) -> OpenBitFunResult<InterruptedTurnRecoveryPlan> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let session = self
            .get_session(session_id)
            .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {session_id}")))?;
        let turn_index = session
            .dialog_turn_ids
            .len()
            .checked_sub(1)
            .filter(|index| session.dialog_turn_ids[*index] == turn_id)
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Only the latest dialog turn can be recovered: {turn_id}"
                ))
            })?;
        let workspace_path = self.require_session_storage_path(session_id).await?;
        let mut turn = self
            .persistence_manager
            .load_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {turn_id}"))
            })?;
        let original_turn = turn.clone();
        let recovery = turn.recovery.as_ref().ok_or_else(|| {
            OpenBitFunError::Validation(format!("Dialog turn has no recovery metadata: {turn_id}"))
        })?;
        if recovery.status != DialogTurnRecoveryStatus::Interrupted
            || recovery.execution_generation != expected_execution_generation
        {
            return Err(OpenBitFunError::Validation(format!(
                "Interrupted turn generation mismatch: expected={}, actual={}",
                expected_execution_generation, recovery.execution_generation
            )));
        }
        if recovery.model_id != session.config.model_id {
            return Err(OpenBitFunError::Validation(
                "The session model changed after interruption; start a new turn instead"
                    .to_string(),
            ));
        }
        if turn
            .agent_type
            .as_deref()
            .is_some_and(|agent_type| agent_type != session.agent_type)
        {
            return Err(OpenBitFunError::Validation(
                "The session agent mode changed after interruption; start a new turn instead"
                    .to_string(),
            ));
        }
        if turn.kind != DialogTurnKind::UserDialog || turn.status != TurnStatus::Cancelled {
            return Err(OpenBitFunError::Validation(format!(
                "Dialog turn is not recoverably interrupted: {turn_id}"
            )));
        }
        if turn
            .user_message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("acp_transport"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(OpenBitFunError::Validation(
                "Interrupted ACP turns cannot be recovered through the native runtime".to_string(),
            ));
        }
        if !matches!(session.state, SessionState::Idle) {
            return Err(OpenBitFunError::Validation(format!(
                "Session must be idle before recovering turn: {turn_id}"
            )));
        }
        let mut messages = self
            .persistence_manager
            .load_turn_context_snapshot(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Context snapshot is unavailable for interrupted turn: {turn_id}"
                ))
            })?;
        normalize_incomplete_tool_calls(
            &mut messages,
            "Tool execution was still in progress while resuming context; no result was available.",
        );
        messages.retain(|message| {
            message.internal_reminder_kind() != Some(InternalReminderKind::InterruptedContinue)
        });
        messages.push(
            Message::internal_reminder(
                InternalReminderKind::InterruptedContinue,
                "Your previous work was interrupted by the user. Continue the same task from the last safe context boundary. Do not repeat completed work or assume that an interrupted tool call had no side effects; verify uncertain external state before acting again.",
            )
            .with_turn_id(turn_id.to_string()),
        );

        let next_generation = recovery.execution_generation.saturating_add(1);
        let next_resume_count = recovery.resume_count.saturating_add(1);
        turn.status = TurnStatus::InProgress;
        turn.finish_reason = None;
        turn.end_time = None;
        turn.duration_ms = None;
        turn.error = None;
        turn.error_detail = None;
        turn.recovery = Some(DialogTurnRecoveryData {
            status: DialogTurnRecoveryStatus::Recovering,
            execution_generation: next_generation,
            resume_count: next_resume_count,
            interrupted_at: recovery.interrupted_at,
            model_id: recovery.model_id.clone(),
        });
        turn.recovery_epoch = Some(next_generation);
        let initial_round_index = turn
            .model_rounds
            .iter()
            .map(|round| round.round_index)
            .max()
            .map_or(0, |index| index.saturating_add(1));
        let agent_type = turn
            .agent_type
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Interrupted turn has no agent type: {turn_id}"
                ))
            })?;
        let user_message_metadata = turn.user_message.metadata.clone();
        let resolved_permission_mode = user_message_metadata
            .as_ref()
            .and_then(|metadata| metadata.get(INTERRUPTED_TURN_PERMISSION_MODE_METADATA_KEY))
            .and_then(serde_json::Value::as_str)
            .and_then(PermissionMode::parse)
            .ok_or_else(|| {
                OpenBitFunError::Validation(
                    "Interrupted turn has no frozen permission mode; start a new turn instead"
                        .to_string(),
                )
            })?;
        let pool_id = user_message_metadata
            .as_ref()
            .and_then(|metadata| {
                metadata.get(crate::agentic::execution::model_pool::POOL_CONTEXT_KEY)
            })
            .and_then(serde_json::Value::as_str);
        let (resolved_model_id, expected_model_binding_fingerprint) = if let Some(pool_id) = pool_id
        {
            let service = crate::service::config::get_global_config_service().await?;
            let pool = service.model_pool_store().await.get(pool_id).await?;
            let config: crate::service::config::types::AIConfig =
                service.get_config(Some("ai")).await?;
            let bound = session
                .config
                .model_pool_binding
                .as_ref()
                .filter(|binding| binding.pool_id == pool_id);
            let model = bound
                .and_then(|binding| {
                    config.models.iter().find(|model| {
                        model.enabled
                            && model.id == binding.model_id
                            && pool.member_model_ids.contains(&model.id)
                            && model_runtime_binding_fingerprint(model)
                                == binding.configuration_fingerprint
                    })
                })
                .or_else(|| {
                    pool.member_model_ids.iter().find_map(|id| {
                        config.models.iter().find(|model| {
                            &model.id == id && model.enabled && model.supports_text_generation()
                        })
                    })
                })
                .ok_or_else(|| {
                    OpenBitFunError::validation(
                        "Interrupted turn model pool has no compatible configured members",
                    )
                })?;
            (model.id.clone(), model_runtime_binding_fingerprint(model))
        } else {
            let resolved_model_id = user_message_metadata
                .as_ref()
                .and_then(|metadata| metadata.get(INTERRUPTED_TURN_RESOLVED_MODEL_ID_METADATA_KEY))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|model_id| !model_id.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(
                        "Interrupted turn has no frozen resolved model; start a new turn instead"
                            .to_string(),
                    )
                })?;
            let expected_model_binding_fingerprint = user_message_metadata
                .as_ref()
                .and_then(|metadata| {
                    metadata.get(INTERRUPTED_TURN_MODEL_BINDING_FINGERPRINT_METADATA_KEY)
                })
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|fingerprint| !fingerprint.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(
                        "Interrupted turn has no frozen model binding; start a new turn instead"
                            .to_string(),
                    )
                })?;
            let ai_config = Self::load_ai_config_for_model_resolution()
                .await
                .ok_or_else(|| {
                    OpenBitFunError::AIClient(
                        "AI configuration is unavailable; retry interrupted turn recovery"
                            .to_string(),
                    )
                })?;
            let canonical_model_id = ai_config
                .resolve_model_reference(&resolved_model_id)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(format!(
                        "The interrupted turn model is unavailable: {resolved_model_id}"
                    ))
                })?;
            let model = ai_config
                .models
                .iter()
                .find(|model| model.enabled && model.id == canonical_model_id)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(format!(
                        "The interrupted turn model is unavailable: {resolved_model_id}"
                    ))
                })?;
            if model_runtime_binding_fingerprint(model) != expected_model_binding_fingerprint {
                return Err(OpenBitFunError::Validation(format!(
                "The interrupted turn model binding changed after interruption: {resolved_model_id}"
            )));
            }
            let resolved_reasoning_preset =
                match user_message_metadata.as_ref().and_then(|metadata| {
                    metadata.get(INTERRUPTED_TURN_REASONING_PRESET_METADATA_KEY)
                }) {
                    Some(serde_json::Value::Null) => None,
                    Some(serde_json::Value::String(value)) => value
                        .trim()
                        .is_empty()
                        .then_some(None)
                        .unwrap_or_else(|| Some(value.trim().to_string())),
                    _ => return Err(OpenBitFunError::Validation(
                        "Interrupted turn has no frozen reasoning preset; start a new turn instead"
                            .to_string(),
                    )),
                };
            let expected_reasoning_fingerprint = user_message_metadata
                .as_ref()
                .and_then(|metadata| {
                    metadata.get(INTERRUPTED_TURN_REASONING_FINGERPRINT_METADATA_KEY)
                })
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|fingerprint| !fingerprint.is_empty())
                .ok_or_else(|| {
                    OpenBitFunError::Validation(
                    "Interrupted turn has no frozen reasoning fingerprint; start a new turn instead"
                        .to_string(),
                )
                })?;
            let reasoning_selection = match user_message_metadata.as_ref().and_then(|metadata| {
                metadata.get(INTERRUPTED_TURN_REASONING_SELECTION_METADATA_KEY)
            }) {
                Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(value)) => value
                    .trim()
                    .is_empty()
                    .then_some(None)
                    .unwrap_or_else(|| Some(value.trim().to_string())),
                _ => return Err(OpenBitFunError::Validation(
                    "Interrupted turn has no frozen reasoning selection; start a new turn instead"
                        .to_string(),
                )),
            };
            if reasoning_selection != session.config.reasoning_preset {
                return Err(OpenBitFunError::Validation(
                "The session reasoning preset changed after interruption; start a new turn instead"
                    .to_string(),
            ));
            }
            let (current_resolved_reasoning_preset, current_reasoning_fingerprint) =
                Self::resolve_effective_reasoning_preset_from_config(
                    &ai_config,
                    &resolved_model_id,
                    reasoning_selection.as_deref(),
                )
                .await?;
            if current_resolved_reasoning_preset != resolved_reasoning_preset {
                return Err(OpenBitFunError::Validation(
                "The dialog turn reasoning contract changed after interruption; start a new turn instead"
                    .to_string(),
            ));
            }
            if current_reasoning_fingerprint != expected_reasoning_fingerprint {
                return Err(OpenBitFunError::Validation(
                "The dialog turn reasoning runtime contract changed after interruption; start a new turn instead"
                    .to_string(),
            ));
            }
            (resolved_model_id, expected_model_binding_fingerprint)
        };
        let user_input = turn.user_message.content.clone();

        let mut updated_session = session.clone();
        updated_session.state = SessionState::Processing {
            current_turn_id: turn_id.to_string(),
            phase: ProcessingPhase::Starting,
        };
        let now = SystemTime::now();
        updated_session.updated_at = now;
        updated_session.last_activity_at = now;

        self.persistence_manager
            .save_turn_context_snapshot(&workspace_path, session_id, turn_index, &messages)
            .await?;
        if let Err(error) = self
            .persistence_manager
            .save_dialog_turn(&workspace_path, &turn)
            .await
        {
            if let Err(rollback_error) = self
                .persistence_manager
                .save_dialog_turn(&workspace_path, &original_turn)
                .await
            {
                return Err(OpenBitFunError::session(format!(
                    "Recovery turn persistence failed and rollback did not complete: session_id={session_id}, turn_id={turn_id}, error={error}, rollback_error={rollback_error}"
                )));
            }
            return Err(error);
        }
        if let Err(error) = self
            .persistence_manager
            .save_session(&workspace_path, &updated_session)
            .await
        {
            let session_rollback = self
                .persistence_manager
                .save_session(&workspace_path, &session)
                .await;
            let turn_rollback = self
                .persistence_manager
                .save_dialog_turn(&workspace_path, &original_turn)
                .await;
            if session_rollback.is_err() || turn_rollback.is_err() {
                return Err(OpenBitFunError::session(format!(
                    "Recovery persistence failed and rollback did not complete: session_id={session_id}, turn_id={turn_id}, error={error}, session_rollback_error={:?}, turn_rollback_error={:?}",
                    session_rollback.err(),
                    turn_rollback.err(),
                )));
            }
            return Err(error);
        }

        self.sessions
            .insert(session_id.to_string(), updated_session);
        self.context_store
            .replace_context(session_id, messages.clone());
        self.clear_turn_settlement_result(session_id, turn_id);

        Ok(InterruptedTurnRecoveryPlan {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            turn_index,
            agent_type,
            execution_generation: next_generation,
            resume_count: next_resume_count,
            initial_round_index,
            resolved_permission_mode,
            resolved_model_id,
            model_binding_fingerprint: expected_model_binding_fingerprint,
            user_input,
            messages,
            user_message_metadata,
        })
    }

    pub(crate) async fn latest_dialog_turn_holds_dispatch(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<bool> {
        if !self.should_persist_session_id(session_id) {
            return Ok(false);
        }
        let Some(session) = self.get_session(session_id) else {
            return Ok(false);
        };
        let Some(turn_index) = session.dialog_turn_ids.len().checked_sub(1) else {
            return Ok(false);
        };
        let workspace_path = self.require_session_storage_path(session_id).await?;
        let Some(turn) = self
            .persistence_manager
            .load_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
        else {
            return Ok(false);
        };
        Ok(turn.status == TurnStatus::Cancelled
            && turn
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.status == DialogTurnRecoveryStatus::Interrupted))
    }

    pub(crate) async fn abandon_interrupted_dialog_turn(
        &self,
        session_id: &str,
        expected_turn_id: Option<&str>,
    ) -> OpenBitFunResult<Option<String>> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        let Some(session) = self.get_session(session_id) else {
            return Ok(None);
        };
        let turn_index = match expected_turn_id {
            Some(expected) => session
                .dialog_turn_ids
                .iter()
                .position(|turn_id| turn_id == expected),
            None => session.dialog_turn_ids.len().checked_sub(1),
        };
        let Some(turn_index) = turn_index else {
            return Ok(None);
        };
        let workspace_path = self.require_session_storage_path(session_id).await?;
        let Some(mut turn) = self
            .persistence_manager
            .load_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
        else {
            return Ok(None);
        };
        if turn.status != TurnStatus::Cancelled
            || !turn
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.status == DialogTurnRecoveryStatus::Interrupted)
        {
            return Ok(None);
        }
        turn.recovery = None;
        turn.finish_reason = Some("cancelled".to_string());
        let turn_id = turn.turn_id.clone();
        self.persistence_manager
            .save_dialog_turn(&workspace_path, &turn)
            .await?;
        Ok(Some(turn_id))
    }

    /// Complete a maintenance turn and persist its synthetic model round payload.
    pub async fn complete_maintenance_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        model_rounds: Vec<ModelRoundData>,
        duration_ms: u64,
    ) -> OpenBitFunResult<()> {
        self.complete_turn_with_model_rounds(
            session_id,
            turn_id,
            model_rounds,
            duration_ms,
            "maintenance_turn_completed",
        )
        .await
    }

    pub(crate) async fn complete_synthetic_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        model_rounds: Vec<ModelRoundData>,
        duration_ms: u64,
    ) -> OpenBitFunResult<()> {
        self.complete_turn_with_model_rounds(
            session_id,
            turn_id,
            model_rounds,
            duration_ms,
            "synthetic_dialog_turn_completed",
        )
        .await
    }

    async fn complete_turn_with_model_rounds(
        &self,
        session_id: &str,
        turn_id: &str,
        model_rounds: Vec<ModelRoundData>,
        duration_ms: u64,
        snapshot_reason: &str,
    ) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(session_id)
            && !self.transient_turns.contains_key(session_id)
        {
            debug!(
                "Skipping turn persistence for transient session completion: session_id={}, turn_id={}, rounds={}, duration_ms={}",
                session_id,
                turn_id,
                model_rounds.len(),
                duration_ms
            );
            return Ok(());
        }

        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let mut turn = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;

        let completion_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        turn.model_rounds = model_rounds;
        turn.status = TurnStatus::Completed;
        turn.duration_ms = Some(duration_ms);
        turn.end_time = Some(completion_timestamp);

        self.persist_context_snapshot_for_turn_best_effort(
            session_id,
            turn.turn_index,
            snapshot_reason,
        )
        .await;

        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;

        Ok(())
    }

    /// Mark a maintenance turn as failed while preserving its synthetic tool state.
    pub async fn fail_maintenance_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        error: String,
        model_rounds: Vec<ModelRoundData>,
    ) -> OpenBitFunResult<()> {
        self.fail_turn_with_model_rounds(
            session_id,
            turn_id,
            error,
            model_rounds,
            "maintenance_turn_failed",
        )
        .await
    }

    pub(crate) async fn fail_synthetic_dialog_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        error: String,
        model_rounds: Vec<ModelRoundData>,
    ) -> OpenBitFunResult<()> {
        self.fail_turn_with_model_rounds(
            session_id,
            turn_id,
            error,
            model_rounds,
            "synthetic_dialog_turn_failed",
        )
        .await
    }

    async fn fail_turn_with_model_rounds(
        &self,
        session_id: &str,
        turn_id: &str,
        error: String,
        model_rounds: Vec<ModelRoundData>,
        snapshot_reason: &str,
    ) -> OpenBitFunResult<()> {
        if !self.should_persist_session_id(session_id)
            && !self.transient_turns.contains_key(session_id)
        {
            debug!(
                "Skipping turn persistence for transient session failure: session_id={}, turn_id={}, rounds={}, error={}",
                session_id,
                turn_id,
                model_rounds.len(),
                error
            );
            return Ok(());
        }

        let workspace_path = self
            .effective_session_storage_path(session_id)
            .await
            .ok_or_else(|| {
                OpenBitFunError::Validation(format!(
                    "Session workspace_path is missing: {}",
                    session_id
                ))
            })?;
        let turn_index = self
            .sessions
            .get(session_id)
            .and_then(|session| session.dialog_turn_ids.iter().position(|id| id == turn_id))
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;
        let mut turn = self
            .load_runtime_dialog_turn(&workspace_path, session_id, turn_index)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Dialog turn not found: {}", turn_id))
            })?;

        let completion_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        turn.model_rounds = model_rounds;
        turn.status = TurnStatus::Error;
        turn.error = Some(error.clone());
        turn.finish_reason = Some("failed".to_string());
        turn.has_final_response = Some(false);
        turn.duration_ms = Some(completion_timestamp.saturating_sub(turn.start_time));
        turn.end_time = Some(completion_timestamp);

        self.persist_context_snapshot_for_turn_best_effort(
            session_id,
            turn.turn_index,
            snapshot_reason,
        )
        .await;

        self.save_runtime_dialog_turn(&workspace_path, &turn)
            .await?;

        debug!(
            "Turn marked as failed: turn_id={}, turn_index={}, error={}",
            turn_id, turn.turn_index, error
        );

        Ok(())
    }

    // ============ Helper Methods ============

    /// Get a best-effort message view for the session.
    /// When persistence is enabled, rebuild from persisted turns so callers see the
    /// canonical turn history instead of the runtime context cache.
    pub async fn get_messages(&self, session_id: &str) -> OpenBitFunResult<Vec<Message>> {
        if self.config.enable_persistence {
            if let Some(workspace_path) = self.effective_session_storage_path(session_id).await {
                let _history_read = self.acquire_session_mutation(session_id).await?;
                let messages = self
                    .rebuild_messages_from_turns(&workspace_path, session_id)
                    .await?;
                if !messages.is_empty() {
                    return Ok(messages);
                }
            }
        }

        Ok(self.context_store.get_context_messages(session_id))
    }

    /// Load canonical persisted turns for a user-facing transcript.
    ///
    /// This is intentionally separate from `get_messages`: runtime context
    /// reconstruction excludes model-invisible maintenance turns, while a
    /// transcript must be able to project those turns for the UI.
    pub(crate) async fn load_persisted_transcript_turns_locked(
        &self,
        session_id: &str,
    ) -> OpenBitFunResult<Option<Vec<DialogTurnData>>> {
        if !self.should_persist_session_id(session_id) {
            return Ok(None);
        }
        let Some(workspace_path) = self.effective_session_storage_path(session_id).await else {
            return Ok(None);
        };
        let mut turns = self
            .persistence_manager
            .load_session_turns(&workspace_path, session_id)
            .await?;
        if let Some(revert) = self
            .persistence_manager
            .load_session_revert_state(&workspace_path, session_id)
            .await?
        {
            turns.retain(|turn| turn.turn_index < revert.boundary_turn);
        }
        Ok(Some(turns))
    }

    /// Get a paginated best-effort message view for the session.
    pub async fn get_messages_paginated(
        &self,
        session_id: &str,
        limit: usize,
        before_message_id: Option<&str>,
    ) -> OpenBitFunResult<(Vec<Message>, bool)> {
        let messages = self.get_messages(session_id).await?;
        Ok(Self::paginate_messages(&messages, limit, before_message_id))
    }

    /// Get session's runtime context messages (may already include compressed reminders).
    pub async fn get_context_messages(&self, session_id: &str) -> OpenBitFunResult<Vec<Message>> {
        let context_messages = self.context_store.get_context_messages(session_id);

        Ok(context_messages)
    }

    /// Add a semantic message to the runtime context cache and immediately refresh the current
    /// turn snapshot so crashes do not lose the latest in-memory context change.
    pub async fn add_message(&self, session_id: &str, message: Message) -> OpenBitFunResult<()> {
        let memory_citation = message.metadata.memory_citation.clone();
        let turn_id = message.metadata.turn_id.clone();
        let round_id = message.metadata.round_id.clone();
        let message_id = message.id.clone();
        self.context_store.add_message(session_id, message);
        if let Some(citation) = memory_citation.as_ref() {
            if let Err(error) = self
                .memory_database
                .record_memory_citation(
                    session_id,
                    turn_id.as_deref(),
                    round_id.as_deref(),
                    &message_id,
                    citation,
                )
                .await
            {
                warn!(
                    "Failed to record memory citation: session_id={}, message_id={}, error={}",
                    session_id, message_id, error
                );
            }
        }
        self.persist_current_turn_context_snapshot_best_effort(session_id, "context_message_added")
            .await;
        Ok(())
    }

    /// Append a complete model round atomically from the context store's point
    /// of view.  In particular, an assistant tool-call message and its tool
    /// results must not be observable by fork/snapshot readers separately.
    pub async fn add_messages(
        &self,
        session_id: &str,
        messages: Vec<Message>,
    ) -> OpenBitFunResult<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        for message in &messages {
            if let Some(citation) = message.metadata.memory_citation.as_ref() {
                if let Err(error) = self
                    .memory_database
                    .record_memory_citation(
                        session_id,
                        message.metadata.turn_id.as_deref(),
                        message.metadata.round_id.as_deref(),
                        &message.id,
                        citation,
                    )
                    .await
                {
                    warn!(
                        "Failed to record memory citation: session_id={}, message_id={}, error={}",
                        session_id, message.id, error
                    );
                }
            }
        }
        self.context_store.add_messages(session_id, messages);
        self.persist_current_turn_context_snapshot_best_effort(
            session_id,
            "context_messages_added",
        )
        .await;
        Ok(())
    }

    /// Replace the runtime context cache for a session and immediately refresh the current turn
    /// snapshot. This is primarily used after compression rewrites the model-visible context.
    pub async fn replace_context_messages(&self, session_id: &str, messages: Vec<Message>) {
        self.context_store
            .replace_context(session_id, messages.clone());
        self.review_read_receipt_store.clear_session(session_id);
        self.prune_token_anchors_to_messages(session_id, &messages)
            .await;
        self.persist_current_turn_context_snapshot_best_effort(session_id, "context_replaced")
            .await;
    }

    /// Caller holds the session mutation guard through the subsequent formal
    /// compression side effects. The context entry lock also fences appends.
    pub(crate) fn transform_compression_context<T>(
        &self,
        session_id: &str,
        transform: impl FnOnce(&[Message]) -> OpenBitFunResult<(Option<Vec<Message>>, T)>,
    ) -> OpenBitFunResult<T> {
        self.context_store
            .try_transform_context(session_id, transform)
    }

    pub(crate) async fn persist_compression_context(&self, session_id: &str, messages: &[Message]) {
        self.review_read_receipt_store.clear_session(session_id);
        self.prune_token_anchors_to_messages(session_id, messages)
            .await;
        self.persist_current_turn_context_snapshot_best_effort(session_id, "context_compressed")
            .await;
    }

    pub fn record_review_read(
        &self,
        session_id: &str,
        logical_path: &str,
        revision: FileRevision,
        start_line: usize,
        end_line: usize,
        total_lines: usize,
    ) {
        self.review_read_receipt_store.record_review_read(
            session_id,
            logical_path,
            revision,
            start_line,
            end_line,
            total_lines,
        );
    }

    pub fn review_read_coverage(
        &self,
        session_id: &str,
        logical_path: &str,
        revision: FileRevision,
        start_line: usize,
        limit: usize,
    ) -> Option<ReviewReadCoverage> {
        self.review_read_receipt_store.review_read_coverage(
            session_id,
            logical_path,
            revision,
            start_line,
            limit,
        )
    }

    /// Get dialog turn count
    pub fn get_turn_count(&self, session_id: &str) -> usize {
        self.sessions
            .get(session_id)
            .map(|s| s.dialog_turn_ids.len())
            .unwrap_or(0)
    }

    /// Get session's compression state
    pub fn get_compression_state(&self, session_id: &str) -> Option<CompressionState> {
        self.sessions
            .get(session_id)
            .map(|s| s.compression_state.clone())
    }

    /// Update session's compression state
    pub async fn update_compression_state(
        &self,
        session_id: &str,
        compression_state: CompressionState,
    ) -> OpenBitFunResult<()> {
        let _mutation_guard = self.acquire_session_mutation(session_id).await?;
        self.update_compression_state_locked(session_id, compression_state)
            .await
    }

    pub(crate) async fn update_compression_state_locked(
        &self,
        session_id: &str,
        compression_state: CompressionState,
    ) -> OpenBitFunResult<()> {
        let effective_path = self.effective_session_storage_path(session_id).await;

        // IMPORTANT: keep the DashMap guard scope short -- do NOT hold it across .await.
        let session_snapshot = if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.compression_state = compression_state;
            session.updated_at = SystemTime::now();
            session.last_activity_at = SystemTime::now();
            if self.config.enable_persistence && self.should_persist_session(&session) {
                Some(session.clone())
            } else {
                None
            }
        } else {
            return Err(OpenBitFunError::NotFound(format!(
                "Session not found: {}",
                session_id
            )));
        };
        // RefMut guard released here -- DashMap shard lock is free.

        if let Some(session) = session_snapshot {
            if let Some(ref workspace_path) = effective_path {
                self.persistence_manager
                    .save_session(workspace_path, &session)
                    .await?;
            }
        }

        Ok(())
    }

    async fn try_generate_session_title_with_ai(
        source: TitleModelSource<'_>,
        user_message: &str,
        max_length: usize,
    ) -> OpenBitFunResult<Option<String>> {
        use crate::util::types::Message;

        // Match agent `LANGUAGE_PREFERENCE`: use `app.language`, not I18nService (see `app_language` module).
        let lang_code = get_app_language_code().await;
        let language_instruction = short_model_user_language_instruction(lang_code.as_str());

        // Construct system prompt
        let system_prompt = format!(
            "You are a professional session title generation assistant. Based on the user's message content, generate a concise and accurate session title.\n\nRequirements:\n- Title should not exceed {} characters\n- {}\n- Concise and accurate, reflecting the conversation topic\n- Do not add quotes or other decorative symbols\n- Return only the title text, no other content",
            max_length, language_instruction
        );

        // Truncate message to save tokens (max 200 characters)
        let truncated_message = if user_message.chars().count() > 200 {
            format!("{}...", user_message.chars().take(200).collect::<String>())
        } else {
            user_message.to_string()
        };

        let user_prompt = format!(
            "User message: {}\n\nPlease generate session title:",
            truncated_message
        );

        // Construct messages (using AIClient's Message type)
        let messages = vec![
            Message {
                role: "system".to_string(),
                content: Some(system_prompt),
                reasoning_content: None,
                thinking_signature: None,
                tool_calls: None,
                tool_call_id: None,
                name: None,
                is_error: None,
                tool_image_attachments: None,
                model_response_replay: None,
            },
            Message {
                role: "user".to_string(),
                content: Some(user_prompt),
                reasoning_content: None,
                thinking_signature: None,
                tool_calls: None,
                tool_call_id: None,
                name: None,
                is_error: None,
                tool_image_attachments: None,
                model_response_replay: None,
            },
        ];

        // Resolve the task model. Inherit uses the session's resolved model
        // identity but deliberately does not carry its reasoning preset.
        let ai_client_factory = get_global_ai_client_factory().await.map_err(|e| {
            OpenBitFunError::AIClient(format!("Failed to get AI client factory: {}", e))
        })?;
        let ai_config = match &source {
            TitleModelSource::Session {
                session: Some(session),
                ..
            } => Self::load_ai_config_for_session(&session.config)
                .await?
                .ok_or_else(|| {
                    OpenBitFunError::AIClient("Failed to load AI configuration".to_string())
                })?,
            TitleModelSource::Session { id, session: None } => {
                return Err(OpenBitFunError::NotFound(format!(
                    "Session not found: {id}"
                )));
            }
            TitleModelSource::Detached => Self::load_ai_config_for_model_resolution()
                .await
                .ok_or_else(|| {
                    OpenBitFunError::AIClient("Failed to load AI configuration".to_string())
                })?,
        };
        let ai_client = match &ai_config.task_models.session_title {
            crate::service::config::types::TaskModelSelection::Fixed { model_id } => {
                ai_client_factory.get_client_resolved(model_id).await
            }
            crate::service::config::types::TaskModelSelection::Inherit
                if matches!(&source, TitleModelSource::Detached) =>
            {
                // A detached caller has no Runtime session to inherit from;
                // use the default primary selection a session without an
                // explicit model would resolve to.
                let resolved_model_id =
                    ai_config
                        .resolve_model_selection("primary")
                        .ok_or_else(|| {
                            OpenBitFunError::AIClient(
                                "Failed to resolve primary model for session title".to_string(),
                            )
                        })?;
                ai_client_factory.get_client_by_id(&resolved_model_id).await
            }
            crate::service::config::types::TaskModelSelection::Inherit => {
                let TitleModelSource::Session { id, session } = &source else {
                    unreachable!("detached title source is handled above");
                };
                let session = session
                    .as_ref()
                    .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {id}")))?;
                let explicit_model_id = session
                    .config
                    .model_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|model_id| !model_id.is_empty());
                let fallback_model_id = if explicit_model_id.is_none() {
                    let workspace = session.config.workspace_id.as_deref();
                    Some(
                        get_agent_registry()
                            .get_model_id_for_agent(&session.agent_type, workspace)
                            .await
                            .map_err(|error| {
                                OpenBitFunError::AIClient(format!(
                                    "Failed to resolve session Agent model: {error}"
                                ))
                            })?,
                    )
                } else {
                    None
                };
                let configured_model_id = explicit_model_id
                    .or(fallback_model_id.as_deref())
                    .unwrap_or("primary");
                let selector = configured_model_id;
                let resolved_model_id =
                    ai_config.resolve_model_selection(selector).ok_or_else(|| {
                        OpenBitFunError::AIClient(format!(
                            "Failed to resolve inherited session model: {selector}"
                        ))
                    })?;
                if matches!(
                    session.config.model_binding_policy,
                    SessionModelBindingPolicy::ApprovedImmutable
                ) {
                    let fingerprint = session
                        .config
                        .model_binding_fingerprint
                        .as_deref()
                        .ok_or_else(|| {
                            OpenBitFunError::AIClient(
                                "Inherited immutable session model has no approved fingerprint"
                                    .to_string(),
                            )
                        })?;
                    ai_client_factory
                        .get_client_by_approved_binding(&resolved_model_id, fingerprint)
                        .await
                } else {
                    ai_client_factory.get_client_by_id(&resolved_model_id).await
                }
            }
        }
        .map_err(|e| OpenBitFunError::AIClient(format!("Failed to get AI client: {}", e)))?;

        let response = ai_client
            .send_message(messages, None)
            .await
            .map_err(|e| OpenBitFunError::ai(format!("AI call failed: {}", e)))?;

        let title = sanitize_plain_model_output(&response.text);
        if title.is_empty() {
            return Ok(None);
        }

        // Truncate title
        let final_title = if title.chars().count() > max_length {
            title.chars().take(max_length).collect::<String>()
        } else {
            title
        };

        Ok(Some(final_title))
    }

    /// Generate a concise session title, using AI first and falling back to a local heuristic.
    pub async fn resolve_session_title(
        &self,
        session_id: &str,
        user_message: &str,
        max_length: Option<usize>,
        allow_ai: bool,
    ) -> ResolvedSessionTitle {
        let source = TitleModelSource::Session {
            id: session_id,
            session: self.get_session(session_id),
        };
        Self::resolve_title_for(source, user_message, max_length, allow_ai).await
    }

    /// Resolve a title for a session the Runtime does not own (for example a
    /// native external-agent session). An `Inherit` title model resolves to
    /// the default `primary` selection; any failure yields the fallback title.
    pub async fn resolve_detached_session_title(
        user_message: &str,
        max_length: Option<usize>,
        allow_ai: bool,
    ) -> ResolvedSessionTitle {
        Self::resolve_title_for(
            TitleModelSource::Detached,
            user_message,
            max_length,
            allow_ai,
        )
        .await
    }

    async fn resolve_title_for(
        source: TitleModelSource<'_>,
        user_message: &str,
        max_length: Option<usize>,
        allow_ai: bool,
    ) -> ResolvedSessionTitle {
        let max_length = max_length.unwrap_or(20).max(1);

        if allow_ai {
            match Self::try_generate_session_title_with_ai(source, user_message, max_length).await {
                Ok(Some(title)) => {
                    return ResolvedSessionTitle {
                        title,
                        method: SessionTitleMethod::Ai,
                    };
                }
                Ok(None) => {
                    warn!("AI session title generation returned empty output; using fallback");
                }
                Err(error) => {
                    warn!("AI session title generation failed; using fallback: {error}");
                }
            }
        }

        ResolvedSessionTitle {
            title: Self::fallback_session_title(user_message, max_length),
            method: SessionTitleMethod::Fallback,
        }
    }

    /// Generate session title
    ///
    /// Generate a concise and accurate session title based on user message content.
    pub async fn generate_session_title(
        &self,
        session_id: &str,
        user_message: &str,
        max_length: Option<usize>,
    ) -> OpenBitFunResult<String> {
        Ok(self
            .resolve_session_title(session_id, user_message, max_length, true)
            .await
            .title)
    }

    // ============ Background Tasks ============

    /// Start auto-save task
    fn spawn_auto_save_task(&self) {
        let sessions = self.sessions.clone();
        let transient_session_ids = self.transient_session_ids.clone();
        let persistence = self.persistence_manager.clone();
        let session_mutation_locks = self.session_mutation_locks.clone();
        let interval = self.config.auto_save_interval;
        let session_storage_path_index = self.session_storage_path_index.clone();

        tokio::spawn(async move {
            let mut ticker = Self::auto_save_interval(interval);

            loop {
                ticker.tick().await;

                for snapshot in Self::collect_auto_save_snapshots(&sessions, &transient_session_ids)
                {
                    let _mutation_guard = session_mutation_locks.lock(&snapshot.session_id).await;
                    if !Self::auto_save_snapshot_is_current(&sessions, &snapshot) {
                        continue;
                    }
                    let storage_path = match Self::committed_session_storage_path(
                        &session_storage_path_index,
                        &snapshot.session_id,
                    ) {
                        Some(path) => Some(path),
                        None => {
                            Self::effective_storage_path_for_config_with_persistence(
                                persistence.as_ref(),
                                &snapshot.session.config,
                            )
                            .await
                        }
                    };
                    if let Some(workspace_path) = storage_path {
                        if !Self::auto_save_snapshot_is_current(&sessions, &snapshot) {
                            continue;
                        }
                        if let Err(e) = persistence
                            .save_session(&workspace_path, &snapshot.session)
                            .await
                        {
                            error!(
                                "Failed to auto-save session: session_id={}, error={}",
                                snapshot.session_id, e
                            );
                        }
                    }
                }
            }
        });

        debug!("Auto-save task started");
    }

    /// Start cleanup task for expired sessions
    fn spawn_cleanup_task(&self) {
        let sessions = self.sessions.clone();
        let active_turn_permission_modes = self.active_turn_permission_modes.clone();
        let transient_session_ids = self.transient_session_ids.clone();
        let active_session_permits = self.active_session_permits.clone();
        let timeout = self.config.session_idle_timeout;
        let persistence = self.persistence_manager.clone();
        let session_storage_path_index = self.session_storage_path_index.clone();
        let enable_persistence = self.config.enable_persistence;
        let session_mutation_locks = self.session_mutation_locks.clone();
        let session_write_locks = self.session_write_locks.clone();
        let context_store = self.context_store.clone();
        let transient_turns = self.transient_turns.clone();
        let prompt_cache_store = self.prompt_cache_store.clone();
        let token_anchor_store = self.token_anchor_store.clone();
        let turn_skill_agent_snapshot_store = self.turn_skill_agent_snapshot_store.clone();
        let skill_agent_baseline_override_snapshot_store =
            self.skill_agent_baseline_override_snapshot_store.clone();
        let edit_constraints_store = self.edit_constraints_store.clone();
        let review_read_receipt_store = self.review_read_receipt_store.clone();
        let evidence_ledger = self.evidence_ledger.clone();

        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(60));

            loop {
                ticker.tick().await;

                let now = SystemTime::now();
                let candidates = Self::collect_expired_session_candidates(
                    &sessions,
                    &transient_session_ids,
                    now,
                    timeout,
                );

                for candidate in candidates {
                    let _mutation_guard = session_mutation_locks.lock(&candidate.session_id).await;
                    debug!(
                        "Cleaning up expired session: session_id={}",
                        candidate.session_id
                    );

                    let cleanup_now = SystemTime::now();
                    let Some(session) = Self::cleanup_snapshot_for_candidate(
                        &sessions,
                        &candidate,
                        cleanup_now,
                        timeout,
                    ) else {
                        continue;
                    };

                    let mut can_remove = true;
                    if enable_persistence
                        && Self::should_persist_session_with_transient_ids(
                            &session,
                            &transient_session_ids,
                        )
                    {
                        let storage_path = match Self::committed_session_storage_path(
                            &session_storage_path_index,
                            &session.session_id,
                        ) {
                            Some(path) => Some(path),
                            None => {
                                Self::effective_storage_path_for_config_with_persistence(
                                    persistence.as_ref(),
                                    &session.config,
                                )
                                .await
                            }
                        };
                        if let Some(workspace_path) = storage_path {
                            if Self::cleanup_snapshot_for_candidate(
                                &sessions,
                                &candidate,
                                SystemTime::now(),
                                timeout,
                            )
                            .is_some()
                            {
                                if let Err(error) =
                                    persistence.save_session(&workspace_path, &session).await
                                {
                                    error!(
                                        "Failed to save Session before idle eviction: session_id={}, error={}",
                                        candidate.session_id, error
                                    );
                                    can_remove = false;
                                }
                            }
                        } else {
                            can_remove = false;
                        }
                    }

                    if !can_remove {
                        continue;
                    }

                    let removal_now = SystemTime::now();
                    if sessions
                        .remove_if(&candidate.session_id, |_, session| {
                            Self::cleanup_candidate_matches_session(
                                session,
                                &candidate,
                                removal_now,
                                timeout,
                            )
                        })
                        .is_some()
                    {
                        active_session_permits.remove(&candidate.session_id);
                        session_write_locks.remove(&candidate.session_id);
                        active_turn_permission_modes.remove(&candidate.session_id);
                        clear_session_runtime_stores(
                            &candidate.session_id,
                            context_store.as_ref(),
                            transient_turns.as_ref(),
                            prompt_cache_store.as_ref(),
                            token_anchor_store.as_ref(),
                            turn_skill_agent_snapshot_store.as_ref(),
                            skill_agent_baseline_override_snapshot_store.as_ref(),
                            review_read_receipt_store.as_ref(),
                            evidence_ledger.as_ref(),
                        );
                        edit_constraints_store.remove(&candidate.session_id);
                    }
                }
            }
        });

        debug!("Cleanup task started");
    }
}

#[cfg(test)]
mod tests;
