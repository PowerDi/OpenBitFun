use std::path::Path;
use std::sync::Arc;

use openbitfun_core::agentic::persistence::PersistenceManager;
use openbitfun_core::infrastructure::PathManager;
use openbitfun_core::service::session::{
    SessionMetadata, SESSION_PROVIDER_ACP, SESSION_PROVIDER_METADATA_KEY,
};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::native::{
    preferences::{NativeAgentPreferences, METADATA_KEY},
    NativeAgentPreferencesStore,
};

// Persistence reads these back to tell an externally projected Session from a
// Runtime-owned one, so both sides share one definition. It reaches this crate
// through the Session module it already reads metadata types from, because the
// ACP client owns no direct dependency on the core contracts.
pub(super) const CUSTOM_METADATA_PROVIDER_KEY: &str = SESSION_PROVIDER_METADATA_KEY;
pub(super) const CUSTOM_METADATA_PROVIDER_VALUE: &str = SESSION_PROVIDER_ACP;
pub(super) const CUSTOM_METADATA_CLIENT_ID_KEY: &str = "acpClientId";
pub(super) const CUSTOM_METADATA_REMOTE_SESSION_ID_KEY: &str = "acpRemoteSessionId";
pub(super) const CUSTOM_METADATA_RESUME_STRATEGY_KEY: &str = "acpResumeStrategy";
pub(super) const CUSTOM_METADATA_LAST_RESUME_ERROR_KEY: &str = "acpLastResumeError";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAcpFlowSessionRecordResponse {
    pub session_id: String,
    pub session_name: String,
    pub agent_type: String,
}

pub(super) struct AcpSessionPersistence {
    manager: PersistenceManager,
    native_preferences: NativeAgentPreferencesStore,
}

impl AcpSessionPersistence {
    pub(super) fn new(path_manager: Arc<PathManager>) -> OpenBitFunResult<Self> {
        Ok(Self {
            native_preferences: NativeAgentPreferencesStore::new(&path_manager.user_config_dir()),
            manager: PersistenceManager::new(path_manager)?,
        })
    }

    /// Session preferences fall back to this client's global choice per field.
    pub(crate) async fn load_native_preferences(
        &self,
        client_id: &str,
        storage: &Path,
        session_id: &str,
    ) -> NativeAgentPreferences {
        let global = self.native_preferences.get(client_id).await;
        let own = self
            .manager
            .load_session_metadata(storage, session_id)
            .await
            .ok()
            .flatten()
            .and_then(|metadata| metadata.custom_metadata)
            .map(|custom| NativeAgentPreferences::from_metadata(&custom, client_id))
            .unwrap_or_default();
        NativeAgentPreferences {
            model: own.model.or(global.model),
            permission_mode: own.permission_mode.or(global.permission_mode),
            thinking: own.thinking.or(global.thinking),
        }
    }

    /// Record the choice on the session and as this client's global default.
    pub(crate) async fn save_native_preference(
        &self,
        client_id: &str,
        storage: &Path,
        session_id: &str,
        update: impl Fn(&mut NativeAgentPreferences),
    ) -> OpenBitFunResult<()> {
        self.native_preferences.update(client_id, &update).await?;
        self.update_metadata(storage, session_id, |metadata| {
            let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
            ensure_object(&mut custom)?;
            let mut own = NativeAgentPreferences::from_metadata(&custom, client_id);
            update(&mut own);
            custom[METADATA_KEY] = serde_json::to_value(own).unwrap_or(Value::Null);
            metadata.custom_metadata = Some(custom);
            Ok(())
        })
        .await
    }

    pub(super) async fn create_flow_session_record(
        &self,
        session_storage_path: &Path,
        workspace_id: &str,
        workspace_path: &str,
        client_id: &str,
        session_name: Option<String>,
    ) -> OpenBitFunResult<CreateAcpFlowSessionRecordResponse> {
        let session_id = format!("acp_{}_{}", client_id, uuid::Uuid::new_v4());
        let agent_type = format!("acp:{}", client_id);
        let session_name = session_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{} ACP", client_id));

        let mut metadata = SessionMetadata::new(
            session_id.clone(),
            session_name.clone(),
            agent_type.clone(),
            "primary".to_string(),
        );
        metadata.workspace_id = Some(workspace_id.to_string());
        metadata.workspace_path = Some(workspace_path.to_string());
        metadata.custom_metadata = Some(json!({
            "kind": "normal",
            CUSTOM_METADATA_PROVIDER_KEY: CUSTOM_METADATA_PROVIDER_VALUE,
            CUSTOM_METADATA_CLIENT_ID_KEY: client_id,
            CUSTOM_METADATA_REMOTE_SESSION_ID_KEY: null,
            CUSTOM_METADATA_RESUME_STRATEGY_KEY: null,
            CUSTOM_METADATA_LAST_RESUME_ERROR_KEY: null,
        }));

        if !self
            .manager
            .create_session_metadata_if_absent(session_storage_path, &metadata)
            .await?
        {
            return Err(OpenBitFunError::Validation(format!(
                "ACP flow session ID already exists: {}",
                session_id
            )));
        }

        Ok(CreateAcpFlowSessionRecordResponse {
            session_id,
            session_name,
            agent_type,
        })
    }

    /// Persist a hidden subagent child record so the frontend can save and
    /// reload its transcript (same shape as a core subagent session).
    pub(crate) async fn create_subagent_session_record(
        &self,
        session_storage_path: &Path,
        child_session_id: &str,
        parent: &SessionMetadata,
        relationship: openbitfun_core::service::session::SessionRelationship,
        title: String,
        model_name: Option<String>,
    ) -> OpenBitFunResult<()> {
        // The child mode is the Claude subagent type, never `acp:claude-native`:
        // that mode would route a child submission into a new Claude session.
        let agent_type = relationship
            .subagent_type
            .clone()
            .unwrap_or_else(|| "subagent".to_string());
        let mut metadata = SessionMetadata::new(
            child_session_id.to_string(),
            title,
            agent_type,
            model_name.unwrap_or_else(|| parent.model_name.clone()),
        );
        metadata.workspace_id = parent.workspace_id.clone();
        metadata.workspace_path = parent.workspace_path.clone();
        metadata.session_kind = openbitfun_core::service::session::SessionKind::Subagent;
        metadata.created_by = Some(format!("session-{}", parent.session_id));
        openbitfun_core::service::session::apply_session_lineage(&mut metadata, relationship);
        // Mark the child as externally projected so the desktop saves its
        // turns as-is instead of loading it as a core runtime session.
        metadata.custom_metadata = Some(json!({
            CUSTOM_METADATA_PROVIDER_KEY: CUSTOM_METADATA_PROVIDER_VALUE,
        }));
        self.manager
            .create_session_metadata_if_absent(session_storage_path, &metadata)
            .await?;
        Ok(())
    }

    pub(crate) async fn load_metadata(
        &self,
        session_storage_path: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<Option<SessionMetadata>> {
        self.manager
            .load_session_metadata(session_storage_path, session_id)
            .await
    }

    pub(super) async fn delete_flow_session_record(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
    ) -> OpenBitFunResult<()> {
        self.manager
            .delete_session(session_storage_path, openbitfun_session_id)
            .await
    }

    pub(super) async fn load_remote_session_id(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
    ) -> OpenBitFunResult<Option<String>> {
        let Some(metadata) = self
            .manager
            .load_session_metadata(session_storage_path, openbitfun_session_id)
            .await?
        else {
            return Ok(None);
        };

        Ok(metadata
            .custom_metadata
            .as_ref()
            .and_then(|custom| custom.get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string))
    }

    pub(super) async fn update_remote_session_state(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        remote_session_id: &str,
        resume_strategy: &str,
        last_resume_error: Option<String>,
    ) -> OpenBitFunResult<()> {
        self.update_metadata(session_storage_path, openbitfun_session_id, |metadata| {
            let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
            ensure_object(&mut custom)?;
            custom[CUSTOM_METADATA_PROVIDER_KEY] = json!(CUSTOM_METADATA_PROVIDER_VALUE);
            custom[CUSTOM_METADATA_REMOTE_SESSION_ID_KEY] = json!(remote_session_id);
            custom[CUSTOM_METADATA_RESUME_STRATEGY_KEY] = json!(resume_strategy);
            custom[CUSTOM_METADATA_LAST_RESUME_ERROR_KEY] =
                last_resume_error.map(Value::String).unwrap_or(Value::Null);
            metadata.custom_metadata = Some(custom);
            metadata.touch();
            Ok(())
        })
        .await
    }

    pub(super) async fn update_model_id(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        model_id: &str,
    ) -> OpenBitFunResult<()> {
        self.update_metadata(session_storage_path, openbitfun_session_id, |metadata| {
            metadata.model_name = model_id.to_string();
            metadata.touch();
            Ok(())
        })
        .await
    }

    /// Replace one driver-owned custom-metadata key, keeping every other field.
    pub(crate) async fn set_custom_metadata(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        key: &str,
        value: Value,
    ) -> OpenBitFunResult<()> {
        self.update_metadata(session_storage_path, openbitfun_session_id, |metadata| {
            let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
            ensure_object(&mut custom)?;
            custom[key] = value;
            metadata.custom_metadata = Some(custom);
            Ok(())
        })
        .await
    }

    /// Copy turns through `turn_id` into a new session, then let `metadata`
    /// replace driver-owned custom keys. The copy is deleted if that fails.
    pub(crate) async fn branch_native_session(
        &self,
        session_storage_path: &Path,
        source_session_id: &str,
        turn_id: &str,
        metadata: impl FnOnce(&Value, &[String]) -> OpenBitFunResult<serde_json::Map<String, Value>>,
    ) -> OpenBitFunResult<CreateAcpFlowSessionRecordResponse> {
        use openbitfun_core::agentic::persistence::SessionBranchRequest;
        let source = self
            .manager
            .load_session_metadata(session_storage_path, source_session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {source_session_id}"))
            })?;
        let source_custom = source.custom_metadata.clone().unwrap_or_else(|| json!({}));
        let branch = self
            .manager
            .branch_session(
                session_storage_path,
                &SessionBranchRequest {
                    source_session_id: source_session_id.to_string(),
                    source_turn_id: turn_id.to_string(),
                    boundary: Default::default(),
                },
            )
            .await?;
        let result = async {
            let turn_ids = self
                .manager
                .load_session_turns(session_storage_path, &branch.session_id)
                .await?
                .into_iter()
                .map(|turn| turn.turn_id)
                .collect::<Vec<_>>();
            let keys = metadata(&source_custom, &turn_ids)?;
            self.update_metadata(session_storage_path, &branch.session_id, |metadata| {
                let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
                ensure_object(&mut custom)?;
                for (key, value) in keys {
                    custom[key.as_str()] = value;
                }
                metadata.custom_metadata = Some(custom);
                Ok(())
            })
            .await
        }
        .await;
        if let Err(error) = result {
            if let Err(cleanup) = self
                .manager
                .delete_session(session_storage_path, &branch.session_id)
                .await
            {
                log::warn!(
                    "Failed to remove incomplete native session fork: session_id={} error={}",
                    branch.session_id,
                    cleanup
                );
            }
            return Err(error);
        }
        Ok(CreateAcpFlowSessionRecordResponse {
            session_id: branch.session_id,
            session_name: branch.session_name,
            agent_type: branch.agent_type,
        })
    }

    /// Copy a followed external history view through `turn_id` into a new
    /// session owned by `client_id`; `metadata` receives the source ecosystem,
    /// its session id and the copied turns, and returns the driver keys. The
    /// copy is deleted if that fails, so the source view stays the only record.
    pub(crate) async fn branch_imported_session(
        &self,
        session_storage_path: &Path,
        source_session_id: &str,
        turn_id: &str,
        client_id: &str,
        metadata: impl FnOnce(
            &str,
            &str,
            &[openbitfun_core::service::session::DialogTurnData],
        ) -> OpenBitFunResult<serde_json::Map<String, Value>>,
    ) -> OpenBitFunResult<CreateAcpFlowSessionRecordResponse> {
        let source = self
            .manager
            .load_session_metadata(session_storage_path, source_session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Session not found: {source_session_id}"))
            })?;
        let (ecosystem, source_id) = imported_source_identity(&source).ok_or_else(|| {
            OpenBitFunError::Validation("Session is not an imported external history".into())
        })?;
        let branch = self
            .manager
            .branch_external_history(session_storage_path, source_session_id, turn_id)
            .await?;
        let agent_type = format!("acp:{client_id}");
        let result = async {
            let turns = self
                .manager
                .load_session_turns(session_storage_path, &branch.session_id)
                .await?;
            let keys = metadata(&ecosystem, &source_id, &turns)?;
            self.update_metadata(session_storage_path, &branch.session_id, |metadata| {
                metadata.agent_type = agent_type.clone();
                let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
                ensure_object(&mut custom)?;
                custom["kind"] = json!("normal");
                custom[CUSTOM_METADATA_PROVIDER_KEY] = json!(CUSTOM_METADATA_PROVIDER_VALUE);
                custom[CUSTOM_METADATA_CLIENT_ID_KEY] = json!(client_id);
                for (key, value) in keys {
                    custom[key.as_str()] = value;
                }
                metadata.custom_metadata = Some(custom);
                Ok(())
            })
            .await
        }
        .await;
        if let Err(error) = result {
            if let Err(cleanup) = self
                .manager
                .delete_session(session_storage_path, &branch.session_id)
                .await
            {
                log::warn!(
                    "Failed to remove incomplete imported session branch: session_id={} error={}",
                    branch.session_id,
                    cleanup
                );
            }
            return Err(error);
        }
        Ok(CreateAcpFlowSessionRecordResponse {
            session_id: branch.session_id,
            session_name: branch.session_name,
            agent_type,
        })
    }

    /// Refuse a rollback whose target is not the latest persisted turn.
    pub(crate) async fn ensure_latest_turn(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<()> {
        let turns = self
            .manager
            .load_session_turns(session_storage_path, openbitfun_session_id)
            .await?;
        match turns.iter().max_by_key(|turn| turn.turn_index) {
            Some(latest) if latest.turn_id == turn_id => Ok(()),
            Some(_) => Err(OpenBitFunError::Validation(
                "Only the latest turn of a native agent session can be rolled back".into(),
            )),
            None => Err(OpenBitFunError::Validation(
                "The turn to roll back has not been saved yet; retry shortly".into(),
            )),
        }
    }

    pub(crate) async fn truncate_turns_from(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        turn_index: usize,
    ) -> OpenBitFunResult<()> {
        self.manager
            .delete_dialog_turns_from(session_storage_path, openbitfun_session_id, turn_index)
            .await
    }

    pub(super) async fn update_metadata(
        &self,
        session_storage_path: &Path,
        openbitfun_session_id: &str,
        update: impl FnOnce(&mut SessionMetadata) -> OpenBitFunResult<()>,
    ) -> OpenBitFunResult<()> {
        self.manager
            .update_session_metadata_if_present(session_storage_path, openbitfun_session_id, update)
            .await
            .map(|_| ())
    }
}

fn ensure_object(value: &mut Value) -> OpenBitFunResult<()> {
    if value.is_object() {
        return Ok(());
    }

    *value = json!({});
    if value.is_object() {
        Ok(())
    } else {
        Err(OpenBitFunError::service(
            "Failed to initialize ACP session custom metadata".to_string(),
        ))
    }
}

/// Ecosystem and source session id of a followed external history view.
fn imported_source_identity(metadata: &SessionMetadata) -> Option<(String, String)> {
    let custom = metadata.custom_metadata.as_ref()?;
    if custom.get("externalHistoryMode").and_then(Value::as_str) != Some("follow") {
        return None;
    }
    let field = |key: &str| {
        custom
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    Some((field("importedFrom")?, field("sourceSessionId")?))
}
