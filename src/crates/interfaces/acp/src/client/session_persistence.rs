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

pub(super) const CUSTOM_METADATA_CLAUDE_PREFERENCES_KEY: &str = "claudeNativePreferences";

/// Last model / permission mode / thinking level picked for Claude native
/// sessions. Stored per session (resume keeps its own choice) and globally
/// (new sessions start from the most recent choice).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeNativePreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
}

pub(super) struct AcpSessionPersistence {
    manager: PersistenceManager,
    claude_preferences_path: std::path::PathBuf,
}

impl AcpSessionPersistence {
    pub(super) fn new(path_manager: Arc<PathManager>) -> OpenBitFunResult<Self> {
        Ok(Self {
            claude_preferences_path: path_manager
                .user_config_dir()
                .join("claude_native_preferences.json"),
            manager: PersistenceManager::new(path_manager)?,
        })
    }

    async fn load_global_claude_preferences(&self) -> ClaudeNativePreferences {
        match tokio::fs::read(&self.claude_preferences_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => ClaudeNativePreferences::default(),
        }
    }

    /// Session preferences, falling back to the global last choice per field.
    pub(crate) async fn load_claude_preferences(
        &self,
        storage: &Path,
        session_id: &str,
    ) -> ClaudeNativePreferences {
        let global = self.load_global_claude_preferences().await;
        let own = self
            .manager
            .load_session_metadata(storage, session_id)
            .await
            .ok()
            .flatten()
            .and_then(|metadata| metadata.custom_metadata)
            .and_then(|custom| custom.get(CUSTOM_METADATA_CLAUDE_PREFERENCES_KEY).cloned())
            .and_then(|value| serde_json::from_value::<ClaudeNativePreferences>(value).ok())
            .unwrap_or_default();
        ClaudeNativePreferences {
            model: own.model.or(global.model),
            permission_mode: own.permission_mode.or(global.permission_mode),
            thinking: own.thinking.or(global.thinking),
        }
    }

    /// Record a choice on the session and as the global default.
    pub(crate) async fn save_claude_preference(
        &self,
        storage: &Path,
        session_id: &str,
        update: impl Fn(&mut ClaudeNativePreferences),
    ) -> OpenBitFunResult<()> {
        let mut global = self.load_global_claude_preferences().await;
        update(&mut global);
        if let Some(parent) = self.claude_preferences_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| OpenBitFunError::service(error.to_string()))?;
        }
        let bytes = serde_json::to_vec_pretty(&global)
            .map_err(|error| OpenBitFunError::service(error.to_string()))?;
        tokio::fs::write(&self.claude_preferences_path, bytes)
            .await
            .map_err(|error| OpenBitFunError::service(error.to_string()))?;
        self.update_metadata(storage, session_id, |metadata| {
            let mut custom = metadata.custom_metadata.take().unwrap_or_else(|| json!({}));
            ensure_object(&mut custom)?;
            let mut own = custom
                .get(CUSTOM_METADATA_CLAUDE_PREFERENCES_KEY)
                .cloned()
                .and_then(|value| serde_json::from_value::<ClaudeNativePreferences>(value).ok())
                .unwrap_or_default();
            update(&mut own);
            custom[CUSTOM_METADATA_CLAUDE_PREFERENCES_KEY] =
                serde_json::to_value(own).unwrap_or(Value::Null);
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

    pub(super) async fn load_claude_native_state(
        &self,
        storage: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<(Option<String>, bool, usize)> {
        let metadata = self
            .manager
            .load_session_metadata(storage, session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Claude session record not found: {session_id}"))
            })?;
        claude_native_state_from_metadata(&metadata)
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

    async fn update_metadata(
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

fn claude_native_state_from_metadata(
    metadata: &SessionMetadata,
) -> OpenBitFunResult<(Option<String>, bool, usize)> {
    let custom = metadata.custom_metadata.as_ref();
    if custom
        .and_then(|c| c.get(CUSTOM_METADATA_CLIENT_ID_KEY))
        .and_then(Value::as_str)
        != Some("claude-native")
    {
        return Err(OpenBitFunError::Validation(
            "Session is not a Claude native session".into(),
        ));
    }
    let id = match custom.and_then(|c| c.get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)) {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) if uuid::Uuid::parse_str(id).is_ok() => Some(id.clone()),
        Some(_) => {
            return Err(OpenBitFunError::Validation(
                "Invalid persisted Claude UUID; session data was preserved".into(),
            ))
        }
    };
    let prepared = custom
        .and_then(|c| c.get(CUSTOM_METADATA_RESUME_STRATEGY_KEY))
        .and_then(Value::as_str)
        == Some("claude-native-prepared");
    Ok((id.clone(), id.is_some() && !prepared, metadata.turn_count))
}

#[cfg(test)]
mod claude_native_tests {
    use super::*;

    #[test]
    fn legacy_metadata_round_trip_preserves_unknown_fields_and_provider_uuid() {
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            "Native".into(),
            "acp:claude-native".into(),
            "primary".into(),
        );
        metadata.custom_metadata =
            Some(json!({"acpClientId":"claude-native", "futureField":{"keep":true}}));
        let old = serde_json::to_value(&metadata).unwrap();
        let mut restored: SessionMetadata = serde_json::from_value(old.clone()).unwrap();
        assert_eq!(
            claude_native_state_from_metadata(&restored).unwrap(),
            (None, false, 0)
        );
        let id = uuid::Uuid::new_v4().to_string();
        let custom = restored.custom_metadata.as_mut().unwrap();
        custom["acpRemoteSessionId"] = json!(id);
        custom["acpResumeStrategy"] = json!("claude-native-prepared");
        assert!(!claude_native_state_from_metadata(&restored).unwrap().1);
        restored.custom_metadata.as_mut().unwrap()["acpResumeStrategy"] =
            json!("claude-native-stream-json");
        let new_wire = serde_json::to_value(&restored).unwrap();
        let round_trip: SessionMetadata = serde_json::from_value(new_wire).unwrap();
        assert_eq!(
            claude_native_state_from_metadata(&round_trip).unwrap(),
            (Some(id), true, 0)
        );
        assert_eq!(
            round_trip.custom_metadata.unwrap()["futureField"],
            old["customMetadata"]["futureField"]
        );
    }

    #[test]
    fn corrupt_provider_identity_is_not_reset() {
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            "Native".into(),
            "acp:claude-native".into(),
            "primary".into(),
        );
        metadata.custom_metadata =
            Some(json!({"acpClientId":"claude-native", "acpRemoteSessionId":123}));
        assert!(claude_native_state_from_metadata(&metadata).is_err());
        assert_eq!(metadata.custom_metadata.unwrap()["acpRemoteSessionId"], 123);
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
