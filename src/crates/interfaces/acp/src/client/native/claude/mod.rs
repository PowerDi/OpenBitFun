//! Claude Code stream-json identity, resume policy and session implementation.
mod commands;
mod projector;
mod session;
mod tasks;
pub use session::{ClaudeNativeSession, CLIENT_ID, DEFAULT_MODEL_ID};

use super::{
    NativeAgentCapabilities, NativeAgentDriver, NativeAgentOpenContext, NativeAgentSession,
    NativeSubagentCapabilities,
};
use crate::client::session_persistence::{
    CUSTOM_METADATA_CLIENT_ID_KEY, CUSTOM_METADATA_REMOTE_SESSION_ID_KEY,
    CUSTOM_METADATA_RESUME_STRATEGY_KEY,
};
use async_trait::async_trait;
use openbitfun_core::service::session::SessionMetadata;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::Value;
use std::sync::Arc;

pub struct ClaudeNativeDriver;

#[async_trait]
impl NativeAgentDriver for ClaudeNativeDriver {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn name(&self) -> &'static str {
        "Claude Code (Native)"
    }
    fn session_label(&self) -> &'static str {
        "Claude native"
    }
    fn command(&self) -> &'static str {
        "claude"
    }
    fn local_executable(&self) -> std::path::PathBuf {
        session::local_executable()
    }
    fn capabilities(&self) -> NativeAgentCapabilities {
        NativeAgentCapabilities {
            select_model: true,
            permission_mode: true,
            thinking: true,
            commands: true,
            plan: true,
            subagents: NativeSubagentCapabilities {
                observe: true,
                read_transcript: true,
                ..Default::default()
            },
            autonomous_turns: true,
            ..Default::default()
        }
    }
    async fn open(
        &self,
        context: NativeAgentOpenContext,
    ) -> OpenBitFunResult<Arc<dyn NativeAgentSession>> {
        let NativeAgentOpenContext {
            session_id,
            workspace_path,
            remote_connection_id,
            session_storage_path,
            queue,
            notifier,
            persistence,
        } = context;
        let metadata = persistence
            .load_metadata(&session_storage_path, &session_id)
            .await?
            .ok_or_else(|| {
                OpenBitFunError::NotFound(format!("Claude session record not found: {session_id}"))
            })?;
        let (persisted, resume, turn_index) = claude_native_state_from_metadata(&metadata)?;
        let provider_id = persisted.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        uuid::Uuid::parse_str(&provider_id).map_err(|_| {
            OpenBitFunError::Validation(
                "Invalid persisted Claude session UUID; session data was preserved".into(),
            )
        })?;
        persistence
            .update_remote_session_state(
                &session_storage_path,
                &session_id,
                &provider_id,
                if resume {
                    "claude-native-stream-json"
                } else {
                    "claude-native-prepared"
                },
                None,
            )
            .await?;
        let session = ClaudeNativeSession::new(
            session_id.clone(),
            provider_id,
            workspace_path,
            remote_connection_id,
            resume,
            queue,
            Some((persistence.clone(), session_storage_path.clone())),
            turn_index,
            notifier,
        );
        session.apply_preferences(
            persistence
                .load_native_preferences(CLIENT_ID, &session_storage_path, &session_id)
                .await,
        );
        Ok(session)
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
    use serde_json::json;

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
