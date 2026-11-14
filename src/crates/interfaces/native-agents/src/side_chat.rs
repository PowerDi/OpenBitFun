//! Durable native side-chat relationships. Forking remains owned by each driver.
use std::path::Path;

use openbitfun_core::service::session::SessionKind;
use openbitfun_core::service::session::{
    apply_session_lineage, SessionMetadata, SessionRelationship, SessionRelationshipKind,
    TurnStatus,
};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde::{Deserialize, Serialize};

use crate::AcpSessionPersistence;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSideChat {
    pub request_id: String,
    pub session_name: String,
}

impl AcpSessionPersistence {
    /// Validate against host history too: a controller may have stale turns.
    pub async fn validate_side_chat_boundary(
        &self,
        storage: &Path,
        source: &str,
        turn_id: &str,
    ) -> OpenBitFunResult<()> {
        let turns = self.manager.load_session_turns(storage, source).await?;
        validate_boundary(&turns, turn_id)
    }

    pub async fn mark_native_side_chat(
        &self,
        storage: &Path,
        child: &str,
        parent: &str,
        turn_id: &str,
        side_chat: &NativeSideChat,
        options: Option<crate::AcpSessionOptions>,
    ) -> OpenBitFunResult<()> {
        if child == parent {
            return Err(OpenBitFunError::Validation(
                "Native fork returned the parent session".into(),
            ));
        }
        let result = async {
            let source = self
                .load_metadata(storage, parent)
                .await?
                .ok_or_else(|| OpenBitFunError::NotFound(format!("Session not found: {parent}")))?;
            let client_id = source.agent_type.strip_prefix("acp:").ok_or_else(|| {
                OpenBitFunError::Validation("Parent is not a native session".into())
            })?;
            let mut preferences = self
                .load_native_preferences(client_id, storage, parent)
                .await;
            if let Some(options) = options {
                preferences.model = options.current_model_id.or(preferences.model);
                for option in options.config_options {
                    if let crate::AcpSessionConfigKind::Select { current_value, .. } = option.kind {
                        match option.id.as_str() {
                            "permission-mode" => preferences.permission_mode = Some(current_value),
                            "thinking" => preferences.thinking = Some(current_value),
                            _ => {}
                        }
                    }
                }
            }
            self.update_metadata(storage, child, |metadata| {
                let custom = metadata
                    .custom_metadata
                    .get_or_insert_with(|| serde_json::json!({}));
                let inherited = serde_json::to_value(&preferences)
                    .map_err(|error| OpenBitFunError::Validation(error.to_string()))?;
                let native = custom
                    .as_object_mut()
                    .and_then(|object| {
                        object
                            .entry("nativeAgentPreferences")
                            .or_insert_with(|| serde_json::json!({}))
                            .as_object_mut()
                    })
                    .ok_or_else(|| {
                        OpenBitFunError::Validation(
                            "Native preferences are unreadable; cannot fork side chat".into(),
                        )
                    })?;
                for (key, value) in inherited.as_object().into_iter().flatten() {
                    native.insert(key.clone(), value.clone());
                }
                metadata.session_name = side_chat.session_name.clone();
                metadata.session_kind = SessionKind::EphemeralChild;
                apply_session_lineage(
                    metadata,
                    SessionRelationship {
                        kind: Some(SessionRelationshipKind::Btw),
                        parent_session_id: Some(parent.into()),
                        parent_dialog_turn_id: Some(turn_id.into()),
                        parent_request_id: Some(side_chat.request_id.clone()),
                        ..Default::default()
                    },
                );
                Ok(())
            })
            .await
        }
        .await;
        if result.is_err() {
            if let Err(error) = self.manager.delete_session(storage, child).await {
                log::warn!(
                    "Failed to remove incomplete native side chat: session_id={} error={}",
                    child,
                    error
                );
            }
        }
        result
    }

    pub async fn save_native_side_chat(
        &self,
        storage: &Path,
        session_id: &str,
    ) -> OpenBitFunResult<()> {
        self.update_metadata(storage, session_id, promote_side_chat)
            .await
    }
}

fn validate_boundary(
    turns: &[openbitfun_core::service::session::DialogTurnData],
    turn_id: &str,
) -> OpenBitFunResult<()> {
    let latest = turns
        .iter()
        .filter(|t| t.status == TurnStatus::Completed)
        .max_by_key(|t| t.turn_index);
    if latest.is_none_or(|turn| turn.turn_id != turn_id) {
        return Err(OpenBitFunError::Validation(
            "Native side chat requires the parent's last completed turn".into(),
        ));
    }
    if latest.is_some_and(|turn| {
        turn.user_message
            .metadata
            .as_ref()
            .and_then(|m| m.get("nativeSteeredInput"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    }) {
        return Err(OpenBitFunError::Validation(
            "Cannot fork a native side chat from a turn that took steered input".into(),
        ));
    }
    Ok(())
}

fn promote_side_chat(metadata: &mut SessionMetadata) -> OpenBitFunResult<()> {
    if !crate::drivers()
        .iter()
        .any(|driver| metadata.agent_type == format!("acp:{}", driver.client_id()))
    {
        return Err(OpenBitFunError::Validation(
            "Session is not a native agent side chat".into(),
        ));
    }
    // Idempotent when an acknowledgement was lost after the host saved it.
    if metadata.relationship.as_ref().and_then(|r| r.kind.as_ref())
        == Some(&SessionRelationshipKind::Btw)
    {
        apply_session_lineage(metadata, SessionRelationship::default());
        metadata.session_kind = SessionKind::Standard;
    } else if metadata.session_kind != SessionKind::Standard {
        return Err(OpenBitFunError::Validation(
            "Session is not a side chat".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn side_chat_promotion_keeps_native_identity_preferences_and_future_metadata() {
        let mut metadata = SessionMetadata::new(
            "child".into(),
            "Side".into(),
            "acp:codex-native".into(),
            "primary".into(),
        );
        metadata.session_kind = SessionKind::EphemeralChild;
        metadata.relationship = Some(SessionRelationship {
            kind: Some(SessionRelationshipKind::Btw),
            parent_session_id: Some("parent".into()),
            ..Default::default()
        });
        metadata.custom_metadata = Some(
            json!({"acpRemoteSessionId":"fork-thread", "nativeAgentPreferences":{"model":"gpt-5.5","permissionMode":"full-access"}, "future": 42}),
        );
        let original_custom = metadata.custom_metadata.clone();
        promote_side_chat(&mut metadata).unwrap();
        promote_side_chat(&mut metadata).unwrap();
        assert_eq!(metadata.session_id, "child");
        assert_eq!(metadata.session_kind, SessionKind::Standard);
        assert_eq!(
            metadata.relationship.as_ref().unwrap().parent_session_id,
            None
        );
        assert_eq!(metadata.custom_metadata, original_custom);
        // Legacy records without side-chat fields remain readable and round-trip.
        let mut old = serde_json::to_value(&metadata).unwrap();
        old.as_object_mut().unwrap().remove("relationship");
        let restored: SessionMetadata = serde_json::from_value(old.clone()).unwrap();
        assert!(restored.relationship.is_none());
        assert_eq!(
            serde_json::to_value(restored).unwrap()["customMetadata"],
            old["customMetadata"]
        );
    }

    #[test]
    fn side_chat_boundary_ignores_running_turn_and_refuses_steered_or_stale_context() {
        use openbitfun_core::service::session::{DialogTurnData, UserMessageData};
        let turn = |id: &str, index| {
            DialogTurnData::new(
                id.into(),
                index,
                "parent".into(),
                UserMessageData {
                    id: id.into(),
                    content: "question".into(),
                    timestamp: 0,
                    metadata: None,
                },
            )
        };
        let mut turns = vec![turn("done", 0), turn("running", 1)];
        turns[0].mark_completed();
        assert!(validate_boundary(&turns, "done").is_ok());
        assert!(validate_boundary(&turns, "running").is_err());
        assert!(validate_boundary(&[], "done").is_err());
        turns[0].user_message.metadata = Some(json!({"nativeSteeredInput":true}));
        assert!(validate_boundary(&turns, "done")
            .unwrap_err()
            .to_string()
            .contains("steered input"));
    }

    #[test]
    fn capabilities_side_chat_reminder_preserves_visible_input() {
        let mut input = crate::NativeTurnInput::from("Why?");
        assert_eq!(input.prompt_text(), "Why?");
        input.metadata = Some(json!({"nativeSideChat": true, "future": 1}));
        let prompt = input.prompt_text();
        assert!(prompt.contains(openbitfun_core::agentic::side_question::btw_system_reminder()));
        assert!(prompt.ends_with("\n\nWhy?"));
        assert_eq!(input.text, "Why?");
        assert_eq!(input.metadata.as_ref().unwrap()["future"], 1);
    }
}
