//! Pi coding agent (`pi --mode rpc`) identity, resume policy and session.
//!
//! Pi persists sessions per project cwd; the driver pins one Pi session id
//! per OpenBitFun session and always launches with `--session-id <id>`, which
//! opens the transcript or creates it on first launch.
mod cli;
mod errors;
mod projector;
mod session;
pub use session::{PiNativeSession, CLIENT_ID};

use super::{
    NativeAgentCapabilities, NativeAgentDriver, NativeAgentOpenContext, NativeAgentSession,
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

/// The Pi session already holds a transcript under this id.
pub(crate) const RESUME_STRATEGY: &str = "pi-native-rpc";
/// The id is reserved; no prompt reached Pi yet.
pub(crate) const PREPARED_STRATEGY: &str = "pi-native-prepared";

pub struct PiNativeDriver;

#[async_trait]
impl NativeAgentDriver for PiNativeDriver {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn name(&self) -> &'static str {
        "Pi (Native)"
    }
    fn session_label(&self) -> &'static str {
        "Pi native"
    }
    fn command(&self) -> &'static str {
        "pi"
    }
    fn local_executable(&self) -> std::path::PathBuf {
        session::local_executable()
    }
    fn cli_status(
        &self,
        tool: &crate::client::AcpRequirementProbeItem,
        windows: bool,
        remote: bool,
    ) -> Option<crate::client::config::NativeCliStatus> {
        Some(cli::cli_status(tool, windows, remote))
    }
    async fn update_cli(&self) -> OpenBitFunResult<String> {
        cli::run_update(&session::local_executable()).await
    }
    fn capabilities(&self) -> NativeAgentCapabilities {
        capabilities()
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
                OpenBitFunError::NotFound(format!("Pi session record not found: {session_id}"))
            })?;
        let (persisted, resume, turn_index) = pi_state_from_metadata(&metadata)?;
        ensure_bound_cwd(metadata.workspace_path.as_deref(), &workspace_path, resume)?;
        let pi_session_id = persisted.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        persistence
            .update_remote_session_state(
                &session_storage_path,
                &session_id,
                &pi_session_id,
                if resume {
                    RESUME_STRATEGY
                } else {
                    PREPARED_STRATEGY
                },
                None,
            )
            .await?;
        let session = PiNativeSession::new(
            session_id.clone(),
            pi_session_id,
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

/// Only what `pi --mode rpc` actually supports. Pi runs tools without
/// approval prompts, so there is no permission mode; rollback/fork stay off
/// until branch anchoring is implemented.
pub(crate) fn capabilities() -> NativeAgentCapabilities {
    NativeAgentCapabilities {
        select_model: true,
        thinking: true,
        commands: true,
        compact: true,
        context_usage: true,
        input: super::capabilities::NativeInputCapabilities {
            images: true,
            steering: true,
            queue: false,
        },
        // OpenBitFun-managed worktrees; the cwd is fixed once Pi has a transcript.
        worktree: true,
        ..Default::default()
    }
}

/// Pi accepts letters, numbers, `.`, `_` and `-` in `--session-id`.
fn valid_pi_session_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

fn pi_state_from_metadata(
    metadata: &SessionMetadata,
) -> OpenBitFunResult<(Option<String>, bool, usize)> {
    let custom = metadata.custom_metadata.as_ref();
    if custom
        .and_then(|c| c.get(CUSTOM_METADATA_CLIENT_ID_KEY))
        .and_then(Value::as_str)
        != Some(CLIENT_ID)
    {
        return Err(OpenBitFunError::Validation(
            "Session is not a Pi native session".into(),
        ));
    }
    let id = match custom.and_then(|c| c.get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)) {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) if valid_pi_session_id(id) => Some(id.clone()),
        Some(_) => {
            return Err(OpenBitFunError::Validation(
                "Invalid persisted Pi session id; session data was preserved".into(),
            ))
        }
    };
    let resumed = custom
        .and_then(|c| c.get(CUSTOM_METADATA_RESUME_STRATEGY_KEY))
        .and_then(Value::as_str)
        == Some(RESUME_STRATEGY);
    Ok((id.clone(), id.is_some() && resumed, metadata.turn_count))
}

/// Pi groups sessions by cwd, so a session with a transcript must launch in
/// the directory it recorded. Separators, a trailing slash and (on Windows)
/// case are not significant.
fn ensure_bound_cwd(recorded: Option<&str>, requested: &str, bound: bool) -> OpenBitFunResult<()> {
    let normalize = |path: &str| {
        let path = path.replace('\\', "/");
        let path = path.trim_end_matches('/').to_string();
        if cfg!(windows) {
            path.to_lowercase()
        } else {
            path
        }
    };
    match recorded.map(str::trim).filter(|path| !path.is_empty()) {
        Some(recorded) if bound && normalize(recorded) != normalize(requested) => {
            Err(OpenBitFunError::Validation(format!(
                "This Pi session is bound to {recorded}, where its transcript lives; it cannot continue in {requested}. Start a new session instead."
            )))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metadata(custom: Value) -> SessionMetadata {
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            "Pi".into(),
            "acp:pi-native".into(),
            "primary".into(),
        );
        metadata.custom_metadata = Some(custom);
        metadata
    }

    #[test]
    fn metadata_round_trip_resumes_only_after_a_prompt_reached_pi() {
        let fresh = metadata(json!({"acpClientId":"pi-native", "futureField":1}));
        assert_eq!(pi_state_from_metadata(&fresh).unwrap(), (None, false, 0));
        let prepared = metadata(json!({"acpClientId":"pi-native",
            "acpRemoteSessionId":"abc-1", "acpResumeStrategy":"pi-native-prepared"}));
        assert_eq!(
            pi_state_from_metadata(&prepared).unwrap(),
            (Some("abc-1".into()), false, 0)
        );
        let wire = serde_json::to_value(metadata(json!({"acpClientId":"pi-native",
            "acpRemoteSessionId":"abc-1", "acpResumeStrategy":"pi-native-rpc", "futureField":1})))
        .unwrap();
        let restored: SessionMetadata = serde_json::from_value(wire).unwrap();
        assert_eq!(
            pi_state_from_metadata(&restored).unwrap(),
            (Some("abc-1".into()), true, 0)
        );
        assert_eq!(restored.custom_metadata.unwrap()["futureField"], 1);
    }

    #[test]
    fn foreign_or_corrupt_identity_is_refused_without_reset() {
        assert!(pi_state_from_metadata(&metadata(json!({"acpClientId":"claude-native"}))).is_err());
        let corrupt = metadata(json!({"acpClientId":"pi-native", "acpRemoteSessionId":"a b/c"}));
        assert!(pi_state_from_metadata(&corrupt).is_err());
        assert_eq!(
            corrupt.custom_metadata.unwrap()["acpRemoteSessionId"],
            "a b/c"
        );
    }

    #[test]
    fn bound_session_keeps_its_recorded_cwd() {
        assert!(ensure_bound_cwd(Some("D:/repo"), "D:/wt", false).is_ok());
        assert!(ensure_bound_cwd(Some("D:/repo"), r"D:\repo\", true).is_ok());
        assert!(ensure_bound_cwd(Some("D:/repo"), "D:/wt", true).is_err());
    }

    #[test]
    fn capabilities_advertise_no_permission_or_history_support() {
        let caps = capabilities();
        assert!(!caps.permission_mode && !caps.plan && !caps.background_tasks);
        assert!(!caps.history.rollback_last_turn && !caps.history.fork);
        assert!(caps.select_model && caps.thinking && caps.compact && caps.context_usage);
        assert!(caps.input.images && caps.input.steering && !caps.input.queue);
    }
}
