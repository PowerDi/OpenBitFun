//! Codex `app-server` (JSON-RPC over stdio) identity, resume policy and session.
mod cli;
mod errors;
mod history;
mod projector;
mod session;
pub use session::{CodexNativeSession, CLIENT_ID};

use super::{
    NativeAgentCapabilities, NativeAgentDriver, NativeAgentOpenContext, NativeAgentSession,
    NativeSubagentCapabilities,
};
use crate::client::session_persistence::{
    CUSTOM_METADATA_CLIENT_ID_KEY, CUSTOM_METADATA_LAST_RESUME_ERROR_KEY,
    CUSTOM_METADATA_REMOTE_SESSION_ID_KEY, CUSTOM_METADATA_RESUME_STRATEGY_KEY,
};
use async_trait::async_trait;
use history::{CodexHistory, PendingFork};
use openbitfun_core::service::session::SessionMetadata;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::{json, Value};
use std::sync::Arc;

pub struct CodexNativeDriver;

#[async_trait]
impl NativeAgentDriver for CodexNativeDriver {
    fn client_id(&self) -> &'static str {
        CLIENT_ID
    }
    fn name(&self) -> &'static str {
        "Codex (Native)"
    }
    fn session_label(&self) -> &'static str {
        "Codex native"
    }
    fn command(&self) -> &'static str {
        "codex"
    }
    fn local_executable(&self) -> std::path::PathBuf {
        cli::local_executable()
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
        cli::run_update(&cli::local_executable()).await
    }
    fn capabilities(&self) -> NativeAgentCapabilities {
        NativeAgentCapabilities {
            select_model: true,
            permission_mode: true,
            thinking: true,
            plan: true,
            history: super::capabilities::NativeHistoryCapabilities {
                // Conversation only: Codex has no file checkpoints.
                rollback_last_turn: true,
                fork: true,
                import_transcripts: false,
            },
            input: super::capabilities::NativeInputCapabilities {
                images: true,
                steering: true,
                queue: false,
            },
            // OpenBitFun-managed worktrees; the thread keeps its first cwd.
            worktree: true,
            // The driver wakes an idle parent when a spawned agent finishes.
            autonomous_turns: true,
            // `/compact` starts `thread/compact/start`; context usage comes
            // from `thread/tokenUsage/updated`.
            compact: true,
            context_usage: true,
            // `spawn_agent` threads are projected as linked child sessions.
            subagents: NativeSubagentCapabilities {
                observe: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn fork_metadata(
        &self,
        source: &Value,
        turn_id: &str,
        copied_turn_ids: &[String],
    ) -> OpenBitFunResult<serde_json::Map<String, Value>> {
        codex_fork_metadata(source, turn_id, copied_turn_ids)
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
                OpenBitFunError::NotFound(format!("Codex session record not found: {session_id}"))
            })?;
        let (thread_id, turn_index) = codex_state_from_metadata(&metadata)?;
        let history = CodexHistory::from_metadata(metadata.custom_metadata.as_ref());
        if thread_id.is_none() {
            persistence
                .set_custom_metadata(
                    &session_storage_path,
                    &session_id,
                    CUSTOM_METADATA_RESUME_STRATEGY_KEY,
                    json!(session::STRATEGY_PREPARED),
                )
                .await?;
        }
        let session = CodexNativeSession::new(
            session_id.clone(),
            thread_id,
            workspace_path,
            remote_connection_id,
            queue,
            Some((persistence.clone(), session_storage_path.clone())),
            turn_index,
            notifier,
        );
        session.apply_history(history);
        session.apply_preferences(
            persistence
                .load_native_preferences(CLIENT_ID, &session_storage_path, &session_id)
                .await,
        );
        Ok(session)
    }
}

/// `(thread id to resume, turn count)`. A thread id is only resumed under the
/// app-server strategy; a malformed id is an error and is never reset.
fn codex_state_from_metadata(
    metadata: &SessionMetadata,
) -> OpenBitFunResult<(Option<String>, usize)> {
    let custom = metadata.custom_metadata.as_ref();
    if custom
        .and_then(|c| c.get(CUSTOM_METADATA_CLIENT_ID_KEY))
        .and_then(Value::as_str)
        != Some(CLIENT_ID)
    {
        return Err(OpenBitFunError::Validation(
            "Session is not a Codex native session".into(),
        ));
    }
    let resumes = custom
        .and_then(|c| c.get(CUSTOM_METADATA_RESUME_STRATEGY_KEY))
        .and_then(Value::as_str)
        == Some(session::STRATEGY_RESUME);
    let thread = match custom.and_then(|c| c.get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)) {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) if !id.trim().is_empty() => Some(id.clone()),
        Some(_) => {
            return Err(OpenBitFunError::Validation(
                "Invalid persisted Codex thread id; session data was preserved".into(),
            ))
        }
    };
    Ok((thread.filter(|_| resumes), metadata.turn_count))
}

/// The fork continues on `thread/fork { lastTurnId }` of the source thread
/// (or of the thread the source itself still forks from) at the turn's
/// Codex turn id.
fn codex_fork_metadata(
    source: &Value,
    turn_id: &str,
    copied_turn_ids: &[String],
) -> OpenBitFunResult<serde_json::Map<String, Value>> {
    if source
        .get(CUSTOM_METADATA_CLIENT_ID_KEY)
        .and_then(Value::as_str)
        != Some(CLIENT_ID)
    {
        return Err(OpenBitFunError::Validation(
            "Session is not a Codex native session".into(),
        ));
    }
    let history = CodexHistory::from_metadata(Some(source));
    let (anchors, last_turn) = history.fork_point(turn_id).ok_or_else(|| {
        OpenBitFunError::Validation("This turn never reached Codex; fork a later turn".into())
    })?;
    let thread_id = match history.pending {
        // The source has not launched its own fork yet; its anchors still
        // name turns of the origin thread.
        Some(pending) => pending.thread_id,
        None => source
            .get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)
            .and_then(Value::as_str)
            .filter(|id| {
                !id.is_empty()
                    && source
                        .get(CUSTOM_METADATA_RESUME_STRATEGY_KEY)
                        .and_then(Value::as_str)
                        == Some(session::STRATEGY_RESUME)
            })
            .map(str::to_string)
            .ok_or_else(|| {
                OpenBitFunError::Validation(
                    "The Codex thread of this session is unavailable for forking".into(),
                )
            })?,
    };
    let turns = anchors
        .into_iter()
        .filter_map(|mut anchor| {
            anchor.turn_index = copied_turn_ids
                .iter()
                .position(|id| *id == anchor.turn_id)?;
            Some(anchor)
        })
        .collect();
    let forked = CodexHistory {
        turns,
        pending: Some(PendingFork {
            thread_id,
            last_turn_id: Some(last_turn),
        }),
    };
    let mut keys = serde_json::Map::new();
    keys.insert(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY.into(), Value::Null);
    keys.insert(
        CUSTOM_METADATA_RESUME_STRATEGY_KEY.into(),
        json!(session::STRATEGY_PREPARED),
    );
    keys.insert(CUSTOM_METADATA_LAST_RESUME_ERROR_KEY.into(), Value::Null);
    keys.insert(history::METADATA_KEY.into(), forked.to_value());
    Ok(keys)
}

#[cfg(test)]
mod codex_driver_tests {
    use super::*;

    fn metadata(custom: Value) -> SessionMetadata {
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            "Native".into(),
            "acp:codex-native".into(),
            "primary".into(),
        );
        metadata.turn_count = 2;
        metadata.custom_metadata = Some(custom);
        metadata
    }

    #[test]
    fn resume_requires_app_server_strategy_and_keeps_corrupt_ids() {
        let prepared = metadata(
            json!({"acpClientId":"codex-native","acpRemoteSessionId":"th1",
            "acpResumeStrategy":"codex-native-prepared"}),
        );
        assert_eq!(codex_state_from_metadata(&prepared).unwrap(), (None, 2));
        let resumed = metadata(
            json!({"acpClientId":"codex-native","acpRemoteSessionId":"th1",
            "acpResumeStrategy":"codex-native-app-server","futureField":1}),
        );
        assert_eq!(
            codex_state_from_metadata(&resumed).unwrap(),
            (Some("th1".into()), 2)
        );
        let corrupt = metadata(json!({"acpClientId":"codex-native","acpRemoteSessionId":7}));
        assert!(codex_state_from_metadata(&corrupt).is_err());
        let other = metadata(json!({"acpClientId":"claude-native"}));
        assert!(codex_state_from_metadata(&other).is_err());
    }

    #[test]
    fn fork_targets_codex_turn_and_chains_unlaunched_forks() {
        let source = json!({
            "acpClientId": "codex-native",
            "acpRemoteSessionId": "th-source",
            "acpResumeStrategy": "codex-native-app-server",
            "codexNativeHistory": {"turns": [
                {"turnId":"t1","turnIndex":0,"codexTurnId":"c1"},
                {"turnId":"t2","turnIndex":1,"codexTurnId":"c2"},
                {"turnId":"t3","turnIndex":2,"codexTurnId":"c3"}]}
        });
        let keys = codex_fork_metadata(&source, "t2", &["t1".into(), "t2".into()]).unwrap();
        assert_eq!(keys["acpRemoteSessionId"], Value::Null);
        assert_eq!(keys["acpResumeStrategy"], "codex-native-prepared");
        let history = CodexHistory::from_metadata(Some(&Value::Object(keys.clone())));
        assert_eq!(
            history.pending,
            Some(PendingFork {
                thread_id: "th-source".into(),
                last_turn_id: Some("c2".into())
            })
        );
        assert_eq!(history.turns.len(), 2);
        // Forking the unlaunched fork branches the origin thread again.
        let mut fork = Value::Object(keys);
        fork["acpClientId"] = json!("codex-native");
        let again = codex_fork_metadata(&fork, "t1", &["t1".into()]).unwrap();
        let history = CodexHistory::from_metadata(Some(&Value::Object(again)));
        assert_eq!(history.pending.unwrap().thread_id, "th-source");
        assert!(codex_fork_metadata(&source, "missing", &[]).is_err());
    }
}
