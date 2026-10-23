//! Claude Code stream-json identity, resume policy and session implementation.
mod background;
mod cli;
mod commands;
mod errors;
mod history;
mod projector;
mod session;
mod tasks;
pub use session::{ClaudeNativeSession, CLIENT_ID, DEFAULT_MODEL_ID};

use super::{
    NativeAgentCapabilities, NativeAgentDriver, NativeAgentOpenContext, NativeAgentSession,
    NativeSubagentCapabilities,
};
use crate::client::session_persistence::{
    CUSTOM_METADATA_CLIENT_ID_KEY, CUSTOM_METADATA_LAST_RESUME_ERROR_KEY,
    CUSTOM_METADATA_REMOTE_SESSION_ID_KEY, CUSTOM_METADATA_RESUME_STRATEGY_KEY,
};
use async_trait::async_trait;
use history::{ClaudeHistory, PendingResume};
use openbitfun_core::service::session::{DialogTurnData, SessionMetadata};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::{json, Value};
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
        NativeAgentCapabilities {
            select_model: true,
            permission_mode: true,
            thinking: true,
            commands: true,
            plan: true,
            background_tasks: true,
            subagents: NativeSubagentCapabilities {
                observe: true,
                read_transcript: true,
                ..Default::default()
            },
            autonomous_turns: true,
            history: super::capabilities::NativeHistoryCapabilities {
                rollback_last_turn: true,
                fork: true,
                import_transcripts: true,
            },
            // `queue` stays off: the host dialog queue drives OpenBitFun
            // turns only; queued Claude messages drain from the composer queue.
            input: super::capabilities::NativeInputCapabilities {
                images: true,
                steering: true,
                queue: false,
            },
            // OpenBitFun-managed worktrees only (never `claude --worktree`);
            // the cwd is fixed once the first turn creates a transcript.
            worktree: true,
            ..Default::default()
        }
    }
    fn fork_metadata(
        &self,
        source: &Value,
        turn_id: &str,
        copied_turn_ids: &[String],
    ) -> OpenBitFunResult<serde_json::Map<String, Value>> {
        claude_fork_metadata(source, turn_id, copied_turn_ids)
    }
    fn import_metadata(
        &self,
        ecosystem_id: &str,
        source_session_id: &str,
        turns: &[DialogTurnData],
    ) -> OpenBitFunResult<serde_json::Map<String, Value>> {
        claude_import_metadata(ecosystem_id, source_session_id, turns)
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
        let history = ClaudeHistory::from_metadata(metadata.custom_metadata.as_ref());
        ensure_bound_cwd(
            metadata.workspace_path.as_deref(),
            &workspace_path,
            resume || history.pending.is_some(),
        )?;
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
        session.apply_history(history);
        session.apply_preferences(
            persistence
                .load_native_preferences(CLIENT_ID, &session_storage_path, &session_id)
                .await,
        );
        Ok(session)
    }
}

/// A transcript-bearing session must launch in the directory it recorded:
/// Claude finds `--resume` transcripts by cwd. Separators, a trailing slash
/// and (on Windows) case are not significant.
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
                "This Claude session is bound to {recorded}, where its transcript lives; it cannot continue in {requested}. Start a new session instead."
            )))
        }
        _ => Ok(()),
    }
}

/// The fork resumes the source transcript (or the transcript the source itself
/// still forks from) at the turn's last entry under a new Claude session id.
fn claude_fork_metadata(
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
            "Session is not a Claude native session".into(),
        ));
    }
    let history = ClaudeHistory::from_metadata(Some(source));
    let (anchors, at) = history.fork_point(turn_id).ok_or_else(|| {
        OpenBitFunError::Validation(
            "This turn predates Claude fork support or never reached Claude; fork a later turn"
                .into(),
        )
    })?;
    let fork_from = match history.pending.and_then(|pending| pending.fork_from) {
        // The source has not written to its own transcript since it forked.
        Some(origin) => origin,
        None => {
            let resumed = source
                .get(CUSTOM_METADATA_RESUME_STRATEGY_KEY)
                .and_then(Value::as_str)
                == Some("claude-native-stream-json");
            source
                .get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)
                .and_then(Value::as_str)
                .filter(|id| resumed && uuid::Uuid::parse_str(id).is_ok())
                .map(str::to_string)
                .ok_or_else(|| {
                    OpenBitFunError::Validation(
                        "The Claude transcript of this session is unavailable for forking".into(),
                    )
                })?
        }
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
    let forked = ClaudeHistory {
        turns,
        pending: Some(PendingResume {
            fork_from: Some(fork_from),
            resume_at: Some(at),
        }),
    };
    let mut keys = serde_json::Map::new();
    keys.insert(
        CUSTOM_METADATA_REMOTE_SESSION_ID_KEY.into(),
        json!(uuid::Uuid::new_v4().to_string()),
    );
    keys.insert(
        CUSTOM_METADATA_RESUME_STRATEGY_KEY.into(),
        json!("claude-native-prepared"),
    );
    keys.insert(CUSTOM_METADATA_LAST_RESUME_ERROR_KEY.into(), Value::Null);
    keys.insert(history::METADATA_KEY.into(), forked.to_value());
    Ok(keys)
}

/// Ecosystem id of Claude Code transcripts imported from `~/.claude/projects`.
const CLAUDE_CODE_ECOSYSTEM: &str = "claude-code";

/// A branch of an imported Claude Code transcript forks that transcript at the
/// last copied turn under a new Claude session id, so the user's original
/// transcript is never written. Imported turns carry the transcript uuids the
/// reader recorded; they become anchors for later rollback and fork.
fn claude_import_metadata(
    ecosystem_id: &str,
    source_session_id: &str,
    turns: &[DialogTurnData],
) -> OpenBitFunResult<serde_json::Map<String, Value>> {
    if ecosystem_id != CLAUDE_CODE_ECOSYSTEM {
        return Err(OpenBitFunError::Validation(format!(
            "Claude native sessions cannot continue {ecosystem_id} transcripts"
        )));
    }
    uuid::Uuid::parse_str(source_session_id).map_err(|_| {
        OpenBitFunError::Validation("The Claude Code transcript id is not a session UUID".into())
    })?;
    let source_field = |turn: &DialogTurnData, key: &str| {
        turn.user_message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let resume_at = turns
        .last()
        .and_then(|turn| source_field(turn, "sourceResumeEntryId"))
        .ok_or_else(|| {
            OpenBitFunError::Validation(
                "The viewed Claude Code history predates native continuation; reopen it to refresh and retry"
                    .into(),
            )
        })?;
    let anchors = turns
        .iter()
        .enumerate()
        .map(|(index, turn)| history::TurnAnchor {
            turn_id: turn.turn_id.clone(),
            turn_index: index,
            prompt_uuid: source_field(turn, "sourceTurnId"),
            last_uuid: source_field(turn, "sourceResumeEntryId"),
            prompt_replayed: false,
        })
        .collect();
    let imported = ClaudeHistory {
        turns: anchors,
        pending: Some(PendingResume {
            fork_from: Some(source_session_id.to_string()),
            resume_at: Some(resume_at),
        }),
    };
    let mut keys = serde_json::Map::new();
    keys.insert(
        CUSTOM_METADATA_REMOTE_SESSION_ID_KEY.into(),
        json!(uuid::Uuid::new_v4().to_string()),
    );
    keys.insert(
        CUSTOM_METADATA_RESUME_STRATEGY_KEY.into(),
        json!("claude-native-prepared"),
    );
    keys.insert(CUSTOM_METADATA_LAST_RESUME_ERROR_KEY.into(), Value::Null);
    keys.insert(history::METADATA_KEY.into(), imported.to_value());
    Ok(keys)
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
    fn bound_session_keeps_its_recorded_cwd() {
        let repo = "D:/repo";
        let worktree = "D:/repo-worktrees/wt-1";
        // Unbound sessions (no transcript yet) may move, e.g. into a worktree.
        assert!(ensure_bound_cwd(Some(repo), worktree, false).is_ok());
        assert!(ensure_bound_cwd(None, worktree, true).is_ok());
        assert!(ensure_bound_cwd(Some(worktree), r"D:\repo-worktrees\wt-1\", true).is_ok());
        let error = ensure_bound_cwd(Some(repo), worktree, true).unwrap_err();
        assert!(error.to_string().contains("Start a new session"), "{error}");
    }

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

#[cfg(test)]
mod claude_import_tests {
    use super::*;
    use openbitfun_core::service::session::UserMessageData;

    const SOURCE: &str = "22222222-2222-4222-8222-222222222222";

    fn turn(id: &str, metadata: Value) -> DialogTurnData {
        DialogTurnData::new(
            id.into(),
            0,
            "branch".into(),
            UserMessageData {
                id: format!("{id}-user"),
                content: "Prompt".into(),
                timestamp: 1,
                metadata: Some(metadata),
            },
        )
    }

    #[test]
    fn imported_branch_forks_original_transcript_on_first_launch() {
        let turns = [
            turn(
                "t0",
                json!({"importedFrom":"claude-code","sourceTurnId":"u1","sourceResumeEntryId":"a1"}),
            ),
            turn(
                "t1",
                json!({"importedFrom":"claude-code","sourceTurnId":"u2","sourceResumeEntryId":"a2"}),
            ),
        ];
        let keys = claude_import_metadata("claude-code", SOURCE, &turns).unwrap();
        let mut custom = Value::Object(keys);
        custom[CUSTOM_METADATA_CLIENT_ID_KEY] = json!(CLIENT_ID);
        let mut metadata = SessionMetadata::new(
            "branch".into(),
            "Imported".into(),
            "acp:claude-native".into(),
            "primary".into(),
        );
        metadata.turn_count = turns.len();
        metadata.custom_metadata = Some(custom.clone());
        // Prepared under a new id: the first launch must not resume it as its own.
        let (id, resume, index) = claude_native_state_from_metadata(&metadata).unwrap();
        let id = id.unwrap();
        assert_ne!(id, SOURCE);
        assert!(!resume);
        assert_eq!(index, 2);
        let history = ClaudeHistory::from_metadata(Some(&custom));
        assert_eq!(
            history::resume_args(&id, resume, history.pending.as_ref()),
            [
                "--resume",
                SOURCE,
                "--fork-session",
                "--session-id",
                &id,
                "--resume-session-at",
                "a2"
            ]
        );
        // Imported turns are anchored so later rollback/fork can find them.
        let anchors: Vec<_> = history
            .turns
            .iter()
            .map(|a| {
                (
                    a.turn_index,
                    a.prompt_uuid.as_deref(),
                    a.last_uuid.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            anchors,
            [(0, Some("u1"), Some("a1")), (1, Some("u2"), Some("a2"))]
        );
    }

    #[test]
    fn import_refuses_other_ecosystems_and_views_without_resume_positions() {
        let legacy = [turn(
            "t0",
            json!({"importedFrom":"claude-code","sourceTurnId":"u1"}),
        )];
        assert!(claude_import_metadata("claude-code", SOURCE, &legacy).is_err());
        let current = [turn("t0", json!({"sourceResumeEntryId":"a1"}))];
        assert!(claude_import_metadata("codex", SOURCE, &current).is_err());
        assert!(claude_import_metadata("claude-code", "not-a-uuid", &current).is_err());
    }
}

#[cfg(test)]
mod claude_fork_tests {
    use super::*;

    fn source(history: Value) -> Value {
        json!({
            "acpClientId": "claude-native",
            "acpRemoteSessionId": "11111111-1111-4111-8111-111111111111",
            "acpResumeStrategy": "claude-native-stream-json",
            "claudeNativeHistory": history,
            "futureField": 1,
        })
    }

    #[test]
    fn fork_resumes_source_transcript_at_turn_and_reindexes_anchors() {
        let custom = source(json!({"turns":[
            {"turnId":"t1","turnIndex":0,"promptUuid":"p1","lastUuid":"a1"},
            {"turnId":"t2","turnIndex":2,"promptUuid":"p2","lastUuid":"a2"},
            {"turnId":"t3","turnIndex":3,"promptUuid":"p3","lastUuid":"a3"}]}));
        let keys =
            claude_fork_metadata(&custom, "t2", &["t1".into(), "tx".into(), "t2".into()]).unwrap();
        assert_eq!(keys["acpResumeStrategy"], "claude-native-prepared");
        assert_ne!(keys["acpRemoteSessionId"], custom["acpRemoteSessionId"]);
        assert!(!keys.contains_key("futureField"));
        let history = ClaudeHistory::from_metadata(Some(&Value::Object(keys)));
        let pending = history.pending.unwrap();
        assert_eq!(
            pending.fork_from.as_deref(),
            Some("11111111-1111-4111-8111-111111111111")
        );
        assert_eq!(pending.resume_at.as_deref(), Some("a2"));
        let indexes: Vec<_> = history.turns.iter().map(|a| a.turn_index).collect();
        assert_eq!(indexes, [0, 2]);
    }

    #[test]
    fn fork_of_unforked_fork_chains_to_origin_and_legacy_turns_are_refused() {
        let custom = source(json!({
            "turns":[{"turnId":"t1","turnIndex":0,"lastUuid":"a1"}],
            "pending":{"forkFrom":"origin","resumeAt":"a1"}}));
        let keys = claude_fork_metadata(&custom, "t1", &["t1".into()]).unwrap();
        let history = ClaudeHistory::from_metadata(Some(&Value::Object(keys)));
        assert_eq!(
            history.pending.unwrap().fork_from.as_deref(),
            Some("origin")
        );
        // Sessions recorded before anchors existed have no fork point.
        let mut legacy = source(Value::Null);
        legacy
            .as_object_mut()
            .unwrap()
            .remove("claudeNativeHistory");
        assert!(claude_fork_metadata(&legacy, "t1", &["t1".into()]).is_err());
    }
}
