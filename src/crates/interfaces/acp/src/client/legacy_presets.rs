//! Retired generic ACP presets `claude-code` and `codex`.
//!
//! The native drivers `claude-native` and `codex-native` replace them. Old
//! installs still carry the retired ids in two places, and both must keep
//! working without manual repair:
//!
//! - ACP client config entries keyed `claude-code` / `codex`. An entry that is
//!   exactly the retired builtin default is dropped (the native driver covers
//!   it). A user-customized entry is kept as a custom ACP client under a new
//!   id, so the alias below never shadows it.
//! - Sessions created as `acp:claude-code` / `acp:codex`. They are migrated to
//!   the native id the first time they open. History is never touched.
//!
//! Resume limits: claude-agent-acp ran Claude Code with the ACP session id as
//! the Claude session UUID, so a valid UUID resumes natively with `--resume`.
//! A codex-acp session id is not known to be an app-server thread id, so a
//! migrated Codex session starts a fresh thread and tells the user once.

use std::collections::HashMap;

use openbitfun_core::service::session::SessionMetadata;
use serde_json::{json, Value};

use super::config::{AcpClientConfig, AcpClientConfigFile};
use super::session_persistence::{
    CUSTOM_METADATA_CLIENT_ID_KEY, CUSTOM_METADATA_LAST_RESUME_ERROR_KEY,
    CUSTOM_METADATA_REMOTE_SESSION_ID_KEY, CUSTOM_METADATA_RESUME_STRATEGY_KEY,
};

const CLAUDE_LEGACY_ID: &str = "claude-code";
const CODEX_LEGACY_ID: &str = "codex";
const CLAUDE_NATIVE_ID: &str = "claude-native";
const CODEX_NATIVE_ID: &str = "codex-native";

/// The retired id an old session was created under, kept for diagnostics.
pub(crate) const CUSTOM_METADATA_LEGACY_CLIENT_ID_KEY: &str = "acpLegacyClientId";
/// The retired adapter's session id when it cannot be resumed natively.
pub(crate) const CUSTOM_METADATA_LEGACY_REMOTE_SESSION_ID_KEY: &str = "acpLegacyRemoteSessionId";
/// One-time note shown on the first native turn of a migrated session.
pub(crate) const CUSTOM_METADATA_LEGACY_NOTICE_KEY: &str = "acpLegacyMigrationNotice";

const CODEX_FRESH_NOTICE: &str = "This conversation was started with the retired Codex ACP adapter. It cannot be resumed by the native Codex driver, so Codex starts a new conversation from here and does not remember the earlier messages above.";

/// Every launch the retired presets ever shipped as their default.
const RETIRED_DEFAULT_ARGS: &[(&str, &[&str])] = &[
    (
        CLAUDE_LEGACY_ID,
        &["--yes", "@agentclientprotocol/claude-agent-acp@latest"],
    ),
    (
        CLAUDE_LEGACY_ID,
        &["--yes", "@zed-industries/claude-code-acp@latest"],
    ),
    (
        CODEX_LEGACY_ID,
        &["--yes", "@agentclientprotocol/codex-acp@latest"],
    ),
    (
        CODEX_LEGACY_ID,
        &["--yes", "@zed-industries/codex-acp@latest"],
    ),
];

/// Native driver id that replaces a retired preset id.
pub(crate) fn native_alias(client_id: &str) -> Option<&'static str> {
    match client_id {
        CLAUDE_LEGACY_ID => Some(CLAUDE_NATIVE_ID),
        CODEX_LEGACY_ID => Some(CODEX_NATIVE_ID),
        _ => None,
    }
}

fn is_retired_default(client_id: &str, config: &AcpClientConfig) -> bool {
    config.command == "npx"
        && config.env.is_empty()
        && RETIRED_DEFAULT_ARGS.iter().any(|(id, args)| {
            *id == client_id
                && config
                    .args
                    .iter()
                    .map(String::as_str)
                    .eq(args.iter().copied())
        })
}

/// Drop untouched retired presets; keep customized ones under a free custom id.
/// Idempotent: a migrated file has no retired keys left.
pub(crate) fn migrate_retired_preset_configs(config_file: &mut AcpClientConfigFile) {
    for legacy_id in [CLAUDE_LEGACY_ID, CODEX_LEGACY_ID] {
        let Some(config) = config_file.acp_clients.remove(legacy_id) else {
            continue;
        };
        if is_retired_default(legacy_id, &config) {
            log::info!("Dropped retired ACP preset config replaced by a native driver: client_id={legacy_id}");
            continue;
        }
        let target = free_custom_id(&config_file.acp_clients, legacy_id);
        log::info!(
            "Kept customized retired ACP preset config as a custom client: client_id={legacy_id} new_client_id={target}"
        );
        config_file.acp_clients.insert(target, config);
    }
}

fn free_custom_id(existing: &HashMap<String, AcpClientConfig>, legacy_id: &str) -> String {
    let base = format!("{legacy_id}-acp");
    if !existing.contains_key(&base) {
        return base;
    }
    (2..)
        .map(|n| format!("{base}-{n}"))
        .find(|id| !existing.contains_key(id))
        .expect("an unused id exists")
}

/// Rewrite an `acp:claude-code` / `acp:codex` session record to its native
/// driver in place. Returns whether anything changed; dialog turns are not
/// part of the metadata and stay as they are.
pub(crate) fn migrate_legacy_session_metadata(metadata: &mut SessionMetadata) -> bool {
    let custom_id = metadata
        .custom_metadata
        .as_ref()
        .and_then(|custom| custom.get(CUSTOM_METADATA_CLIENT_ID_KEY))
        .and_then(Value::as_str)
        .map(str::to_string);
    let agent_type_id = metadata.agent_type.strip_prefix("acp:").map(str::to_string);
    let Some((legacy_id, native_id)) = [custom_id, agent_type_id]
        .into_iter()
        .flatten()
        .find_map(|id| native_alias(&id).map(|native| (id, native)))
    else {
        return false;
    };

    metadata.agent_type = format!("acp:{native_id}");
    let mut custom = match metadata.custom_metadata.take() {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    custom.insert(CUSTOM_METADATA_CLIENT_ID_KEY.into(), json!(native_id));
    custom.insert(
        CUSTOM_METADATA_LEGACY_CLIENT_ID_KEY.into(),
        json!(legacy_id),
    );
    let remote_id = custom
        .get(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string);

    let resumable_claude = native_id == CLAUDE_NATIVE_ID
        && remote_id
            .as_deref()
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok());
    if resumable_claude {
        custom.insert(
            CUSTOM_METADATA_RESUME_STRATEGY_KEY.into(),
            json!("claude-native-stream-json"),
        );
    } else {
        if let Some(id) = &remote_id {
            custom.insert(
                CUSTOM_METADATA_LEGACY_REMOTE_SESSION_ID_KEY.into(),
                json!(id),
            );
        }
        custom.insert(CUSTOM_METADATA_REMOTE_SESSION_ID_KEY.into(), Value::Null);
        let prepared = if native_id == CLAUDE_NATIVE_ID {
            "claude-native-prepared"
        } else {
            "codex-native-prepared"
        };
        custom.insert(CUSTOM_METADATA_RESUME_STRATEGY_KEY.into(), json!(prepared));
        // Only a conversation that existed upstream loses context.
        if remote_id.is_some() && native_id == CODEX_NATIVE_ID {
            custom.insert(
                CUSTOM_METADATA_LAST_RESUME_ERROR_KEY.into(),
                json!("Retired Codex ACP session cannot be resumed natively; a new Codex thread starts"),
            );
            custom.insert(
                CUSTOM_METADATA_LEGACY_NOTICE_KEY.into(),
                json!(CODEX_FRESH_NOTICE),
            );
        }
    }
    metadata.custom_metadata = Some(Value::Object(custom));
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::config::AcpClientPermissionMode;

    fn legacy_session(agent_type: &str, custom: Value) -> SessionMetadata {
        // Shape written by the retired presets, as read back from disk.
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            "Old".into(),
            agent_type.into(),
            "primary".into(),
        );
        metadata.custom_metadata = Some(custom);
        let wire = serde_json::to_value(&metadata).unwrap();
        serde_json::from_value(wire).unwrap()
    }

    #[test]
    fn legacy_claude_session_resumes_natively_with_its_claude_uuid() {
        let id = "11111111-1111-4111-8111-111111111111";
        let mut metadata = legacy_session(
            "acp:claude-code",
            json!({"provider":"acp","acpClientId":"claude-code","acpRemoteSessionId":id,
                   "acpResumeStrategy":"load","futureField":1}),
        );
        assert!(migrate_legacy_session_metadata(&mut metadata));
        assert_eq!(metadata.agent_type, "acp:claude-native");
        let custom = metadata.custom_metadata.clone().unwrap();
        assert_eq!(custom["acpClientId"], "claude-native");
        assert_eq!(custom["acpRemoteSessionId"], id);
        assert_eq!(custom["acpResumeStrategy"], "claude-native-stream-json");
        assert_eq!(custom["acpLegacyClientId"], "claude-code");
        assert_eq!(custom["futureField"], 1);
        assert!(custom.get(CUSTOM_METADATA_LEGACY_NOTICE_KEY).is_none());
        // Idempotent: a migrated record is left alone.
        assert!(!migrate_legacy_session_metadata(&mut metadata));
    }

    #[test]
    fn legacy_codex_session_starts_fresh_and_keeps_the_old_id() {
        let mut metadata = legacy_session(
            "acp:codex",
            json!({"acpClientId":"codex","acpRemoteSessionId":"0199-legacy",
                   "acpResumeStrategy":"resume"}),
        );
        assert!(migrate_legacy_session_metadata(&mut metadata));
        assert_eq!(metadata.agent_type, "acp:codex-native");
        let custom = metadata.custom_metadata.clone().unwrap();
        assert_eq!(custom["acpClientId"], "codex-native");
        assert_eq!(custom["acpRemoteSessionId"], Value::Null);
        assert_eq!(custom["acpLegacyRemoteSessionId"], "0199-legacy");
        assert_eq!(custom["acpResumeStrategy"], "codex-native-prepared");
        assert!(custom[CUSTOM_METADATA_LEGACY_NOTICE_KEY].is_string());
        assert!(!migrate_legacy_session_metadata(&mut metadata));
    }

    #[test]
    fn unrelated_sessions_are_not_migrated() {
        for (agent_type, client) in [("acp:omp", "omp"), ("acp:codex-native", "codex-native")] {
            let mut metadata = legacy_session(agent_type, json!({ "acpClientId": client }));
            assert!(!migrate_legacy_session_metadata(&mut metadata));
        }
    }

    fn config(command: &str, args: &[&str]) -> AcpClientConfig {
        AcpClientConfig {
            name: None,
            command: command.into(),
            args: args.iter().map(|a| (*a).to_string()).collect(),
            env: HashMap::new(),
            enabled: true,
            readonly: false,
            subagent: Default::default(),
            permission_mode: AcpClientPermissionMode::Ask,
        }
    }

    #[test]
    fn retired_defaults_are_dropped_and_customized_entries_kept() {
        let legacy: AcpClientConfigFile = serde_json::from_value(json!({"acpClients":{
            "claude-code": {"command":"npx","args":["--yes","@zed-industries/claude-code-acp@latest"]},
            "codex": {"name":"Pinned","command":"npx","args":["--yes","@zed-industries/codex-acp@0.16.0"]},
            "codex-acp": {"command":"codex-acp"},
            "omp": {"command":"omp","args":["acp"]}
        }}))
        .unwrap();
        let mut file = legacy;
        migrate_retired_preset_configs(&mut file);
        assert!(!file.acp_clients.contains_key("claude-code"));
        assert!(!file.acp_clients.contains_key("codex"));
        assert_eq!(
            file.acp_clients["codex-acp-2"].name.as_deref(),
            Some("Pinned")
        );
        assert_eq!(file.acp_clients["codex-acp"].command, "codex-acp");
        assert!(file.acp_clients.contains_key("omp"));
        let before = file.acp_clients.len();
        migrate_retired_preset_configs(&mut file);
        assert_eq!(file.acp_clients.len(), before);
        // A default with user env is a customization.
        let mut env = config("npx", &["--yes", "@agentclientprotocol/codex-acp@latest"]);
        env.env.insert("CODEX_HOME".into(), "/x".into());
        assert!(!is_retired_default("codex", &env));
    }
}
