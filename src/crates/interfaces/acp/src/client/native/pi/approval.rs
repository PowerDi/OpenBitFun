//! Tool approvals for Pi through the OpenBitFun approval extension.
//!
//! Pi has no approval prompts of its own. The driver loads
//! `approval_extension.ts` with `-e`; its `tool_call` handler opens an RPC
//! `select` dialog (`extension_ui_request`) titled with [`MARKER`] plus the
//! call as JSON, and blocks the call unless the answer allows it. The driver
//! answers from the permission mode, the calls allowed for this session, or a
//! permission card. Other extension dialogs are not approvals.
use std::path::PathBuf;

use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::{json, Value};

use super::projector::card_input;

pub(super) const EXTENSION_SOURCE: &str = include_str!("approval_extension.ts");
pub(super) const MARKER: &str = "openbitfun-approval:";
/// Registered by the extension; its presence in `get_commands` proves it loaded.
pub(super) const BRIDGE_COMMAND: &str = "openbitfun-approvals";
pub(super) const ALLOW_ONCE: &str = "allow_once";
pub(super) const ALLOW_SESSION: &str = "allow_session";
pub(super) const REJECT_ONCE: &str = "reject_once";
pub(super) const REJECT_READ_ONLY: &str = "reject_read_only";

pub(super) const PERMISSION_MODE_CONFIG_ID: &str = "permission-mode";
pub(super) const MODE_ASK: &str = "default";
pub(super) const MODE_READ_ONLY: &str = "read-only";
pub(super) const MODE_FULL_ACCESS: &str = "full-access";
/// id, label, description.
pub(super) const PERMISSION_MODES: &[(&str, &str, &str)] = &[
    (
        MODE_ASK,
        "Ask",
        "Ask before tools that can change files, run commands or reach the network.",
    ),
    (
        MODE_READ_ONLY,
        "Read only",
        "Run read-only tools; block everything else without asking.",
    ),
    (
        MODE_FULL_ACCESS,
        "Full access",
        "Run every tool without asking. Use with care.",
    ),
];

/// One tool call the extension asks about.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ApprovalRequest {
    /// `extension_ui_request` id to answer.
    pub ui_id: Value,
    pub tool_call_id: String,
    pub tool_name: String,
    pub input: Value,
}

impl ApprovalRequest {
    /// The approval dialog of the OpenBitFun extension, or `None` for any
    /// other extension dialog.
    pub(super) fn parse(frame: &Value) -> Option<Self> {
        if frame["method"] != "select" {
            return None;
        }
        let body = frame["title"].as_str()?.strip_prefix(MARKER)?;
        let offers_allow = frame["options"]
            .as_array()
            .is_some_and(|options| options.iter().any(|o| o == ALLOW_ONCE));
        if !offers_allow {
            return None;
        }
        let call: Value = serde_json::from_str(body).ok()?;
        Some(Self {
            ui_id: frame["id"].clone(),
            tool_call_id: call["toolCallId"].as_str()?.to_string(),
            tool_name: call["toolName"].as_str()?.to_string(),
            input: call.get("input").cloned().unwrap_or_else(|| json!({})),
        })
    }

    /// What "allow for this session" covers: one exact shell command, or
    /// every call of any other tool.
    pub(super) fn session_key(&self) -> String {
        match self.tool_name.as_str() {
            "bash" | "powershell" => format!(
                "{}\u{0}{}",
                self.tool_name,
                self.input["command"].as_str().unwrap_or_default()
            ),
            other => other.to_string(),
        }
    }

    /// `backend-event-acppermissionrequest` payload (same card as the other
    /// native agents).
    pub(super) fn permission_payload(&self, permission_id: &str, session_id: &str) -> Value {
        let (card_name, params) = card_input(&self.tool_name, &self.input);
        let option = |id: &str, name: &str, kind: &str| {
            json!({"optionId": id, "name": name, "kind": kind})
        };
        let title = match card_name.as_str() {
            "ExecCommand" => self.input["command"].as_str().map(str::to_string),
            "Edit" | "Write" => self.input["path"].as_str().map(|p| format!("{card_name} {p}")),
            _ => None,
        }
        .unwrap_or_else(|| self.tool_name.clone());
        let kind = match card_name.as_str() {
            "ExecCommand" => "execute",
            "Edit" | "Write" => "edit",
            "Read" | "Grep" | "Glob" | "LS" => "read",
            _ => "other",
        };
        json!({"permissionId": permission_id, "sessionId": session_id, "toolCall": {
            "toolCallId": self.tool_call_id, "title": title, "kind": kind,
            "toolName": card_name, "status": "pending", "rawInput": params, "content": []},
            "options": [
                option(ALLOW_ONCE, "Allow once", "allow_once"),
                option(ALLOW_SESSION, "Allow for this session", "allow_session"),
                option(REJECT_ONCE, "Reject", "reject_once"),
            ]})
    }
}

/// `extension_ui_response` selecting `value`.
pub(super) fn select_response(ui_id: &Value, value: &str) -> Value {
    json!({"type": "extension_ui_response", "id": ui_id, "value": value})
}

/// `extension_ui_response` dismissing a dialog (the extension blocks the call).
pub(super) fn cancel_response(ui_id: &Value) -> Value {
    json!({"type": "extension_ui_response", "id": ui_id, "cancelled": true})
}

/// The extension's option for a user decision.
pub(super) fn decision(
    request: &crate::client::SubmitAcpPermissionResponseRequest,
) -> OpenBitFunResult<&'static str> {
    match request.option_id.as_deref() {
        Some(ALLOW_ONCE) => Ok(ALLOW_ONCE),
        Some(ALLOW_SESSION) => Ok(ALLOW_SESSION),
        Some(REJECT_ONCE) => Ok(REJECT_ONCE),
        None if request.approve => Ok(ALLOW_ONCE),
        None => Ok(REJECT_ONCE),
        Some(other) => Err(OpenBitFunError::Validation(format!(
            "Unknown Pi approval option: {other}"
        ))),
    }
}

/// Answer that needs no user, from the mode or a session grant.
pub(super) fn automatic_decision(mode: &str, allowed_for_session: bool) -> Option<&'static str> {
    match mode {
        MODE_FULL_ACCESS => Some(ALLOW_ONCE),
        MODE_READ_ONLY => Some(REJECT_READ_ONLY),
        _ if allowed_for_session => Some(ALLOW_ONCE),
        _ => None,
    }
}

/// File name versioned by content, so hosts never run a stale bridge.
pub(super) fn extension_file_name() -> String {
    // FNV-1a: stable across builds and platforms.
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in EXTENSION_SOURCE.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("openbitfun-approval-{hash:016x}.ts")
}

/// Write the extension under the app data directory (never the workspace)
/// and return its path.
pub(super) async fn install_local_extension() -> OpenBitFunResult<PathBuf> {
    let base = openbitfun_core::infrastructure::try_get_path_manager_arc()
        .map(|paths| paths.user_data_dir())
        .unwrap_or_else(|_| std::env::temp_dir().join("openbitfun"));
    let dir = base.join("native-agents").join("pi");
    let path = dir.join(extension_file_name());
    if tokio::fs::read_to_string(&path).await.ok().as_deref() == Some(EXTENSION_SOURCE) {
        return Ok(path);
    }
    let error = |e: std::io::Error| {
        OpenBitFunError::service(format!(
            "Failed to install the Pi approval extension at {}: {e}",
            path.display()
        ))
    };
    tokio::fs::create_dir_all(&dir).await.map_err(error)?;
    let staging = dir.join(format!("{}.{}", extension_file_name(), uuid::Uuid::new_v4()));
    tokio::fs::write(&staging, EXTENSION_SOURCE)
        .await
        .map_err(error)?;
    if let Err(e) = tokio::fs::rename(&staging, &path).await {
        let _ = tokio::fs::remove_file(&staging).await;
        // A concurrent launch may have installed the same content.
        if tokio::fs::read_to_string(&path).await.ok().as_deref() != Some(EXTENSION_SOURCE) {
            return Err(error(e));
        }
    }
    Ok(path)
}

const HEREDOC_DELIMITER: &str = "OPENBITFUN_PI_APPROVAL_EOF";

/// Shell lines that write the extension on a remote host (under its cache
/// directory, not the workspace) and leave its path in `$obf_pi_ext`. The
/// launch chain stops if the write fails, so Pi never starts without it.
pub(super) fn remote_install_script() -> String {
    debug_assert!(!EXTENSION_SOURCE.contains(HEREDOC_DELIMITER));
    format!(
        "obf_pi_dir=\"${{XDG_CACHE_HOME:-$HOME/.cache}}/openbitfun/pi\" && mkdir -p \"$obf_pi_dir\" && obf_pi_ext=\"$obf_pi_dir/{name}\" && if [ ! -s \"$obf_pi_ext\" ]; then cat > \"$obf_pi_ext.$$\" <<'{HEREDOC_DELIMITER}' && mv \"$obf_pi_ext.$$\" \"$obf_pi_ext\"\n{EXTENSION_SOURCE}\n{HEREDOC_DELIMITER}\nfi",
        name = extension_file_name(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_marked_selects_are_approvals() {
        let frame = json!({"type":"extension_ui_request","id":"u1","method":"select",
            "title": format!("{MARKER}{}", json!({"toolCallId":"c1","toolName":"bash","input":{"command":"rm -rf x"}})),
            "options":[ALLOW_ONCE, ALLOW_SESSION, REJECT_ONCE, REJECT_READ_ONLY]});
        let request = ApprovalRequest::parse(&frame).unwrap();
        assert_eq!(request.ui_id, "u1");
        assert_eq!((request.tool_call_id.as_str(), request.tool_name.as_str()), ("c1", "bash"));
        assert_eq!(request.session_key(), "bash\u{0}rm -rf x");
        let mut other = frame.clone();
        other["title"] = json!("Allow dangerous command?");
        assert!(ApprovalRequest::parse(&other).is_none());
        let mut confirm = frame.clone();
        confirm["method"] = json!("confirm");
        assert!(ApprovalRequest::parse(&confirm).is_none());
    }

    #[test]
    fn modes_answer_without_a_card_only_when_they_decide() {
        assert_eq!(automatic_decision(MODE_FULL_ACCESS, false), Some(ALLOW_ONCE));
        assert_eq!(automatic_decision(MODE_READ_ONLY, true), Some(REJECT_READ_ONLY));
        assert_eq!(automatic_decision(MODE_ASK, true), Some(ALLOW_ONCE));
        assert_eq!(automatic_decision(MODE_ASK, false), None);
        // Unknown persisted modes fall back to asking.
        assert_eq!(automatic_decision("future", false), None);
    }

    #[test]
    fn extension_options_match_the_driver() {
        for option in [ALLOW_ONCE, ALLOW_SESSION, REJECT_ONCE, REJECT_READ_ONLY] {
            assert!(EXTENSION_SOURCE.contains(&format!("\"{option}\"")));
        }
        assert!(EXTENSION_SOURCE.contains(&format!("\"{MARKER}\"")));
        assert!(EXTENSION_SOURCE.contains(&format!("\"{BRIDGE_COMMAND}\"")));
        assert!(EXTENSION_SOURCE.contains("signal: ctx.signal"));
        assert!(!EXTENSION_SOURCE.contains("timeout"));
        let script = remote_install_script();
        assert!(script.contains(&extension_file_name()));
        assert!(script.ends_with("\nfi"));
    }
}
