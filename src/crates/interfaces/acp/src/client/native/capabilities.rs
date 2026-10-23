use serde::{Deserialize, Serialize};

/// Observational native-driver facts. Missing fields advertise no support.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeAgentCapabilities {
    pub select_model: bool,
    pub permission_mode: bool,
    pub thinking: bool,
    /// Slash commands published by the external agent itself.
    pub commands: bool,
    /// OpenBitFun prompt commands, actions and local command delegation.
    pub host_commands: bool,
    /// OpenBitFun shared voice transcript history.
    pub voice_history: bool,
    /// OpenBitFun /btw side chats and excerpt Ask.
    pub side_chat: bool,
    pub subagents: NativeSubagentCapabilities,
    pub autonomous_turns: bool,
    pub history: NativeHistoryCapabilities,
    pub input: NativeInputCapabilities,
    pub compact: bool,
    pub plan: bool,
    pub context_usage: bool,
    pub background_tasks: bool,
    pub thread_goal: bool,
    pub worktree: bool,
    pub dispatch: bool,
    pub mcp_selection: bool,
    pub skills: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeSubagentCapabilities {
    pub observe: bool,
    pub read_transcript: bool,
    pub interact: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeHistoryCapabilities {
    pub rollback_last_turn: bool,
    pub fork: bool,
    pub import_transcripts: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeInputCapabilities {
    pub images: bool,
    pub steering: bool,
    pub queue: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::native::{claude::ClaudeNativeDriver, NativeAgentDriver};
    use serde_json::json;

    #[test]
    fn legacy_capabilities_do_not_enable_host_features() {
        let legacy = json!({
            "selectModel": true,
            "permissionMode": true,
            "thinking": true,
            "commands": true,
            "subagents": { "observe": true, "readTranscript": true },
            "autonomousTurns": true,
            "history": { "rollbackLastTurn": false, "fork": true, "importTranscripts": true }
        });
        let caps: NativeAgentCapabilities = serde_json::from_value(legacy.clone()).unwrap();
        assert!(caps.commands && caps.history.fork && caps.history.import_transcripts);
        assert!(!caps.host_commands && !caps.voice_history && !caps.side_chat);
        let round_trip: NativeAgentCapabilities =
            serde_json::from_value(serde_json::to_value(&caps).unwrap()).unwrap();
        assert_eq!(round_trip, caps);
        let serialized = serde_json::to_value(round_trip).unwrap();
        for key in [
            "selectModel",
            "permissionMode",
            "thinking",
            "commands",
            "autonomousTurns",
            "history",
        ] {
            assert_eq!(serialized[key], legacy[key]);
        }
    }

    #[test]
    fn host_capabilities_round_trip_with_independent_camel_case_fields() {
        let caps = NativeAgentCapabilities {
            host_commands: true,
            voice_history: true,
            side_chat: true,
            ..Default::default()
        };
        let serialized = serde_json::to_value(&caps).unwrap();
        for key in ["hostCommands", "voiceHistory", "sideChat"] {
            assert_eq!(serialized[key], true);
        }
        assert_eq!(serialized["commands"], false);
        assert_eq!(serialized["history"]["fork"], false);
        assert_eq!(serialized["history"]["importTranscripts"], false);
        assert_eq!(
            serde_json::from_value::<NativeAgentCapabilities>(serialized).unwrap(),
            caps
        );
    }

    #[test]
    fn claude_keeps_host_features_disabled() {
        let caps = ClaudeNativeDriver.capabilities();
        assert!(!caps.host_commands && !caps.voice_history && !caps.side_chat);
        assert!(caps.history.rollback_last_turn && caps.history.fork);
        assert!(caps.history.import_transcripts);
        assert!(caps.input.images && caps.input.steering && !caps.input.queue);
    }
}
