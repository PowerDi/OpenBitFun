use serde::{Deserialize, Serialize};

/// Observational native-driver facts. Missing fields advertise no support.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeAgentCapabilities {
    pub select_model: bool,
    pub permission_mode: bool,
    pub thinking: bool,
    pub commands: bool,
    pub subagents: NativeSubagentCapabilities,
    pub autonomous_turns: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeSubagentCapabilities {
    pub observe: bool,
    pub read_transcript: bool,
    pub interact: bool,
}
