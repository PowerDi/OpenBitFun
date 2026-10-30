//! Install, version and probe facts for an external agent CLI on one host.
//!
//! Shared by native drivers and the ACP client requirement probes; the wire
//! shapes are persisted in remote capability snapshots and read by the UI.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpClientRequirementProbe {
    pub id: String,
    pub tool: AcpRequirementProbeItem,
    #[serde(default)]
    pub adapter: Option<AcpRequirementProbeItem>,
    pub runnable: bool,
    #[serde(default)]
    pub notes: Vec<String>,
    /// Native drivers only: CLI install, version and update facts. Absent on
    /// generic ACP clients and in snapshots written by older builds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_cli: Option<NativeCliStatus>,
}

/// How the external CLI of a native driver was installed, as far as its path tells.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum NativeCliInstallMethod {
    /// The vendor's own installer (self-updating).
    Native,
    Npm,
    Homebrew,
    Winget,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Install/version/update state of a native driver's CLI on one host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeCliStatus {
    pub install_method: NativeCliInstallMethod,
    /// Parsed `x.y.z`, when the version output could be read.
    pub version: Option<String>,
    pub min_version: String,
    /// True only when the version was parsed and is below `min_version`.
    pub too_old: bool,
    /// The host can run the CLI's own update command from the app.
    pub update_supported: bool,
    /// Official install command for the probed host's platform.
    pub install_command: Option<String>,
    pub docs_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpRequirementProbeItem {
    pub name: String,
    pub installed: bool,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}
