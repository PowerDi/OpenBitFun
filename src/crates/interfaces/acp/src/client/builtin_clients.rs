use std::collections::HashMap;

use super::config::{AcpClientConfig, AcpClientPermissionMode};

pub(crate) struct BuiltinAcpClientPreset {
    pub(crate) id: &'static str,
    pub(crate) command: &'static str,
    pub(crate) args: &'static [&'static str],
    pub(crate) tool_command: &'static str,
    /// npm package OpenBitFun can install on the user's behalf. `None` means the
    /// agent is user-managed (OpenBitFun only provides the integration, the user
    /// installs the CLI themselves) — the UI then shows no one-click installer.
    pub(crate) install_package: Option<&'static str>,
    pub(crate) adapter_package: Option<&'static str>,
    pub(crate) adapter_bin: Option<&'static str>,
    /// A profile directory OpenBitFun ships and copies into the agent's own home
    /// before launching it. `None` — every preset but dsh — means the CLI is
    /// self-contained and the command runs as-is.
    pub(crate) bundled_profile: Option<&'static str>,
}

/// The profile directory OpenBitFun materializes for DeepSeek Harness. See
/// `dsh_profile.rs`: the bridge ships with OpenBitFun, the harness does not.
pub(crate) const DSH_BUNDLED_PROFILE: &str = "openbitfun-acp";

const BUILTIN_ACP_CLIENT_PRESETS: &[BuiltinAcpClientPreset] = &[
    BuiltinAcpClientPreset {
        id: "opencode",
        command: "opencode",
        args: &["acp"],
        tool_command: "opencode",
        install_package: Some("opencode-ai"),
        adapter_package: None,
        adapter_bin: None,
        bundled_profile: None,
    },
    // DeepSeek Harness (dsh) — the harness has no ACP entry point of its own,
    // so OpenBitFun ships one as a dsh PROFILE (packages/dsh-acp) and launches it
    // through the user's own installation. The model and the API key stay in
    // dsh, where the user configured them; OpenBitFun stores neither. Installable
    // from npm, hence install_package; native ACP, hence no adapter.
    BuiltinAcpClientPreset {
        id: "dsh",
        command: "dsh",
        args: &["--profile", DSH_BUNDLED_PROFILE],
        tool_command: "dsh",
        install_package: Some("@deepseek-ai/dsh"),
        adapter_package: None,
        adapter_bin: None,
        bundled_profile: Some(DSH_BUNDLED_PROFILE),
    },
    // Oh My Pi (omp) — a terminal coding agent that speaks ACP natively via
    // `omp acp` (no adapter needed, like opencode). User-managed: omp targets
    // the bun runtime (installed via `bun install -g @oh-my-pi/pi-coding-agent`
    // or `curl -fsSL https://omp.sh/install | sh`), which OpenBitFun's npm-based
    // installer cannot provide — so install_package is None and OpenBitFun only
    // detects `omp` on PATH and launches it. https://github.com/can1357/oh-my-pi
    BuiltinAcpClientPreset {
        id: "omp",
        command: "omp",
        args: &["acp"],
        tool_command: "omp",
        install_package: None,
        adapter_package: None,
        adapter_bin: None,
        bundled_profile: None,
    },
];

pub(crate) fn builtin_client_ids() -> impl Iterator<Item = &'static str> {
    BUILTIN_ACP_CLIENT_PRESETS.iter().map(|preset| preset.id)
}

pub(crate) fn builtin_acp_client_preset(
    client_id: &str,
) -> Option<&'static BuiltinAcpClientPreset> {
    BUILTIN_ACP_CLIENT_PRESETS
        .iter()
        .find(|preset| preset.id == client_id)
}

pub(crate) fn default_config_for_builtin_client(client_id: &str) -> Option<AcpClientConfig> {
    let preset = builtin_acp_client_preset(client_id)?;
    Some(AcpClientConfig {
        name: None,
        command: preset.command.to_string(),
        args: preset
            .args
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        env: HashMap::new(),
        enabled: true,
        readonly: false,
        subagent: Default::default(),
        permission_mode: AcpClientPermissionMode::Ask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_default_config_for_builtin_client() {
        let config = default_config_for_builtin_client("opencode").expect("builtin config");
        assert!(config.enabled);
        assert_eq!(config.command, "opencode");
        assert_eq!(config.args, vec!["acp"]);
        // Native drivers replace the retired generic presets.
        assert!(builtin_acp_client_preset("claude-code").is_none());
        assert!(builtin_acp_client_preset("codex").is_none());
    }

    #[test]
    fn omp_is_a_native_acp_preset() {
        let preset = builtin_acp_client_preset("omp").expect("omp preset registered");
        assert_eq!(preset.command, "omp");
        assert_eq!(preset.args, &["acp"]);
        assert_eq!(preset.tool_command, "omp");
        // Native ACP — no adapter package/bin, like opencode.
        assert!(preset.adapter_package.is_none());
        assert!(preset.adapter_bin.is_none());
        // User-managed: OpenBitFun provides no installer for omp.
        assert!(preset.install_package.is_none());

        let config = default_config_for_builtin_client("omp").expect("omp config");
        assert!(config.enabled);
        assert_eq!(config.command, "omp");
        assert_eq!(config.args, vec!["acp"]);
    }

    #[test]
    fn dsh_preset_launches_the_openbitfun_profile() {
        let preset = builtin_acp_client_preset("dsh").expect("dsh preset registered");
        assert_eq!(preset.command, "dsh");
        assert_eq!(preset.tool_command, "dsh");
        // The launch IS the profile: OpenBitFun ships the bridge, dsh runs it.
        assert_eq!(preset.args, &["--profile", DSH_BUNDLED_PROFILE]);
        assert_eq!(preset.bundled_profile, Some(DSH_BUNDLED_PROFILE));
        // Native ACP — the bridge is the profile, not a separate adapter.
        assert!(preset.adapter_package.is_none());
        assert!(preset.adapter_bin.is_none());
        // The harness itself is a plain npm global, so the installer applies.
        assert_eq!(preset.install_package, Some("@deepseek-ai/dsh"));

        // Every other preset is self-contained: nothing to materialize.
        for preset in BUILTIN_ACP_CLIENT_PRESETS.iter().filter(|p| p.id != "dsh") {
            assert!(
                preset.bundled_profile.is_none(),
                "{} needs no profile",
                preset.id
            );
        }
    }
}
