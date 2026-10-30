use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::Duration;

use openbitfun_core::service::remote_ssh::{SSHCommandOptions, SSHConnectionManager};
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};

use super::builtin_clients::builtin_acp_client_preset;
use super::config::{AcpClientConfig, AcpRequirementProbeItem};
pub(crate) use openbitfun_native_agents::cli_probe::{
    apply_command_environment, resolve_configured_command,
};
use openbitfun_native_agents::cli_probe::{
    command_error_summary, find_executable, find_executable_with_path, run_command_with_timeout,
};
use openbitfun_native_agents::remote_shell::{
    remote_user_shell_command, render_remote_env_assignments, shell_escape,
};

const REQUIREMENT_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const ADAPTER_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const CLI_INSTALL_TIMEOUT: Duration = Duration::from_secs(600);

pub(crate) struct AcpRequirementSpec<'a> {
    pub(crate) tool_command: &'a str,
    pub(crate) install_package: Option<&'a str>,
    pub(crate) adapter: Option<AcpAdapterSpec<'a>>,
}

pub(crate) struct AcpAdapterSpec<'a> {
    pub(crate) package: &'a str,
    pub(crate) bin: &'a str,
}

pub(crate) fn acp_requirement_spec<'a>(
    client_id: &'a str,
    config: Option<&'a AcpClientConfig>,
) -> AcpRequirementSpec<'a> {
    if let Some(driver) = openbitfun_native_agents::native_driver(client_id) {
        return AcpRequirementSpec {
            tool_command: driver.command(),
            install_package: None,
            adapter: None,
        };
    }
    if let Some(preset) = builtin_acp_client_preset(client_id) {
        return AcpRequirementSpec {
            tool_command: preset.tool_command,
            install_package: preset.install_package,
            adapter: match (preset.adapter_package, preset.adapter_bin) {
                (Some(package), Some(bin)) => Some(AcpAdapterSpec { package, bin }),
                _ => None,
            },
        };
    }

    AcpRequirementSpec {
        tool_command: config
            .map(|config| config.command.as_str())
            .unwrap_or(client_id),
        install_package: None,
        adapter: None,
    }
}

pub(crate) async fn probe_executable(command: &str) -> AcpRequirementProbeItem {
    let path = find_executable(command);
    let mut item = AcpRequirementProbeItem {
        name: command.to_string(),
        installed: path.is_some(),
        version: None,
        path: path.as_ref().map(|path| path.to_string_lossy().to_string()),
        error: None,
    };

    if let Some(path) = path {
        match run_command_with_timeout(path.as_os_str(), ["--version"], REQUIREMENT_PROBE_TIMEOUT)
            .await
        {
            Ok(output) if output.status.success() => {
                item.version = parse_version_text(&output.stdout)
                    .or_else(|| parse_version_text(&output.stderr));
            }
            Ok(output) => {
                item.error = Some(command_error_summary(&output.stderr, &output.stdout));
            }
            Err(error) => {
                item.error = Some(error);
            }
        }
    }

    item
}

pub(crate) async fn probe_npm_adapter(package: &str, bin: &str) -> AcpRequirementProbeItem {
    probe_npm_adapter_with_path(package, bin, None).await
}

async fn probe_npm_adapter_with_path(
    package: &str,
    bin: &str,
    configured_path: Option<&OsStr>,
) -> AcpRequirementProbeItem {
    let mut item = AcpRequirementProbeItem {
        name: package.to_string(),
        installed: false,
        version: None,
        path: None,
        error: None,
    };
    if find_executable_with_path("npx", configured_path).is_some() {
        return npx_adapter_probe_item(package);
    }

    let npm_path = find_executable_with_path("npm", configured_path);
    let Some(npm_path) = npm_path else {
        item.error = Some("npm and npx are not available on PATH".to_string());
        return item;
    };

    let global_args = ["ls", "-g", "--json", "--depth=0", package];
    match run_command_with_timeout(npm_path.as_os_str(), global_args, REQUIREMENT_PROBE_TIMEOUT)
        .await
    {
        Ok(output) if output.status.success() => {
            if let Some(version) = npm_ls_package_version(&output.stdout, package) {
                item.installed = true;
                item.version = Some(version);
                item.path = Some("npm global".to_string());
                return item;
            }
        }
        Ok(output) => {
            item.error = Some(command_error_summary(&output.stderr, &output.stdout));
        }
        Err(error) => {
            item.error = Some(error);
        }
    }

    let offline_args = npm_offline_probe_args(package, bin);
    match run_command_with_timeout(
        npm_path.as_os_str(),
        offline_args.iter().map(String::as_str),
        REQUIREMENT_PROBE_TIMEOUT,
    )
    .await
    {
        Ok(output) if output.status.success() => {
            item.installed = true;
            item.path = Some("npm offline cache".to_string());
            item.error = None;
        }
        Ok(output) => {
            item.error = Some(command_error_summary(&output.stderr, &output.stdout));
        }
        Err(error) => {
            item.error = Some(error);
        }
    }

    item
}

fn npm_offline_probe_args(package: &str, bin: &str) -> [String; 7] {
    [
        "exec".to_string(),
        "--offline".to_string(),
        "--yes".to_string(),
        format!("--package={package}"),
        "--".to_string(),
        bin.to_string(),
        "--help".to_string(),
    ]
}

fn npx_adapter_probe_item(package: &str) -> AcpRequirementProbeItem {
    AcpRequirementProbeItem {
        name: package.to_string(),
        installed: true,
        version: None,
        path: Some("npx auto-install".to_string()),
        error: None,
    }
}

pub(crate) async fn probe_remote_executable(
    ssh_manager: &SSHConnectionManager,
    connection_id: &str,
    command: &str,
    env: Option<&HashMap<String, String>>,
) -> AcpRequirementProbeItem {
    let mut item = AcpRequirementProbeItem {
        name: command.to_string(),
        installed: false,
        version: None,
        path: None,
        error: None,
    };

    let env_prefix = render_remote_env_prefix(env);
    let resolve_command =
        remote_user_shell_command(&format!("{env_prefix}command -v {}", shell_escape(command)));
    match ssh_manager
        .execute_command(connection_id, &resolve_command)
        .await
    {
        Ok((stdout, _stderr, 0)) => {
            let resolved_path = stdout
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty())
                .map(ToString::to_string);
            item.installed = resolved_path.is_some();
            item.path = resolved_path;
        }
        Ok((stdout, stderr, _)) => {
            let summary = remote_command_error_summary(&stderr, &stdout);
            if !summary.is_empty() {
                item.error = Some(summary);
            }
        }
        Err(error) => {
            item.error = Some(error.to_string());
        }
    }

    if item.installed {
        let version_command =
            remote_user_shell_command(&format!("{env_prefix}{} --version", shell_escape(command)));
        match ssh_manager
            .execute_command(connection_id, &version_command)
            .await
        {
            Ok((stdout, stderr, 0)) => {
                item.version = parse_version_text(stdout.as_bytes())
                    .or_else(|| parse_version_text(stderr.as_bytes()));
            }
            Ok((stdout, stderr, _)) => {
                item.error = Some(remote_command_error_summary(&stderr, &stdout));
            }
            Err(error) => {
                item.error = Some(error.to_string());
            }
        }
    }

    item
}

pub(crate) async fn probe_remote_npx_adapter(
    ssh_manager: &SSHConnectionManager,
    connection_id: &str,
    package: &str,
    env: Option<&HashMap<String, String>>,
) -> AcpRequirementProbeItem {
    let mut item = AcpRequirementProbeItem {
        name: package.to_string(),
        installed: false,
        version: None,
        path: None,
        error: None,
    };

    let env_prefix = render_remote_env_prefix(env);
    let resolve_command = remote_user_shell_command(&format!("{env_prefix}command -v npx"));
    match ssh_manager
        .execute_command(connection_id, &resolve_command)
        .await
    {
        Ok((stdout, _stderr, 0)) => {
            item.installed = true;
            item.path = stdout
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty())
                .map(ToString::to_string)
                .or_else(|| Some("remote npx auto-install".to_string()));
        }
        Ok((stdout, stderr, _)) => {
            let summary = remote_command_error_summary(&stderr, &stdout);
            item.error = Some(if summary.is_empty() {
                "npx is not available on remote PATH".to_string()
            } else {
                summary
            });
        }
        Err(error) => {
            item.error = Some(error.to_string());
        }
    }

    item
}

pub(crate) async fn predownload_npm_adapter(package: &str, bin: &str) -> OpenBitFunResult<()> {
    let npm_path = find_executable("npm")
        .ok_or_else(|| OpenBitFunError::service("npm is not available on PATH".to_string()))?;
    let args = npm_predownload_args(package, bin);

    match run_command_with_timeout(
        npm_path.as_os_str(),
        args.iter().map(String::as_str),
        ADAPTER_DOWNLOAD_TIMEOUT,
    )
    .await
    {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(OpenBitFunError::service(format!(
            "Failed to predownload ACP adapter '{}': {}",
            package,
            command_error_summary(&output.stderr, &output.stdout)
        ))),
        Err(error) => Err(OpenBitFunError::service(format!(
            "Failed to predownload ACP adapter '{}': {}",
            package, error
        ))),
    }
}

fn npm_predownload_args(package: &str, bin: &str) -> [String; 6] {
    [
        "exec".to_string(),
        "--yes".to_string(),
        format!("--package={package}"),
        "--".to_string(),
        bin.to_string(),
        "--help".to_string(),
    ]
}

pub(crate) async fn install_npm_cli_package(package: &str) -> OpenBitFunResult<()> {
    let npm_path = find_executable("npm")
        .ok_or_else(|| OpenBitFunError::service("npm is not available on PATH".to_string()))?;
    let args = ["install", "-g", package];

    match run_command_with_timeout(npm_path.as_os_str(), args, CLI_INSTALL_TIMEOUT).await {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(OpenBitFunError::service(format!(
            "Failed to install ACP agent CLI '{}': {}",
            package,
            command_error_summary(&output.stderr, &output.stdout)
        ))),
        Err(error) => Err(OpenBitFunError::service(format!(
            "Failed to install ACP agent CLI '{}': {}",
            package, error
        ))),
    }
}

pub(crate) async fn install_remote_npm_cli_package(
    ssh_manager: &SSHConnectionManager,
    connection_id: &str,
    package: &str,
) -> OpenBitFunResult<()> {
    let command = remote_user_shell_command(&format!("npm install -g {}", shell_escape(package)));
    let timeout_ms = u64::try_from(CLI_INSTALL_TIMEOUT.as_millis()).unwrap_or(u64::MAX);
    match ssh_manager
        .execute_command_with_options(
            connection_id,
            &command,
            SSHCommandOptions {
                timeout_ms: Some(timeout_ms),
                cancellation_token: None,
            },
        )
        .await
    {
        Ok(output) if output.exit_code == 0 && !output.timed_out && !output.interrupted => Ok(()),
        Ok(output) if output.timed_out => Err(OpenBitFunError::service(format!(
            "Failed to install remote ACP agent CLI '{}': command timed out",
            package
        ))),
        Ok(output) if output.interrupted => Err(OpenBitFunError::service(format!(
            "Failed to install remote ACP agent CLI '{}': command was cancelled",
            package
        ))),
        Ok(output) => Err(OpenBitFunError::service(format!(
            "Failed to install remote ACP agent CLI '{}': {}",
            package,
            remote_command_error_summary(&output.stderr, &output.stdout)
        ))),
        Err(error) => Err(OpenBitFunError::service(format!(
            "Failed to install remote ACP agent CLI '{}': {}",
            package, error
        ))),
    }
}

fn npm_ls_package_version(stdout: &[u8], package: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    value
        .get("dependencies")?
        .get(package)?
        .get("version")?
        .as_str()
        .map(ToString::to_string)
}

fn parse_version_text(output: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(output);
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(ToString::to_string)
}

fn remote_command_error_summary(stderr: &str, stdout: &str) -> String {
    let stderr = stderr.trim().to_string();
    if !stderr.is_empty() {
        return truncate_error(stderr);
    }
    let stdout = stdout.trim().to_string();
    if !stdout.is_empty() {
        return truncate_error(stdout);
    }
    String::new()
}

fn truncate_error(value: String) -> String {
    const MAX_LEN: usize = 240;
    if value.chars().count() <= MAX_LEN {
        return value;
    }
    format!("{}...", value.chars().take(MAX_LEN).collect::<String>())
}

fn render_remote_env_prefix(env: Option<&HashMap<String, String>>) -> String {
    let Some(env) = env else {
        return String::new();
    };
    let assignments = render_remote_env_assignments(env);
    if assignments.is_empty() {
        return String::new();
    }
    format!("{} ", assignments.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::{Path, PathBuf};

    #[test]
    fn npm_adapter_argument_builders_preserve_cli_order() {
        assert_eq!(
            npm_offline_probe_args("@agentclientprotocol/codex-acp", "codex-acp"),
            [
                "exec",
                "--offline",
                "--yes",
                "--package=@agentclientprotocol/codex-acp",
                "--",
                "codex-acp",
                "--help",
            ]
            .map(str::to_string)
        );
        assert_eq!(
            npm_predownload_args("@agentclientprotocol/codex-acp", "codex-acp"),
            [
                "exec",
                "--yes",
                "--package=@agentclientprotocol/codex-acp",
                "--",
                "codex-acp",
                "--help",
            ]
            .map(str::to_string)
        );
    }

    #[test]
    fn builtin_adapter_specs_use_maintained_agent_client_protocol_packages() {
        let claude = acp_requirement_spec("claude-code", None)
            .adapter
            .expect("Claude adapter");
        assert_eq!(claude.package, "@agentclientprotocol/claude-agent-acp");
        assert_eq!(claude.bin, "claude-agent-acp");

        let codex = acp_requirement_spec("codex", None)
            .adapter
            .expect("Codex adapter");
        assert_eq!(codex.package, "@agentclientprotocol/codex-acp");
        assert_eq!(codex.bin, "codex-acp");
    }

    #[test]
    fn npx_adapter_probe_item_marks_auto_install_available() {
        let item = npx_adapter_probe_item("@agentclientprotocol/codex-acp");

        assert_eq!(item.name, "@agentclientprotocol/codex-acp");
        assert!(item.installed);
        assert_eq!(item.path.as_deref(), Some("npx auto-install"));
        assert!(item.error.is_none());
    }

    #[test]
    fn probe_npm_adapter_skips_npm_probe_when_npx_is_available() {
        let test_dir = env::temp_dir().join(format!("openbitfun-acp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir).expect("test dir should be created");
        let npm_marker = test_dir.join("npm-was-run");

        write_command_stub(&test_dir, "npx", "");
        write_command_stub(&test_dir, "npm", &npm_probe_marker_script(&npm_marker));

        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should be created");
        let item = runtime.block_on(probe_npm_adapter_with_path(
            "@agentclientprotocol/codex-acp",
            "codex-acp",
            Some(test_dir.as_os_str()),
        ));

        assert!(item.installed);
        assert_eq!(item.path.as_deref(), Some("npx auto-install"));
        assert!(
            !npm_marker.exists(),
            "npm probe should not run when npx can launch the adapter"
        );
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn remote_env_prefix_uses_valid_keys_in_stable_order() {
        let env = HashMap::from([
            ("INVALID-NAME".to_string(), "ignored".to_string()),
            ("PATH".to_string(), "/remote/bin:/usr/bin".to_string()),
            ("ACP_HOME".to_string(), "/tmp/acp home".to_string()),
        ]);

        assert_eq!(
            render_remote_env_prefix(Some(&env)),
            "ACP_HOME='/tmp/acp home' PATH=/remote/bin:/usr/bin "
        );
    }

    fn write_command_stub(directory: &Path, command: &str, body: &str) -> PathBuf {
        #[cfg(windows)]
        let path = directory.join(format!("{command}.cmd"));
        #[cfg(not(windows))]
        let path = directory.join(command);

        std::fs::write(&path, body).expect("command stub should be written");

        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&path)
                .expect("command stub metadata should be readable")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&path, permissions)
                .expect("command stub should be executable");
        }

        path
    }

    fn npm_probe_marker_script(marker_path: &Path) -> String {
        #[cfg(windows)]
        {
            format!(
                "@echo off\r\necho called > \"{}\"\r\nexit /b 42\r\n",
                marker_path.display()
            )
        }
        #[cfg(not(windows))]
        {
            format!(
                "#!/bin/sh\necho called > '{}'\nexit 42\n",
                marker_path.display()
            )
        }
    }
}
