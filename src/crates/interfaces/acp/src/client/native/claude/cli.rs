//! Claude Code CLI install facts: version parsing, minimum-version policy,
//! install-method detection, official install guidance and `claude update`.
use std::path::Path;
use std::time::Duration;

use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};

use crate::client::config::{AcpRequirementProbeItem, NativeCliInstallMethod, NativeCliStatus};
use crate::client::requirements::{command_error_summary, run_command_with_timeout};

/// Lowest Claude Code release the stream-json driver supports. The driver
/// relies on `--replay-user-messages`, `--resume-session-at`, and the
/// `get_context_usage` / `rewind_files` / `stop_task` control requests, all of
/// which are present in the 2.1 line; earlier lines are not verified.
pub(crate) const MIN_VERSION: &str = "2.1.0";
pub(crate) const DOCS_URL: &str = "https://docs.anthropic.com/en/docs/claude-code/setup";
const POSIX_INSTALL_COMMAND: &str = "curl -fsSL https://claude.ai/install.sh | bash";
const WINDOWS_INSTALL_COMMAND: &str = "irm https://claude.ai/install.ps1 | iex";
const UPDATE_TIMEOUT: Duration = Duration::from_secs(300);

/// First `x.y.z` token of `claude --version` output (`2.1.302 (Claude Code)`).
pub(crate) fn parse_version(output: &str) -> Option<semver::Version> {
    output.split_whitespace().find_map(|token| {
        let token = token.trim_start_matches('v');
        let core = token
            .split(|c: char| !(c.is_ascii_digit() || c == '.'))
            .next()
            .unwrap_or_default();
        if core.split('.').count() != 3 {
            return None;
        }
        semver::Version::parse(core).ok()
    })
}

/// `Some(true)` when the reported version is below [`MIN_VERSION`]; `None`
/// when the output cannot be parsed (callers must not block on that).
pub(crate) fn is_too_old(version_output: &str) -> Option<bool> {
    let version = parse_version(version_output)?;
    let minimum = semver::Version::parse(MIN_VERSION).ok()?;
    Some(version < minimum)
}

/// Best-effort install source from the resolved executable path. Accepts
/// Windows and POSIX separators because remote paths are POSIX on every client.
pub(crate) fn detect_install_method(path: &str) -> NativeCliInstallMethod {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let has = |needle: &str| normalized.contains(needle);
    if has("/.local/bin/claude") || has("/.local/share/claude/") || has("/.claude/local/") {
        NativeCliInstallMethod::Native
    } else if has("/node_modules/")
        || has("/npm/")
        || has("/.npm-global/")
        || has("/.nvm/")
        || has("/fnm/")
        || has("/volta/")
        || normalized.ends_with("/claude.cmd")
        || normalized.ends_with("/claude.ps1")
    {
        NativeCliInstallMethod::Npm
    } else if has("/cellar/") || has("/caskroom/") || has("/homebrew/") || has("/linuxbrew/") {
        NativeCliInstallMethod::Homebrew
    } else if has("/winget/") {
        NativeCliInstallMethod::Winget
    } else {
        NativeCliInstallMethod::Unknown
    }
}

/// Build the status for one probed host. `windows` is the probed host's OS.
pub(crate) fn cli_status(
    tool: &AcpRequirementProbeItem,
    windows: bool,
    remote: bool,
) -> NativeCliStatus {
    let parsed = tool.version.as_deref().and_then(parse_version);
    let too_old = tool
        .version
        .as_deref()
        .and_then(is_too_old)
        .unwrap_or(false);
    let install_method = match tool.path.as_deref() {
        // Follow the native installer's symlink so a launcher in a neutral
        // directory still reports its real source (local only).
        Some(path) if !remote => detect_install_method(
            &std::fs::canonicalize(path)
                .map(|resolved| resolved.to_string_lossy().into_owned())
                .unwrap_or_else(|_| path.to_string()),
        ),
        Some(path) => detect_install_method(path),
        None => NativeCliInstallMethod::Unknown,
    };
    NativeCliStatus {
        install_method,
        version: parsed.map(|version| version.to_string()),
        min_version: MIN_VERSION.to_string(),
        too_old,
        update_supported: tool.installed && !remote,
        install_command: Some(
            if windows {
                WINDOWS_INSTALL_COMMAND
            } else {
                POSIX_INSTALL_COMMAND
            }
            .to_string(),
        ),
        docs_url: Some(DOCS_URL.to_string()),
    }
}

pub(crate) fn too_old_message(found: &str) -> String {
    format!(
        "Claude Code {found} is older than the minimum supported version {MIN_VERSION}. \
         Run `claude update` (or update it from Settings > External agents), then retry."
    )
}

/// Gate a running CLI on [`MIN_VERSION`] using the `claude_code_version` it
/// reports in its `system/init` frame, so launches never pay for a separate
/// `claude --version` round trip (slow over SSH or a poor network).
/// Missing or unparsable versions are logged and allowed.
pub(crate) fn check_reported_version(reported: Option<&str>) -> OpenBitFunResult<()> {
    let Some(version) = reported else {
        log::warn!("Claude version check skipped: init frame has no claude_code_version");
        return Ok(());
    };
    match is_too_old(version) {
        Some(true) => Err(OpenBitFunError::Validation(too_old_message(
            &parse_version(version)
                .map(|version| version.to_string())
                .unwrap_or_else(|| version.to_string()),
        ))),
        Some(false) => Ok(()),
        None => {
            log::warn!("Claude version check skipped: unparsable version {version}");
            Ok(())
        }
    }
}

/// Run `claude update` on the local host and return its combined output.
pub(crate) async fn run_update(executable: &Path) -> OpenBitFunResult<String> {
    let output = run_command_with_timeout(executable.as_os_str(), ["update"], UPDATE_TIMEOUT)
        .await
        .map_err(|error| {
            OpenBitFunError::service(format!("Failed to run `claude update`: {error}"))
        })?;
    if !output.status.success() {
        return Err(OpenBitFunError::service(format!(
            "`claude update` failed: {}",
            command_error_summary(&output.stderr, &output.stdout)
        )));
    }
    let mut text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&stderr);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(installed: bool, version: Option<&str>, path: Option<&str>) -> AcpRequirementProbeItem {
        AcpRequirementProbeItem {
            name: "claude".into(),
            installed,
            version: version.map(str::to_string),
            path: path.map(str::to_string),
            error: None,
        }
    }

    #[test]
    fn parses_and_compares_cli_versions() {
        assert_eq!(
            parse_version("2.1.302 (Claude Code)"),
            Some(semver::Version::new(2, 1, 302))
        );
        assert_eq!(
            parse_version("claude v2.0.14-beta"),
            Some(semver::Version::new(2, 0, 14))
        );
        assert_eq!(parse_version("Claude Code"), None);
        assert_eq!(parse_version("2.1"), None);
        assert_eq!(is_too_old("2.1.302 (Claude Code)"), Some(false));
        assert_eq!(is_too_old("2.1.0 (Claude Code)"), Some(false));
        assert_eq!(is_too_old("2.0.77 (Claude Code)"), Some(true));
        assert_eq!(is_too_old("1.0.128 (Claude Code)"), Some(true));
        assert_eq!(is_too_old("garbage"), None);
    }

    #[test]
    fn detects_install_method_from_path() {
        let cases = [
            (
                r"C:\Users\me\.local\bin\claude.exe",
                NativeCliInstallMethod::Native,
            ),
            (
                "/home/me/.local/share/claude/versions/2.1.302",
                NativeCliInstallMethod::Native,
            ),
            (
                r"C:\Users\me\AppData\Roaming\npm\claude.cmd",
                NativeCliInstallMethod::Npm,
            ),
            (
                "/usr/local/lib/node_modules/@anthropic-ai/claude-code/cli.js",
                NativeCliInstallMethod::Npm,
            ),
            (
                "/home/me/.nvm/versions/node/v22.0.0/bin/claude",
                NativeCliInstallMethod::Npm,
            ),
            (
                "/opt/homebrew/Caskroom/claude-code/2.1.0/claude",
                NativeCliInstallMethod::Homebrew,
            ),
            (
                r"C:\Users\me\AppData\Local\Microsoft\WinGet\Packages\Anthropic.ClaudeCode\claude.exe",
                NativeCliInstallMethod::Winget,
            ),
            ("/usr/bin/claude", NativeCliInstallMethod::Unknown),
        ];
        for (path, expected) in cases {
            assert_eq!(detect_install_method(path), expected, "{path}");
        }
    }

    #[test]
    fn min_version_gate_blocks_only_parsed_old_versions() {
        let old = check_reported_version(Some("2.0.77")).expect_err("old CLI must be refused");
        let message = old.to_string();
        assert!(
            message.contains("2.0.77") && message.contains(MIN_VERSION),
            "{message}"
        );
        assert!(message.contains("claude update"), "{message}");
        assert!(check_reported_version(Some("2.1.302")).is_ok());
        assert!(check_reported_version(Some("unknown build")).is_ok());
        assert!(check_reported_version(None).is_ok());
    }

    #[test]
    fn status_reports_platform_install_guidance_and_remote_update_stance() {
        let missing = cli_status(&item(false, None, None), true, false);
        assert_eq!(
            missing.install_command.as_deref(),
            Some(WINDOWS_INSTALL_COMMAND)
        );
        assert!(!missing.update_supported);
        assert!(!missing.too_old);

        let remote = cli_status(
            &item(
                true,
                Some("2.0.1 (Claude Code)"),
                Some("/home/me/.local/bin/claude"),
            ),
            false,
            true,
        );
        assert_eq!(
            remote.install_command.as_deref(),
            Some(POSIX_INSTALL_COMMAND)
        );
        assert_eq!(remote.install_method, NativeCliInstallMethod::Native);
        assert_eq!(remote.version.as_deref(), Some("2.0.1"));
        assert!(remote.too_old);
        assert!(!remote.update_supported, "remote update is not offered");
    }

    #[test]
    fn legacy_probe_without_native_cli_still_deserializes() {
        let probe: crate::client::AcpClientRequirementProbe =
            serde_json::from_value(serde_json::json!({
                "id": "claude-native",
                "tool": {"name": "claude", "installed": true},
                "runnable": true
            }))
            .unwrap();
        assert!(probe.native_cli.is_none());
        let method: NativeCliInstallMethod = serde_json::from_str("\"future_method\"").unwrap();
        assert_eq!(method, NativeCliInstallMethod::Unknown);
    }
}
