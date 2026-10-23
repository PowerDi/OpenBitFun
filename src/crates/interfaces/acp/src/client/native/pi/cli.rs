//! Pi CLI install facts: version parsing, minimum-version policy, install
//! guidance and `pi update` (which updates Pi itself when run without a target).
use std::path::Path;
use std::time::Duration;

use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};

use crate::client::config::{AcpRequirementProbeItem, NativeCliInstallMethod, NativeCliStatus};
use crate::client::requirements::{command_error_summary, run_command_with_timeout};

/// Lowest Pi release the RPC driver was verified against (`agent_settled`,
/// `steer`, `get_session_stats.contextUsage`, `--session-id`). Earlier
/// releases are not verified.
pub(crate) const MIN_VERSION: &str = "1.3.0";
pub(crate) const NPM_INSTALL_COMMAND: &str = "npm i -g @earendil-works/pi-coding-agent";
pub(crate) const DOCS_URL: &str = "https://github.com/earendil-works/pi";
const UPDATE_TIMEOUT: Duration = Duration::from_secs(300);

/// First `x.y.z` token of `pi --version` output (`1.3.0`).
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

pub(crate) fn is_too_old(version_output: &str) -> Option<bool> {
    let version = parse_version(version_output)?;
    let minimum = semver::Version::parse(MIN_VERSION).ok()?;
    Some(version < minimum)
}

/// Pi is distributed through npm (or a standalone Bun binary / Nix). Accepts
/// Windows and POSIX separators because remote paths are POSIX.
pub(crate) fn detect_install_method(path: &str) -> NativeCliInstallMethod {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let has = |needle: &str| normalized.contains(needle);
    if has("/node_modules/")
        || has("/npm/")
        || has("/.npm-global/")
        || has("/.nvm/")
        || has("/nvm4w/")
        || has("/fnm/")
        || has("/volta/")
        || normalized.ends_with("/pi.cmd")
        || normalized.ends_with("/pi.ps1")
    {
        NativeCliInstallMethod::Npm
    } else if has("/cellar/") || has("/homebrew/") || has("/linuxbrew/") {
        NativeCliInstallMethod::Homebrew
    } else {
        NativeCliInstallMethod::Unknown
    }
}

pub(crate) fn cli_status(
    tool: &AcpRequirementProbeItem,
    _windows: bool,
    remote: bool,
) -> NativeCliStatus {
    let parsed = tool.version.as_deref().and_then(parse_version);
    let too_old = tool
        .version
        .as_deref()
        .and_then(is_too_old)
        .unwrap_or(false);
    let install_method = tool
        .path
        .as_deref()
        .map(detect_install_method)
        .unwrap_or_default();
    NativeCliStatus {
        install_method,
        version: parsed.map(|version| version.to_string()),
        min_version: MIN_VERSION.to_string(),
        too_old,
        update_supported: tool.installed && !remote,
        install_command: Some(NPM_INSTALL_COMMAND.to_string()),
        docs_url: Some(DOCS_URL.to_string()),
    }
}

/// Run `pi update` (Pi itself, not extensions) on the local host.
pub(crate) async fn run_update(executable: &Path) -> OpenBitFunResult<String> {
    let output = run_command_with_timeout(executable.as_os_str(), ["update"], UPDATE_TIMEOUT)
        .await
        .map_err(|error| OpenBitFunError::service(format!("Failed to run `pi update`: {error}")))?;
    if !output.status.success() {
        return Err(OpenBitFunError::service(format!(
            "`pi update` failed: {}",
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

    #[test]
    fn parses_versions_and_detects_npm_installs() {
        assert_eq!(parse_version("1.3.0"), Some(semver::Version::new(1, 3, 0)));
        assert_eq!(is_too_old("1.2.9"), Some(true));
        assert_eq!(is_too_old("v1.4.0"), Some(false));
        assert_eq!(is_too_old("pi"), None);
        assert_eq!(
            detect_install_method(r"C:\nvm4w\nodejs\pi.cmd"),
            NativeCliInstallMethod::Npm
        );
        assert_eq!(
            detect_install_method(
                "/usr/lib/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"
            ),
            NativeCliInstallMethod::Npm
        );
        assert_eq!(
            detect_install_method("/opt/pi"),
            NativeCliInstallMethod::Unknown
        );
    }
}
