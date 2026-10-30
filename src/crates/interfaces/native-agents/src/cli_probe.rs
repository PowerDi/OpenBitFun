//! Local executable discovery and bounded command execution for agent CLIs.
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Command;

pub fn resolve_configured_command(command: &str, extra_env: &HashMap<String, String>) -> PathBuf {
    let configured_path = configured_path_value(extra_env);
    find_executable_with_path(command, configured_path.as_deref())
        .unwrap_or_else(|| PathBuf::from(command))
}

pub fn apply_command_environment(
    command: &mut Command,
    extra_env: Option<&HashMap<String, String>>,
) {
    let configured_path = extra_env.and_then(configured_path_value);
    let search_path = joined_command_search_path(configured_path.as_deref());
    if !search_path.is_empty() {
        command.env("PATH", search_path);
    }

    if let Some(extra_env) = extra_env {
        for (key, value) in extra_env {
            if !key.eq_ignore_ascii_case("PATH") {
                command.env(key, value);
            }
        }
    }
}

pub async fn run_command_with_timeout<I, S>(
    program: &OsStr,
    args: I,
    timeout: Duration,
) -> Result<std::process::Output, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = openbitfun_core::util::process_manager::create_tokio_command(program);
    command.args(args);
    apply_command_environment(&mut command, None);
    // A timed-out check must not leave the child running.
    command.kill_on_drop(true);
    match tokio::time::timeout(timeout, command.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("Timed out while checking command".to_string()),
    }
}

pub fn command_error_summary(stderr: &[u8], stdout: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    if !stderr.is_empty() {
        return truncate_error(stderr);
    }
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    if !stdout.is_empty() {
        return truncate_error(stdout);
    }
    "Command exited unsuccessfully".to_string()
}

fn truncate_error(value: String) -> String {
    const MAX_LEN: usize = 240;
    if value.chars().count() <= MAX_LEN {
        return value;
    }
    format!("{}...", value.chars().take(MAX_LEN).collect::<String>())
}

pub fn find_executable(command: &str) -> Option<PathBuf> {
    find_executable_with_path(command, None)
}

pub fn find_executable_with_path(
    command: &str,
    configured_path: Option<&OsStr>,
) -> Option<PathBuf> {
    let command_path = PathBuf::from(command);
    if command_path.components().count() > 1 {
        return executable_file(&command_path).then_some(command_path);
    }

    for directory in command_search_paths(configured_path) {
        for candidate in executable_candidates(&directory, command) {
            if executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

fn configured_path_value(extra_env: &HashMap<String, String>) -> Option<OsString> {
    extra_env
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
        .map(|(_, value)| OsString::from(value))
}

fn joined_command_search_path(configured_path: Option<&OsStr>) -> OsString {
    let paths = command_search_paths(configured_path);
    if paths.is_empty() {
        return OsString::new();
    }
    env::join_paths(paths).unwrap_or_else(|_| env::var_os("PATH").unwrap_or_default())
}

fn command_search_paths(configured_path: Option<&OsStr>) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();

    if let Some(configured_path) = configured_path {
        push_split_paths(&mut paths, &mut seen, configured_path);
    }
    if let Some(env_path) = env::var_os("PATH") {
        push_split_paths(&mut paths, &mut seen, &env_path);
    }

    push_user_bin_paths(&mut paths, &mut seen);
    push_system_bin_paths(&mut paths, &mut seen);
    paths
}

fn push_split_paths(paths: &mut Vec<PathBuf>, seen: &mut HashSet<OsString>, value: &OsStr) {
    for directory in env::split_paths(value) {
        push_search_path(paths, seen, directory);
    }
}

fn push_user_bin_paths(paths: &mut Vec<PathBuf>, seen: &mut HashSet<OsString>) {
    let Some(home) = env::var_os("HOME") else {
        return;
    };
    let home = PathBuf::from(home);
    push_existing_search_path(paths, seen, home.join(".local/bin"));
    push_existing_search_path(paths, seen, home.join(".cargo/bin"));
    push_existing_search_path(paths, seen, home.join(".npm-global/bin"));
}

fn push_system_bin_paths(paths: &mut Vec<PathBuf>, seen: &mut HashSet<OsString>) {
    #[cfg(target_os = "macos")]
    {
        for prefix in ["/opt/homebrew", "/usr/local"] {
            push_existing_search_path(paths, seen, PathBuf::from(format!("{prefix}/bin")));
            push_existing_search_path(paths, seen, PathBuf::from(format!("{prefix}/sbin")));
            for node in ["node", "node@18", "node@20", "node@22", "node@24"] {
                push_existing_search_path(
                    paths,
                    seen,
                    PathBuf::from(format!("{prefix}/opt/{node}/bin")),
                );
            }
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (paths, seen);
    }
}

fn push_existing_search_path(
    paths: &mut Vec<PathBuf>,
    seen: &mut HashSet<OsString>,
    path: PathBuf,
) {
    if path.is_dir() {
        push_search_path(paths, seen, path);
    }
}

fn push_search_path(paths: &mut Vec<PathBuf>, seen: &mut HashSet<OsString>, path: PathBuf) {
    if path.as_os_str().is_empty() {
        return;
    }

    let key = search_path_key(&path);
    if seen.insert(key) {
        paths.push(path);
    }
}

fn search_path_key(path: &Path) -> OsString {
    #[cfg(windows)]
    {
        OsString::from(path.to_string_lossy().to_ascii_lowercase())
    }
    #[cfg(not(windows))]
    {
        path.as_os_str().to_os_string()
    }
}

fn executable_candidates(directory: &Path, command: &str) -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        let command_path = PathBuf::from(command);
        if command_path.extension().is_some() {
            return vec![directory.join(command)];
        }
        let extensions = env::var_os("PATHEXT").unwrap_or_else(|| OsString::from(".EXE;.BAT;.CMD"));
        extensions
            .to_string_lossy()
            .split(';')
            .filter(|extension| !extension.is_empty())
            .map(|extension| directory.join(format!("{command}{extension}")))
            .collect()
    }

    #[cfg(not(windows))]
    {
        vec![directory.join(command)]
    }
}

fn executable_file(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_search_paths_keep_configured_path_first() {
        let configured_paths = env::join_paths([
            PathBuf::from("/tmp/openbitfun-acp-first"),
            PathBuf::from("/tmp/openbitfun-acp-second"),
        ])
        .expect("test paths should be joinable");

        let paths = command_search_paths(Some(&configured_paths));

        assert_eq!(
            paths.first(),
            Some(&PathBuf::from("/tmp/openbitfun-acp-first"))
        );
        assert_eq!(
            paths.get(1),
            Some(&PathBuf::from("/tmp/openbitfun-acp-second"))
        );
    }

    #[test]
    fn find_executable_uses_configured_path() {
        let test_dir = env::temp_dir().join(format!("openbitfun-acp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir).expect("test dir should be created");

        #[cfg(windows)]
        let file_name = "openbitfun-test-tool.EXE";
        #[cfg(not(windows))]
        let file_name = "openbitfun-test-tool";

        let executable = test_dir.join(file_name);
        std::fs::write(&executable, b"").expect("test executable should be written");

        let found = find_executable_with_path("openbitfun-test-tool", Some(test_dir.as_os_str()));

        let _ = std::fs::remove_dir_all(&test_dir);
        assert_eq!(found, Some(executable));
    }
}
