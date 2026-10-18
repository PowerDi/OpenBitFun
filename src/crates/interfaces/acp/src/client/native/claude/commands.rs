//! Claude's live command catalog, separate from OpenBitFun-owned controls.
use serde_json::Value;

use crate::client::AcpAvailableCommand;

const EXCLUDED: &[&str] = &[
    "model",
    "config",
    "statusline",
    "rewind",
    "resume",
    "fast",
    "login",
    "logout",
    "permissions",
    "exit",
    "clear",
    "vim",
    "theme",
    "terminal-setup",
    "ide",
    "doctor",
    "upgrade",
    "help",
];

pub(super) fn parse(value: &Value) -> Vec<AcpAvailableCommand> {
    let Some(rows) = value.as_array() else {
        return Vec::new();
    };
    let mut commands: Vec<AcpAvailableCommand> = Vec::new();
    for row in rows {
        let Some(name) = row.as_str().or_else(|| row["name"].as_str()) else {
            continue;
        };
        let name = name.trim().trim_start_matches('/');
        if name.is_empty()
            || EXCLUDED.contains(&name.to_ascii_lowercase().as_str())
            || commands.iter().any(|command| command.name == name)
        {
            continue;
        }
        commands.push(AcpAvailableCommand {
            name: name.to_string(),
            description: row["description"].as_str().unwrap_or_default().to_string(),
            input_hint: row["argumentHint"]
                .as_str()
                .filter(|hint| !hint.is_empty())
                .map(str::to_string),
        });
    }
    commands
}
