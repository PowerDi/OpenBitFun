//! One tool catalog for every driver. Each driver maps these specs onto its
//! own channel (Claude SDK MCP `tools/list`, Codex `dynamicTools`, Pi
//! extension tools) and routes calls to [`call_tool`]. Results are JSON text
//! so every channel carries the same shape.
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use super::service::{self, DelegationCaller, StartRequest};

/// MCP server / tool namespace name the drivers expose the tools under.
pub const TOOL_NAMESPACE: &str = "openbitfun";

/// Names of the catalog in [`tool_specs`].
const TOOL_NAMES: [&str; 8] = [
    "delegate_start",
    "agent_inspect",
    "session_read",
    "session_wait",
    "session_send",
    "session_cancel",
    "session_watch",
    "session_list",
];

/// Card name of a delegation tool call, the same for every driver: Claude
/// already reports `mcp__openbitfun__<tool>`; Codex dynamic tools and Pi
/// extension tools report the bare name.
pub fn card_tool_name(name: &str) -> Option<String> {
    TOOL_NAMES
        .contains(&name)
        .then(|| format!("mcp__{TOOL_NAMESPACE}__{name}"))
}

/// Same bounds as codex-host `thread wait` (default 30 s).
const DEFAULT_WAIT_MS: u64 = 30_000;
const MAX_WAIT_MS: u64 = 10 * 60 * 1000;
const DEFAULT_LIST_LIMIT: usize = 25;
const MAX_LIST_LIMIT: usize = 100;

#[derive(Debug, Clone)]
pub struct DelegationToolSpec {
    pub name: &'static str,
    pub description: String,
    pub input_schema: Value,
}

/// Result of one tool call: JSON text for the model, and whether it failed.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegationToolOutput {
    pub text: String,
    pub is_error: bool,
}

impl DelegationToolOutput {
    fn ok(value: Value) -> Self {
        Self {
            text: serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
            is_error: false,
        }
    }

    fn error(code: &str, message: impl Into<String>) -> Self {
        Self::error_with(code, message, None)
    }

    fn error_with(code: &str, message: impl Into<String>, details: Option<Value>) -> Self {
        let mut error = json!({"code": code, "message": message.into()});
        if let Some(details) = details {
            error["details"] = details;
        }
        Self {
            text: json!({"error": error}).to_string(),
            is_error: true,
        }
    }
}

const SESSION_ID: &str = "Session id returned by delegate_start or session_list.";

fn agents_description() -> String {
    crate::drivers()
        .iter()
        .map(|driver| format!("\"{}\" ({})", driver.client_id(), driver.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Tool catalog shared by all drivers.
pub fn tool_specs() -> Vec<DelegationToolSpec> {
    let agents: Vec<&str> = crate::drivers().iter().map(|d| d.client_id()).collect();
    vec![
        DelegationToolSpec {
            name: "delegate_start",
            description: format!(
                "Delegate a self-contained task to another coding agent in a new, independent session in this workspace, and return immediately. Use it only when the user asks to hand work to another agent. The child runs unattended (no approval prompts) and appears in the user's session list, where the user can watch or continue it. Prefer watch=true and then end your turn, so the user can keep working with you; one notice arrives here when the child stops, and you then read its result with session_read. Use session_wait only when the user needs the result before you can continue. Agents: {}.",
                agents_description()
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "enum": agents, "description": "Target agent."},
                    "task": {"type": "string", "description": "Complete, self-contained task with goal, context, constraints and the expected output. The child does not see this conversation."},
                    "task_name": {"type": "string", "description": "Short session title (default: first line of the task)."},
                    "request_id": {"type": "string", "description": "Reuse to retry without creating a second session."},
                    "watch": {"type": "boolean", "description": "Notify this session once when the child stops (default false; recommended)."},
                    "model": {"type": "string", "description": "Exact model id from agent_inspect. Omit to use the agent's default; set it only when the user asks for a specific model."}
                },
                "required": ["agent", "task"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "agent_inspect",
            description: "List the models a target agent offers in this workspace, as that agent reports them, without creating a session. Call it before delegate_start when the user asks for a specific model, and pass the matching id exactly. When model is omitted the child uses the user's last choice for that agent, else defaultModel.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "enum": agents, "description": "Target agent."}
                },
                "required": ["agent"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_read",
            description: "Read a delegated session now without waiting. The default view returns its status and the final agent message of the latest turn; view=messages also lists the visible user and agent messages. Tool calls, file activity and reasoning are not included.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string", "description": SESSION_ID},
                    "view": {"type": "string", "enum": ["result", "messages"]}
                },
                "required": ["session_id"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_wait",
            description: format!(
                "Wait until a delegated session stops or the timeout expires (default {} ms, max {} ms), then return the same view as session_read plus timedOut. It holds your turn, so the user cannot talk to you meanwhile; use it only when the result is needed now, otherwise use watch. timedOut=true is a checkpoint, not a failure: the session keeps running.",
                DEFAULT_WAIT_MS, MAX_WAIT_MS
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string", "description": SESSION_ID},
                    "timeout_ms": {"type": "integer", "minimum": 1, "maximum": MAX_WAIT_MS},
                    "view": {"type": "string", "enum": ["result", "messages"]}
                },
                "required": ["session_id"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_send",
            description: "Send a follow-up message to a delegated session and return immediately. If it is still working, the message is queued and runs after the current turn (status=queued). Set watch=true to be notified once when it stops.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string", "description": SESSION_ID},
                    "message": {"type": "string"},
                    "watch": {"type": "boolean"}
                },
                "required": ["session_id", "message"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_cancel",
            description: "Cancel the running turn of a delegated session. The session and its history are kept and it can receive new messages.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"session_id": {"type": "string", "description": SESSION_ID}},
                "required": ["session_id"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_watch",
            description: "Ask to be notified once when a delegated session stops; returns immediately, so you may end your turn. The notice arrives as a new message in this session (queued if this session is busy) and only reports the status; read the session for the result.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string", "description": SESSION_ID},
                    "timeout_ms": {"type": "integer", "minimum": 1}
                },
                "required": ["session_id"],
                "additionalProperties": false
            }),
        },
        DelegationToolSpec {
            name: "session_list",
            description: format!(
                "List sessions this session delegated, newest first (default {DEFAULT_LIST_LIMIT}, max {MAX_LIST_LIMIT})."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIST_LIMIT}},
                "additionalProperties": false
            }),
        },
    ]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartInput {
    agent: String,
    task: String,
    #[serde(default)]
    task_name: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    watch: bool,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectInput {
    agent: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    session_id: String,
    #[serde(default)]
    view: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendInput {
    session_id: String,
    message: String,
    #[serde(default)]
    watch: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInput {
    session_id: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ListInput {
    #[serde(default)]
    limit: Option<usize>,
}

fn parse<T: for<'de> Deserialize<'de>>(input: Value) -> Result<T, DelegationToolOutput> {
    // Some channels send no arguments object for an empty call.
    let input = if input.is_null() { json!({}) } else { input };
    serde_json::from_value(input)
        .map_err(|error| DelegationToolOutput::error("INVALID_ARGUMENT", error.to_string()))
}

fn messages_view(view: Option<&str>) -> Result<bool, DelegationToolOutput> {
    match view.unwrap_or("result") {
        "result" => Ok(false),
        "messages" => Ok(true),
        other => Err(DelegationToolOutput::error(
            "INVALID_ARGUMENT",
            format!("view must be result or messages, got '{other}'"),
        )),
    }
}

/// codex-host `validateConfiguration`: an explicit model must be an exact id
/// of the agent's own catalog; anything else fails with the valid ids and
/// the caller decides what to do. Nothing is matched loosely.
fn check_model(
    options: &crate::AcpSessionOptions,
    model: &str,
) -> Result<(), DelegationToolOutput> {
    if options
        .available_models
        .iter()
        .any(|option| option.id == model)
    {
        return Ok(());
    }
    let valid: Vec<&str> = options
        .available_models
        .iter()
        .map(|option| option.id.as_str())
        .collect();
    Err(DelegationToolOutput::error_with(
        "INVALID_ARGUMENT",
        "Model is unavailable for the target agent",
        Some(json!({"validModelIds": valid})),
    ))
}

fn inspection(client_id: &str, options: &crate::AcpSessionOptions) -> Value {
    let models: Vec<Value> = options
        .available_models
        .iter()
        .map(|option| {
            let mut model = json!({"id": option.id, "name": option.name});
            if let Some(provider) = &option.provider_name {
                model["provider"] = json!(provider);
            }
            if let Some(description) = &option.description {
                model["description"] = json!(description);
            }
            model
        })
        .collect();
    json!({
        "agent": client_id,
        "defaultModel": options.current_model_id,
        "models": models,
    })
}

fn failure(error: openbitfun_core::util::errors::OpenBitFunError) -> DelegationToolOutput {
    use openbitfun_core::util::errors::OpenBitFunError;
    let code = match &error {
        OpenBitFunError::Validation(_) => "INVALID_ARGUMENT",
        OpenBitFunError::NotFound(_) => "NOT_FOUND",
        _ => "DELEGATION_FAILED",
    };
    DelegationToolOutput::error(code, error.to_string())
}

/// Run one delegation tool for `caller`. Unknown names are an error result,
/// never a panic, so a driver can forward any call in the namespace.
pub async fn call_tool(
    caller: &DelegationCaller,
    name: &str,
    input: Value,
) -> DelegationToolOutput {
    match run(caller, name, input).await {
        Ok(output) | Err(output) => output,
    }
}

async fn run(
    caller: &DelegationCaller,
    name: &str,
    input: Value,
) -> Result<DelegationToolOutput, DelegationToolOutput> {
    match name {
        "agent_inspect" => {
            let input: InspectInput = parse(input)?;
            let options = service::inspect(caller, &input.agent)
                .await
                .map_err(failure)?;
            Ok(DelegationToolOutput::ok(inspection(&input.agent, &options)))
        }
        "delegate_start" => {
            let input: StartInput = parse(input)?;
            let model = input.model.filter(|model| !model.trim().is_empty());
            if let Some(model) = model.as_deref() {
                // Host pre-check against the live catalog; the agent's own
                // launch stays the final confirmation (codex-host).
                let options = service::inspect(caller, &input.agent)
                    .await
                    .map_err(failure)?;
                check_model(&options, model)?;
            }
            let started = service::start(
                caller,
                StartRequest {
                    client_id: input.agent,
                    task: input.task,
                    task_name: input.task_name,
                    request_id: input.request_id,
                    watch: input.watch,
                    model: model.clone(),
                },
            )
            .await
            .map_err(failure)?;
            let watching = input.watch && !started.reused;
            Ok(DelegationToolOutput::ok(json!({
                "sessionId": started.session_id,
                "agent": started.client_id,
                "model": model,
                "status": if started.reused { "existing" } else if started.queued { "queued" } else { "running" },
                "watching": watching,
                "next": if watching {
                    "You will be notified here when it stops; you may end your turn."
                } else {
                    "Use session_watch to be notified when it stops, or session_read later with this sessionId."
                },
            })))
        }
        "session_read" => {
            let input: ReadInput = parse(input)?;
            if input.timeout_ms.is_some() {
                return Err(DelegationToolOutput::error(
                    "INVALID_ARGUMENT",
                    "session_read does not wait; use session_wait",
                ));
            }
            let messages = messages_view(input.view.as_deref())?;
            let snapshot = caller
                .snapshot(&input.session_id, messages)
                .await
                .map_err(failure)?;
            Ok(DelegationToolOutput::ok(json!(snapshot)))
        }
        "session_wait" => {
            let input: ReadInput = parse(input)?;
            let messages = messages_view(input.view.as_deref())?;
            caller
                .child_metadata(&input.session_id)
                .await
                .map_err(failure)?;
            let timeout = input
                .timeout_ms
                .unwrap_or(DEFAULT_WAIT_MS)
                .clamp(1, MAX_WAIT_MS);
            let stopped =
                service::wait_idle(&input.session_id, Duration::from_millis(timeout)).await;
            let snapshot = caller
                .snapshot(&input.session_id, messages)
                .await
                .map_err(failure)?;
            let mut value = json!(snapshot);
            value["timedOut"] = json!(!stopped);
            Ok(DelegationToolOutput::ok(value))
        }
        "session_send" => {
            let input: SendInput = parse(input)?;
            let (turn_id, queued) =
                service::send(caller, &input.session_id, &input.message, input.watch)
                    .await
                    .map_err(failure)?;
            Ok(DelegationToolOutput::ok(json!({
                "sessionId": input.session_id,
                "turnId": turn_id,
                "status": if queued { "queued" } else { "running" },
                "watching": input.watch,
            })))
        }
        "session_cancel" => {
            let input: SessionInput = parse(input)?;
            let turn_id = service::cancel(caller, &input.session_id)
                .await
                .map_err(failure)?;
            Ok(DelegationToolOutput::ok(json!({
                "sessionId": input.session_id,
                "turnId": turn_id,
                "cancelled": turn_id.is_some(),
            })))
        }
        "session_watch" => {
            let input: SessionInput = parse(input)?;
            caller
                .child_metadata(&input.session_id)
                .await
                .map_err(failure)?;
            if !service::is_running(&input.session_id) {
                let snapshot = caller
                    .snapshot(&input.session_id, false)
                    .await
                    .map_err(failure)?;
                return Ok(DelegationToolOutput::ok(json!({
                    "state": "alreadyTerminal",
                    "status": snapshot.status,
                })));
            }
            let timeout = input
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(service::DEFAULT_WATCH_TIMEOUT);
            service::watch(caller, &input.session_id, timeout);
            Ok(DelegationToolOutput::ok(json!({"state": "watching"})))
        }
        "session_list" => {
            let input: ListInput = parse(input)?;
            let limit = input
                .limit
                .unwrap_or(DEFAULT_LIST_LIMIT)
                .clamp(1, MAX_LIST_LIMIT);
            let children = service::list_children(caller, limit)
                .await
                .map_err(failure)?;
            let sessions: Vec<Value> = children
                .into_iter()
                .map(|child| {
                    json!({
                        "sessionId": child.session_id,
                        "agent": child.agent,
                        "title": child.title,
                        "status": child.status,
                    })
                })
                .collect();
            Ok(DelegationToolOutput::ok(json!({"sessions": sessions})))
        }
        other => Err(DelegationToolOutput::error(
            "UNKNOWN_TOOL",
            format!("Unknown delegation tool: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs_have_unique_names_and_object_schemas() {
        let specs = tool_specs();
        let mut names: Vec<_> = specs.iter().map(|spec| spec.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), specs.len());
        let mut known = TOOL_NAMES.to_vec();
        known.sort_unstable();
        assert_eq!(names, known);
        assert_eq!(
            card_tool_name("session_read").as_deref(),
            Some("mcp__openbitfun__session_read")
        );
        assert_eq!(card_tool_name("read"), None);
        for spec in &specs {
            assert_eq!(spec.input_schema["type"], "object", "{}", spec.name);
        }
        let agents = &specs[0].input_schema["properties"]["agent"]["enum"];
        assert_eq!(
            agents,
            &json!(["claude-native", "codex-native", "pi-native"])
        );
    }

    #[test]
    fn invalid_input_is_a_structured_error() {
        let error = parse::<StartInput>(json!({"agent": "x"})).unwrap_err();
        assert!(error.is_error);
        assert!(error.text.contains("INVALID_ARGUMENT"));
        assert!(messages_view(Some("raw")).is_err());
        assert!(parse::<ListInput>(Value::Null).is_ok());
    }

    fn catalog(ids: &[(&str, &str)]) -> crate::AcpSessionOptions {
        crate::AcpSessionOptions {
            current_model_id: ids.first().map(|(id, _)| id.to_string()),
            available_models: ids
                .iter()
                .map(|(id, name)| crate::AcpSessionModelOption {
                    id: id.to_string(),
                    name: name.to_string(),
                    provider_name: id.split_once('/').map(|(p, _)| p.to_string()),
                    description: None,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn explicit_model_must_be_an_exact_catalog_id() {
        // Shapes reported by the real CLIs: Claude aliases with display
        // names, Pi provider/model ids where one model has two providers.
        let claude = catalog(&[("default", "Default"), ("sonnet", "claude-opus-5")]);
        assert!(check_model(&claude, "sonnet").is_ok());
        let pi = catalog(&[
            ("a/glm-5.3-flash", "glm-5.3-flash"),
            ("b/glm-5.3-flash", "glm-5.3-flash"),
        ]);
        assert!(check_model(&pi, "b/glm-5.3-flash").is_ok());
        for loose in ["claude-opus-5", "SONNET", "glm-5.3-flash", "deepseek-flash"] {
            let catalog = if loose.contains("glm") { &pi } else { &claude };
            let error = check_model(catalog, loose).unwrap_err();
            let value: Value = serde_json::from_str(&error.text).unwrap();
            assert_eq!(value["error"]["code"], "INVALID_ARGUMENT", "{loose}");
            assert!(value["error"]["details"]["validModelIds"].is_array());
        }
        let shown = inspection("pi-native", &pi);
        assert_eq!(shown["defaultModel"], "a/glm-5.3-flash");
        assert_eq!(shown["models"][1]["provider"], "b");
    }
}
