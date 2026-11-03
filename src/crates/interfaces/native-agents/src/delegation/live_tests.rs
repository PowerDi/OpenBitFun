//! Live checks of the delegation tool channel against the real CLIs.
//!
//! Each driver session is opened with real persistence in a temp directory,
//! but without the runtime scheduler. The model is asked to call
//! `session_list` and `delegate_start`; the channel works when the agent
//! receives the tool results (an empty list, then the explicit "no
//! scheduler" error) and reports them. Every test is `#[ignore]` and needs
//! `OBF_LIVE_DELEGATION=1`:
//!
//! ```text
//! OBF_LIVE_DELEGATION=1 cargo test -p openbitfun-native-agents --lib delegation::live_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `OBF_LIVE_CLAUDE_MODEL` / `OBF_LIVE_PI_MODEL` pick a model when the CLI
//! default is unusable on the test machine.
use std::sync::Arc;
use std::time::Duration;

use openbitfun_core::agentic::events::{EventQueue, EventQueueConfig};
use openbitfun_core::infrastructure::PathManager;
use openbitfun_events::{AgenticEvent, ToolEventData};

use crate::{AcpSessionPersistence, NativeAgentOpenContext, NativeAgentSession};

const TURN_TIMEOUT: Duration = Duration::from_secs(300);
const PROMPT: &str = "Call the openbitfun session_list tool. Then call delegate_start with agent \"pi-native\" and task \"say hi\". Report each tool's raw result (including any error code) in one short line each. Do not use any other tool.";

fn enabled() -> bool {
    std::env::var("OBF_LIVE_DELEGATION").as_deref() == Ok("1")
}

struct Fixture {
    session: Arc<dyn NativeAgentSession>,
    queue: Arc<EventQueue>,
}

async fn open(client_id: &str, model: Option<&str>) -> Fixture {
    let root = std::env::temp_dir().join(format!("obf-live-delegation-{}", uuid::Uuid::new_v4()));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // Keep OpenBitFun data out of the user's real directories.
    std::env::set_var("OPENBITFUN_USER_ROOT", root.join("user"));
    std::env::set_var("OPENBITFUN_HOME", root.join("home"));
    let paths = Arc::new(PathManager::new().unwrap());
    let persistence = Arc::new(AcpSessionPersistence::new(paths).unwrap());
    let storage = root.join("sessions");
    std::fs::create_dir_all(&storage).unwrap();
    let workspace = workspace.to_string_lossy().to_string();
    let record = persistence
        .create_flow_session_record(
            &storage,
            "ws-live",
            &workspace,
            client_id,
            Some("live".into()),
        )
        .await
        .unwrap();
    if let Some(model) = model {
        // A remembered session model is passed to the CLI as is (`--model`),
        // even when the CLI's own catalog does not list it.
        persistence
            .save_native_preference(client_id, &storage, &record.session_id, |p| {
                p.model = Some(model.to_string())
            })
            .await
            .unwrap();
    }
    // The parent keeps the default (asking) mode: delegation tools must
    // run without an approval prompt, which no one answers here.
    let queue = Arc::new(EventQueue::new(EventQueueConfig::default()));
    let session = crate::native_driver(client_id)
        .unwrap()
        .open(NativeAgentOpenContext {
            session_id: record.session_id,
            workspace_path: workspace,
            remote_connection_id: None,
            session_storage_path: storage,
            queue: queue.clone(),
            notifier: None,
            persistence,
        })
        .await
        .unwrap();
    Fixture { session, queue }
}

/// Run one prompt; returns the agent text and the delegation tool results.
async fn run(fixture: &Fixture, turn_id: &str) -> (String, Vec<(String, String)>) {
    fixture
        .session
        .clone()
        .start_turn(turn_id.into(), PROMPT.into(), None)
        .await
        .unwrap();
    let mut text = String::new();
    let mut tools = Vec::new();
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    loop {
        assert!(tokio::time::Instant::now() < deadline, "turn timed out");
        for envelope in fixture.queue.dequeue_batch(1000).await {
            match envelope.event {
                AgenticEvent::TextChunk { text: chunk, .. } => text.push_str(&chunk),
                AgenticEvent::ToolEvent {
                    tool_event:
                        ToolEventData::Completed {
                            identity, result, ..
                        },
                    ..
                } => {
                    println!("  tool {} -> {}", identity.tool_name, result);
                    tools.push((identity.tool_name, result.to_string()));
                }
                AgenticEvent::ToolEvent {
                    tool_event:
                        ToolEventData::Failed {
                            identity, error, ..
                        },
                    ..
                } => {
                    println!("  tool {} failed: {error}", identity.tool_name);
                    tools.push((identity.tool_name, error));
                }
                AgenticEvent::ToolEvent {
                    tool_event: ToolEventData::Started { identity, .. },
                    ..
                } => println!("  tool {} started", identity.tool_name),
                AgenticEvent::DialogTurnCompleted { turn_id: id, .. } if id == turn_id => {
                    return (text, tools)
                }
                AgenticEvent::DialogTurnFailed {
                    turn_id: id, error, ..
                } if id == turn_id => {
                    panic!("turn failed: {error}")
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn check(client_id: &str, model: Option<String>) {
    if !enabled() {
        return;
    }
    let fixture = open(client_id, model.as_deref()).await;
    let (text, tools) = run(&fixture, "t1").await;
    println!("{client_id} answered: {text}");
    let delegation_calls: Vec<_> = tools
        .iter()
        .filter(|(name, _)| name.contains("session_list") || name.contains("delegate_start"))
        .collect();
    assert!(
        delegation_calls
            .iter()
            .any(|(name, _)| name.contains("session_list")),
        "{client_id} did not call session_list: {tools:?}"
    );
    let start = delegation_calls
        .iter()
        .find(|(name, _)| name.contains("delegate_start"))
        .unwrap_or_else(|| panic!("{client_id} did not call delegate_start: {tools:?}"));
    assert!(
        start.1.contains("runtime scheduler"),
        "delegate_start must fail loudly without a scheduler: {}",
        start.1
    );
    fixture.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real claude CLI; set OBF_LIVE_DELEGATION=1"]
async fn live_claude_delegation_tools() {
    check(
        crate::claude::CLIENT_ID,
        std::env::var("OBF_LIVE_CLAUDE_MODEL").ok(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_DELEGATION=1"]
async fn live_codex_delegation_tools() {
    check(crate::codex::CLIENT_ID, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_DELEGATION=1"]
async fn live_pi_delegation_tools() {
    check(
        crate::pi::CLIENT_ID,
        std::env::var("OBF_LIVE_PI_MODEL").ok(),
    )
    .await;
}
