//! Live protocol checks against the real `pi --mode rpc` on PATH.
//!
//! Every test is `#[ignore]` and additionally requires `OBF_LIVE_PI=1`, so CI
//! and a plain `cargo test` never spawn Pi or spend model tokens. Run:
//!
//! ```text
//! OBF_LIVE_PI=1 cargo test -p openbitfun-acp --lib pi::session::live_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Each test uses a throwaway temp directory as the workspace and Pi's
//! configured default model at the lowest thinking level it offers.
use super::*;
use openbitfun_core::agentic::events::EventQueueConfig;
use openbitfun_core::agentic::session::{ResolvedSessionTitle, SessionTitleMethod};
use openbitfun_events::ToolEventData;

const TURN_TIMEOUT: Duration = Duration::from_secs(240);

fn enabled() -> bool {
    std::env::var("OBF_LIVE_PI").as_deref() == Ok("1")
}

fn workspace() -> String {
    let dir = std::env::temp_dir().join(format!("obf-live-pi-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().to_string()
}

fn session(workspace: &str, pi_session: &str, resume: bool, index: usize) -> Arc<PiNativeSession> {
    let session = PiNativeSession::new(
        "live".into(),
        pi_session.into(),
        workspace.into(),
        None,
        resume,
        Arc::new(EventQueue::new(EventQueueConfig::default())),
        None,
        index,
        None,
    );
    // Titles would call the configured OpenBitFun model; keep them local.
    *lock(&session.title_resolver) = Arc::new(|message: String| {
        Box::pin(async move {
            ResolvedSessionTitle {
                title: message,
                method: SessionTitleMethod::Fallback,
            }
        })
    });
    session
}

/// Launch and pick the cheapest thinking level Pi offers.
async fn cheap(session: &Arc<PiNativeSession>) {
    session.warm_up().await.unwrap();
    let levels = lock(&session.thinking_levels).clone();
    println!(
        "model={:?} thinking={:?} levels={levels:?}",
        session.options().current_model_id,
        lock(&session.thinking)
    );
    for level in ["off", "minimal", "low"] {
        if levels.iter().any(|l| l == level) {
            if lock(&session.thinking).as_deref() != Some(level) {
                session
                    .set_config_option(
                        THINKING_CONFIG_ID,
                        AcpSessionConfigValue::Select {
                            value: level.into(),
                        },
                    )
                    .await
                    .unwrap();
            }
            println!("thinking level: {level}");
            return;
        }
    }
}

async fn set_mode(session: &PiNativeSession, mode: &str) -> OpenBitFunResult<AcpSessionOptions> {
    session
        .set_config_option(
            approval::PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select { value: mode.into() },
        )
        .await
}

fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 300 {
        format!("{}...", &text[..text.floor_char_boundary(300)])
    } else {
        text
    }
}

fn describe(event: &AgenticEvent) -> String {
    match event {
        AgenticEvent::TextChunk { text, .. } => format!("TextChunk {text:?}"),
        AgenticEvent::ToolEvent { tool_event, .. } => match tool_event {
            ToolEventData::Started {
                identity, params, ..
            } => format!(
                "Tool Started {} {} {}",
                identity.tool_id,
                identity.tool_name,
                short(params)
            ),
            ToolEventData::Completed {
                identity, result, ..
            } => format!(
                "Tool Completed {} {} {}",
                identity.tool_id,
                identity.tool_name,
                short(result)
            ),
            ToolEventData::Failed {
                identity, error, ..
            } => format!(
                "Tool Failed {} {} {error}",
                identity.tool_id, identity.tool_name
            ),
            ToolEventData::Cancelled { identity, .. } => {
                format!("Tool Cancelled {} {}", identity.tool_id, identity.tool_name)
            }
            other => format!("Tool {other:?}").chars().take(160).collect(),
        },
        AgenticEvent::TokenUsageUpdated {
            input_tokens,
            total_tokens,
            max_context_tokens,
            ..
        } => format!(
            "TokenUsage input={input_tokens} total={total_tokens} window={max_context_tokens:?}"
        ),
        AgenticEvent::UserSteeringInjected {
            steering_id,
            content,
            round_index,
            ..
        } => format!("UserSteeringInjected {steering_id} round={round_index} {content:?}"),
        AgenticEvent::DialogTurnFailed { error, .. } => format!("DialogTurnFailed {error}"),
        other => format!("{other:?}")
            .split([' ', '{', '('])
            .next()
            .unwrap_or_default()
            .to_string(),
    }
}

fn is_end(event: &AgenticEvent, turn: &str) -> bool {
    match event {
        AgenticEvent::DialogTurnCompleted { turn_id, .. }
        | AgenticEvent::DialogTurnCancelled { turn_id, .. }
        | AgenticEvent::DialogTurnFailed { turn_id, .. } => turn_id == turn,
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum OnApproval {
    Answer(&'static str),
    Leave,
}

struct TurnRun {
    events: Vec<AgenticEvent>,
    /// (permission id, tool name, input)
    approvals: Vec<(String, String, Value)>,
}

impl TurnRun {
    fn text(&self) -> String {
        self.events
            .iter()
            .filter_map(|e| match e {
                AgenticEvent::TextChunk { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }
    fn ended_with(&self) -> &'static str {
        match self.events.last() {
            Some(AgenticEvent::DialogTurnCompleted { .. }) => "completed",
            Some(AgenticEvent::DialogTurnCancelled { .. }) => "cancelled",
            Some(AgenticEvent::DialogTurnFailed { .. }) => "failed",
            _ => "none",
        }
    }
    fn tools(&self, name: &str) -> Vec<&ToolEventData> {
        self.events
            .iter()
            .filter_map(|e| match e {
                AgenticEvent::ToolEvent { tool_event, .. } => Some(tool_event),
                _ => None,
            })
            .filter(|t| match t {
                ToolEventData::Completed { identity, .. }
                | ToolEventData::Failed { identity, .. } => identity.tool_name == name,
                _ => false,
            })
            .collect()
    }
}

/// Collect events of `turn` until it ends, answering approvals per `policy`.
/// `during` runs once, when `trigger` first matches an event or a pending
/// approval exists (`trigger` = None).
async fn drive<F, Fut>(
    session: &Arc<PiNativeSession>,
    turn: &str,
    policy: OnApproval,
    trigger: Option<fn(&AgenticEvent) -> bool>,
    during: Option<F>,
) -> TurnRun
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut during = during;
    let mut run = TurnRun {
        events: Vec::new(),
        approvals: Vec::new(),
    };
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("turn {turn} timed out");
        }
        let mut fire = false;
        for envelope in session.queue.dequeue_batch(1000).await {
            println!("  event: {}", describe(&envelope.event));
            if trigger.is_some_and(|t| t(&envelope.event)) {
                fire = true;
            }
            let end = is_end(&envelope.event, turn);
            run.events.push(envelope.event);
            if end {
                return run;
            }
        }
        let pending: Vec<(String, String, Value)> = session
            .approvals
            .iter()
            .filter(|entry| !run.approvals.iter().any(|(id, _, _)| id == entry.key()))
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.request.tool_name.clone(),
                    entry.request.input.clone(),
                )
            })
            .collect();
        for (id, tool, input) in pending {
            println!("  approval: {tool} {}", short(&input));
            run.approvals.push((id.clone(), tool, input));
            if trigger.is_none() {
                fire = true;
            }
            if let OnApproval::Answer(option) = policy {
                let handled = session
                    .submit_permission(&SubmitAcpPermissionResponseRequest {
                        permission_id: id,
                        approve: option != approval::REJECT_ONCE,
                        option_id: Some(option.into()),
                        answers: None,
                        feedback: None,
                    })
                    .await;
                assert!(matches!(handled, Some(Ok(()))), "approval answer failed");
                println!("  answered: {option}");
            }
        }
        if fire {
            if let Some(during) = during.take() {
                during().await;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn turn(
    session: &Arc<PiNativeSession>,
    turn_id: &str,
    prompt: &str,
    policy: OnApproval,
) -> TurnRun {
    println!("--- turn {turn_id}: {prompt}");
    session
        .start_turn(turn_id.into(), prompt.into(), None)
        .await
        .unwrap();
    drive(
        session,
        turn_id,
        policy,
        Some(|_| false),
        None::<fn() -> std::future::Ready<()>>,
    )
    .await
}

async fn process(session: &PiNativeSession) -> Arc<PiProcess> {
    PiNativeSession::live_process(&*session.process.lock().await).expect("live Pi process")
}

/// User message texts of the active branch (`get_messages`, no model call).
async fn user_texts(session: &PiNativeSession) -> Vec<String> {
    let data = process(session)
        .await
        .request(json!({"type":"get_messages"}), Some(COMMAND_TIMEOUT))
        .await
        .unwrap();
    data["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| m["role"] == "user")
        .map(|m| match &m["content"] {
            Value::String(text) => text.clone(),
            content => content
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|c| c["text"].as_str())
                .collect::<Vec<_>>()
                .join(""),
        })
        .collect()
}

async fn entry_ids(session: &PiNativeSession) -> Vec<String> {
    let data = process(session)
        .await
        .request(json!({"type":"get_entries"}), Some(COMMAND_TIMEOUT))
        .await
        .unwrap();
    data["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e["id"].as_str().map(str::to_string))
        .collect()
}

/// Wait for the background `get_entries` sync after a turn to attribute it.
async fn anchored(session: &PiNativeSession, turn_id: &str) -> String {
    for _ in 0..100 {
        let entry = lock(&session.history)
            .turns
            .iter()
            .find(|a| a.turn_id == turn_id)
            .and_then(|a| a.entry_id.clone());
        if let Some(entry) = entry {
            return entry;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "turn {turn_id} has no Pi entry id: {:?}",
        lock(&session.history)
    );
}

/// Scenarios 1, 7 and 8: prompt, restart, rollback, restart again, fork.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_PI=1"]
async fn live_prompt_restart_rollback_fork() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let pi_id = uuid::Uuid::new_v4().to_string();
    let first = session(&dir, &pi_id, false, 0);
    cheap(&first).await;
    let options = first.options();
    println!(
        "models: {} current={:?}",
        options.available_models.len(),
        options.current_model_id
    );
    assert!(!options.available_models.is_empty(), "model list is empty");

    // 1. Plain prompt.
    let run = turn(
        &first,
        "t1",
        "Remember the word kiwi. Reply with exactly the word pong and nothing else.",
        OnApproval::Leave,
    )
    .await;
    assert_eq!(run.ended_with(), "completed");
    assert!(run.text().to_lowercase().contains("pong"), "{}", run.text());
    let mut usage = None;
    for _ in 0..50 {
        usage = first.options().context_usage;
        if usage.as_ref().is_some_and(|u| u.used > 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("context usage after t1: {usage:?}");
    assert!(usage.is_some_and(|u| u.used > 0 && u.size > 0));
    let e1 = anchored(&first, "t1").await;
    assert!(first.resume.load(Ordering::Acquire));
    assert_eq!(first.active_pi_session(), pi_id);
    let history = lock(&first.history).clone();
    println!("t1 entry={e1} history={history:?}");
    first.shutdown().await;
    drop(first);

    // 7. Restart: a new process opens the stored Pi session id.
    let second = session(&dir, &pi_id, true, 1);
    second.apply_history(history);
    cheap(&second).await;
    assert_eq!(second.active_pi_session(), pi_id);
    let run = turn(
        &second,
        "t2",
        "What word did I ask you to remember? Reply with that word only.",
        OnApproval::Leave,
    )
    .await;
    assert_eq!(run.ended_with(), "completed");
    assert!(run.text().to_lowercase().contains("kiwi"), "context lost");
    let e2 = anchored(&second, "t2").await;

    // 8a. Rollback the latest turn on the running process.
    let run = turn(
        &second,
        "t3",
        "Now forget kiwi and remember the word plum instead. Reply with ok.",
        OnApproval::Leave,
    )
    .await;
    assert_eq!(run.ended_with(), "completed");
    let e3 = anchored(&second, "t3").await;
    let before = entry_ids(&second).await;
    let outcome = second.rollback_last_turn("t3").await.unwrap();
    let rolled_id = second.active_pi_session();
    let after = entry_ids(&second).await;
    println!(
        "rollback: {outcome:?} pi session {pi_id} -> {rolled_id}\n  entries before={before:?}\n  entries after={after:?}"
    );
    assert_ne!(
        rolled_id, pi_id,
        "rollback must branch into a new Pi session"
    );
    assert!(lock(&second.history).pending.is_none());
    assert!(
        after.contains(&e1) && after.contains(&e2),
        "fork renumbered entries"
    );
    assert!(!after.contains(&e3));
    let users = user_texts(&second).await;
    println!("users after rollback: {users:?}");
    assert!(!users.iter().any(|u| u.contains("plum")));
    let run = turn(
        &second,
        "t3b",
        "Which words have I asked you to remember so far in this conversation? List them, comma separated, nothing else.",
        OnApproval::Leave,
    )
    .await;
    assert_eq!(run.ended_with(), "completed");
    let text = run.text().to_lowercase();
    assert!(text.contains("kiwi") && !text.contains("plum"), "{text}");
    let e3b = anchored(&second, "t3b").await;
    let history = lock(&second.history).clone();
    println!("history after t3b: {history:?}");
    second.shutdown().await;
    drop(second);

    // 8b. The rollback survives a restart (no model call).
    let third = session(&dir, &rolled_id, true, 3);
    third.apply_history(history.clone());
    third.warm_up().await.unwrap();
    let users = user_texts(&third).await;
    println!("users after restart: {users:?}");
    assert_eq!(third.active_pi_session(), rolled_id);
    assert!(!users.iter().any(|u| u.contains("plum")));
    assert!(users.last().is_some_and(|u| u.contains("comma separated")));
    third.shutdown().await;
    drop(third);

    // 8c. Fork through t1 (no model call): `--fork` copies the session,
    // then `fork` cuts before t2's user message, whose id the copy keeps.
    let source = json!({
        "acpClientId": CLIENT_ID,
        "acpRemoteSessionId": rolled_id,
        "acpResumeStrategy": super::super::RESUME_STRATEGY,
        super::super::history::METADATA_KEY: history.to_value(),
    });
    let keys = super::super::pi_fork_metadata(&source, "t1", &["t1".into()]).unwrap();
    let forked_history = PiHistory::from_metadata(Some(&Value::Object(keys)));
    println!("fork pending: {:?}", forked_history.pending);
    assert_eq!(
        forked_history
            .pending
            .as_ref()
            .unwrap()
            .cut_before
            .as_deref(),
        Some(e2.as_str())
    );
    let fork = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 1);
    fork.apply_history(forked_history);
    fork.warm_up().await.unwrap();
    let fork_id = fork.active_pi_session();
    let users = user_texts(&fork).await;
    let ids = entry_ids(&fork).await;
    println!("fork: pi session {fork_id} users={users:?} entries={ids:?}");
    assert_ne!(fork_id, rolled_id);
    assert!(fork.resume.load(Ordering::Acquire));
    assert_eq!(users.len(), 1);
    assert!(users[0].contains("kiwi"));
    assert!(ids.contains(&e1) && !ids.contains(&e2) && !ids.contains(&e3b));
    fork.shutdown().await;
}

/// Scenarios 2-5: allow once, reject, allow for session, read-only.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_PI=1"]
async fn live_approvals() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let s = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 0);
    cheap(&s).await;
    assert_eq!(*lock(&s.permission_mode), approval::MODE_ASK);
    assert!(s.bridge_loaded.load(Ordering::Acquire));
    let path = |name: &str| std::path::Path::new(&dir).join(name);

    // 2. Allow once: the write runs.
    let run = turn(
        &s,
        "a1",
        "Use the write tool to create the file a.txt containing exactly: x. Then reply done.",
        OnApproval::Answer(approval::ALLOW_ONCE),
    )
    .await;
    assert_eq!(run.ended_with(), "completed");
    assert_eq!(run.approvals.len(), 1, "expected one approval card");
    assert_eq!(run.approvals[0].1, "write");
    assert_eq!(std::fs::read_to_string(path("a.txt")).unwrap().trim(), "x");

    // 3. Reject: the call is blocked and the model is told.
    let run = turn(
        &s,
        "a2",
        "Use the write tool to create the file b.txt containing y. If the tool call is blocked or rejected, do not retry; reply with the single word blocked.",
        OnApproval::Answer(approval::REJECT_ONCE),
    )
    .await;
    println!("a2 text={:?}", run.text());
    assert_eq!(run.ended_with(), "completed");
    assert_eq!(run.approvals.len(), 1);
    assert!(!path("b.txt").exists());
    let failed = run.tools("Write");
    println!("a2 write results: {failed:?}");
    assert!(run.text().to_lowercase().contains("blocked"));

    // 4. Allow for this session: the identical second call does not ask.
    let run = turn(
        &s,
        "a3",
        "Use the bash tool to run the command `echo hi`. Then run the exact same command `echo hi` again in a second, separate bash call. Then reply done.",
        OnApproval::Answer(approval::ALLOW_SESSION),
    )
    .await;
    let bash = run
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                AgenticEvent::ToolEvent {
                    tool_event: ToolEventData::Completed { .. },
                    ..
                }
            )
        })
        .count();
    println!(
        "a3 approvals={} completed tools={bash}",
        run.approvals.len()
    );
    assert_eq!(run.ended_with(), "completed");
    assert_eq!(run.approvals.len(), 1, "second identical call asked again");
    assert!(bash >= 2, "model did not run the command twice");

    // 5. Read only blocks a write without asking.
    set_mode(&s, approval::MODE_READ_ONLY).await.unwrap();
    let run = turn(
        &s,
        "a4",
        "Use the write tool to create the file c.txt containing z. If the tool call is blocked, do not retry; reply with the single word blocked.",
        OnApproval::Leave,
    )
    .await;
    println!("a4 text={:?}", run.text());
    assert_eq!(run.ended_with(), "completed");
    assert!(run.approvals.is_empty());
    assert!(!path("c.txt").exists());
    s.shutdown().await;
}

/// Scenarios 5 (full access) and 6: steering a running turn.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_PI=1"]
async fn live_full_access_steer() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let s = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 0);
    cheap(&s).await;
    set_mode(&s, approval::MODE_FULL_ACCESS).await.unwrap();
    println!("--- turn f1");
    s.start_turn(
        "f1".into(),
        "Use the bash tool to run `sleep 6 && echo hi > d.txt`. After it finishes, reply with the single word done.".into(),
        None,
    )
    .await
    .unwrap();
    let steerer = s.clone();
    let steer_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let ids = steer_ids.clone();
    let run = drive(
        &s,
        "f1",
        OnApproval::Leave,
        Some(|e| {
            matches!(
                e,
                AgenticEvent::ToolEvent {
                    tool_event: ToolEventData::Started { .. },
                    ..
                }
            )
        }),
        Some(move || async move {
            for text in [
                "Also include the word mango in your final reply.",
                "And include the word lime too.",
            ] {
                let id = steerer.steer("f1", text.into(), None).await.unwrap();
                println!("  steered {id}: {text}");
                ids.lock().unwrap().push(id);
            }
        }),
    )
    .await;
    let text = run.text().to_lowercase();
    println!("f1 text={text:?}");
    assert_eq!(run.ended_with(), "completed");
    assert!(run.approvals.is_empty(), "full access asked");
    assert_eq!(
        std::fs::read_to_string(std::path::Path::new(&dir).join("d.txt"))
            .unwrap()
            .trim(),
        "hi"
    );
    let injected: Vec<(String, String)> = run
        .events
        .iter()
        .filter_map(|e| match e {
            AgenticEvent::UserSteeringInjected {
                steering_id,
                content,
                ..
            } => Some((steering_id.clone(), content.clone())),
            _ => None,
        })
        .collect();
    let user_starts: Vec<String> = lock(&s.frame_tap)
        .iter()
        .filter(|f| f["type"] == "message_start" && f["message"]["role"] == "user")
        .map(|f| short(&f["message"]["content"]))
        .collect();
    println!("injected={injected:?}\nuser message_start echoes={user_starts:?}");
    let sent = steer_ids.lock().unwrap().clone();
    assert_eq!(
        injected
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        sent,
        "each steer must be attributed in order"
    );
    assert!(injected[0].1.contains("mango") && injected[1].1.contains("lime"));
    assert_eq!(user_starts.len(), 3);
    assert!(user_starts[1].contains("mango") && user_starts[2].contains("lime"));
    assert!(text.contains("mango"));
    s.shutdown().await;
}

/// Scenario 9: abort while an approval is pending.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_PI=1"]
async fn live_abort_pending_approval() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let s = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 0);
    cheap(&s).await;
    println!("--- turn x1");
    s.start_turn(
        "x1".into(),
        "Use the write tool to create the file e.txt containing q. Then reply done.".into(),
        None,
    )
    .await
    .unwrap();
    let canceller = s.clone();
    let run = drive(
        &s,
        "x1",
        OnApproval::Leave,
        None,
        Some(move || async move {
            canceller.cancel().await.unwrap();
            println!("  cancelled");
        }),
    )
    .await;
    assert_eq!(run.approvals.len(), 1);
    assert_eq!(run.ended_with(), "cancelled");
    assert!(s.approvals.is_empty(), "approval left pending");
    assert!(s.state.lock().await.projector.is_none());
    let state = process(&s)
        .await
        .request(json!({"type":"get_state"}), Some(COMMAND_TIMEOUT))
        .await
        .unwrap();
    println!("after abort: isStreaming={}", state["isStreaming"]);
    assert_eq!(state["isStreaming"], false);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!std::path::Path::new(&dir).join("e.txt").exists());
    s.shutdown().await;
}

/// Scenario 10: without the approval bridge, approval modes refuse loudly.
/// No model call is made.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real pi CLI; set OBF_LIVE_PI=1"]
async fn live_bridge_missing() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let scratch = std::env::temp_dir().join(format!("obf-live-pi-ext-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&scratch).unwrap();

    // An extension that loads but is not the bridge.
    let inert = scratch.join("inert.ts");
    std::fs::write(&inert, "export default function (_pi: any) {}\n").unwrap();
    let s = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 0);
    *lock(&s.extension_override) = Some(inert);
    s.warm_up().await.unwrap();
    assert!(!s.bridge_loaded.load(Ordering::Acquire));
    let error = s
        .start_turn("b1".into(), "reply with pong".into(), None)
        .await
        .unwrap_err()
        .to_string();
    println!("ask mode without bridge: {error}");
    assert!(error.contains("approval extension did not load"));
    let events: Vec<String> = s
        .queue
        .dequeue_batch(100)
        .await
        .iter()
        .map(|e| describe(&e.event))
        .collect();
    println!("events: {events:?}");
    assert!(events.iter().any(|e| e.starts_with("DialogTurnFailed")));
    assert!(!s.resume.load(Ordering::Acquire), "prompt reached Pi");
    let error = set_mode(&s, approval::MODE_READ_ONLY)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("approval extension did not load"));
    s.shutdown().await;

    // A bridge path that does not exist: Pi refuses to start.
    let s = session(&dir, &uuid::Uuid::new_v4().to_string(), false, 0);
    *lock(&s.extension_override) = Some(scratch.join("missing.ts"));
    let error = s.warm_up().await.unwrap_err().to_string();
    println!("missing extension: {error}");
    assert!(
        error.contains("missing.ts"),
        "error does not name the extension"
    );
    s.shutdown().await;
}
