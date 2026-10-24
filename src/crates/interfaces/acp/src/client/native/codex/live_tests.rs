//! Live protocol checks against the real `codex app-server` on PATH.
//!
//! Every test is `#[ignore]` and additionally requires `OBF_LIVE_CODEX=1`, so
//! CI and a plain `cargo test` never spawn Codex or spend model tokens. Run:
//!
//! ```text
//! OBF_LIVE_CODEX=1 cargo test -p openbitfun-acp --lib live_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Each test uses a throwaway temp directory as the workspace.
use super::*;
use openbitfun_core::agentic::events::EventQueueConfig;
use openbitfun_core::agentic::session::{ResolvedSessionTitle, SessionTitleMethod};
use openbitfun_events::ToolEventData;

const TURN_TIMEOUT: Duration = Duration::from_secs(240);

fn enabled() -> bool {
    std::env::var("OBF_LIVE_CODEX").as_deref() == Ok("1")
}

fn workspace() -> String {
    let dir = std::env::temp_dir().join(format!("obf-live-codex-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().to_string()
}

fn session(workspace: &str, thread: Option<String>) -> Arc<CodexNativeSession> {
    let session = CodexNativeSession::new(
        "live".into(),
        thread,
        workspace.into(),
        None,
        Arc::new(EventQueue::new(EventQueueConfig::default())),
        None,
        0,
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

/// Prefer a cheap reasoning effort when the model offers one.
async fn cheap(session: &Arc<CodexNativeSession>) {
    session.warm_up().await.unwrap();
    for effort in ["none", "minimal", "low"] {
        if session
            .set_config_option(
                THINKING_CONFIG_ID,
                AcpSessionConfigValue::Select {
                    value: effort.into(),
                },
            )
            .await
            .is_ok()
        {
            println!("reasoning effort: {effort}");
            return;
        }
    }
    let rows = session.model_rows();
    println!(
        "reasoning effort: default (no cheap effort offered; model={:?} efforts={:?})",
        session.effective_model(&rows),
        session.supported_efforts(&rows)
    );
}

fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 360 {
        format!("{}...", &text[..text.floor_char_boundary(360)])
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
            other => format!("Tool {other:?}").chars().take(200).collect(),
        },
        AgenticEvent::TokenUsageUpdated {
            input_tokens,
            total_tokens,
            max_context_tokens,
            cached_tokens,
            ..
        } => format!(
            "TokenUsage input={input_tokens} total={total_tokens} cached={cached_tokens:?} window={max_context_tokens:?}"
        ),
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

fn frame_summary(frame: &Value) -> String {
    let method = frame["method"].as_str().unwrap_or("<response>");
    if method.ends_with("/delta") {
        return format!("{method} {:?}", frame["params"]["delta"]);
    }
    format!("{method} id={} {}", frame["id"], short(&frame["params"]))
}

/// What the test does with an approval request.
#[derive(Clone, Copy)]
enum OnApproval {
    Answer(&'static str),
    Leave,
}

struct TurnRun {
    events: Vec<AgenticEvent>,
    /// (permission id, {method, params})
    approvals: Vec<(String, Value)>,
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
}

/// Collect events of `turn` until it ends, answering approvals per `policy`.
/// `during` runs once, as soon as Codex reported the turn id.
async fn drive<F, Fut>(
    session: &Arc<CodexNativeSession>,
    turn: &str,
    policy: OnApproval,
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
            print_tap(&tap(session));
            panic!("turn {turn} timed out");
        }
        for envelope in session.queue.dequeue_batch(1000).await {
            println!("  event: {}", describe(&envelope.event));
            let end = is_end(&envelope.event, turn);
            run.events.push(envelope.event);
            if end {
                return run;
            }
        }
        let pending: Vec<(String, String, Value)> = session
            .approvals
            .iter()
            .filter(|entry| !run.approvals.iter().any(|(id, _)| id == entry.key()))
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.method.clone(),
                    entry.params.clone(),
                )
            })
            .collect();
        for (id, method, params) in pending {
            println!("  approval: {method} {}", short(&params));
            run.approvals
                .push((id.clone(), json!({"method": method, "params": params})));
            if let OnApproval::Answer(option) = policy {
                let handled = session
                    .submit_permission(&SubmitAcpPermissionResponseRequest {
                        permission_id: id,
                        approve: option != "reject_once",
                        option_id: Some(option.into()),
                        answers: None,
                        feedback: None,
                    })
                    .await;
                assert!(matches!(handled, Some(Ok(()))), "approval answer failed");
                println!("  answered: {option}");
            }
        }
        if during.is_some() && session.state.lock().await.codex_turn_id.is_some() {
            (during.take().unwrap())().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn turn(
    session: &Arc<CodexNativeSession>,
    turn_id: &str,
    prompt: &str,
    policy: OnApproval,
) -> TurnRun {
    println!("--- turn {turn_id}: {prompt}");
    session
        .start_turn(turn_id.into(), prompt.to_string().into(), None)
        .await
        .unwrap();
    drive(
        session,
        turn_id,
        policy,
        None::<fn() -> std::future::Ready<()>>,
    )
    .await
}

fn tap(session: &CodexNativeSession) -> Vec<Value> {
    lock(&session.frame_tap).clone()
}

fn print_tap(frames: &[Value]) {
    for frame in frames {
        println!("  frame: {}", frame_summary(frame));
    }
}

/// Response to the request of `method` in the tapped frames (responses carry
/// the request id; requests are not tapped, so match by result shape).
fn response_with(frames: &[Value], key: &str) -> Option<Value> {
    frames
        .iter()
        .filter(|f| f.get("method").is_none())
        .map(|f| f["result"].clone())
        .rfind(|r| !r[key].is_null())
}

/// Scenarios 1, 5 and 6: plain prompt, restart + resume, fork and rollback.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_prompt_resume_fork_rollback() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let first = session(&dir, None);
    cheap(&first).await;
    let options = first.options();
    println!(
        "models: {:?} current={:?}",
        options
            .available_models
            .iter()
            .map(|m| m.id.as_str())
            .collect::<Vec<_>>(),
        options.current_model_id
    );
    assert!(!options.available_models.is_empty(), "model list is empty");

    // 1. Plain prompt.
    let run = turn(
        &first,
        "t1",
        "Reply with exactly the word pong and nothing else.",
        OnApproval::Leave,
    )
    .await;
    println!(
        "t1 text={:?} end={} usage={:?}",
        run.text(),
        run.ended_with(),
        first.options().context_usage
    );
    assert_eq!(run.ended_with(), "completed");
    assert!(run.text().to_lowercase().contains("pong"));
    let usage_frames: Vec<Value> = tap(&first)
        .into_iter()
        .filter(|f| f["method"] == "thread/tokenUsage/updated")
        .collect();
    for frame in &usage_frames {
        println!("  usage frame: {}", frame["params"]["tokenUsage"]);
    }
    assert!(first.options().context_usage.is_some());
    let thread = first.thread_id().expect("thread id");
    let c1 = lock(&first.history).turns[0]
        .codex_turn_id
        .clone()
        .expect("codex turn id");
    println!("thread={thread} codex turn t1={c1}");
    first.shutdown().await;
    drop(first);

    // 5. Restart: a new process resumes the stored thread.
    let second = session(&dir, Some(thread.clone()));
    second.apply_history(CodexHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            codex_turn_id: Some(c1.clone()),
        }],
        pending: None,
    });
    second.state.lock().await.index = 1;
    cheap(&second).await;
    let run = turn(
        &second,
        "t2",
        "What single word did you reply with in your previous message? Reply with that word only.",
        OnApproval::Leave,
    )
    .await;
    let frames = tap(&second);
    let resume = frames
        .iter()
        .any(|f| f.get("method").is_none() && f["result"]["thread"]["id"] == thread.as_str());
    println!(
        "t2 text={:?} end={} resumed_same_thread={resume}",
        run.text(),
        run.ended_with()
    );
    assert_eq!(run.ended_with(), "completed");
    assert!(run.text().to_lowercase().contains("pong"), "context lost");
    let c2 = lock(&second.history).turns[1]
        .codex_turn_id
        .clone()
        .expect("codex turn id t2");
    second.shutdown().await;

    // 6a. Fork at t1 (no model call: only thread/fork).
    let fork = session(&dir, None);
    fork.apply_history(CodexHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            codex_turn_id: Some(c1.clone()),
        }],
        pending: Some(PendingFork {
            thread_id: thread.clone(),
            last_turn_id: Some(c1.clone()),
        }),
    });
    let process = fork.ensure_process().await.unwrap();
    let forked = fork.ensure_thread(&process).await.unwrap();
    let response = response_with(&tap(&fork), "thread").expect("fork response");
    let forked_turns: Vec<&str> = response["thread"]["turns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| t["id"].as_str())
        .collect();
    println!(
        "fork: new thread={forked} turns={forked_turns:?} source t1={c1} t2={c2} rebound={:?}",
        lock(&fork.history).turns
    );
    assert_ne!(forked, thread);
    fork.shutdown().await;

    // 6b. Rollback t2 on a session that ran both turns: the next launch
    // forks the source thread through t1.
    let rolled = session(&dir, Some(thread.clone()));
    rolled.apply_history(CodexHistory {
        turns: vec![
            TurnAnchor {
                turn_id: "t1".into(),
                turn_index: 0,
                codex_turn_id: Some(c1.clone()),
            },
            TurnAnchor {
                turn_id: "t2".into(),
                turn_index: 1,
                codex_turn_id: Some(c2.clone()),
            },
        ],
        pending: None,
    });
    rolled.state.lock().await.index = 2;
    let outcome = rolled.rollback_last_turn("t2").await.unwrap();
    println!(
        "rollback outcome={outcome:?} pending={:?}",
        lock(&rolled.history).pending
    );
    let process = rolled.ensure_process().await.unwrap();
    let after = rolled.ensure_thread(&process).await.unwrap();
    let response = response_with(&tap(&rolled), "thread").expect("rollback fork response");
    println!(
        "rollback: thread={after} turns={:?} anchors={:?}",
        response["thread"]["turns"]
            .as_array()
            .map(|t| t.iter().map(|t| t["id"].clone()).collect::<Vec<_>>()),
        lock(&rolled.history).turns
    );
    assert_ne!(after, thread);
    assert_eq!(lock(&rolled.history).turns.len(), 1);
    rolled.shutdown().await;

    // Uncertainty: thread/resume of a thread with no turns.
    let empty = session(&dir, None);
    let process = empty.ensure_process().await.unwrap();
    let blank = empty.ensure_thread(&process).await.unwrap();
    empty.shutdown().await;
    let reopened = session(&dir, Some(blank.clone()));
    let process = reopened.ensure_process().await.unwrap();
    let resumed = reopened.ensure_thread(&process).await;
    println!("resume of empty thread {blank}: {resumed:?}");
    if resumed.is_err() {
        print_tap(&tap(&reopened));
    }
    reopened.shutdown().await;
}

/// Scenarios 2 and 3: approval allow once, then decline.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_approvals() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let mode = std::env::var("OBF_LIVE_CODEX_MODE").unwrap_or_else(|_| "default".into());
    let session = session(&dir, None);
    cheap(&session).await;
    session
        .set_config_option(
            PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select {
                value: mode.clone(),
            },
        )
        .await
        .unwrap();
    println!("permission mode: {mode}");
    let run = turn(
        &session,
        "t1",
        "Create a file named a.txt in the current directory containing exactly: x",
        OnApproval::Answer("allow_once"),
    )
    .await;
    let frames = tap(&session);
    print_tap(
        &frames
            .iter()
            .filter(|f| !f["method"].as_str().unwrap_or("").ends_with("delta"))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let created = std::fs::read_to_string(std::path::Path::new(&dir).join("a.txt"));
    println!(
        "t1 end={} approvals={} a.txt={created:?}",
        run.ended_with(),
        run.approvals.len()
    );
    // Ordering of approval request vs item/started for the same item.
    for (_, approval) in &run.approvals {
        let item = approval["params"]["itemId"].as_str().unwrap_or_default();
        let request = frames.iter().position(|f| {
            f["method"]
                .as_str()
                .is_some_and(|m| m.ends_with("requestApproval"))
                && f["params"]["itemId"] == item
        });
        let started = frames
            .iter()
            .position(|f| f["method"] == "item/started" && f["params"]["item"]["id"] == item);
        println!(
            "ordering {} item={item}: request@{request:?} item/started@{started:?}",
            approval["method"]
        );
    }
    assert_eq!(run.ended_with(), "completed");

    let before = frames.len();
    let run = turn(
        &session,
        "t2",
        "Create a file named b.txt in the current directory containing exactly: y",
        OnApproval::Answer("reject_once"),
    )
    .await;
    print_tap(
        &tap(&session)[before..]
            .iter()
            .filter(|f| !f["method"].as_str().unwrap_or("").ends_with("delta"))
            .cloned()
            .collect::<Vec<_>>(),
    );
    println!(
        "t2 end={} approvals={} b.txt exists={} text={:?}",
        run.ended_with(),
        run.approvals.len(),
        std::path::Path::new(&dir).join("b.txt").exists(),
        run.text()
    );
    session.shutdown().await;
}

/// Scenario 4: steer a running turn.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_steer() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let session = session(&dir, None);
    cheap(&session).await;
    session
        .set_config_option(
            PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select {
                value: "full-access".into(),
            },
        )
        .await
        .unwrap();
    println!("--- turn t1 (steered)");
    session
        .start_turn(
            "t1".into(),
            "Run `ping -n 6 127.0.0.1` in the shell, then reply with the word done."
                .to_string()
                .into(),
            None,
        )
        .await
        .unwrap();
    let steerer = session.clone();
    let run = drive(
        &session,
        "t1",
        OnApproval::Answer("allow_once"),
        Some(move || async move {
            let result = steerer
                .steer(
                    "t1",
                    "Also end your final reply with the word steered."
                        .to_string()
                        .into(),
                    None,
                )
                .await;
            println!("  steer result: {result:?}");
        }),
    )
    .await;
    let steered = run
        .events
        .iter()
        .any(|e| matches!(e, AgenticEvent::UserSteeringInjected { .. }));
    println!(
        "t1 end={} steering_event={steered} text={:?}",
        run.ended_with(),
        run.text()
    );
    print_tap(
        &tap(&session)
            .iter()
            .filter(|f| !f["method"].as_str().unwrap_or("").ends_with("delta"))
            .cloned()
            .collect::<Vec<_>>(),
    );
    session.shutdown().await;
}

/// Scenario 7: interrupt a turn that is waiting on an approval.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_interrupt() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let mode = std::env::var("OBF_LIVE_CODEX_MODE").unwrap_or_else(|_| "read-only".into());
    let session = session(&dir, None);
    cheap(&session).await;
    session
        .set_config_option(
            PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select {
                value: mode.clone(),
            },
        )
        .await
        .unwrap();
    println!("--- turn t1 (interrupted), permission mode {mode}");
    session
        .start_turn(
            "t1".into(),
            "Create a file named c.txt in the current directory containing exactly: z"
                .to_string()
                .into(),
            None,
        )
        .await
        .unwrap();
    // Wait for the approval (or a running tool), then cancel.
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    let mut events = Vec::new();
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no approval arrived"
        );
        for envelope in session.queue.dequeue_batch(1000).await {
            println!("  event: {}", describe(&envelope.event));
            events.push(envelope.event);
        }
        if !session.approvals.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before = tap(&session).len();
    let started = std::time::Instant::now();
    session.cancel().await.unwrap();
    println!(
        "cancel took {:?}; approvals left={}",
        started.elapsed(),
        session.approvals.len()
    );
    for envelope in session.queue.dequeue_batch(1000).await {
        println!("  event: {}", describe(&envelope.event));
        events.push(envelope.event);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    print_tap(&tap(&session)[before..]);
    let process_alive = session
        .process
        .lock()
        .await
        .as_ref()
        .is_some_and(|p| p.alive.load(Ordering::Acquire));
    println!(
        "cancelled={} process_alive={process_alive} c.txt exists={}",
        events
            .iter()
            .any(|e| is_end(e, "t1") && matches!(e, AgenticEvent::DialogTurnCancelled { .. })),
        std::path::Path::new(&dir).join("c.txt").exists()
    );
    assert!(session.approvals.is_empty());
    session.shutdown().await;
}

/// Fork and rollback of an existing thread without any model call:
/// `OBF_LIVE_CODEX_THREAD=<thread> OBF_LIVE_CODEX_TURNS=<c1>,<c2>`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_fork_existing_thread() {
    if !enabled() {
        return;
    }
    let (Ok(thread), Ok(turns)) = (
        std::env::var("OBF_LIVE_CODEX_THREAD"),
        std::env::var("OBF_LIVE_CODEX_TURNS"),
    ) else {
        return;
    };
    let turns: Vec<String> = turns.split(',').map(str::to_string).collect();
    let anchors: Vec<TurnAnchor> = turns
        .iter()
        .enumerate()
        .map(|(index, codex)| TurnAnchor {
            turn_id: format!("t{}", index + 1),
            turn_index: index,
            codex_turn_id: Some(codex.clone()),
        })
        .collect();
    let dir = workspace();
    let fork = session(&dir, None);
    fork.apply_history(CodexHistory {
        turns: anchors[..1].to_vec(),
        pending: Some(PendingFork {
            thread_id: thread.clone(),
            last_turn_id: Some(turns[0].clone()),
        }),
    });
    let process = fork.ensure_process().await.unwrap();
    let forked = fork.ensure_thread(&process).await;
    let response = response_with(&tap(&fork), "thread");
    println!(
        "fork: {forked:?} turns={:?} source={turns:?} rebound={:?}",
        response.as_ref().map(|r| r["thread"]["turns"]
            .as_array()
            .map(|t| t.iter().map(|t| t["id"].clone()).collect::<Vec<_>>())),
        lock(&fork.history).turns
    );
    fork.shutdown().await;
    let forked = forked.unwrap();
    assert_ne!(forked, thread);

    let rolled = session(&dir, Some(thread.clone()));
    rolled.apply_history(CodexHistory {
        turns: anchors.clone(),
        pending: None,
    });
    rolled.state.lock().await.index = anchors.len();
    let last = anchors.last().unwrap().turn_id.clone();
    let outcome = rolled.rollback_last_turn(&last).await.unwrap();
    println!(
        "rollback outcome={outcome:?} pending={:?}",
        lock(&rolled.history).pending
    );
    let process = rolled.ensure_process().await.unwrap();
    let after = rolled.ensure_thread(&process).await;
    let response = response_with(&tap(&rolled), "thread");
    println!(
        "rollback: {after:?} turns={:?} anchors={:?}",
        response.as_ref().map(|r| r["thread"]["turns"]
            .as_array()
            .map(|t| t.iter().map(|t| t["id"].clone()).collect::<Vec<_>>())),
        lock(&rolled.history).turns
    );
    rolled.shutdown().await;
    assert_ne!(after.unwrap(), thread);

    // Uncertainty: thread/resume of a thread with no turns.
    let empty = session(&dir, None);
    let process = empty.ensure_process().await.unwrap();
    let blank = empty.ensure_thread(&process).await.unwrap();
    empty.shutdown().await;
    let reopened = session(&dir, Some(blank.clone()));
    let process = reopened.ensure_process().await.unwrap();
    let resumed = reopened.ensure_thread(&process).await;
    println!("resume of empty thread {blank}: {resumed:?}");
    reopened.shutdown().await;
}

/// File-change approval: does `item/fileChange/requestApproval` precede or
/// follow `item/started` of the same item?
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the real codex app-server; set OBF_LIVE_CODEX=1"]
async fn live_file_change_approval() {
    if !enabled() {
        return;
    }
    let dir = workspace();
    let session = session(&dir, None);
    cheap(&session).await;
    let run = turn(
        &session,
        "t1",
        "Use the apply_patch tool (not the shell) to add a new file d.txt containing the single line w. Do nothing else.",
        OnApproval::Answer("allow_once"),
    )
    .await;
    let frames = tap(&session);
    for (_, approval) in &run.approvals {
        let item = approval["params"]["itemId"].as_str().unwrap_or_default();
        let request = frames.iter().position(|f| {
            f["method"]
                .as_str()
                .is_some_and(|m| m.ends_with("requestApproval"))
                && f["params"]["itemId"] == item
        });
        let started = frames
            .iter()
            .position(|f| f["method"] == "item/started" && f["params"]["item"]["id"] == item);
        println!(
            "ordering {} item={item}: request@{request:?} item/started@{started:?} params={}",
            approval["method"],
            short(&approval["params"])
        );
    }
    println!(
        "end={} d.txt={:?}",
        run.ended_with(),
        std::fs::read_to_string(std::path::Path::new(&dir).join("d.txt"))
    );
    session.shutdown().await;
}
