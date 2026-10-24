use super::*;
use openbitfun_core::agentic::events::EventQueueConfig;
use tokio::io::DuplexStream;

/// A session with a live in-memory Pi process installed in its slot. The
/// returned stream carries what the session writes to Pi's stdin.
async fn fixture() -> (Arc<PiNativeSession>, Arc<PiProcess>, DuplexStream) {
    let session = PiNativeSession::new(
        "s1".into(),
        "pi-1".into(),
        ".".into(),
        None,
        false,
        Arc::new(EventQueue::new(EventQueueConfig::default())),
        None,
        0,
        None,
    );
    let (writer, reader) = tokio::io::duplex(65536);
    let process = Arc::new(PiProcess::new(
        Box::pin(writer),
        None,
        None,
        tokio::sync::watch::channel(None).1,
    ));
    *session.process.lock().await = Some(process.clone());
    // `initialize` would have seen the approval bridge in `get_commands`.
    session.bridge_loaded.store(true, Ordering::Release);
    (session, process, reader)
}

/// Commands sent, without the `get_entries` syncs around prompts.
fn kinds(sent: &Sent) -> Vec<Value> {
    sent.lock()
        .unwrap()
        .iter()
        .filter(|c| c["type"] != "get_entries")
        .cloned()
        .collect()
}

type Sent = Arc<std::sync::Mutex<Vec<Value>>>;

/// Answer every command with `success:true` and `data(command)`, routing
/// the response through the session like the reader does. Commands seen are
/// recorded in `Sent`; abort the task when done (it holds the stdin pipe).
fn respond(
    session: Arc<PiNativeSession>,
    process: Arc<PiProcess>,
    output: DuplexStream,
    data: fn(&Value) -> Value,
) -> (tokio::task::JoinHandle<()>, Sent) {
    let sent: Sent = Default::default();
    let record = sent.clone();
    let task = tokio::spawn(async move {
        let mut lines = BufReader::new(output).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let command: Value = serde_json::from_str(&line).unwrap();
            record.lock().unwrap().push(command.clone());
            if command["type"] == "extension_ui_response" {
                continue;
            }
            if command["type"] == "abort" {
                // Pi answers `abort` once it went idle.
                session
                    .route_frame(&process, json!({"type":"agent_settled","aborted":true}))
                    .await
                    .unwrap();
            }
            let response = json!({"type":"response", "id":command["id"],
                "command":command["type"], "success":true, "data":data(&command)});
            session.route_frame(&process, response).await.unwrap();
        }
    });
    (task, sent)
}

async fn wait_until(sent: &Sent, predicate: impl Fn(&[Value]) -> bool) -> Vec<Value> {
    for _ in 0..200 {
        let snapshot = sent.lock().unwrap().clone();
        if predicate(&snapshot) {
            return snapshot;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!(
        "expected commands were not sent: {:?}",
        sent.lock().unwrap()
    );
}

fn started(_: &Value) -> Value {
    json!({"disposition":"started"})
}

async fn events(session: &PiNativeSession) -> Vec<AgenticEvent> {
    session
        .queue
        .dequeue_batch(200)
        .await
        .into_iter()
        .map(|e| e.event)
        .collect()
}

async fn route(session: &Arc<PiNativeSession>, process: &Arc<PiProcess>, frames: &[Value]) {
    for frame in frames {
        session.route_frame(process, frame.clone()).await.unwrap();
    }
}

#[tokio::test]
async fn prompt_streams_a_turn_and_settles_over_memory_stdio() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, started);
    session
        .start_turn("t1".into(), "list files".into(), None)
        .await
        .unwrap();
    assert!(session.resume.load(Ordering::Acquire));
    route(
        &session,
        &process,
        &[
            json!({"type":"agent_start"}),
            json!({"type":"turn_start"}),
            json!({"type":"message_start","message":{"role":"user","content":"list files"}}),
            json!({"type":"message_start","message":{"role":"assistant"}}),
            json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Here."}}),
            json!({"type":"tool_execution_start","toolCallId":"c1","toolName":"ls","args":{}}),
            json!({"type":"tool_execution_end","toolCallId":"c1","toolName":"ls",
                "result":{"content":[{"type":"text","text":"a.txt"}]},"isError":false}),
            json!({"type":"turn_end"}),
            json!({"type":"agent_settled","aborted":false}),
        ],
    )
    .await;
    assert!(session.state.lock().await.projector.is_none());
    let events = events(&session).await;
    assert!(matches!(
        events[0],
        AgenticEvent::DialogTurnStarted { turn_index: 0, .. }
    ));
    assert!(events
        .iter()
        .any(|e| matches!(e, AgenticEvent::TextChunk { text, .. } if text == "Here.")));
    assert!(matches!(
        events.last(),
        Some(AgenticEvent::DialogTurnCompleted { total_tools: 1, .. })
    ));
    task.abort();
    let sent = kinds(&sent);
    assert_eq!(sent[0]["type"], "prompt");
    assert_eq!(sent[0]["message"], "list files");
}

#[tokio::test]
async fn images_and_steering_reach_pi_and_render_when_taken_in() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, |command| {
        if command["type"] == "steer" {
            json!({"disposition":"queued"})
        } else {
            json!({"disposition":"started"})
        }
    });
    let input = NativeTurnInput {
        text: "what is this".into(),
        images: vec![super::super::super::NativeImageInput {
            media_type: "image/png".into(),
            data: "aGk=".into(),
        }],
    };
    session.start_turn("t1".into(), input, None).await.unwrap();
    route(
        &session,
        &process,
        &[json!({"type":"message_start","message":{"role":"user","content":"what is this"}})],
    )
    .await;
    let steering_id = session
        .steer("t1", "use the other file".into(), None)
        .await
        .unwrap();
    assert!(session.steer("t2", "late".into(), None).await.is_err());
    route(
        &session,
        &process,
        &[json!({"type":"message_start","message":{"role":"user","content":"use the other file"}})],
    )
    .await;
    let events = events(&session).await;
    assert!(events.iter().any(|e| matches!(e,
        AgenticEvent::UserSteeringInjected { steering_id: id, content, .. }
            if *id == steering_id && content == "use the other file")));
    task.abort();
    let sent = kinds(&sent);
    assert_eq!(sent[0]["images"][0]["mimeType"], "image/png");
    assert_eq!(sent[0]["images"][0]["data"], "aGk=");
    assert_eq!(sent[1]["type"], "steer");
}

#[tokio::test]
async fn cancel_aborts_and_closes_the_turn_as_cancelled() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, started);
    session
        .start_turn("t1".into(), "work".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[
            json!({"type":"turn_start"}),
            json!({"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"sleep 60"}}),
        ],
    )
    .await;
    session.cancel().await.unwrap();
    assert!(session.state.lock().await.projector.is_none());
    // The process stays alive for the next turn.
    assert!(process.alive.load(Ordering::Acquire));
    let events = events(&session).await;
    assert!(events.iter().any(|e| matches!(
        e,
        AgenticEvent::ToolEvent {
            tool_event: openbitfun_events::ToolEventData::Cancelled { .. },
            ..
        }
    )));
    assert!(matches!(
        events.last(),
        Some(AgenticEvent::DialogTurnCancelled { .. })
    ));
    task.abort();
    let kinds: Vec<_> = kinds(&sent).iter().map(|c| c["type"].clone()).collect();
    // A settled turn may also refresh `get_session_stats` for the context meter.
    assert_eq!(kinds[..2], [json!("prompt"), json!("abort")]);
}

#[tokio::test]
async fn manual_compact_is_a_command_turn_and_dialogs_are_cancelled() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, |command| {
        if command["type"] == "compact" {
            json!({"summary":"s","tokensBefore":5000,"estimatedTokensAfter":1000})
        } else {
            Value::Null
        }
    });
    session
        .start_turn("t1".into(), "/compact keep the API notes".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[json!({"type":"extension_ui_request","id":"ui-1","method":"confirm","title":"Allow?"})],
    )
    .await;
    for _ in 0..50 {
        if session.state.lock().await.projector.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let events = events(&session).await;
    assert!(events.iter().any(|e| matches!(
        e,
        AgenticEvent::ContextCompressionCompleted {
            tokens_before: 5000,
            tokens_after: 1000,
            ..
        }
    )));
    assert!(matches!(
        events.last(),
        Some(AgenticEvent::DialogTurnCompleted { .. })
    ));
    let sent = wait_until(&sent, |s| {
        s.iter().any(|c| c["type"] == "extension_ui_response")
    })
    .await;
    task.abort();
    let compact = sent.iter().find(|c| c["type"] == "compact").unwrap();
    assert_eq!(compact["customInstructions"], "keep the API notes");
    let dialog = sent
        .iter()
        .find(|c| c["type"] == "extension_ui_response")
        .unwrap();
    assert_eq!(
        (dialog["id"].as_str(), dialog["cancelled"].as_bool()),
        (Some("ui-1"), Some(true))
    );
}

fn approval_frame(ui_id: &str, call_id: &str, tool: &str, input: Value) -> Value {
    let call = json!({"toolCallId": call_id, "toolName": tool, "input": input});
    json!({"type":"extension_ui_request", "id": ui_id, "method":"select",
        "title": format!("{}{}", approval::MARKER, call),
        "options": [approval::ALLOW_ONCE, approval::ALLOW_SESSION,
            approval::REJECT_ONCE, approval::REJECT_READ_ONLY]})
}

fn ui_responses(sent: &Sent) -> Vec<Value> {
    sent.lock()
        .unwrap()
        .iter()
        .filter(|c| c["type"] == "extension_ui_response")
        .cloned()
        .collect()
}

#[tokio::test]
async fn approval_dialog_becomes_a_card_and_the_answer_reaches_pi() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, started);
    session
        .start_turn("t1".into(), "fix it".into(), None)
        .await
        .unwrap();
    let rm = json!({"command":"rm -rf build"});
    route(
        &session,
        &process,
        &[
            json!({"type":"turn_start"}),
            json!({"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":rm}),
            approval_frame("ui-1", "c1", "bash", rm.clone()),
        ],
    )
    .await;
    assert_eq!(session.approvals.len(), 1);
    let permission_id = session.approvals.iter().next().unwrap().key().clone();
    let pending = session.approvals.get(&permission_id).unwrap();
    let payload = pending.request.permission_payload(&permission_id, "s1");
    drop(pending);
    assert_eq!(payload["toolCall"]["toolCallId"], "c1");
    assert_eq!(payload["toolCall"]["kind"], "execute");
    assert_eq!(payload["toolCall"]["title"], "rm -rf build");
    let options: Vec<_> = payload["options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["optionId"].clone())
        .collect();
    assert_eq!(
        options,
        [
            json!("allow_once"),
            json!("allow_session"),
            json!("reject_once")
        ]
    );
    // Another session's permission is left alone.
    let foreign = SubmitAcpPermissionResponseRequest {
        permission_id: "other".into(),
        approve: true,
        option_id: None,
        answers: None,
        feedback: None,
    };
    assert!(session.submit_permission(&foreign).await.is_none());
    let request = SubmitAcpPermissionResponseRequest {
        permission_id,
        approve: true,
        option_id: Some("allow_session".into()),
        answers: None,
        feedback: None,
    };
    session.submit_permission(&request).await.unwrap().unwrap();
    // The same command is allowed for the rest of the session without a card.
    route(
        &session,
        &process,
        &[approval_frame("ui-2", "c2", "bash", rm)],
    )
    .await;
    assert!(session.approvals.is_empty());
    let replies = wait_until(&sent, |s| {
        s.iter()
            .filter(|c| c["type"] == "extension_ui_response")
            .count()
            == 2
    })
    .await;
    task.abort();
    let replies: Vec<_> = replies
        .into_iter()
        .filter(|c| c["type"] == "extension_ui_response")
        .collect();
    assert_eq!(
        replies[0],
        json!({"type":"extension_ui_response","id":"ui-1","value":"allow_session"})
    );
    assert_eq!(
        replies[1],
        json!({"type":"extension_ui_response","id":"ui-2","value":"allow_once"})
    );
}

#[tokio::test]
async fn unanswered_approvals_are_cancelled_when_the_turn_ends() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, started);
    session
        .start_turn("t1".into(), "edit".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[approval_frame(
            "ui-1",
            "c1",
            "write",
            json!({"path":"a.txt","content":"x"}),
        )],
    )
    .await;
    assert_eq!(session.approvals.len(), 1);
    route(
        &session,
        &process,
        &[json!({"type":"agent_settled","aborted":false})],
    )
    .await;
    assert!(session.approvals.is_empty());
    // Outside a turn nothing can show the card: refuse at once.
    route(
        &session,
        &process,
        &[approval_frame("ui-2", "c2", "write", json!({}))],
    )
    .await;
    // A cancelled turn settles its approvals too.
    session
        .start_turn("t2".into(), "again".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[approval_frame("ui-3", "c3", "bash", json!({}))],
    )
    .await;
    session.cancel().await.unwrap();
    assert!(session.approvals.is_empty());
    wait_until(&sent, |s| {
        s.iter()
            .filter(|c| c["type"] == "extension_ui_response")
            .count()
            == 3
    })
    .await;
    task.abort();
    for (reply, id) in ui_responses(&sent).iter().zip(["ui-1", "ui-2", "ui-3"]) {
        assert_eq!(
            *reply,
            json!({"type":"extension_ui_response","id":id,"cancelled":true})
        );
    }
}

#[tokio::test]
async fn permission_modes_answer_without_cards_and_a_missing_bridge_fails_loudly() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, started);
    session
        .set_config_option(
            approval::PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select {
                value: "read-only".into(),
            },
        )
        .await
        .unwrap();
    session
        .start_turn("t1".into(), "look".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[approval_frame("ui-1", "c1", "edit", json!({}))],
    )
    .await;
    *lock(&session.permission_mode) = approval::MODE_FULL_ACCESS.into();
    route(
        &session,
        &process,
        &[approval_frame("ui-2", "c2", "edit", json!({}))],
    )
    .await;
    assert!(session.approvals.is_empty());
    route(
        &session,
        &process,
        &[json!({"type":"agent_settled","aborted":false})],
    )
    .await;
    wait_until(&sent, |s| {
        s.iter()
            .filter(|c| c["type"] == "extension_ui_response")
            .count()
            == 2
    })
    .await;
    let replies = ui_responses(&sent);
    assert_eq!(replies[0]["value"], "reject_read_only");
    assert_eq!(replies[1]["value"], "allow_once");
    let options = session.options();
    let mode = options
        .config_options
        .iter()
        .find(|o| o.id == approval::PERMISSION_MODE_CONFIG_ID)
        .unwrap();
    assert_eq!(mode.category.as_deref(), Some("mode"));
    assert!(options.capabilities.unwrap().permission_mode);
    // Without the bridge, an approval mode must not run Pi unguarded.
    session.bridge_loaded.store(false, Ordering::Release);
    *lock(&session.permission_mode) = approval::MODE_ASK.into();
    let error = session
        .start_turn("t2".into(), "go".into(), None)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("approval extension did not load"));
    assert!(session
        .set_config_option(
            approval::PERMISSION_MODE_CONFIG_ID,
            AcpSessionConfigValue::Select {
                value: "read-only".into()
            },
        )
        .await
        .is_err());
    task.abort();
}

fn tree_responses(command: &Value) -> Value {
    match command["type"].as_str() {
        Some("get_entries") if command.get("since").is_none() => json!({"entries": [
            {"type":"message","id":"u1","message":{"role":"user"}},
            {"type":"message","id":"a1","message":{"role":"assistant"}}]}),
        Some("get_entries") => json!({"entries": [
            {"type":"message","id":"u2","message":{"role":"user"}},
            {"type":"message","id":"a2","message":{"role":"assistant"}}]}),
        Some("get_state") => json!({"sessionId": "pi-branch"}),
        Some("fork") => json!({"text": "second", "cancelled": false}),
        _ => json!({"disposition":"started"}),
    }
}

#[tokio::test]
async fn rollback_forks_before_the_turn_prompt_and_rebinds_the_session() {
    let (session, process, output) = fixture().await;
    let (task, sent) = respond(session.clone(), process.clone(), output, tree_responses);
    // A session loaded with an anchored first turn.
    session.apply_history(PiHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            prompted: true,
            entry_id: Some("u1".into()),
        }],
        pending: None,
        cursor: Some("a1".into()),
        synced: true,
    });
    session.state.lock().await.index = 1;
    session
        .start_turn("t2".into(), "second".into(), None)
        .await
        .unwrap();
    route(
        &session,
        &process,
        &[json!({"type":"agent_settled","aborted":false})],
    )
    .await;
    // The prompt's user message is attributed from `get_entries`.
    for _ in 0..100 {
        if lock(&session.history).turns[1].entry_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        lock(&session.history).turns[1].entry_id.as_deref(),
        Some("u2")
    );
    assert!(session.clone().rollback_last_turn("t1").await.is_err());
    let outcome = session.clone().rollback_last_turn("t2").await.unwrap();
    assert!(!outcome.files_rewound);
    assert!(outcome
        .note
        .unwrap()
        .contains("does not restore workspace files"));
    task.abort();
    let fork = kinds(&sent)
        .into_iter()
        .find(|c| c["type"] == "fork")
        .unwrap();
    assert_eq!(fork["entryId"], "u2");
    assert_eq!(session.active_pi_session(), "pi-branch");
    let history = lock(&session.history).clone();
    assert_eq!(history.turns.len(), 1);
    assert!(history.pending.is_none() && !history.synced);
    assert_eq!(session.state.lock().await.index, 1);
}

#[test]
fn legacy_turns_cannot_roll_back() {
    let history = PiHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            prompted: true,
            entry_id: None,
        }],
        ..Default::default()
    };
    assert!(rollback_target(&history, "t1").is_err());
    assert!(rollback_target(&PiHistory::default(), "old").is_err());
}

#[test]
fn launch_args_pin_or_fork_the_pi_session_and_compact_parsing_is_strict() {
    let open = LaunchTarget::Open("abc-1".into());
    assert_eq!(
        cli_args(&open, Some("/data/ext.ts")),
        [
            "--mode",
            "rpc",
            "--session-id",
            "abc-1",
            "-e",
            "/data/ext.ts"
        ]
    );
    assert_eq!(
        cli_args(&LaunchTarget::Fork("src-1".into()), None),
        ["--mode", "rpc", "--fork", "src-1"]
    );
    let remote = remote_command("/home/me/repo", &cli_args(&open, None));
    assert!(remote.contains("exec pi --mode rpc"));
    assert!(remote.contains("obf_pi_ext"));
    assert_eq!(compact_instructions("/compact"), Some(None));
    assert_eq!(
        compact_instructions(" /compact  focus "),
        Some(Some("focus".into()))
    );
    assert_eq!(compact_instructions("/compaction"), None);
    assert_eq!(compact_instructions("please /compact"), None);
}
