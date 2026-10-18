use super::*;
use openbitfun_core::agentic::events::EventQueueConfig;
use tokio::io::{AsyncWriteExt, DuplexStream};

fn fixture() -> (Arc<ClaudeNativeSession>, Arc<NativeProcess>, DuplexStream) {
    fixture_with_notifier(None)
}

fn fixture_with_notifier(
    notifier: Option<NativeAgentNotificationSink>,
) -> (Arc<ClaudeNativeSession>, Arc<NativeProcess>, DuplexStream) {
    let session = ClaudeNativeSession::new(
        "s1".into(),
        uuid::Uuid::new_v4().to_string(),
        ".".into(),
        None,
        false,
        Arc::new(EventQueue::new(EventQueueConfig::default())),
        None,
        0,
        notifier,
    );
    let (writer, reader) = tokio::io::duplex(65536);
    let process = Arc::new(NativeProcess {
        id: "process-1".into(),
        writer: Mutex::new(Box::pin(writer)),
        controls: DashMap::new(),
        shutdown: Mutex::new(None),
        alive: AtomicBool::new(true),
        terminated: Mutex::new(None),
        exit: tokio::sync::watch::channel(None).1,
        stderr: Mutex::new(VecDeque::new()),
    });
    (session, process, reader)
}

#[tokio::test]
async fn routes_control_response_by_request_id_over_memory_stdio() {
    let (session, process, output) = fixture();
    let requester = process.clone();
    let task = tokio::spawn(async move {
        requester
            .request(
                json!({"subtype":"initialize", "hooks":null}),
                Duration::from_secs(1),
            )
            .await
    });
    let mut lines = BufReader::new(output).lines();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["request"]["subtype"], "initialize");
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":"unmatched", "response":{}}}),
        )
        .await
        .unwrap();
    assert_eq!(process.controls.len(), 1);
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":sent["request_id"], "response":{"commands":[]}}}),
        )
        .await
        .unwrap();
    assert_eq!(task.await.unwrap().unwrap()["commands"], json!([]));
    assert!(process.controls.is_empty());
}

#[tokio::test]
async fn control_timeout_does_not_leak_request() {
    let (_, process, _output) = fixture();
    assert!(process
        .request(json!({"subtype":"interrupt"}), Duration::from_millis(1))
        .await
        .is_err());
    assert!(process.controls.is_empty());
}

#[tokio::test]
async fn permission_always_allow_and_denial_use_native_wire_shape() {
    let (session, process, output) = fixture();
    let suggestions = json!([{"type":"addRules", "destination":"session", "rules":[]}]);
    for (id, option) in [("p1", "allow_always"), ("p2", "reject_once")] {
        session.permissions.insert(
            id.into(),
            PendingPermission {
                process: process.clone(),
                turn_id: "t1".into(),
                request_id: id.into(),
                tool_id: "tool-123".into(),
                input: json!({"command":"echo test"}),
                suggestions: suggestions.clone(),
            },
        );
        session
            .submit_permission(&SubmitAcpPermissionResponseRequest {
                permission_id: id.into(),
                approve: false,
                option_id: Some(option.into()),
            })
            .await
            .unwrap()
            .unwrap();
    }
    let mut lines = BufReader::new(output).lines();
    let allow: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(
        allow["response"]["response"]["updatedPermissions"],
        suggestions
    );
    assert_eq!(
        allow["response"]["response"]["updatedInput"]["command"],
        "echo test"
    );
    assert_eq!(allow["response"]["response"]["toolUseID"], "tool-123");
    let deny: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(deny["response"]["response"]["behavior"], "deny");
    assert!(session.permissions.is_empty());
}

#[tokio::test]
async fn turn_permission_cleanup_denies_only_that_turn() {
    let (session, process, output) = fixture();
    for (id, turn) in [("p1", "t1"), ("p2", "t2")] {
        session.permissions.insert(
            id.into(),
            PendingPermission {
                process: process.clone(),
                turn_id: turn.into(),
                request_id: id.into(),
                tool_id: "tool-1".into(),
                input: json!({}),
                suggestions: json!([]),
            },
        );
    }
    session.clear_permissions("t1").await;
    assert!(session.permissions.contains_key("p2"));
    let sent: Value = serde_json::from_str(
        &BufReader::new(output)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sent["response"]["request_id"], "p1");
    assert_eq!(sent["response"]["response"]["behavior"], "deny");
}

#[tokio::test]
async fn result_keeps_process_alive_and_background_turn_settles() {
    let (session, process, _output) = fixture();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session
        .route_frame(
            &process,
            json!({"type":"result", "subtype":"success", "is_error":false}),
        )
        .await
        .unwrap();
    assert!(session.state.lock().await.projector.is_none());
    assert!(process.alive.load(Ordering::Acquire));
    session
        .route_frame(
            &process,
            json!({"type":"stream_event", "event":{"type":"message_start", "message":{"id":"m2"}}}),
        )
        .await
        .unwrap();
    session.route_frame(&process, json!({"type":"assistant", "message":{"id":"m2", "content":[{"type":"text", "text":"background response"}]}})).await.unwrap();
    session
        .route_frame(
            &process,
            json!({"type":"stream_event", "event":{"type":"message_stop"}}),
        )
        .await
        .unwrap();
    assert!(session.state.lock().await.projector.is_none());
    let events = session.queue.dequeue_batch(100).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, AgenticEvent::DialogTurnStarted { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, AgenticEvent::DialogTurnCompleted { .. }))
            .count(),
        2
    );
    assert!(events.iter().any(|e| matches!(&e.event, AgenticEvent::TextChunk { text, .. } if text == "background response")));
}

#[tokio::test]
async fn cancellation_routes_interrupt_then_closes_turn_without_kill() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    let cancelling = session.clone();
    let task = tokio::spawn(async move { cancelling.cancel().await });
    let mut lines = BufReader::new(output).lines();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["request"]["subtype"], "interrupt");
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":sent["request_id"], "response":{}}}),
        )
        .await
        .unwrap();
    session
        .route_frame(&process, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    assert!(process.alive.load(Ordering::Acquire));
    assert!(session.state.lock().await.projector.is_none());
    assert!(session
        .queue
        .dequeue_batch(100)
        .await
        .iter()
        .any(|e| matches!(e.event, AgenticEvent::DialogTurnCancelled { .. })));
}

#[test]
fn permission_card_matches_existing_acp_frontend_contract() {
    let payload = permission_payload(
        "p1",
        "s1",
        &json!({"tool_use_id":"tu1", "tool_name":"Bash", "input":{"command":"echo test"}}),
    );
    assert_eq!(payload["sessionId"], "s1");
    assert_eq!(payload["toolCall"]["toolCallId"], "tu1");
    assert_eq!(payload["toolCall"]["kind"], "execute");
    assert_eq!(payload["options"][0]["optionId"], "allow_once");
    // Deserialize using the actual ACP schema, not a hand-maintained approximation.
    serde_json::from_value::<agent_client_protocol::schema::ToolCallUpdate>(
        payload["toolCall"].clone(),
    )
    .unwrap();
    serde_json::from_value::<Vec<agent_client_protocol::schema::PermissionOption>>(
        payload["options"].clone(),
    )
    .unwrap();
}

#[test]
fn ssh_command_quotes_posix_path_and_resumes_without_acp_adapter() {
    let command = remote_command("/srv/my repo's files", &cli_args("uuid-1", true));
    assert!(command.starts_with("bash -lc "));
    assert!(command.contains("exec claude"));
    assert!(command.contains("--resume uuid-1"));
    assert!(!command.contains("claude-agent-acp"));
    assert!(!command.contains("--session-id"));
    assert_eq!(cli_args("uuid-1", false).last().unwrap(), "uuid-1");
}

#[tokio::test]
async fn cancellation_timeout_signals_only_owned_supervisor() {
    let (session, process, _output) = fixture();
    let (tx, rx) = oneshot::channel();
    *process.shutdown.lock().await = Some(tx);
    *session.process.lock().await = Some(process.clone());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "cancel-timeout".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), session.cancel())
        .await
        .unwrap()
        .unwrap();
    rx.await
        .expect("only this fixture's supervisor receives kill");
    assert!(!process.alive.load(Ordering::Acquire));
    assert!(process.controls.is_empty());
    assert!(session.state.lock().await.projector.is_none());
    assert!(session
        .queue
        .dequeue_batch(100)
        .await
        .iter()
        .any(|e| matches!(e.event, AgenticEvent::DialogTurnCancelled { .. })));
}

#[tokio::test]
async fn read_loop_drains_frames_and_reports_stderr_on_eof() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    process
        .stderr
        .lock()
        .await
        .push_back("fixture diagnostic".into());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t-eof".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    let (mut writer, reader) = tokio::io::duplex(4096);
    writer.write_all(b"{\"type\":\"assistant\",\"message\":{\"id\":\"m1\",\"content\":[{\"type\":\"text\",\"text\":\"partial output\"}]}}\n").await.unwrap();
    writer.shutdown().await.unwrap();
    session
        .clone()
        .read_loop(process.clone(), Box::pin(reader), tokio::spawn(async {}))
        .await;
    let events = session.queue.dequeue_batch(100).await;
    assert!(matches!(
        events.first().map(|e| &e.event),
        Some(AgenticEvent::DialogTurnStarted { .. })
    ));
    assert!(events.iter().any(
        |e| matches!(&e.event, AgenticEvent::TextChunk { text, .. } if text == "partial output")
    ));
    assert!(
        matches!(events.last().map(|e| &e.event), Some(AgenticEvent::DialogTurnFailed { error, .. }) if error.contains("fixture diagnostic"))
    );
    assert!(!process.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn stale_process_frames_do_not_mutate_new_turn_or_permissions() {
    let (session, _process, _output) = fixture();
    let (_, retired, _retired_output) = fixture();
    // Fixture ids are deliberately equal by default; give current a new generation.
    let process = Arc::new(NativeProcess {
        id: "replacement".into(),
        writer: Mutex::new(Box::pin(tokio::io::sink())),
        controls: DashMap::new(),
        shutdown: Mutex::new(None),
        alive: AtomicBool::new(true),
        terminated: Mutex::new(None),
        exit: tokio::sync::watch::channel(None).1,
        stderr: Mutex::new(VecDeque::new()),
    });
    *session.process.lock().await = Some(process);
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "new-turn".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session
        .route_frame(&retired, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    session
        .route_frame(
            &retired,
            json!({"type":"control_request", "request_id":"old-permission", "request":{
        "subtype":"can_use_tool", "tool_use_id":"old-tool", "tool_name":"Bash", "input":{}}}),
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .state
            .lock()
            .await
            .projector
            .as_ref()
            .unwrap()
            .turn_id(),
        "new-turn"
    );
    assert!(session.permissions.is_empty());
}

#[tokio::test]
async fn permission_before_tool_use_is_projected_once_and_routed() {
    let (session, process, output) = fixture();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session.route_frame(&process, json!({"type":"control_request", "request_id":"r1", "request":{
        "subtype":"can_use_tool", "tool_name":"Bash", "tool_use_id":"tu1", "input":{"command":"echo test"}}})).await.unwrap();
    let id = session.permissions.iter().next().unwrap().key().clone();
    session
        .route_frame(
            &process,
            json!({"type":"assistant", "message":{"id":"m1", "content":[{
        "type":"tool_use", "id":"tu1", "name":"Bash", "input":{"command":"echo test"}}]}}),
        )
        .await
        .unwrap();
    session
        .submit_permission(&SubmitAcpPermissionResponseRequest {
            permission_id: id,
            approve: true,
            option_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    let sent: Value = serde_json::from_str(
        &BufReader::new(output)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sent["response"]["request_id"], "r1");
    let events = session.queue.dequeue_batch(100).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e.event,
                AgenticEvent::ToolEvent {
                    tool_event: openbitfun_events::ToolEventData::Started { .. },
                    ..
                }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn subagent_streams_into_linked_read_only_child_session() {
    let (session, process, _output) = fixture();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "parent-turn".into(),
            "go".into(),
            None,
            false,
        )
        .await
        .unwrap();
    for frame in [
        json!({"type":"system", "subtype":"task_started", "task_id":"a1", "tool_use_id":"tool1",
            "subagent_type":"general-purpose", "prompt":"say pong"}),
        json!({"type":"assistant", "parent_tool_use_id":"tool1", "agent_id":"a1",
            "message":{"id":"m-child", "content":[{"type":"text", "text":"pong"}]}}),
        json!({"type":"system", "subtype":"task_notification", "task_id":"a1",
            "tool_use_id":"tool1", "status":"completed", "summary":"pong"}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    assert!(session.state.lock().await.children.is_empty());
    let events = session.queue.dequeue_batch(200).await;
    let child = "s1-subagent-tool1";
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::SubagentSessionLinked {
        session_id, parent_session_id, parent_dialog_turn_id, parent_tool_call_id,
        continuation_policy: Some(SessionContinuationPolicy::FreshOnly), .. }
        if session_id == child && parent_session_id == "s1"
            && parent_dialog_turn_id == "parent-turn" && parent_tool_call_id == "tool1")));
    assert!(
        events.iter().any(|e| matches!(&e.event,
        AgenticEvent::TextChunk { session_id, text, .. } if session_id == child && text == "pong"))
    );
    assert!(events.iter().any(|e| matches!(&e.event,
        AgenticEvent::DialogTurnCompleted { session_id, .. } if session_id == child)));
    // Sidechain text never leaks into the parent transcript.
    assert!(!events.iter().any(|e| matches!(&e.event,
        AgenticEvent::TextChunk { session_id, .. } if session_id == "s1")));
}

#[tokio::test]
async fn trailing_autonomous_result_cannot_settle_next_user_turn() {
    let (session, process, _output) = fixture();
    session.route_frame(&process, json!({"type":"assistant", "message":{"id":"m1", "stop_reason":"end_turn", "content":[{"type":"text", "text":"background"}]}})).await.unwrap();
    assert!(session.state.lock().await.projector.is_none());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "next".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session
        .route_frame(&process, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    assert_eq!(
        session
            .state
            .lock()
            .await
            .projector
            .as_ref()
            .unwrap()
            .turn_id(),
        "next"
    );
    session
        .route_frame(
            &process,
            json!({"type":"stream_event", "event":{"type":"message_start", "message":{"id":"m2"}}}),
        )
        .await
        .unwrap();
    session
        .route_frame(&process, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    assert!(session.state.lock().await.projector.is_none());
}

#[tokio::test]
async fn submissions_reuse_stdin_across_two_foreground_turns() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    for (turn_id, input) in [("t1", "first input"), ("t2", "second input")] {
        session
            .start_turn(turn_id.into(), input.into(), Some(input.into()))
            .await
            .unwrap();
        let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(sent["type"], "user");
        assert_eq!(sent["session_id"], session.claude_session_id);
        assert_eq!(sent["message"]["content"], input);
        session
            .route_frame(&process, json!({"type":"result", "subtype":"success"}))
            .await
            .unwrap();
        assert!(process.alive.load(Ordering::Acquire));
    }
    assert!(session.resume.load(Ordering::Acquire));
    let events = session.queue.dequeue_batch(100).await;
    let turns: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            AgenticEvent::DialogTurnStarted { turn_index, .. } => Some(*turn_index),
            _ => None,
        })
        .collect();
    assert_eq!(turns, vec![0, 1]);
}

#[tokio::test]
async fn supervisor_exit_settles_turn_even_when_stdout_is_still_open() {
    let (session, mut process, _output) = fixture();
    let (exit_tx, exit_rx) = tokio::sync::watch::channel(None);
    Arc::get_mut(&mut process).unwrap().exit = exit_rx;
    *session.process.lock().await = Some(process.clone());
    {
        let mut state = session.state.lock().await;
        session
            .begin_turn(&mut state, "t1".into(), "hello".into(), None, false)
            .await
            .unwrap();
        state.process_id = Some(process.id.clone());
    }
    let (_held_stdout, reader) = tokio::io::duplex(4096);
    let pump = session.clone();
    let pumping_process = process.clone();
    let task = tokio::spawn(async move {
        pump.read_loop(pumping_process, Box::pin(reader), tokio::spawn(async {}))
            .await;
    });
    exit_tx
        .send(Some("fixture process exited: code=17".into()))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(session.state.lock().await.projector.is_none());
    assert!(session
        .queue
        .dequeue_batch(100)
        .await
        .iter()
        .any(|e| matches!(&e.event,
        AgenticEvent::DialogTurnFailed { error, .. } if error.contains("code=17"))));
}

#[tokio::test]
async fn retired_reader_eof_cannot_fail_unbound_startup_error_turn() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "startup-error".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    let (mut stdout, reader) = tokio::io::duplex(4096);
    stdout.shutdown().await.unwrap();
    session
        .clone()
        .read_loop(process, Box::pin(reader), tokio::spawn(async {}))
        .await;
    assert_eq!(
        session
            .state
            .lock()
            .await
            .projector
            .as_ref()
            .unwrap()
            .turn_id(),
        "startup-error"
    );
    session.fail_turn("initialize failed").await;
    assert!(session.state.lock().await.projector.is_none());
}

#[tokio::test]
async fn set_model_sends_live_control_request() {
    let (session, process, output) = fixture();
    *session.sdk_models.lock().unwrap() = vec![json!({"value":"sonnet","displayName":"Sonnet"})];
    *session.process.lock().await = Some(process.clone());
    let task = {
        let session = session.clone();
        tokio::spawn(async move { session.set_model("sonnet").await })
    };
    let mut lines = BufReader::new(output).lines();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["request"]["subtype"], "set_model");
    assert_eq!(sent["request"]["model"], "sonnet");
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
        "request_id":sent["request_id"], "response":{}}}),
        )
        .await
        .unwrap();
    assert_eq!(
        task.await.unwrap().unwrap().current_model_id.as_deref(),
        Some("sonnet")
    );
    assert!(process.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn commands_cache_initialize_replace_live_catalog_and_exclude_host_controls() {
    use crate::client::native::NativeAgentSession;
    let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = updates.clone();
    let (session, process, _output) = fixture_with_notifier(Some(Arc::new(move |update| {
        recorded.lock().unwrap().push(update);
    })));
    let mut commands = vec![
        json!({"name":"/compact", "description":"Compact context", "argumentHint":"instructions"}),
        json!({"name":"init"}),
        json!({"name":"review"}),
        json!({"name":"recap"}),
    ];
    commands.extend(
        [
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
        ]
        .iter()
        .map(|name| json!({"name":name})),
    );
    session.cache_initialize_response(&json!({"commands":commands, "models":[]}));
    let cached = session.commands();
    assert_eq!(
        cached.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["compact", "init", "review", "recap"]
    );
    assert_eq!(cached[0].input_hint.as_deref(), Some("instructions"));
    session.route_frame(&process, json!({"type":"system", "subtype":"init", "skills":["my-skill"], "slash_commands":["compact", "help"]})).await.unwrap();
    assert_eq!(session.commands().len(), 5);
    assert_eq!(session.commands()[0], cached[0]);
    session.route_frame(&process, json!({"type":"system", "subtype":"commands_changed", "commands":[{"name":"review", "description":"Review changes"}, {"name":"model"}]})).await.unwrap();
    assert_eq!(
        session
            .commands()
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        ["review"]
    );
    session
        .route_frame(
            &process,
            json!({"type":"system", "subtype":"commands_changed", "commands":[]}),
        )
        .await
        .unwrap();
    assert!(session.commands().is_empty());
    let updates = updates.lock().unwrap();
    assert_eq!(updates.len(), 4);
    assert!(
        matches!(&updates[3], NativeAgentNotification::AvailableCommandsUpdated(commands) if commands.is_empty())
    );
}

#[tokio::test]
async fn compaction_frames_emit_one_lifecycle_and_local_command_text() {
    for trigger in ["manual", "auto"] {
        let (session, process, _output) = fixture();
        session
            .begin_turn(
                &mut *session.state.lock().await,
                "t1".into(),
                if trigger == "manual" {
                    "/compact foo"
                } else {
                    "continue"
                }
                .into(),
                None,
                false,
            )
            .await
            .unwrap();
        for frame in [
            json!({"type":"system", "subtype":"status", "status":"compacting", "compact_metadata":{"pre_tokens":1000}}),
            json!({"type":"system", "subtype":"status", "status":"compacting"}),
            json!({"type":"system", "subtype":"compact_boundary", "compact_metadata":{"trigger":trigger, "pre_tokens":1000}}),
            json!({"type":"system", "subtype":"status", "compact_result":"success"}),
            json!({"type":"system", "subtype":"local_command_output", "content":"Compacted context"}),
        ] {
            session.route_frame(&process, frame).await.unwrap();
        }
        let events = session.queue.dequeue_batch(100).await;
        let started: Vec<_> = events
            .iter()
            .filter_map(|e| match &e.event {
                AgenticEvent::ContextCompressionStarted {
                    compression_id,
                    trigger: actual,
                    tokens_before,
                    ..
                } => {
                    assert_eq!(actual, trigger);
                    assert_eq!(*tokens_before, 1000);
                    Some(compression_id)
                }
                _ => None,
            })
            .collect();
        let completed: Vec<_> = events
            .iter()
            .filter_map(|e| match &e.event {
                AgenticEvent::ContextCompressionCompleted {
                    compression_id,
                    tokens_before,
                    tokens_after,
                    applied,
                    ..
                } => {
                    assert_eq!(
                        (*tokens_before, *tokens_after, *applied),
                        (1000, 1000, true)
                    );
                    Some(compression_id)
                }
                _ => None,
            })
            .collect();
        assert_eq!(started.len(), 1);
        assert_eq!(started, completed);
        assert!(events.iter().any(|e| matches!(&e.event, AgenticEvent::TextChunk { text, .. } if text == "Compacted context")));
    }
}

#[tokio::test]
async fn boundary_without_status_and_failed_compaction_are_projected() {
    let (session, process, _output) = fixture();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hello".into(),
            None,
            false,
        )
        .await
        .unwrap();
    for frame in [
        json!({"type":"system", "subtype":"compact_boundary", "compact_metadata":{"pre_tokens":900, "post_tokens":200}}),
        json!({"type":"system", "subtype":"status", "status":"compacting"}),
        json!({"type":"system", "subtype":"status", "compact_result":"failed", "error":"Could not compact"}),
        json!({"type":"system", "subtype":"status", "status":"compacting"}),
        json!({"type":"result", "subtype":"error_during_execution", "is_error":true, "result":"Compaction interrupted"}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let events = session.queue.dequeue_batch(100).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(&e.event, AgenticEvent::ContextCompressionStarted { .. }))
            .count(),
        3
    );
    assert!(events.iter().any(|e| matches!(
        &e.event,
        AgenticEvent::ContextCompressionCompleted {
            tokens_before: 900,
            tokens_after: 200,
            ..
        }
    )));
    assert!(events.iter().any(|e| matches!(&e.event, AgenticEvent::ContextCompressionFailed { error, .. } if error == "Could not compact")));
    assert!(events.iter().any(|e| matches!(&e.event, AgenticEvent::ContextCompressionFailed { error, .. } if error == "Compaction interrupted")));
}

#[tokio::test]
async fn todo_write_publishes_session_plan_without_consuming_tool_cards() {
    use openbitfun_events::ToolEventData;
    let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = updates.clone();
    let (session, process, _output) = fixture_with_notifier(Some(Arc::new(move |update| {
        recorded.lock().unwrap().push(update);
    })));
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "plan".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session.route_frame(&process, json!({"type":"assistant", "message":{"id":"m1", "content":[{"type":"tool_use", "id":"todo1", "name":"TodoWrite", "input":{"todos":[
        {"content":"Inspect", "status":"completed", "activeForm":"Inspecting"},
        {"content":"Implement", "status":"in_progress", "activeForm":"Implementing"},
        {"content":"Verify", "status":"pending", "activeForm":"Verifying"}
    ]}}]}})).await.unwrap();
    assert!(updates.lock().unwrap().is_empty());
    session.route_frame(&process, json!({"type":"user", "message":{"content":[{"type":"tool_result", "tool_use_id":"todo1", "content":"Todos updated"}]}})).await.unwrap();
    {
        let updates = updates.lock().unwrap();
        let NativeAgentNotification::PlanUpdated { turn_id, entries } = &updates[0] else {
            panic!("plan update")
        };
        assert_eq!(turn_id, "t1");
        assert_eq!(
            entries
                .iter()
                .map(|e| (e.content.as_str(), e.status.as_str(), e.priority.as_str()))
                .collect::<Vec<_>>(),
            [
                ("Inspect", "completed", "medium"),
                ("Implement", "in_progress", "medium"),
                ("Verify", "pending", "medium")
            ]
        );
    }
    let events = session.queue.dequeue_batch(100).await;
    assert!(events.iter().any(|e| matches!(
        &e.event,
        AgenticEvent::ToolEvent {
            tool_event: ToolEventData::Started { .. },
            ..
        }
    )));
    assert!(events.iter().any(|e| matches!(
        &e.event,
        AgenticEvent::ToolEvent {
            tool_event: ToolEventData::Completed { .. },
            ..
        }
    )));
    session
        .route_frame(&process, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t2".into(),
            "continue".into(),
            None,
            false,
        )
        .await
        .unwrap();
    assert!(
        matches!(&updates.lock().unwrap()[1], NativeAgentNotification::PlanUpdated { turn_id, entries } if turn_id == "t2" && entries.len() == 3)
    );
}
