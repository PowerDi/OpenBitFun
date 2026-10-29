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

fn pending(
    process: &Arc<NativeProcess>,
    id: &str,
    kind: PermissionKind,
    input: Value,
    suggestions: Value,
) -> PendingPermission {
    PendingPermission {
        process: process.clone(),
        turn_id: "t1".into(),
        request_id: id.into(),
        tool_id: "tool-123".into(),
        input,
        suggestions,
        kind,
    }
}

fn decision(id: &str, approve: bool, option: Option<&str>) -> SubmitAcpPermissionResponseRequest {
    SubmitAcpPermissionResponseRequest {
        permission_id: id.into(),
        approve,
        option_id: option.map(str::to_string),
        answers: None,
        feedback: None,
    }
}

async fn next_reply(lines: &mut tokio::io::Lines<BufReader<DuplexStream>>) -> Value {
    let frame: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    frame["response"]["response"].clone()
}

#[tokio::test]
async fn permission_scopes_split_suggestions_by_destination() {
    let (session, process, output) = fixture();
    let session_rule = json!({"type":"addRules", "destination":"session", "behavior":"allow",
        "rules":[{"toolName":"Bash", "ruleContent":"echo:*"}]});
    let local_rule = json!({"type":"addRules", "destination":"localSettings", "behavior":"allow",
        "rules":[{"toolName":"Bash"}]});
    let suggestions = json!([session_rule, local_rule]);
    for (id, option) in [
        ("p1", "allow_session"),
        ("p2", "allow_always"),
        ("p3", "allow_once"),
        ("p4", "reject_once"),
    ] {
        session.permissions.insert(
            id.into(),
            pending(
                &process,
                id,
                PermissionKind::Approval,
                json!({"command":"echo test"}),
                suggestions.clone(),
            ),
        );
        session
            .submit_permission(&decision(id, false, Some(option)))
            .await
            .unwrap()
            .unwrap();
    }
    let mut lines = BufReader::new(output).lines();
    let scoped = next_reply(&mut lines).await;
    assert_eq!(scoped["behavior"], "allow");
    assert_eq!(scoped["updatedPermissions"], json!([session_rule]));
    assert_eq!(scoped["updatedInput"]["command"], "echo test");
    assert_eq!(scoped["toolUseID"], "tool-123");
    assert_eq!(scoped["decisionClassification"], "user_permanent");
    let always = next_reply(&mut lines).await;
    assert_eq!(always["updatedPermissions"], json!([local_rule]));
    let once = next_reply(&mut lines).await;
    assert_eq!(once["behavior"], "allow");
    assert!(once.get("updatedPermissions").is_none());
    assert_eq!(once["decisionClassification"], "user_temporary");
    let deny = next_reply(&mut lines).await;
    assert_eq!(deny["behavior"], "deny");
    assert!(session.permissions.is_empty());

    // A scope Claude did not suggest is refused and the request stays pending.
    session.permissions.insert(
        "p5".into(),
        pending(
            &process,
            "p5",
            PermissionKind::Approval,
            json!({}),
            json!([session_rule]),
        ),
    );
    assert!(session
        .submit_permission(&decision("p5", true, Some("allow_always")))
        .await
        .unwrap()
        .is_err());
    assert!(session.permissions.contains_key("p5"));

    let payload = permission_payload(
        "p",
        "s1",
        &json!({"tool_use_id":"tu1", "tool_name":"Bash", "input":{},
            "permission_suggestions":[session_rule]}),
    );
    let ids: Vec<_> = payload["options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|option| option["optionId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["allow_once", "allow_session", "reject_once"]);
}

#[tokio::test]
async fn ask_user_question_answers_keyed_by_question_text() {
    let (session, process, output) = fixture();
    let input = json!({"questions":[
        {"question":"Which DB?", "header":"DB", "multiSelect":false,
            "options":[{"label":"Postgres", "description":""}, {"label":"SQLite", "description":""}]},
        {"question":"Which extras?", "header":"Extras", "multiSelect":true,
            "options":[{"label":"Auth", "description":""}, {"label":"Cache", "description":""}]}]});
    session.permissions.insert(
        "q1".into(),
        pending(
            &process,
            "q1",
            PermissionKind::Question,
            input.clone(),
            Value::Null,
        ),
    );
    // Missing answers are refused and keep the question pending.
    assert!(session
        .submit_permission(&decision("q1", true, None))
        .await
        .unwrap()
        .is_err());
    let mut request = decision("q1", true, None);
    request.answers = Some(json!({"0":"My own DB", "1":["Auth", "Cache"]}));
    session.submit_permission(&request).await.unwrap().unwrap();
    session.permissions.insert(
        "q2".into(),
        pending(
            &process,
            "q2",
            PermissionKind::Question,
            input.clone(),
            Value::Null,
        ),
    );
    session
        .submit_permission(&decision("q2", false, Some("reject_once")))
        .await
        .unwrap()
        .unwrap();
    let mut lines = BufReader::new(output).lines();
    let answered = next_reply(&mut lines).await;
    assert_eq!(answered["behavior"], "allow");
    assert_eq!(answered["updatedInput"]["questions"], input["questions"]);
    assert_eq!(
        answered["updatedInput"]["answers"],
        json!({"Which DB?":"My own DB", "Which extras?":"Auth, Cache"})
    );
    let cancelled = next_reply(&mut lines).await;
    assert_eq!(cancelled["behavior"], "deny");
    assert_eq!(cancelled["message"], "User cancelled the Question");
}

#[tokio::test]
async fn exit_plan_mode_approval_restores_mode_and_keep_planning_denies() {
    let (session, process, output) = fixture();
    for mode in ["acceptEdits", "plan"] {
        session
            .set_config_option(
                PERMISSION_MODE_CONFIG_ID,
                AcpSessionConfigValue::Select { value: mode.into() },
            )
            .await
            .unwrap();
    }
    let input = json!({"plan":"# Plan\n1. Do it"});
    for id in ["e1", "e2"] {
        session.permissions.insert(
            id.into(),
            pending(
                &process,
                id,
                PermissionKind::PlanReview,
                input.clone(),
                Value::Null,
            ),
        );
    }
    let mut keep = decision("e1", false, Some("keep_planning"));
    keep.feedback = Some("Cover rollback too".into());
    session.submit_permission(&keep).await.unwrap().unwrap();
    assert_eq!(read_lock(&session.permission_mode), "plan");
    session
        .submit_permission(&decision("e2", true, Some("approve_plan")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_lock(&session.permission_mode), "acceptEdits");
    let mut lines = BufReader::new(output).lines();
    let denied = next_reply(&mut lines).await;
    assert_eq!(denied["behavior"], "deny");
    let message = denied["message"].as_str().unwrap();
    assert!(message.starts_with(KEEP_PLANNING_MESSAGE));
    assert!(message.ends_with("User feedback: Cover rollback too"));
    let approved = next_reply(&mut lines).await;
    assert_eq!(approved["behavior"], "allow");
    assert_eq!(approved["updatedInput"], input);
    assert!(approved.get("updatedPermissions").is_none());
    let payload = permission_payload(
        "p",
        "s1",
        &json!({"tool_name":"ExitPlanMode", "input":input}),
    );
    assert_eq!(payload["options"][0]["optionId"], APPROVE_PLAN_OPTION);
    assert_eq!(payload["options"][1]["optionId"], KEEP_PLANNING_OPTION);
}

#[test]
fn context_usage_response_parses_codexhost_shape() {
    assert_eq!(
        parse_context_usage(&json!({"totalTokens":42000, "maxTokens":200000, "model":"claude"})),
        Some((42000, 200000))
    );
    assert_eq!(
        parse_context_usage(&json!({"totalTokens":1, "maxTokens":0})),
        None
    );
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
                kind: PermissionKind::Approval,
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
    let command = remote_command("/srv/my repo's files", &cli_args("uuid-1", true, None));
    assert!(command.starts_with("bash -lc "));
    assert!(command.contains("exec claude"));
    assert!(command.contains("--resume uuid-1"));
    assert!(!command.contains("claude-agent-acp"));
    assert!(!command.contains("--session-id"));
    assert!(command.contains("export CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING=true"));
    assert_eq!(cli_args("uuid-1", false, None).last().unwrap(), "uuid-1");
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
            answers: None,
            feedback: None,
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

fn linked_children<'a>(
    events: impl IntoIterator<Item = &'a AgenticEvent>,
) -> Vec<(String, String, String)> {
    events
        .into_iter()
        .filter_map(|event| match event {
            AgenticEvent::SubagentSessionLinked {
                session_id,
                parent_dialog_turn_id,
                parent_tool_call_id,
                ..
            } => Some((
                session_id.clone(),
                parent_dialog_turn_id.clone(),
                parent_tool_call_id.clone(),
            )),
            _ => None,
        })
        .collect()
}

fn child_turn_indexes<'a>(
    events: impl IntoIterator<Item = &'a AgenticEvent>,
    child: &str,
) -> Vec<usize> {
    events
        .into_iter()
        .filter_map(|event| match event {
            AgenticEvent::DialogTurnStarted {
                session_id,
                turn_index,
                ..
            } if session_id == child => Some(*turn_index),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn resumed_subagent_appends_to_its_original_child_session() {
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
            "task_type":"local_agent", "subagent_type":"general-purpose", "prompt":"first"}),
        json!({"type":"system", "subtype":"task_notification", "task_id":"a1",
            "tool_use_id":"tool1", "status":"completed", "summary":"done"}),
        // A background shell carries a tool_use_id but is not a subagent.
        json!({"type":"system", "subtype":"task_started", "task_id":"b1", "tool_use_id":"bash1",
            "task_type":"local_bash", "description":"npm run dev"}),
        // SendMessage resumes agent a1 under its own tool_use_id.
        json!({"type":"system", "subtype":"task_started", "task_id":"a1", "tool_use_id":"send1",
            "task_type":"local_agent", "prompt":"second"}),
        json!({"type":"assistant", "parent_tool_use_id":"send1", "agent_id":"a1",
            "message":{"id":"m-resumed", "content":[{"type":"text", "text":"again"}]}}),
        json!({"type":"system", "subtype":"task_notification", "task_id":"a1",
            "tool_use_id":"send1", "status":"completed", "summary":"again"}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let events = session.queue.dequeue_batch(200).await;
    let child = "s1-subagent-tool1";
    // One child session for the agent; the resume links the SendMessage card.
    assert_eq!(
        linked_children(events.iter().map(|e| &e.event)),
        vec![
            (
                child.to_string(),
                "parent-turn".to_string(),
                "tool1".to_string()
            ),
            (
                child.to_string(),
                "parent-turn".to_string(),
                "send1".to_string()
            ),
        ]
    );
    // The resumed run appends a turn instead of overwriting turn 0.
    assert_eq!(
        child_turn_indexes(events.iter().map(|e| &e.event), child),
        vec![0, 1]
    );
    assert!(events.iter().any(|e| matches!(&e.event,
        AgenticEvent::TextChunk { session_id, text, .. } if session_id == child && text == "again")));
    assert!(!events.iter().any(|e| matches!(&e.event,
        AgenticEvent::SubagentSessionLinked { session_id, .. } if session_id.contains("bash1"))));
    assert!(session.state.lock().await.children.is_empty());
}

#[tokio::test]
async fn subagent_resumed_between_parent_turns_reopens_under_its_last_card() {
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
            "task_type":"local_agent", "subagent_type":"general-purpose", "prompt":"first"}),
        json!({"type":"system", "subtype":"task_notification", "task_id":"a1",
            "tool_use_id":"tool1", "status":"completed", "summary":"done"}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    session.state.lock().await.projector = None;
    session.queue.dequeue_batch(200).await;

    // Parent idle: a background agent resumes and streams again.
    for frame in [
        json!({"type":"system", "subtype":"task_started", "task_id":"a1", "tool_use_id":"send1",
            "task_type":"local_agent", "prompt":"second"}),
        json!({"type":"assistant", "parent_tool_use_id":"send1", "agent_id":"a1",
            "message":{"id":"m-idle", "content":[{"type":"text", "text":"late"}]}}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let events = session.queue.dequeue_batch(200).await;
    let child = "s1-subagent-tool1";
    assert_eq!(
        linked_children(events.iter().map(|e| &e.event)),
        vec![(
            child.to_string(),
            "parent-turn".to_string(),
            "tool1".to_string()
        )]
    );
    assert_eq!(
        child_turn_indexes(events.iter().map(|e| &e.event), child),
        vec![1]
    );
    assert!(
        events.iter().any(|e| matches!(&e.event,
        AgenticEvent::TextChunk { session_id, text, .. } if session_id == child && text == "late"))
    );
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
        assert_eq!(sent["session_id"], session.claude_id());
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
            // The CLI reports success before the boundary that carries the counts.
            json!({"type":"system", "subtype":"status", "compact_result":"success"}),
            json!({"type":"system", "subtype":"compact_boundary", "compact_metadata":{"trigger":trigger, "pre_tokens":1000, "post_tokens":300}}),
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
                    assert_eq!((*tokens_before, *tokens_after, *applied), (1000, 300, true));
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

async fn finish_turn(
    session: &Arc<ClaudeNativeSession>,
    process: &Arc<NativeProcess>,
    lines: &mut tokio::io::Lines<BufReader<DuplexStream>>,
    turn_id: &str,
    prompt_echo: &str,
    last: &str,
) -> Value {
    session
        .start_turn(turn_id.into(), "hello".into(), None)
        .await
        .unwrap();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    for frame in [
        json!({"type":"user", "uuid":prompt_echo, "isReplay":true, "parent_tool_use_id":null,
            "message":{"role":"user", "content":"hello"}}),
        // Subagent entries are not on the main chain.
        json!({"type":"assistant", "uuid":"child-entry", "parent_tool_use_id":"task-1",
            "message":{"id":"c1", "content":[]}}),
        json!({"type":"assistant", "uuid":last, "parent_tool_use_id":null,
            "message":{"id":format!("m-{turn_id}"), "content":[{"type":"text", "text":"hi"}]}}),
        json!({"type":"result", "subtype":"success", "is_error":false}),
    ] {
        session.route_frame(process, frame).await.unwrap();
    }
    sent
}

#[tokio::test]
async fn tracks_prompt_and_last_chain_uuid_per_turn() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    let sent = finish_turn(&session, &process, &mut lines, "t1", "echo-1", "a-1").await;
    assert!(uuid::Uuid::parse_str(sent["uuid"].as_str().unwrap()).is_ok());
    finish_turn(&session, &process, &mut lines, "t2", "echo-2", "a-2").await;
    let history = session.history().clone();
    let anchors: Vec<_> = history
        .turns
        .iter()
        .map(|a| {
            (
                a.turn_id.as_str(),
                a.turn_index,
                a.prompt_uuid.as_deref(),
                a.last_uuid.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        anchors,
        [
            ("t1", 0, Some("echo-1"), Some("a-1")),
            ("t2", 1, Some("echo-2"), Some("a-2"))
        ]
    );
    // Persisted shape round-trips through the tolerant reader.
    let custom = json!({ super::super::history::METADATA_KEY: history.to_value() });
    let restored = ClaudeHistory::from_metadata(Some(&custom));
    assert_eq!(restored.turns[1].last_uuid.as_deref(), Some("a-2"));
    // Legacy metadata (no key, or an unreadable value) yields no anchors.
    assert!(ClaudeHistory::from_metadata(Some(&json!({})))
        .turns
        .is_empty());
    let broken = json!({ super::super::history::METADATA_KEY: "garbage" });
    assert!(ClaudeHistory::from_metadata(Some(&broken)).turns.is_empty());
}

#[tokio::test]
async fn rollback_rewinds_files_then_resumes_at_previous_turn() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    finish_turn(&session, &process, &mut lines, "t1", "echo-1", "a-1").await;
    finish_turn(&session, &process, &mut lines, "t2", "echo-2", "a-2").await;
    let rolling = session.clone();
    let task = tokio::spawn(async move { rolling.rollback_last_turn("t2").await });
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["request"]["subtype"], "rewind_files");
    assert_eq!(sent["request"]["user_message_id"], "echo-2");
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
            "request_id":sent["request_id"], "response":{"canRewind":true,
            "filesChanged":["src/a.rs"], "insertions":1, "deletions":2}}}),
        )
        .await
        .unwrap();
    let outcome = task.await.unwrap().unwrap();
    assert!(outcome.files_rewound);
    assert_eq!(outcome.files_changed, ["src/a.rs"]);
    assert!(!process.alive.load(Ordering::Acquire));
    assert_eq!(session.state.lock().await.index, 1);
    let history = session.history().clone();
    assert_eq!(history.turns.len(), 1);
    let id = session.claude_id();
    let args = cli_args(&id, true, history.pending.as_ref());
    let tail = &args[args.len() - 4..];
    assert_eq!(
        tail,
        ["--resume", id.as_str(), "--resume-session-at", "a-1"]
    );
}

#[tokio::test]
async fn rollback_of_first_turn_starts_a_fresh_claude_session() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    finish_turn(&session, &process, &mut lines, "t1", "echo-1", "a-1").await;
    let before = session.claude_id();
    let rolling = session.clone();
    let task = tokio::spawn(async move { rolling.rollback_last_turn("t1").await });
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    // A checkpoint failure is reported, not fatal.
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
            "request_id":sent["request_id"], "response":{"canRewind":false,
            "error":"No file checkpoint found for this message."}}}),
        )
        .await
        .unwrap();
    let outcome = task.await.unwrap().unwrap();
    assert!(!outcome.files_rewound);
    assert!(outcome.note.unwrap().contains("No file checkpoint"));
    assert_ne!(session.claude_id(), before);
    assert!(!session.resume.load(Ordering::Acquire));
    let history = session.history().clone();
    assert!(history.turns.is_empty() && history.pending.is_none());
    let args = cli_args(&session.claude_id(), false, None);
    assert_eq!(args[args.len() - 2], "--session-id");
}

#[tokio::test]
async fn rollback_is_refused_mid_turn_and_for_non_latest_or_legacy_turns() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    finish_turn(&session, &process, &mut lines, "t1", "echo-1", "a-1").await;
    session
        .start_turn("t2".into(), "again".into(), None)
        .await
        .unwrap();
    let error = session.rollback_last_turn("t2").await.unwrap_err();
    assert!(error.to_string().contains("Wait until"));
    session
        .route_frame(&process, json!({"type":"result", "subtype":"success"}))
        .await
        .unwrap();
    assert!(session.rollback_last_turn("t1").await.is_err());
    // Turns recorded before anchors existed cannot be targeted.
    session.apply_history(ClaudeHistory::default());
    let error = session.rollback_last_turn("t2").await.unwrap_err();
    assert!(error.to_string().contains("recorded by this version"));
    // A legacy predecessor without anchors leaves the position unknown.
    session.apply_history(ClaudeHistory {
        turns: vec![TurnAnchor {
            turn_id: "t2".into(),
            turn_index: 1,
            prompt_uuid: Some("p".into()),
            last_uuid: Some("l".into()),
            ..Default::default()
        }],
        pending: None,
    });
    let error = session.rollback_last_turn("t2").await.unwrap_err();
    assert!(error.to_string().contains("predate"));
    assert!(process.alive.load(Ordering::Acquire));
}

#[test]
fn fork_launch_forks_source_transcript_at_turn_with_new_id() {
    let pending = PendingResume {
        fork_from: Some("src-id".into()),
        resume_at: Some("a-1".into()),
    };
    let args = cli_args("new-id", false, Some(&pending));
    let tail = &args[args.len() - 7..];
    assert_eq!(
        tail,
        [
            "--resume",
            "src-id",
            "--fork-session",
            "--session-id",
            "new-id",
            "--resume-session-at",
            "a-1"
        ]
    );
    assert!(args.contains(&"--replay-user-messages".to_string()));
}

fn background_bash(id: &str) -> Value {
    json!({"type":"system", "subtype":"task_started", "task_id":id, "description":"npm run dev",
        "task_type":"local_bash", "is_backgrounded":true, "uuid":"u", "session_id":"s"})
}

#[tokio::test]
async fn background_frames_between_turns_publish_tasks_and_stop_task_uses_control_request() {
    let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = updates.clone();
    let (session, process, output) = fixture_with_notifier(Some(Arc::new(move |update| {
        sink.lock().unwrap().push(update);
    })));
    *session.process.lock().await = Some(process.clone());
    // No active turn: background frames must still be tracked.
    session
        .route_frame(&process, background_bash("b1"))
        .await
        .unwrap();
    assert!(session.state.lock().await.projector.is_none());
    assert!(matches!(&updates.lock().unwrap()[..],
        [NativeAgentNotification::BackgroundTasksUpdated(tasks)] if tasks.len() == 1 && tasks[0].is_running()));
    assert!(session.stop_task("unknown").await.is_err());

    let stopper = session.clone();
    let stop = tokio::spawn(async move { stopper.stop_task("b1").await });
    let mut lines = BufReader::new(output).lines();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["type"], "control_request");
    assert_eq!(
        sent["request"],
        json!({"subtype":"stop_task", "task_id":"b1"})
    );
    session
        .route_frame(
            &process,
            json!({"type":"control_response", "response":{"subtype":"success",
                "request_id":sent["request_id"], "response":{}}}),
        )
        .await
        .unwrap();
    stop.await.unwrap().unwrap();
    session
        .route_frame(
            &process,
            json!({"type":"system", "subtype":"task_notification", "task_id":"b1",
                "status":"stopped", "output_file":"/tmp/b1", "summary":"", "uuid":"u", "session_id":"s"}),
        )
        .await
        .unwrap();
    assert_eq!(session.background_tasks()[0].status, "killed");
}

#[tokio::test(start_paused = true)]
async fn idle_release_waits_for_running_background_tasks() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    session
        .route_frame(&process, background_bash("b1"))
        .await
        .unwrap();
    let period = Duration::from_secs(60);
    let watcher = session.spawn_idle_release(process.clone(), period);
    tokio::time::sleep(period * 3).await;
    assert!(
        process.alive.load(Ordering::Acquire),
        "running task keeps the process"
    );
    session
        .route_frame(
            &process,
            json!({"type":"system", "subtype":"background_tasks_changed", "tasks":[],
                "uuid":"u", "session_id":"s"}),
        )
        .await
        .unwrap();
    tokio::time::sleep(period * 3).await;
    assert!(
        !process.alive.load(Ordering::Acquire),
        "idle process is released"
    );
    tokio::time::timeout(Duration::from_secs(1), watcher)
        .await
        .unwrap()
        .unwrap();
    // Released, not failed: the next turn lazily resumes the same transcript.
    assert!(session.state.lock().await.projector.is_none());
    assert!(session
        .queue
        .dequeue_batch(100)
        .await
        .iter()
        .all(|e| !matches!(e.event, AgenticEvent::DialogTurnFailed { .. })));
}

#[test]
fn image_input_becomes_base64_content_blocks_after_text() {
    use crate::client::native::NativeImageInput;
    // "PNG" in base64; a jpg alias normalizes to image/jpeg.
    let png = NativeImageInput::from_data_url("image/png", "data:image/png;base64,UE5H").unwrap();
    let jpeg = NativeImageInput::from_data_url("image/jpg", "data:;base64,UE5H").unwrap();
    assert_eq!(jpeg.media_type, "image/jpeg");
    assert!(
        NativeImageInput::from_data_url("image/svg+xml", "data:image/svg+xml;base64,UE5H").is_err()
    );
    assert!(NativeImageInput::from_data_url("image/png", "data:image/png,raw").is_err());
    assert!(NativeImageInput::from_data_url("image/png", "data:image/png;base64,U$5H").is_err());

    let input = NativeTurnInput {
        text: "look".into(),
        images: vec![png, jpeg],
    };
    let frame = user_frame("c1", "u1", &input, None);
    assert_eq!(
        frame["message"]["content"],
        json!([
            {"type":"text", "text":"look"},
            {"type":"image", "source":{"type":"base64", "media_type":"image/png", "data":"UE5H"}},
            {"type":"image", "source":{"type":"base64", "media_type":"image/jpeg", "data":"UE5H"}},
        ])
    );
    assert!(frame.get("priority").is_none());
    // Text-only input keeps the plain string content.
    assert_eq!(
        user_frame("c1", "u1", &"hi".into(), None)["message"]["content"],
        "hi"
    );
}

async fn running_turn(
    session: &Arc<ClaudeNativeSession>,
    process: &Arc<NativeProcess>,
    lines: &mut tokio::io::Lines<BufReader<DuplexStream>>,
) {
    session
        .start_turn("t1".into(), "hello".into(), None)
        .await
        .unwrap();
    let _prompt = lines.next_line().await.unwrap().unwrap();
    for frame in [
        json!({"type":"user", "uuid":"echo-1", "isReplay":true, "parent_tool_use_id":null,
            "message":{"role":"user", "content":"hello"}}),
        json!({"type":"assistant", "uuid":"a-1", "parent_tool_use_id":null,
            "message":{"id":"m1", "content":[{"type":"tool_use", "id":"tool-1", "name":"Bash",
                "input":{"command":"ls"}}]}}),
    ] {
        session.route_frame(process, frame).await.unwrap();
    }
}

#[tokio::test]
async fn steering_folds_into_running_turn_without_opening_a_turn() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    running_turn(&session, &process, &mut lines).await;
    assert!(session.steer("other", "x".into(), None).await.is_err());
    let steering_id = session.steer("t1", "use pnpm".into(), None).await.unwrap();
    let sent: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(sent["uuid"], steering_id);
    assert_eq!(sent["priority"], "next");
    assert_eq!(sent["message"]["content"], "use pnpm");
    for frame in [
        json!({"type":"user", "parent_tool_use_id":null, "uuid":"r-1",
            "message":{"role":"user", "content":[{"type":"tool_result", "tool_use_id":"tool-1", "content":"ok"}]}}),
        json!({"type":"user", "uuid":steering_id, "isReplay":true, "parent_tool_use_id":null,
            "message":{"role":"user", "content":"use pnpm"}}),
        json!({"type":"assistant", "uuid":"a-2", "parent_tool_use_id":null,
            "message":{"id":"m2", "content":[{"type":"text", "text":"ok"}]}}),
        json!({"type":"result", "subtype":"success", "is_error":false,
            "user_message_uuids":[steering_id]}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let events = session.queue.dequeue_batch(200).await;
    let started = events
        .iter()
        .filter(|e| matches!(e.event, AgenticEvent::DialogTurnStarted { .. }))
        .count();
    assert_eq!(started, 1, "a folded steer does not open a turn");
    let steered: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            AgenticEvent::UserSteeringInjected {
                turn_id,
                steering_id,
                content,
                ..
            } => Some((turn_id.clone(), steering_id.clone(), content.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        steered,
        [(
            "t1".to_string(),
            steering_id.clone(),
            "use pnpm".to_string()
        )]
    );
    let history = session.history().clone();
    assert_eq!(history.turns.len(), 1);
    assert_eq!(history.turns[0].prompt_uuid.as_deref(), Some("echo-1"));
    assert_eq!(history.turns[0].last_uuid.as_deref(), Some("a-2"));
    assert!(session.state.lock().await.steers.is_empty());
}

#[tokio::test]
async fn steer_taken_after_the_turn_ends_opens_its_own_turn() {
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let mut lines = BufReader::new(output).lines();
    running_turn(&session, &process, &mut lines).await;
    let steering_id = session.steer("t1", "next step".into(), None).await.unwrap();
    let _ = lines.next_line().await.unwrap().unwrap();
    for frame in [
        json!({"type":"result", "subtype":"success", "is_error":false}),
        json!({"type":"user", "uuid":steering_id, "isReplay":true, "parent_tool_use_id":null,
            "message":{"role":"user", "content":"next step"}}),
        json!({"type":"assistant", "uuid":"a-3", "parent_tool_use_id":null,
            "message":{"id":"m3", "content":[{"type":"text", "text":"done"}]}}),
        json!({"type":"result", "subtype":"success", "is_error":false}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let inputs: Vec<_> = session
        .queue
        .dequeue_batch(200)
        .await
        .into_iter()
        .filter_map(|e| match e.event {
            AgenticEvent::DialogTurnStarted { user_input, .. } => Some(user_input),
            _ => None,
        })
        .collect();
    assert_eq!(inputs, ["hello", "next step"]);
    let history = session.history().clone();
    assert_eq!(history.turns.len(), 2);
    assert_eq!(
        history.turns[1].prompt_uuid.as_deref(),
        Some(steering_id.as_str())
    );
    assert_eq!(history.turns[1].last_uuid.as_deref(), Some("a-3"));
}

#[tokio::test]
async fn workspace_binding_locks_after_the_first_transcript() {
    use crate::client::native::scope_change_allowed;
    let (session, _process, _output) = fixture();
    // Fresh session: nothing is keyed by its cwd, so it may be reopened.
    assert!(!session.workspace_bound().await);
    assert!(scope_change_allowed("Claude", session.workspace_bound().await).is_ok());
    // A submitted turn (or a resumed transcript) fixes the cwd.
    session.resume.store(true, Ordering::Release);
    assert!(session.workspace_bound().await);
    let error = scope_change_allowed("Claude", true).unwrap_err();
    assert!(error.to_string().contains("worktree isolation"), "{error}");
    // A fork/import branch resumes another transcript by cwd before its first turn.
    let (forked, _process, _output) = fixture();
    forked.history().pending = Some(PendingResume {
        fork_from: Some(uuid::Uuid::new_v4().to_string()),
        resume_at: None,
    });
    assert!(forked.workspace_bound().await);
}

#[tokio::test]
async fn only_the_first_user_turn_generates_a_title_in_the_background() {
    use openbitfun_core::agentic::session::{ResolvedSessionTitle, SessionTitleMethod};
    let (session, process, output) = fixture();
    *session.process.lock().await = Some(process.clone());
    let calls = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen = calls.clone();
    *session.title_resolver.lock().unwrap() = Arc::new(move |message: String| {
        seen.lock().unwrap().push(message);
        Box::pin(async {
            ResolvedSessionTitle {
                title: "Fix the build".into(),
                method: SessionTitleMethod::Fallback,
            }
        })
    });
    let mut lines = BufReader::new(output).lines();
    for (turn_id, input) in [("t1", "fix the build"), ("t2", "and the tests")] {
        session
            .start_turn(turn_id.into(), input.into(), None)
            .await
            .unwrap();
        lines.next_line().await.unwrap().unwrap();
        session
            .route_frame(&process, json!({"type":"result", "subtype":"success"}))
            .await
            .unwrap();
        // Let the detached title task run.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(*calls.lock().unwrap(), vec!["fix the build".to_string()]);
    let titles: Vec<_> = session
        .queue
        .dequeue_batch(200)
        .await
        .into_iter()
        .filter_map(|e| match e.event {
            AgenticEvent::SessionTitleGenerated {
                session_id,
                title,
                method,
            } => Some((session_id, title, method)),
            _ => None,
        })
        .collect();
    assert_eq!(
        titles,
        vec![("s1".into(), "Fix the build".into(), "fallback".into())]
    );
}

fn background_agent(id: &str, tool: &str) -> Value {
    json!({"type":"system", "subtype":"task_started", "task_id":id, "tool_use_id":tool,
        "description":"Sleep task", "task_type":"local_agent", "subagent_type":"general-purpose",
        "is_backgrounded":true, "uuid":"u", "session_id":"s"})
}

fn task_done(id: &str, tool: &str, status: &str) -> Value {
    json!({"type":"system", "subtype":"task_notification", "task_id":id, "tool_use_id":tool,
        "status":status, "summary":"DONE", "uuid":"u", "session_id":"s"})
}

fn started_turns(
    events: &[openbitfun_core::agentic::events::EventEnvelope],
) -> Vec<(String, Option<Value>)> {
    events
        .iter()
        .filter_map(|e| match &e.event {
            // Parent session only; linked subagent children open their own.
            AgenticEvent::DialogTurnStarted {
                session_id,
                user_input,
                user_message_metadata,
                ..
            } if session_id == "s1" => Some((user_input.clone(), user_message_metadata.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn background_agent_completion_labels_the_cli_wake_turn() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "go".into(),
            None,
            false,
        )
        .await
        .unwrap();
    for frame in [
        background_agent("a1", "tool1"),
        json!({"type":"result", "subtype":"success", "is_error":false}),
        // Turn ended; the agent keeps running and then finishes while idle.
        task_done("a1", "tool1", "completed"),
        // Claude Code wakes the main agent on its own.
        json!({"type":"system", "subtype":"init", "model":"m", "session_id":"s"}),
        json!({"type":"assistant", "message":{"id":"m2", "content":[{"type":"text", "text":"RESUMED"}]}}),
        json!({"type":"result", "subtype":"success", "is_error":false}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    assert!(session.state.lock().await.projector.is_none());
    let events = session.queue.dequeue_batch(300).await;
    let turns = started_turns(&events);
    assert_eq!(turns.len(), 2, "user turn and one wake turn");
    let (input, metadata) = &turns[1];
    assert_eq!(input, "Background agent finished: Sleep task");
    let metadata = metadata.as_ref().expect("wake metadata");
    assert_eq!(metadata["triggerSource"], WAKE_TRIGGER_SOURCE);
    assert_eq!(metadata["nativeWake"]["taskId"], "a1");
    assert_eq!(metadata["nativeWake"]["toolUseId"], "tool1");
    assert_eq!(metadata["nativeWake"]["summary"], "DONE");
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::TextChunk { text, .. } if text == "RESUMED")));
    assert!(session.state.lock().await.wake_reasons.is_empty());
}

#[tokio::test]
async fn notification_taken_into_running_turn_does_not_label_a_later_turn() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "go".into(),
            None,
            false,
        )
        .await
        .unwrap();
    for frame in [
        background_agent("a1", "tool1"),
        task_done("a1", "tool1", "completed"),
        // The CLI folds the notification into the running turn.
        json!({"type":"user", "isReplay":true, "uuid":"n1", "parent_tool_use_id":null,
            "message":{"role":"user", "content":"<task-notification>\n<task-id>a1</task-id>\n<status>completed</status>\n</task-notification>"}}),
        json!({"type":"result", "subtype":"success", "is_error":false}),
        // Unrelated background output later opens a generic autonomous turn.
        json!({"type":"assistant", "message":{"id":"m3", "content":[{"type":"text", "text":"late"}]}}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let turns = started_turns(&session.queue.dequeue_batch(300).await);
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[1], (String::new(), None));
}

#[tokio::test]
async fn idle_injected_notification_opens_the_matching_wake_turn() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    for frame in [
        background_agent("a1", "tool1"),
        background_agent("a2", "tool2"),
        task_done("a1", "tool1", "completed"),
        task_done("a2", "tool2", "failed"),
        // The CLI runs the second notification first.
        json!({"type":"user", "isReplay":true, "uuid":"n2", "parent_tool_use_id":null,
            "message":{"role":"user", "content":[{"type":"text", "text":"<task-notification>\n<task-id>a2</task-id>\n</task-notification>"}]}}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let turns = started_turns(&session.queue.dequeue_batch(300).await);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].0, "Background agent failed: Sleep task");
    assert_eq!(turns[0].1.as_ref().unwrap()["nativeWake"]["taskId"], "a2");
    let pending: Vec<_> = session
        .state
        .lock()
        .await
        .wake_reasons
        .iter()
        .map(|r| r.task_id.clone())
        .collect();
    assert_eq!(pending, vec!["a1".to_string()]);
}

#[tokio::test]
async fn stopped_tasks_and_cancel_leave_nothing_to_wake_for() {
    let (session, process, _output) = fixture();
    *session.process.lock().await = Some(process.clone());
    for frame in [
        background_agent("a1", "tool1"),
        background_agent("a2", "tool2"),
        task_done("a1", "tool1", "stopped"),
        task_done("a2", "tool2", "completed"),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let pending: Vec<_> = session
        .state
        .lock()
        .await
        .wake_reasons
        .iter()
        .map(|r| r.task_id.clone())
        .collect();
    assert_eq!(
        pending,
        vec!["a2".to_string()],
        "a stopped task wakes nothing"
    );
    // The process is not attached as live for the interrupt; cancel still
    // clears what the CLI drops on interrupt.
    process.alive.store(false, Ordering::Release);
    session.cancel().await.unwrap();
    assert!(session.state.lock().await.wake_reasons.is_empty());
}

#[test]
fn injected_notification_task_reads_only_main_conversation_notifications() {
    let main = json!({"type":"user", "parent_tool_use_id":null,
        "message":{"content":"<task-notification>\n<task-id> b7 </task-id>\n</task-notification>"}});
    assert_eq!(injected_notification_task(&main).as_deref(), Some("b7"));
    let child = json!({"type":"user", "parent_tool_use_id":"tool1",
        "message":{"content":"<task-notification><task-id>b7</task-id></task-notification>"}});
    assert_eq!(injected_notification_task(&child), None);
    let typed = json!({"type":"user", "message":{"content":"please check <task-id>b7</task-id>"}});
    assert_eq!(injected_notification_task(&typed), None);
}

#[tokio::test]
async fn mcp_elicitation_form_round_trips_through_the_question_card() {
    let (session, process, output) = fixture();
    let mut lines = BufReader::new(output).lines();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hi".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session
        .route_frame(
            &process,
            json!({"type":"control_request", "request_id":"e1", "request":{
                "subtype":"elicitation", "mcp_server_name":"db", "message":"Pick one",
                "requested_schema":{"type":"object","properties":{
                    "env":{"type":"string","enum":["dev","prod"],"enumNames":["Dev","Prod"]}}}}}),
        )
        .await
        .unwrap();
    let permission_id = session.elicitations.iter().next().unwrap().key().clone();
    // An option outside the enum keeps the request pending.
    let mut request = decision(&permission_id, true, Some("allow_once"));
    request.answers = Some(json!({"0": "staging"}));
    assert!(session.submit_permission(&request).await.unwrap().is_err());
    assert!(session.elicitations.contains_key(&permission_id));
    request.answers = Some(json!({"0": "Prod"}));
    session.submit_permission(&request).await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(frame["response"]["request_id"], "e1");
    assert_eq!(
        frame["response"]["response"],
        json!({"action":"accept", "content":{"env":"prod"}})
    );
    // Skip declines explicitly; a URL elicitation is declined with a visible card.
    session
        .route_frame(
            &process,
            json!({"type":"control_request", "request_id":"e2", "request":{
                "subtype":"elicitation", "mcp_server_name":"db", "message":"Confirm?"}}),
        )
        .await
        .unwrap();
    let permission_id = session.elicitations.iter().next().unwrap().key().clone();
    session
        .submit_permission(&decision(&permission_id, false, Some("reject_once")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_reply(&mut lines).await, json!({"action":"decline"}));
    session
        .route_frame(
            &process,
            json!({"type":"control_request", "request_id":"e3", "request":{
                "subtype":"elicitation", "mcp_server_name":"auth", "mode":"url",
                "url":"https://example.test", "message":"Sign in"}}),
        )
        .await
        .unwrap();
    assert_eq!(next_reply(&mut lines).await, json!({"action":"decline"}));
    // Other control requests stay unsupported.
    session
        .route_frame(
            &process,
            json!({"type":"control_request", "request_id":"x1", "request":{"subtype":"request_user_dialog"}}),
        )
        .await
        .unwrap();
    let frame: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(frame["response"]["subtype"], "error");
    let events = session.queue.dequeue_batch(200).await;
    let failed: Vec<String> = events
        .iter()
        .filter_map(|e| match &e.event {
            AgenticEvent::ToolEvent {
                tool_event: openbitfun_events::ToolEventData::Failed { error, .. },
                ..
            } => Some(error.clone()),
            _ => None,
        })
        .collect();
    assert!(failed.iter().any(|e| e.contains("declined")));
    assert!(failed.iter().any(|e| e.contains("URL")));
}

#[tokio::test]
async fn unanswered_elicitation_is_cancelled_when_the_turn_ends() {
    let (session, process, output) = fixture();
    session
        .begin_turn(
            &mut *session.state.lock().await,
            "t1".into(),
            "hi".into(),
            None,
            false,
        )
        .await
        .unwrap();
    session
        .route_frame(
            &process,
            json!({"type":"control_request", "request_id":"e1", "request":{
                "subtype":"elicitation", "mcp_server_name":"db", "message":"Confirm?"}}),
        )
        .await
        .unwrap();
    session.clear_permissions("t1").await;
    assert!(session.elicitations.is_empty());
    let mut lines = BufReader::new(output).lines();
    assert_eq!(next_reply(&mut lines).await, json!({"action":"cancel"}));
}
