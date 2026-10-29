use super::*;
use openbitfun_core::agentic::events::EventQueueConfig;
use tokio::io::DuplexStream;

type Lines = tokio::io::Lines<BufReader<DuplexStream>>;

fn fixture(thread: Option<&str>) -> (Arc<CodexNativeSession>, Arc<RpcProcess>, Lines) {
    let session = CodexNativeSession::new(
        "s1".into(),
        thread.map(str::to_string),
        ".".into(),
        None,
        Arc::new(EventQueue::new(EventQueueConfig::default())),
        None,
        0,
        None,
    );
    let (writer, reader) = tokio::io::duplex(1 << 16);
    let process = Arc::new(RpcProcess::new(
        Box::pin(writer),
        None,
        None,
        tokio::sync::watch::channel(None).1,
    ));
    (session, process, BufReader::new(reader).lines())
}

async fn next_frame(lines: &mut Lines) -> Value {
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

/// Answer the next client request with `result` and return the request.
async fn answer(
    session: &CodexNativeSession,
    process: &Arc<RpcProcess>,
    lines: &mut Lines,
    result: Value,
) -> Value {
    let request = next_frame(lines).await;
    session
        .route_frame(process, json!({"id": request["id"], "result": result}))
        .await
        .unwrap();
    request
}

#[tokio::test]
async fn handshake_sends_initialize_initialized_and_loads_models() {
    let (session, process, mut lines) = fixture(None);
    let init = {
        let session = session.clone();
        let process = process.clone();
        tokio::spawn(async move { session.initialize(&process).await })
    };
    let request = answer(&session, &process, &mut lines,
        json!({"userAgent":"codex_cli_rs/0.163.0 (Windows)","codexHome":"C:/x","platformFamily":"windows","platformOs":"windows"})).await;
    assert_eq!(request["method"], "initialize");
    assert_eq!(request["params"]["clientInfo"]["name"], "openbitfun");
    assert!(request.get("jsonrpc").is_none());
    let initialized = next_frame(&mut lines).await;
    assert_eq!(initialized, json!({"method":"initialized"}));
    let models = answer(&session, &process, &mut lines, json!({"nextCursor":null,"data":[
        {"id":"gpt-5.5","model":"gpt-5.5","displayName":"GPT-5.5","description":"d","hidden":false,"isDefault":true,
         "supportedReasoningEfforts":[{"reasoningEffort":"low","description":"fast"},{"reasoningEffort":"high","description":"deep"}],
         "defaultReasoningEffort":"medium"},
        {"id":"old","model":"old","displayName":"Old","hidden":true}]})).await;
    assert_eq!(models["method"], "model/list");
    init.await.unwrap().unwrap();
    let options = session.options();
    assert_eq!(options.current_model_id.as_deref(), Some("gpt-5.5"));
    assert_eq!(options.available_models.len(), 1);
    let AcpSessionConfigKind::Select {
        options: efforts, ..
    } = &options.config_options[1].kind
    else {
        panic!("effort select");
    };
    let values: Vec<_> = efforts.iter().map(|o| o.value.as_str()).collect();
    assert_eq!(values, ["default", "low", "high"]);
}

#[tokio::test]
async fn old_app_server_is_refused_at_initialize() {
    let (session, process, mut lines) = fixture(None);
    let init = {
        let session = session.clone();
        let process = process.clone();
        tokio::spawn(async move { session.initialize(&process).await })
    };
    answer(
        &session,
        &process,
        &mut lines,
        json!({"userAgent":"codex_cli_rs/0.100.0 (Linux)"}),
    )
    .await;
    let error = init.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("older than the minimum"), "{error}");
}

#[tokio::test]
async fn projects_thread_notifications_and_ignores_other_threads() {
    let (session, process, _lines) = fixture(Some("th1"));
    session
        .begin_turn("t1".into(), "list files".into(), None)
        .await
        .unwrap();
    session.state.lock().await.process_id = Some(process.id.clone());
    let frames = [
        json!({"method":"turn/started","params":{"threadId":"th1","turn":{"id":"ct1","status":"inProgress"}}}),
        // A subagent thread on the same app-server never enters this turn.
        json!({"method":"item/agentMessage/delta","params":{"threadId":"sub","turnId":"x","itemId":"m0","delta":"LEAK"}}),
        json!({"method":"item/started","params":{"threadId":"th1","turnId":"ct1","item":{"type":"commandExecution",
            "id":"c1","command":"ls","cwd":"/w","status":"inProgress","commandActions":[]}}}),
        json!({"method":"item/completed","params":{"threadId":"th1","turnId":"ct1","item":{"type":"commandExecution",
            "id":"c1","command":"ls","cwd":"/w","status":"completed","aggregatedOutput":"a\n","exitCode":0}}}),
        json!({"method":"item/agentMessage/delta","params":{"threadId":"th1","turnId":"ct1","itemId":"m1","delta":"Done"}}),
        json!({"method":"turn/plan/updated","params":{"threadId":"th1","turnId":"ct1","plan":[{"step":"a","status":"completed"}]}}),
        json!({"method":"thread/tokenUsage/updated","params":{"threadId":"th1","turnId":"ct1","tokenUsage":{
            "modelContextWindow":1000,"last":{"totalTokens":250,"inputTokens":200,"cachedInputTokens":0,"outputTokens":50},
            "total":{"totalTokens":250}}}}),
        json!({"method":"turn/completed","params":{"threadId":"th1","turn":{"id":"ct1","status":"completed"}}}),
    ];
    for frame in frames {
        session.route_frame(&process, frame).await.unwrap();
    }
    assert!(session.state.lock().await.projector.is_none());
    let events = session.queue.dequeue_batch(100).await;
    let text: String = events
        .iter()
        .filter_map(|e| match &e.event {
            AgenticEvent::TextChunk { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "Done");
    assert!(events.iter().any(|e| matches!(&e.event, AgenticEvent::ToolEvent {
        tool_event: openbitfun_events::ToolEventData::Completed { identity, .. }, .. } if identity.tool_name == "ExecCommand")));
    assert!(events
        .iter()
        .any(|e| matches!(e.event, AgenticEvent::DialogTurnCompleted { .. })));
    assert_eq!(
        session.options().context_usage.map(|u| (u.used, u.size)),
        Some((250, 1000))
    );
}

#[tokio::test]
async fn resume_usage_replay_of_previous_turn_does_not_capture_the_new_turn() {
    let (session, process, mut lines) = fixture(Some("th1"));
    session
        .begin_turn("t2".into(), "again".into(), None)
        .await
        .unwrap();
    session.state.lock().await.process_id = Some(process.id.clone());
    // Sent by `thread/resume` for the previous turn, before `turn/start`.
    let replay = json!({"method":"thread/tokenUsage/updated","params":{"threadId":"th1","turnId":"old",
        "tokenUsage":{"modelContextWindow":1000,"last":{"totalTokens":120},"total":{"totalTokens":120}}}});
    session.route_frame(&process, replay).await.unwrap();
    assert_eq!(session.options().context_usage.map(|u| u.used), Some(120));
    // The fixture process is the session's loaded app-server: no spawn,
    // no `thread/resume` request.
    *session.process.lock().await = Some(process.clone());
    *lock(&session.thread_process) = Some(process.id.clone());
    let submit = {
        let session = session.clone();
        tokio::spawn(async move { session.submit_turn("t2", &"again".to_string().into()).await })
    };
    let request = next_frame(&mut lines).await;
    assert_eq!(request["method"], "turn/start");
    session
        .route_frame(
            &process,
            json!({"id": request["id"], "result": {"turn":{"id":"new","status":"inProgress"}}}),
        )
        .await
        .unwrap();
    submit.await.unwrap().unwrap();
    for frame in [
        json!({"method":"turn/started","params":{"threadId":"th1","turn":{"id":"new","status":"inProgress"}}}),
        json!({"method":"item/agentMessage/delta","params":{"threadId":"th1","turnId":"new","itemId":"m1","delta":"pong"}}),
        json!({"method":"turn/completed","params":{"threadId":"th1","turn":{"id":"new","status":"completed"}}}),
    ] {
        session.route_frame(&process, frame).await.unwrap();
    }
    let events = session.queue.dequeue_batch(100).await;
    assert!(!events
        .iter()
        .any(|e| matches!(e.event, AgenticEvent::TokenUsageUpdated { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::TextChunk { text, .. } if text == "pong")));
    assert!(events
        .iter()
        .any(|e| matches!(e.event, AgenticEvent::DialogTurnCompleted { .. })));
}

#[tokio::test]
async fn approval_round_trip_and_turn_end_cancels_unanswered_requests() {
    let (session, process, mut lines) = fixture(Some("th1"));
    let params = json!({"threadId":"th1","turnId":"ct1","itemId":"c1","command":"cargo test",
        "proposedExecpolicyAmendment":["cargo","test"]});
    for (id, option, expected) in [
        (
            "p1",
            "allow_session",
            json!({"decision":"acceptForSession"}),
        ),
        ("p2", "reject_once", json!({"decision":"decline"})),
        (
            "p3",
            "allow_always",
            json!({"decision":{"acceptWithExecpolicyAmendment":{"execpolicy_amendment":["cargo","test"]}}}),
        ),
    ] {
        session.approvals.insert(
            id.into(),
            PendingApproval {
                process: process.clone(),
                request_id: json!(id),
                method: "item/commandExecution/requestApproval".into(),
                params: params.clone(),
                turn_id: "t1".into(),
                interaction: None,
            },
        );
        let request = SubmitAcpPermissionResponseRequest {
            permission_id: id.into(),
            approve: option != "reject_once",
            option_id: Some(option.into()),
            answers: None,
            feedback: None,
        };
        session.submit_permission(&request).await.unwrap().unwrap();
        assert_eq!(
            next_frame(&mut lines).await,
            json!({"id": id, "result": expected})
        );
    }
    // Permission profile grants echo the requested profile with a scope.
    let permissions = json!({"permissions":{"network":{"enabled":true},"fileSystem":null}});
    let reply = approval_reply(
        "item/permissions/requestApproval",
        &permissions,
        &SubmitAcpPermissionResponseRequest {
            permission_id: "x".into(),
            approve: true,
            option_id: Some("allow_once".into()),
            answers: None,
            feedback: None,
        },
    )
    .unwrap();
    assert_eq!(
        reply,
        json!({"permissions":{"network":{"enabled":true}},"scope":"turn"})
    );
    // An unanswered request is cancelled when its turn ends.
    session.approvals.insert(
        "p4".into(),
        PendingApproval {
            process: process.clone(),
            request_id: json!(9),
            method: "item/fileChange/requestApproval".into(),
            params: json!({}),
            turn_id: "t1".into(),
            interaction: None,
        },
    );
    session.clear_approvals("t1").await;
    assert_eq!(
        next_frame(&mut lines).await,
        json!({"id": 9, "result": {"decision":"cancel"}})
    );
}

#[tokio::test]
async fn pending_fork_launches_thread_fork_and_rebinds_anchors() {
    let (session, process, mut lines) = fixture(None);
    session.apply_history(CodexHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            codex_turn_id: Some("c1".into()),
        }],
        pending: Some(PendingFork {
            thread_id: "th-src".into(),
            last_turn_id: Some("c1".into()),
        }),
    });
    *lock(&session.permission_mode) = "auto".into();
    let ensure = {
        let session = session.clone();
        let process = process.clone();
        tokio::spawn(async move { session.ensure_thread(&process).await })
    };
    let request = answer(
        &session,
        &process,
        &mut lines,
        json!({"thread":{"id":"th-new","turns":[{"id":"f1"}]},"model":"gpt-5.5"}),
    )
    .await;
    assert_eq!(request["method"], "thread/fork");
    assert_eq!(request["params"]["threadId"], "th-src");
    assert_eq!(request["params"]["lastTurnId"], "c1");
    assert_eq!(request["params"]["sandbox"], "workspace-write");
    assert_eq!(request["params"]["approvalPolicy"], "on-request");
    assert_eq!(ensure.await.unwrap().unwrap(), "th-new");
    let history = lock(&session.history).clone();
    assert!(history.pending.is_none());
    assert_eq!(history.turns[0].codex_turn_id.as_deref(), Some("f1"));
    // Loaded: the next turn reuses the thread without another request.
    assert_eq!(session.ensure_thread(&process).await.unwrap(), "th-new");
}

#[test]
fn turn_params_carry_images_and_overrides() {
    let (session, _, _) = fixture(Some("th1"));
    *lock(&session.selected_model) = "gpt-5.5".into();
    *lock(&session.effort) = "high".into();
    *lock(&session.permission_mode) = "full-access".into();
    let input = NativeTurnInput {
        text: "look".into(),
        images: vec![super::super::super::NativeImageInput {
            media_type: "image/png".into(),
            data: "AAAA".into(),
        }],
    };
    let params = session.turn_params("th1", &input);
    assert_eq!(
        params["input"],
        json!([
        {"type":"text","text":"look","text_elements":[]},
        {"type":"image","url":"data:image/png;base64,AAAA"}])
    );
    assert_eq!(params["model"], "gpt-5.5");
    assert_eq!(params["effort"], "high");
    assert_eq!(params["approvalPolicy"], "never");
    assert_eq!(params["sandboxPolicy"], json!({"type":"dangerFullAccess"}));
    *lock(&session.permission_mode) = "default".into();
    *lock(&session.effort) = "default".into();
    let params = session.turn_params("th1", &"hi".into());
    assert!(params.get("approvalPolicy").is_none() && params.get("effort").is_none());
}

#[tokio::test]
async fn unsupported_server_requests_get_an_error_reply() {
    let (session, process, mut lines) = fixture(Some("th1"));
    session
        .route_frame(
            &process,
            json!({"id": 5, "method": "item/tool/call", "params": {}}),
        )
        .await
        .unwrap();
    let reply = next_frame(&mut lines).await;
    assert_eq!(reply["id"], 5);
    assert_eq!(reply["error"]["code"], -32601);
}

#[tokio::test]
async fn unmaterialized_thread_without_turns_starts_afresh() {
    let (session, process, mut lines) = fixture(Some("th-empty"));
    let ensure = {
        let session = session.clone();
        let process = process.clone();
        tokio::spawn(async move { session.ensure_thread(&process).await })
    };
    let request = next_frame(&mut lines).await;
    assert_eq!(request["method"], "thread/resume");
    session
        .route_frame(
            &process,
            json!({"id": request["id"], "error": {"code": -32600,
                "message": "no rollout found for thread id th-empty"}}),
        )
        .await
        .unwrap();
    let start = answer(
        &session,
        &process,
        &mut lines,
        json!({"thread":{"id":"th-new"}}),
    )
    .await;
    assert_eq!(start["method"], "thread/start");
    assert_eq!(ensure.await.unwrap().unwrap(), "th-new");

    // A thread with recorded Codex turns keeps the error instead.
    let (session, process, mut lines) = fixture(Some("th1"));
    session.apply_history(CodexHistory {
        turns: vec![TurnAnchor {
            turn_id: "t1".into(),
            turn_index: 0,
            codex_turn_id: Some("c1".into()),
        }],
        pending: None,
    });
    let ensure = {
        let session = session.clone();
        let process = process.clone();
        tokio::spawn(async move { session.ensure_thread(&process).await })
    };
    let request = next_frame(&mut lines).await;
    session
        .route_frame(
            &process,
            json!({"id": request["id"], "error": {"code": -32600,
                "message": "no rollout found for thread id th1"}}),
        )
        .await
        .unwrap();
    assert!(ensure.await.unwrap().is_err());
}

#[test]
fn approval_options_follow_available_decisions() {
    let ids = |params: Value| -> Vec<String> {
        permission_payload(
            "p",
            "s",
            "item/commandExecution/requestApproval",
            "c1",
            "ExecCommand",
            "execute",
            &json!({}),
            &params,
        )["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["optionId"].as_str().unwrap().to_string())
            .collect()
    };
    // Shape sent by codex-cli 0.163.0 for a shell write on Windows.
    let live = json!({"command":"pwsh","proposedExecpolicyAmendment":["pwsh"],
        "availableDecisions":["accept",{"acceptWithExecpolicyAmendment":{"execpolicy_amendment":["pwsh"]}},"cancel"]});
    assert_eq!(ids(live), ["allow_once", "allow_always", "reject_once"]);
    let legacy = json!({"command":"ls"});
    assert_eq!(ids(legacy), ["allow_once", "allow_session", "reject_once"]);
}

/// Parent turn `t1` (Codex `ct1`) on thread `th1` that spawns `children`.
async fn parent_turn_spawning(
    session: &Arc<CodexNativeSession>,
    process: &Arc<RpcProcess>,
    children: &[&str],
) {
    *session.process.lock().await = Some(process.clone());
    *lock(&session.thread_process) = Some(process.id.clone());
    session
        .begin_turn("t1".into(), "fan out".into(), None)
        .await
        .unwrap();
    session.state.lock().await.process_id = Some(process.id.clone());
    let mut frames = vec![
        json!({"method":"turn/started","params":{"threadId":"th1","turn":{"id":"ct1","status":"inProgress"}}}),
    ];
    for (index, child) in children.iter().enumerate() {
        // Child output can arrive before the parent's spawn completes.
        frames.push(json!({"method":"turn/started","params":{"threadId":child,"turn":{"id":format!("{child}-t"),"status":"inProgress"}}}));
        frames.push(json!({"method":"item/completed","params":{"threadId":"th1","turnId":"ct1","item":{
            "type":"collabAgentToolCall","id":format!("sp{index}"),"tool":"spawnAgent","status":"completed",
            "senderThreadId":"th1","receiverThreadIds":[child],"prompt":format!("task {child}\nmore"),
            "agentsStates":{}}}}));
    }
    for frame in frames {
        session.route_frame(process, frame).await.unwrap();
    }
}

async fn child_says(
    session: &CodexNativeSession,
    process: &Arc<RpcProcess>,
    child: &str,
    text: &str,
    status: &str,
) {
    for frame in [
        json!({"method":"item/agentMessage/delta","params":{"threadId":child,"turnId":format!("{child}-t"),"itemId":"m","delta":text}}),
        json!({"method":"turn/completed","params":{"threadId":child,"turn":{"id":format!("{child}-t"),"status":status}}}),
    ] {
        session.route_frame(process, frame).await.unwrap();
    }
}

async fn end_parent_turn(session: &CodexNativeSession, process: &Arc<RpcProcess>) {
    session
        .route_frame(process, json!({"method":"turn/completed","params":{"threadId":"th1","turn":{"id":"ct1","status":"completed"}}}))
        .await
        .unwrap();
}

#[tokio::test]
async fn spawned_agent_threads_project_as_linked_child_sessions() {
    let (session, process, _lines) = fixture(Some("th1"));
    parent_turn_spawning(&session, &process, &["c1"]).await;
    // Finishing inside the parent turn: Codex delivers it there, no wake.
    child_says(&session, &process, "c1", "DONE", "completed").await;
    end_parent_turn(&session, &process).await;
    let events = session.queue.dequeue_batch(300).await;
    let child = "s1-subagent-c1";
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::SubagentSessionLinked {
        session_id, parent_session_id, parent_dialog_turn_id, parent_tool_call_id, .. }
        if session_id == child && parent_session_id == "s1" && parent_dialog_turn_id == "t1"
            && parent_tool_call_id == "sp0")));
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::DialogTurnStarted {
        session_id, user_input, .. } if session_id == child && user_input == "task c1\nmore")));
    let child_text: String = events
        .iter()
        .filter_map(|e| match &e.event {
            AgenticEvent::TextChunk {
                session_id, text, ..
            } if session_id == child => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(child_text, "DONE");
    assert!(events.iter().any(|e| matches!(&e.event,
        AgenticEvent::DialogTurnCompleted { session_id, .. } if session_id == child)));
    assert!(!events.iter().any(|e| matches!(&e.event,
        AgenticEvent::TextChunk { session_id, .. } if session_id == "s1")));
    // The spawn card is the subagent card the child links to.
    assert!(events
        .iter()
        .any(|e| matches!(&e.event, AgenticEvent::ToolEvent {
        tool_event: openbitfun_events::ToolEventData::Started { identity, params, .. }, .. }
        if identity.tool_name == "Task" && params["description"] == "task c1")));
    let state = session.state.lock().await;
    assert!(state.threads.wake_reasons.is_empty());
    assert!(!state.threads.any_running());
}

#[tokio::test]
async fn children_finishing_while_idle_wake_the_parent_once() {
    let (session, process, mut lines) = fixture(Some("th1"));
    parent_turn_spawning(&session, &process, &["c1", "c2"]).await;
    end_parent_turn(&session, &process).await;
    assert!(session.state.lock().await.threads.any_running());
    child_says(&session, &process, "c1", "A", "completed").await;
    child_says(&session, &process, "c2", "B", "failed").await;
    session.queue.dequeue_batch(300).await;
    // One debounced wake: an input-less turn/start on the parent thread.
    let request = tokio::time::timeout(Duration::from_secs(5), next_frame(&mut lines))
        .await
        .expect("wake turn/start");
    assert_eq!(request["method"], "turn/start");
    assert_eq!(request["params"]["threadId"], "th1");
    assert_eq!(request["params"]["input"], json!([]));
    session
        .route_frame(
            &process,
            json!({"id": request["id"], "result": {"turn":{"id":"ct2"}}}),
        )
        .await
        .unwrap();
    let turns: Vec<_> = session
        .queue
        .dequeue_batch(300)
        .await
        .into_iter()
        .filter_map(|e| match e.event {
            AgenticEvent::DialogTurnStarted {
                session_id,
                user_input,
                user_message_metadata,
                ..
            } if session_id == "s1" => Some((user_input, user_message_metadata)),
            _ => None,
        })
        .collect();
    assert_eq!(turns.len(), 1);
    assert_eq!(
        turns[0].0,
        "Background agents finished: task c1; task c2 (failed)"
    );
    let metadata = turns[0].1.as_ref().unwrap();
    assert_eq!(
        metadata["triggerSource"],
        crate::client::native::NATIVE_WAKE_TRIGGER_SOURCE
    );
    assert_eq!(
        metadata["nativeWake"]["threads"].as_array().unwrap().len(),
        2
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), next_frame(&mut lines))
            .await
            .is_err(),
        "no second wake"
    );
}

#[tokio::test]
async fn cancel_suppresses_wake_until_the_user_starts_a_turn() {
    let (session, process, mut lines) = fixture(Some("th1"));
    parent_turn_spawning(&session, &process, &["c1"]).await;
    // Cancel interrupts the parent turn and the running child.
    let cancel = {
        let session = session.clone();
        tokio::spawn(async move { session.cancel().await })
    };
    let mut interrupted = Vec::new();
    for _ in 0..2 {
        let request = next_frame(&mut lines).await;
        assert_eq!(request["method"], "turn/interrupt");
        interrupted.push(request["params"]["threadId"].as_str().unwrap().to_string());
        session
            .route_frame(&process, json!({"id": request["id"], "result": {}}))
            .await
            .unwrap();
        if request["params"]["threadId"] == "th1" {
            end_parent_turn(&session, &process).await;
        }
    }
    cancel.await.unwrap().unwrap();
    interrupted.sort();
    assert_eq!(interrupted, ["c1", "th1"]);
    // Even if the child still completes, nothing wakes the parent.
    child_says(&session, &process, "c1", "late", "completed").await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), next_frame(&mut lines))
            .await
            .is_err()
    );
    assert!(session.state.lock().await.threads.wake_reasons.is_empty());
}

#[tokio::test]
async fn request_user_input_is_answered_from_the_question_card() {
    let (session, process, mut lines) = fixture(Some("th1"));
    session
        .begin_turn("t1".into(), "hi".into(), None)
        .await
        .unwrap();
    let params = json!({"threadId":"th1","turnId":"ct1","itemId":"q1","isBlocking":true,
        "autoResolutionMs":null,"questions":[
        {"id":"color","header":"Color","question":"Which color?","isOther":true,"isSecret":false,
         "options":[{"label":"Red","description":"warm"},{"label":"Blue","description":"cool"}]},
        {"id":"token","header":"Token","question":"API token?","isOther":false,"isSecret":true,"options":null}]});
    session
        .route_frame(
            &process,
            json!({"id": 7, "method": "item/tool/requestUserInput", "params": params}),
        )
        .await
        .unwrap();
    let permission_id = session.approvals.iter().next().unwrap().key().clone();
    // Unanswered questions keep the request pending.
    let mut request = SubmitAcpPermissionResponseRequest {
        permission_id: permission_id.clone(),
        approve: true,
        option_id: Some("allow_once".into()),
        answers: Some(json!({"0": "Blue"})),
        feedback: None,
    };
    assert!(session.submit_permission(&request).await.unwrap().is_err());
    request.answers = Some(json!({"0": "Blue", "1": "s3cret"}));
    session.submit_permission(&request).await.unwrap().unwrap();
    assert_eq!(
        next_frame(&mut lines).await,
        json!({"id": 7, "result": {"answers": {
            "color": {"answers": ["Blue"]}, "token": {"answers": ["s3cret"]}}}})
    );
    // The card shows the answers but not the secret one.
    let events = session.queue.dequeue_batch(200).await;
    let completed = events
        .iter()
        .find_map(|e| match &e.event {
            AgenticEvent::ToolEvent {
                tool_event: openbitfun_events::ToolEventData::Completed { result, .. },
                ..
            } => Some(result.clone()),
            _ => None,
        })
        .expect("question card completed");
    assert_eq!(completed["answers"], json!({"0": "Blue", "1": "(hidden)"}));

    // Skip answers with an explicit note, never empty answers.
    session
        .route_frame(
            &process,
            json!({"id": 8, "method": "item/tool/requestUserInput", "params": params}),
        )
        .await
        .unwrap();
    let permission_id = session.approvals.iter().next().unwrap().key().clone();
    session
        .submit_permission(&SubmitAcpPermissionResponseRequest {
            permission_id,
            approve: false,
            option_id: Some("reject_once".into()),
            answers: None,
            feedback: None,
        })
        .await
        .unwrap()
        .unwrap();
    let reply = next_frame(&mut lines).await;
    assert_eq!(reply["id"], 8);
    let note = &reply["result"]["answers"]["color"]["answers"][0];
    assert!(note.as_str().unwrap().starts_with("user_note:"));

    // An unanswered request fails explicitly when its turn ends.
    session
        .route_frame(
            &process,
            json!({"id": 9, "method": "item/tool/requestUserInput", "params": params}),
        )
        .await
        .unwrap();
    session.clear_approvals("t1").await;
    let reply = next_frame(&mut lines).await;
    assert_eq!(reply["id"], 9);
    assert_eq!(reply["error"]["code"], -32800);
}

#[tokio::test]
async fn mcp_elicitations_route_forms_and_decline_url_mode() {
    let (session, process, mut lines) = fixture(Some("th1"));
    session
        .begin_turn("t1".into(), "hi".into(), None)
        .await
        .unwrap();
    session
        .route_frame(
            &process,
            json!({"id": 3, "method": "mcpServer/elicitation/request", "params": {
                "threadId":"th1","turnId":"ct1","serverName":"db","mode":"form","_meta":null,
                "message":"Approve?","requestedSchema":{"type":"object","properties":{
                    "ok":{"type":"boolean","title":"Proceed"}}}}}),
        )
        .await
        .unwrap();
    let permission_id = session.approvals.iter().next().unwrap().key().clone();
    session
        .submit_permission(&SubmitAcpPermissionResponseRequest {
            permission_id,
            approve: true,
            option_id: Some("allow_once".into()),
            answers: Some(json!({"0": "Yes"})),
            feedback: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next_frame(&mut lines).await,
        json!({"id": 3, "result": {"action":"accept","content":{"ok":true},"_meta":null}})
    );
    session
        .route_frame(
            &process,
            json!({"id": 4, "method": "mcpServer/elicitation/request", "params": {
                "threadId":"th1","turnId":"ct1","serverName":"auth","mode":"url","_meta":null,
                "message":"Sign in","url":"https://example.test","elicitationId":"e1"}}),
        )
        .await
        .unwrap();
    assert_eq!(
        next_frame(&mut lines).await,
        json!({"id": 4, "result": {"action":"decline","content":null,"_meta":null}})
    );
    // Pending elicitations are cancelled (not declined) when the turn ends.
    session
        .route_frame(
            &process,
            json!({"id": 5, "method": "mcpServer/elicitation/request", "params": {
                "threadId":"th1","turnId":"ct1","serverName":"db","mode":"form","_meta":null,
                "message":"Confirm","requestedSchema":{"type":"object","properties":{}}}}),
        )
        .await
        .unwrap();
    session.clear_approvals("t1").await;
    assert_eq!(
        next_frame(&mut lines).await,
        json!({"id": 5, "result": {"action":"cancel","content":null,"_meta":null}})
    );
}

#[tokio::test]
async fn compact_command_starts_thread_compaction() {
    let (session, process, mut lines) = fixture(Some("th1"));
    *session.process.lock().await = Some(process.clone());
    *lock(&session.thread_process) = Some(process.id.clone());
    let turn = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .start_turn("t1".into(), "/compact".into(), None)
                .await
        })
    };
    let request = answer(&session, &process, &mut lines, json!({})).await;
    assert_eq!(request["method"], "thread/compact/start");
    assert_eq!(request["params"], json!({"threadId":"th1"}));
    turn.await.unwrap().unwrap();
    assert!(session
        .start_turn("t2".into(), "/compact keep it short".into(), None)
        .await
        .is_err());
}
