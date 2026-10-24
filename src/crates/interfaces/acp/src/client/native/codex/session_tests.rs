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
