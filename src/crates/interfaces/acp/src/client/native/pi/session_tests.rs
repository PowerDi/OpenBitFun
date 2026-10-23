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
    (session, process, reader)
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
    let sent = sent.lock().unwrap().clone();
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
    let sent = sent.lock().unwrap().clone();
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
    let kinds: Vec<_> = sent
        .lock()
        .unwrap()
        .iter()
        .map(|c| c["type"].clone())
        .collect();
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

#[test]
fn launch_args_pin_the_pi_session_and_compact_parsing_is_strict() {
    assert_eq!(
        cli_args("abc-1"),
        ["--mode", "rpc", "--session-id", "abc-1"]
    );
    assert!(remote_command("/home/me/repo", &cli_args("abc-1")).contains("exec pi --mode rpc"));
    assert_eq!(compact_instructions("/compact"), Some(None));
    assert_eq!(
        compact_instructions(" /compact  focus "),
        Some(Some("focus".into()))
    );
    assert_eq!(compact_instructions("/compaction"), None);
    assert_eq!(compact_instructions("please /compact"), None);
}
