use super::*;

fn run(projector: &mut PiTurnProjector, frames: &[Value]) -> Vec<AgenticEvent> {
    frames
        .iter()
        .flat_map(|frame| projector.consume(frame))
        .collect()
}

fn update(event: Value) -> Value {
    json!({"type":"message_update", "usage":{}, "assistantMessageEvent": event})
}

fn texts(events: &[AgenticEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            AgenticEvent::TextChunk { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn projects_text_thinking_and_tool_rounds_then_settles() {
    let mut projector = PiTurnProjector::new("s1", "t1", Some("sonnet"), Some(200_000));
    let mut events = projector.open_pending_round();
    events.extend(run(
        &mut projector,
        &[
            json!({"type":"agent_start"}),
            json!({"type":"turn_start"}),
            json!({"type":"message_start","message":{"role":"user","content":"hi"}}),
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            update(json!({"type":"thinking_delta","contentIndex":0,"delta":"plan"})),
            update(json!({"type":"thinking_end","contentIndex":0,"content":"plan it"})),
            update(json!({"type":"text_delta","contentIndex":1,"delta":"Let me "})),
            update(json!({"type":"text_end","contentIndex":1,"content":"Let me look."})),
            json!({"type":"message_end","message":{"role":"assistant","model":"claude-sonnet-4-5",
                "stopReason":"toolUse","usage":{"input":100,"output":20,"cacheRead":900,"cacheWrite":0}}}),
            json!({"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"ls"}}),
            json!({"type":"tool_execution_end","toolCallId":"c1","toolName":"bash",
                "result":{"content":[{"type":"text","text":"a.txt"}],"details":{}},"isError":false,"durationMs":5}),
            json!({"type":"tool_execution_start","toolCallId":"c2","toolName":"edit",
                "args":{"path":"a.txt","edits":[{"oldText":"x","newText":"y"}]}}),
            json!({"type":"tool_execution_end","toolCallId":"c2","toolName":"edit",
                "result":{"content":[{"type":"text","text":"no match"}]},"isError":true}),
            json!({"type":"turn_end"}),
            json!({"type":"turn_start"}),
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            update(json!({"type":"text_delta","contentIndex":0,"delta":"Done."})),
            json!({"type":"message_end","message":{"role":"assistant","stopReason":"stop","usage":{"input":10,"output":2}}}),
            json!({"type":"turn_end"}),
            json!({"type":"agent_end","willRetry":false}),
            json!({"type":"agent_settled","aborted":false}),
        ],
    ));
    assert_eq!(texts(&events), "Let me look.Done.");
    let thinking: String = events
        .iter()
        .filter_map(|event| match event {
            AgenticEvent::ThinkingChunk { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(thinking, "plan it");
    // The placeholder round is adopted by the first `turn_start`.
    let rounds = events
        .iter()
        .filter(|e| matches!(e, AgenticEvent::ModelRoundStarted { .. }))
        .count();
    assert_eq!(rounds, 2);
    let tools: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgenticEvent::ToolEvent { tool_event, .. } => Some(tool_event),
            _ => None,
        })
        .collect();
    match tools[0] {
        ToolEventData::Started {
            identity, params, ..
        } => {
            assert_eq!(identity.tool_name, "ExecCommand");
            assert_eq!(params["cmd"], "ls");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(matches!(
        tools[1],
        ToolEventData::Completed { duration_ms: 5, .. }
    ));
    match tools[2] {
        ToolEventData::Started {
            identity, params, ..
        } => {
            assert_eq!(identity.tool_name, "Edit");
            assert_eq!(params["file_path"], "a.txt");
            assert_eq!(params["old_string"], "x");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(matches!(tools[3], ToolEventData::Failed { .. }));
    let usage = events.iter().find_map(|event| match event {
        AgenticEvent::TokenUsageUpdated {
            input_tokens,
            max_context_tokens,
            cached_tokens,
            ..
        } => Some((*input_tokens, *max_context_tokens, *cached_tokens)),
        _ => None,
    });
    assert_eq!(usage, Some((1000, Some(200_000), Some(900))));
    assert!(matches!(
        events.last(),
        Some(AgenticEvent::DialogTurnCompleted {
            total_tools: 2,
            has_final_response: Some(true),
            ..
        })
    ));
    assert!(projector.is_finished());
    assert!(projector
        .consume(&json!({"type":"agent_settled"}))
        .is_empty());
}

#[test]
fn provider_error_fails_and_abort_cancels_open_tools() {
    let mut failed = PiTurnProjector::new("s1", "t1", None, None);
    let events = run(
        &mut failed,
        &[
            json!({"type":"turn_start"}),
            json!({"type":"message_start","message":{"role":"assistant"}}),
            json!({"type":"message_end","message":{"role":"assistant","stopReason":"error",
                "errorMessage":"No API key found for anthropic","usage":{}}}),
            json!({"type":"agent_settled","aborted":false}),
        ],
    );
    match events.last() {
        Some(AgenticEvent::DialogTurnFailed {
            error_category,
            error,
            ..
        }) => {
            assert_eq!(
                error_category,
                &Some(openbitfun_events::agentic::ErrorCategory::Auth)
            );
            assert!(error.contains("/login"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let mut aborted = PiTurnProjector::new("s1", "t2", None, None);
    let mut events = run(
        &mut aborted,
        &[
            json!({"type":"turn_start"}),
            json!({"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"sleep 9"}}),
        ],
    );
    aborted.mark_cancel_requested();
    events.extend(aborted.consume(&json!({"type":"agent_settled","aborted":true})));
    assert!(events.iter().any(|e| matches!(
        e,
        AgenticEvent::ToolEvent {
            tool_event: ToolEventData::Cancelled { .. },
            ..
        }
    )));
    assert!(matches!(
        events.last(),
        Some(AgenticEvent::DialogTurnCancelled { .. })
    ));
}

#[test]
fn compaction_events_map_to_context_compression() {
    let mut projector = PiTurnProjector::new("s1", "t1", None, Some(100_000));
    let events = run(
        &mut projector,
        &[
            json!({"type":"compaction_start","reason":"manual"}),
            json!({"type":"compaction_end","reason":"manual","aborted":false,"willRetry":false,
                "result":{"summary":"s","tokensBefore":80000,"estimatedTokensAfter":20000}}),
        ],
    );
    assert!(matches!(
        &events[0],
        AgenticEvent::ContextCompressionStarted { trigger, .. } if trigger == "manual"
    ));
    assert!(matches!(
        &events[1],
        AgenticEvent::ContextCompressionCompleted {
            tokens_before: 80000,
            tokens_after: 20000,
            ..
        }
    ));
    let failed = run(
        &mut projector,
        &[
            json!({"type":"compaction_end","reason":"threshold","aborted":false,"errorMessage":"boom"}),
        ],
    );
    assert!(matches!(
        failed.last(),
        Some(AgenticEvent::ContextCompressionFailed { error, .. }) if error == "boom"
    ));
}

#[test]
fn only_user_messages_after_the_prompt_count_as_injected() {
    let mut projector = PiTurnProjector::new("s1", "t1", None, None);
    let user = json!({"type":"message_start","message":{"role":"user","content":"x"}});
    assert!(!projector.is_injected_user_message(&user));
    assert!(!projector
        .is_injected_user_message(&json!({"type":"message_start","message":{"role":"assistant"}})));
    assert!(projector.is_injected_user_message(&user));
}
