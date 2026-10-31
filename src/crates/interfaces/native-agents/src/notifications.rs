//! Host-neutral delivery of native session presentation updates.
//!
//! Every update becomes one named event on the shared backend event system,
//! so whichever host registered an emitter (Desktop webview, Peer Mode
//! fan-out, a future CLI or server host) receives it. Event names and payload
//! shapes are a Web UI contract and must stay stable.
use openbitfun_core::infrastructure::events::{emit_global_event, BackendEvent};
use serde_json::{json, Value};

use crate::{NativeAgentNotification, NativeAgentNotificationSink};

pub const AVAILABLE_COMMANDS_UPDATED_EVENT: &str = "agentic://acp-available-commands-updated";
pub const PLAN_UPDATED_EVENT: &str = "agentic://acp-plan-updated";
pub const CONTEXT_USAGE_UPDATED_EVENT: &str = "agentic://acp-context-usage-updated";
pub const SESSION_OPTIONS_CHANGED_EVENT: &str = "agentic://acp-session-options-changed";
pub const BACKGROUND_TASKS_UPDATED_EVENT: &str = "agentic://acp-background-tasks-updated";
pub const RETRY_STATUS_CHANGED_EVENT: &str = "agentic://acp-retry-status-changed";
pub const RATE_LIMITS_UPDATED_EVENT: &str = "agentic://acp-rate-limits-updated";

/// Every event name a native session publishes, for host fan-out allowlists.
pub const NATIVE_NOTIFICATION_EVENTS: [&str; 7] = [
    AVAILABLE_COMMANDS_UPDATED_EVENT,
    PLAN_UPDATED_EVENT,
    CONTEXT_USAGE_UPDATED_EVENT,
    SESSION_OPTIONS_CHANGED_EVENT,
    BACKGROUND_TASKS_UPDATED_EVENT,
    RETRY_STATUS_CHANGED_EVENT,
    RATE_LIMITS_UPDATED_EVENT,
];

/// Event name and payload for one update of `session_id` driven by `client_id`.
pub fn notification_event(
    session_id: &str,
    client_id: &str,
    update: NativeAgentNotification,
) -> (&'static str, Value) {
    match update {
        NativeAgentNotification::AvailableCommandsUpdated(commands) => (
            AVAILABLE_COMMANDS_UPDATED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id, "commands": commands}),
        ),
        NativeAgentNotification::PlanUpdated { turn_id, entries } => (
            PLAN_UPDATED_EVENT,
            json!({"sessionId": session_id, "turnId": turn_id, "clientId": client_id,
                "entries": entries}),
        ),
        NativeAgentNotification::ContextUsageUpdated(usage) => (
            CONTEXT_USAGE_UPDATED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id,
                "used": usage.used, "size": usage.size, "cost": usage.cost,
                "subagentParentInfo": null}),
        ),
        NativeAgentNotification::OptionsChanged => (
            SESSION_OPTIONS_CHANGED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id}),
        ),
        NativeAgentNotification::BackgroundTasksUpdated(tasks) => (
            BACKGROUND_TASKS_UPDATED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id, "tasks": tasks}),
        ),
        NativeAgentNotification::RetryStatusChanged { turn_id, retry } => (
            RETRY_STATUS_CHANGED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id, "turnId": turn_id,
                "retry": retry}),
        ),
        NativeAgentNotification::RateLimitsUpdated(rate_limits) => (
            RATE_LIMITS_UPDATED_EVENT,
            json!({"sessionId": session_id, "clientId": client_id, "rateLimits": rate_limits}),
        ),
    }
}

/// Sink that publishes a session's updates on the shared backend event system.
///
/// Drivers call the sink synchronously; delivery is asynchronous. One
/// forwarding task per sink keeps updates in the order the driver produced
/// them (a later full background-task list must never be overtaken by an
/// earlier one). The task ends when the session drops its sink. Must be
/// created inside a Tokio runtime.
pub fn global_notification_sink(session_id: &str, client_id: &str) -> NativeAgentNotificationSink {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<(&'static str, Value)>();
    let log_session_id = session_id.to_string();
    tokio::spawn(async move {
        while let Some((event_name, payload)) = receiver.recv().await {
            if let Err(error) = emit_global_event(BackendEvent::Custom {
                event_name: event_name.to_string(),
                payload,
            })
            .await
            {
                log::warn!(
                    "Failed to publish native agent update: session_id={log_session_id}, event={event_name}, error={error}"
                );
            }
        }
    });
    let session_id = session_id.to_string();
    let client_id = client_id.to_string();
    std::sync::Arc::new(move |update| {
        let event = notification_event(&session_id, &client_id, update);
        // The receiver only closes with the runtime; nothing left to deliver to.
        let _ = sender.send(event);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AcpAvailableCommand, AcpPlanEntry, AcpSessionContextUsage, NativeBackgroundTask,
        NativeRateLimitSnapshot, NativeRetryStatus,
    };

    fn event(update: NativeAgentNotification) -> (&'static str, Value) {
        notification_event("s1", "claude-native", update)
    }

    #[test]
    fn notification_events_keep_the_web_ui_contract() {
        let (name, payload) = event(NativeAgentNotification::AvailableCommandsUpdated(vec![
            AcpAvailableCommand {
                name: "compact".into(),
                description: "Compact".into(),
                input_hint: None,
            },
        ]));
        assert_eq!(name, "agentic://acp-available-commands-updated");
        assert_eq!(payload["sessionId"], "s1");
        assert_eq!(payload["clientId"], "claude-native");
        assert_eq!(payload["commands"][0]["name"], "compact");

        let (name, payload) = event(NativeAgentNotification::PlanUpdated {
            turn_id: "t1".into(),
            entries: vec![AcpPlanEntry {
                content: "step".into(),
                priority: "high".into(),
                status: "pending".into(),
            }],
        });
        assert_eq!(name, "agentic://acp-plan-updated");
        assert_eq!(payload["turnId"], "t1");
        assert_eq!(payload["entries"][0]["content"], "step");

        let (name, payload) = event(NativeAgentNotification::ContextUsageUpdated(
            AcpSessionContextUsage {
                used: 10,
                size: 100,
                cost: None,
            },
        ));
        assert_eq!(name, "agentic://acp-context-usage-updated");
        assert_eq!(
            payload,
            json!({"sessionId": "s1", "clientId": "claude-native", "used": 10, "size": 100,
                "cost": null, "subagentParentInfo": null})
        );

        let (name, payload) = event(NativeAgentNotification::OptionsChanged);
        assert_eq!(name, "agentic://acp-session-options-changed");
        assert_eq!(
            payload,
            json!({"sessionId": "s1", "clientId": "claude-native"})
        );

        let (name, payload) = event(NativeAgentNotification::BackgroundTasksUpdated(vec![
            NativeBackgroundTask {
                id: "task".into(),
                status: "running".into(),
                ..Default::default()
            },
        ]));
        assert_eq!(name, "agentic://acp-background-tasks-updated");
        assert_eq!(payload["tasks"][0]["id"], "task");

        let (name, payload) = event(NativeAgentNotification::RetryStatusChanged {
            turn_id: "t1".into(),
            retry: Some(NativeRetryStatus::default()),
        });
        assert_eq!(name, "agentic://acp-retry-status-changed");
        assert_eq!(payload["turnId"], "t1");
        assert!(payload["retry"].is_object());

        let (name, payload) = event(NativeAgentNotification::RateLimitsUpdated(
            NativeRateLimitSnapshot::default(),
        ));
        assert_eq!(name, "agentic://acp-rate-limits-updated");
        assert!(payload["rateLimits"].is_object());
    }

    #[test]
    fn fan_out_list_names_every_published_event() {
        for update in [
            NativeAgentNotification::AvailableCommandsUpdated(Vec::new()),
            NativeAgentNotification::PlanUpdated {
                turn_id: String::new(),
                entries: Vec::new(),
            },
            NativeAgentNotification::ContextUsageUpdated(AcpSessionContextUsage {
                used: 0,
                size: 0,
                cost: None,
            }),
            NativeAgentNotification::OptionsChanged,
            NativeAgentNotification::BackgroundTasksUpdated(Vec::new()),
            NativeAgentNotification::RetryStatusChanged {
                turn_id: String::new(),
                retry: None,
            },
            NativeAgentNotification::RateLimitsUpdated(NativeRateLimitSnapshot::default()),
        ] {
            let (name, _) = event(update);
            assert!(NATIVE_NOTIFICATION_EVENTS.contains(&name), "{name}");
        }
    }
}
