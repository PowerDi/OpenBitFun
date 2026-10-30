//! Transient runtime status reported by native agents: automatic request
//! retries and account rate limits. Presentation-only; nothing here enters the
//! transcript or the core event contracts.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One automatic retry of the model request, as announced by the agent.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeRetryStatus {
    /// 1-based attempt about to run, when the agent reports it.
    pub attempt: Option<u32>,
    pub max_attempts: Option<u32>,
    /// Wait before the attempt starts.
    pub delay_ms: Option<u64>,
    /// HTTP status of the failed request (e.g. 429, 529).
    pub error_status: Option<u16>,
    /// Agent-provided error text, for diagnostics only.
    pub message: Option<String>,
}

/// Usage of one rate-limit window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeRateLimitWindow {
    /// Agent window id: Claude `five_hour` / `seven_day` / ..., Codex
    /// `primary` / `secondary`.
    pub kind: String,
    /// Claude `allowed` / `allowed_warning` / `rejected`; absent for Codex.
    pub status: Option<String>,
    pub used_percent: Option<f64>,
    pub window_minutes: Option<u64>,
    /// Host clock (Unix ms) when the window resets.
    pub resets_at_ms: Option<u64>,
}

/// Latest known account rate limits of a native session.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NativeRateLimitSnapshot {
    pub windows: Vec<NativeRateLimitWindow>,
    /// Codex `rateLimitReachedType` while a limit blocks requests.
    pub limit_reached: Option<String>,
    /// Host clock (Unix ms) of the latest update.
    pub updated_at_ms: u64,
}

/// Usage share from which a window deserves a warning.
const HIGH_USAGE_PERCENT: f64 = 80.0;

impl NativeRateLimitSnapshot {
    /// Merge a sparse update: windows are replaced by kind, others kept.
    pub fn merge(&mut self, update: NativeRateLimitSnapshot) {
        for window in update.windows {
            match self
                .windows
                .iter_mut()
                .find(|known| known.kind == window.kind)
            {
                Some(known) => *known = window,
                None => self.windows.push(window),
            }
        }
        self.limit_reached = update.limit_reached;
        self.updated_at_ms = update.updated_at_ms;
    }

    /// A limit blocks requests, or a window is close to its limit.
    pub fn needs_attention(&self) -> bool {
        self.limit_reached.is_some()
            || self.windows.iter().any(|window| {
                matches!(
                    window.status.as_deref(),
                    Some("allowed_warning" | "rejected")
                ) || window
                    .used_percent
                    .is_some_and(|used| used >= HIGH_USAGE_PERCENT)
            })
    }
}

fn as_u32(value: &Value) -> Option<u32> {
    value.as_u64().and_then(|n| u32::try_from(n).ok())
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Object(object) => object
            .get("message")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_string),
        _ => None,
    }
}

/// Claude stream-json `system/api_retry`:
/// `{attempt, max_retries, retry_delay_ms, error_status, error}`.
pub(crate) fn claude_retry(frame: &Value) -> Option<NativeRetryStatus> {
    if frame["type"] != "system" || frame["subtype"] != "api_retry" {
        return None;
    }
    Some(NativeRetryStatus {
        attempt: as_u32(&frame["attempt"]),
        max_attempts: as_u32(&frame["max_retries"]),
        delay_ms: frame["retry_delay_ms"].as_u64(),
        error_status: frame["error_status"]
            .as_u64()
            .and_then(|n| u16::try_from(n).ok()),
        message: text(&frame["error"]),
    })
}

/// Claude stream-json `rate_limit_event.rate_limit_info`:
/// `{status, resetsAt (Unix s), rateLimitType, utilization (0..1)}`.
pub(crate) fn claude_rate_limit(frame: &Value, now_ms: u64) -> Option<NativeRateLimitSnapshot> {
    if frame["type"] != "rate_limit_event" {
        return None;
    }
    let info = frame.get("rate_limit_info")?;
    let status = info["status"].as_str().map(str::to_string);
    let window = NativeRateLimitWindow {
        kind: info["rateLimitType"]
            .as_str()
            .unwrap_or("unknown")
            .to_string(),
        window_minutes: match info["rateLimitType"].as_str() {
            Some("five_hour") => Some(300),
            Some(kind) if kind.starts_with("seven_day") => Some(7 * 24 * 60),
            _ => None,
        },
        used_percent: info["utilization"]
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|value| value * 100.0),
        resets_at_ms: info["resetsAt"].as_u64().map(|seconds| seconds * 1000),
        status,
    };
    Some(NativeRateLimitSnapshot {
        windows: vec![window],
        limit_reached: None,
        updated_at_ms: now_ms,
    })
}

/// Codex `error` with `willRetry: true`; Codex reports no attempt counts,
/// so the host counts the retries of the turn.
pub(crate) fn codex_retry(params: &Value, attempt: u32) -> Option<NativeRetryStatus> {
    if params["willRetry"].as_bool() != Some(true) {
        return None;
    }
    let error = &params["error"];
    Some(NativeRetryStatus {
        attempt: Some(attempt),
        max_attempts: None,
        delay_ms: None,
        error_status: error["codexErrorInfo"]
            .as_object()
            .and_then(|info| info.values().next())
            .and_then(|detail| detail["httpStatusCode"].as_u64())
            .and_then(|n| u16::try_from(n).ok()),
        message: text(error),
    })
}

/// Codex `account/rateLimits/updated`: `{rateLimits: {primary, secondary,
/// rateLimitReachedType}}`, windows `{usedPercent, windowDurationMins,
/// resetsAt (Unix s)}`. Absent windows are not reported (sparse update).
pub(crate) fn codex_rate_limits(params: &Value, now_ms: u64) -> Option<NativeRateLimitSnapshot> {
    let limits = params.get("rateLimits")?;
    let windows = ["primary", "secondary"]
        .into_iter()
        .filter_map(|kind| {
            let window = limits.get(kind).filter(|window| window.is_object())?;
            Some(NativeRateLimitWindow {
                kind: kind.to_string(),
                status: None,
                used_percent: window["usedPercent"].as_f64(),
                window_minutes: window["windowDurationMins"].as_u64(),
                resets_at_ms: window["resetsAt"].as_u64().map(|seconds| seconds * 1000),
            })
        })
        .collect();
    Some(NativeRateLimitSnapshot {
        windows,
        limit_reached: limits["rateLimitReachedType"].as_str().map(str::to_string),
        updated_at_ms: now_ms,
    })
}

/// Pi `auto_retry_start`: `{attempt, maxAttempts, delayMs, errorMessage}`.
pub(crate) fn pi_retry(frame: &Value) -> Option<NativeRetryStatus> {
    if frame["type"] != "auto_retry_start" {
        return None;
    }
    Some(NativeRetryStatus {
        attempt: as_u32(&frame["attempt"]),
        max_attempts: as_u32(&frame["maxAttempts"]),
        delay_ms: frame["delayMs"].as_u64(),
        error_status: None,
        message: text(&frame["errorMessage"]),
    })
}

/// Per-session runtime status: the latest rate-limit snapshot (replayed via
/// session options) and the turn whose retry status is currently shown.
#[derive(Debug, Default)]
pub(crate) struct NativeRuntimeStatus {
    rate_limits: std::sync::Mutex<Option<NativeRateLimitSnapshot>>,
    /// Turn with a visible retry status and the retries announced for it.
    retry: std::sync::Mutex<Option<(String, u32)>>,
}

fn guard<T>(slot: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl NativeRuntimeStatus {
    pub(crate) fn rate_limits(&self) -> Option<NativeRateLimitSnapshot> {
        guard(&self.rate_limits).clone()
    }

    /// Merge a (possibly sparse) update and return the notification to publish.
    pub(crate) fn update_rate_limits(
        &self,
        update: NativeRateLimitSnapshot,
    ) -> super::NativeAgentNotification {
        let mut slot = guard(&self.rate_limits);
        let snapshot = slot.get_or_insert_with(NativeRateLimitSnapshot::default);
        snapshot.merge(update);
        super::NativeAgentNotification::RateLimitsUpdated(snapshot.clone())
    }

    /// 1-based number of the next retry of `turn_id` (for agents that do not
    /// count attempts themselves).
    pub(crate) fn next_attempt(&self, turn_id: &str) -> u32 {
        match guard(&self.retry).as_ref() {
            Some((turn, count)) if turn == turn_id => count + 1,
            _ => 1,
        }
    }

    pub(crate) fn retry(
        &self,
        turn_id: &str,
        status: NativeRetryStatus,
    ) -> super::NativeAgentNotification {
        let mut slot = guard(&self.retry);
        let count = match slot.as_ref() {
            Some((turn, count)) if turn == turn_id => count + 1,
            _ => 1,
        };
        *slot = Some((turn_id.to_string(), count));
        super::NativeAgentNotification::RetryStatusChanged {
            turn_id: turn_id.to_string(),
            retry: Some(status),
        }
    }

    /// Clear a visible retry status, e.g. when the agent reports the retry ended.
    pub(crate) fn settle_retry(&self) -> Option<super::NativeAgentNotification> {
        let (turn_id, _) = guard(&self.retry).take()?;
        Some(super::NativeAgentNotification::RetryStatusChanged {
            turn_id,
            retry: None,
        })
    }

    /// Clear the retry status once projected events show output resumed or
    /// the turn ended.
    pub(crate) fn settle_retry_after(
        &self,
        events: &[openbitfun_events::AgenticEvent],
    ) -> Option<super::NativeAgentNotification> {
        use openbitfun_events::AgenticEvent;
        if guard(&self.retry).is_none() {
            return None;
        }
        let resumed = events.iter().any(|event| {
            matches!(
                event,
                AgenticEvent::TextChunk { .. }
                    | AgenticEvent::ThinkingChunk { .. }
                    | AgenticEvent::ToolEvent { .. }
                    | AgenticEvent::DialogTurnCompleted { .. }
                    | AgenticEvent::DialogTurnFailed { .. }
                    | AgenticEvent::DialogTurnCancelled { .. }
            )
        });
        resumed.then(|| self.settle_retry()).flatten()
    }
}

pub(crate) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_claude_api_retry() {
        let retry = claude_retry(&json!({"type":"system","subtype":"api_retry","attempt":2,
            "max_retries":10,"retry_delay_ms":4000,"error_status":529,"error":"overloaded_error"}))
        .expect("retry");
        assert_eq!(retry.attempt, Some(2));
        assert_eq!(retry.max_attempts, Some(10));
        assert_eq!(retry.delay_ms, Some(4000));
        assert_eq!(retry.error_status, Some(529));
        assert_eq!(retry.message.as_deref(), Some("overloaded_error"));
        assert!(claude_retry(&json!({"type":"system","subtype":"init"})).is_none());
    }

    #[test]
    fn maps_claude_rate_limit_event_and_merges_windows() {
        let event = |kind: &str, status: &str, utilization: f64| {
            json!({"type":"rate_limit_event","rate_limit_info":{"status":status,
                "resetsAt":1_800_000_000u64,"rateLimitType":kind,"utilization":utilization}})
        };
        let mut snapshot = claude_rate_limit(&event("five_hour", "allowed", 0.2), 1).unwrap();
        assert_eq!(snapshot.windows[0].window_minutes, Some(300));
        assert_eq!(snapshot.windows[0].resets_at_ms, Some(1_800_000_000_000));
        assert!(!snapshot.needs_attention());
        snapshot.merge(claude_rate_limit(&event("seven_day", "allowed_warning", 0.5), 2).unwrap());
        snapshot.merge(claude_rate_limit(&event("five_hour", "allowed", 0.85), 3).unwrap());
        assert_eq!(snapshot.windows.len(), 2);
        let five_hour = &snapshot.windows[0];
        assert!((five_hour.used_percent.unwrap() - 85.0).abs() < 1e-9);
        assert_eq!(snapshot.updated_at_ms, 3);
        assert!(snapshot.needs_attention());
    }

    #[test]
    fn maps_codex_retry_and_sparse_rate_limits() {
        let retry = codex_retry(
            &json!({"willRetry":true,"error":{"message":"stream disconnected",
                "codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":502}}}}),
            3,
        )
        .expect("retry");
        assert_eq!(retry.attempt, Some(3));
        assert_eq!(retry.error_status, Some(502));
        assert!(codex_retry(&json!({"willRetry":false,"error":{"message":"x"}}), 1).is_none());

        let mut snapshot = codex_rate_limits(
            &json!({"rateLimits":{"primary":{"usedPercent":10.0,"windowDurationMins":300,
                "resetsAt":100},"secondary":{"usedPercent":20.0,"windowDurationMins":10080,
                "resetsAt":200},"rateLimitReachedType":null}}),
            1,
        )
        .unwrap();
        snapshot.merge(
            codex_rate_limits(
                &json!({"rateLimits":{"primary":{"usedPercent":100.0,
                    "windowDurationMins":300,"resetsAt":150},"secondary":null,
                    "rateLimitReachedType":"rate_limit_reached"}}),
                2,
            )
            .unwrap(),
        );
        assert_eq!(
            snapshot.windows.len(),
            2,
            "a null window keeps the last value"
        );
        assert_eq!(snapshot.windows[0].used_percent, Some(100.0));
        assert_eq!(snapshot.windows[1].resets_at_ms, Some(200_000));
        assert_eq!(
            snapshot.limit_reached.as_deref(),
            Some("rate_limit_reached")
        );
        assert!(snapshot.needs_attention());
    }

    #[test]
    fn maps_pi_auto_retry_start() {
        let retry = pi_retry(
            &json!({"type":"auto_retry_start","attempt":1,"maxAttempts":3,
            "delayMs":2000,"errorMessage":"429 Too Many Requests"}),
        )
        .expect("retry");
        assert_eq!(
            (retry.attempt, retry.max_attempts, retry.delay_ms),
            (Some(1), Some(3), Some(2000))
        );
    }

    #[test]
    fn retry_status_counts_per_turn_and_settles_on_output() {
        use super::super::NativeAgentNotification;
        let status = NativeRuntimeStatus::default();
        assert_eq!(status.next_attempt("t1"), 1);
        status.retry("t1", NativeRetryStatus::default());
        assert_eq!(status.next_attempt("t1"), 2);
        assert_eq!(status.next_attempt("t2"), 1);
        let round = openbitfun_events::AgenticEvent::ModelRoundStarted {
            session_id: "s".into(),
            turn_id: "t1".into(),
            round_id: "r".into(),
            round_group_id: None,
            round_index: 0,
            model_config_id: "m".into(),
            effective_model_name: "m".into(),
            routing: None,
        };
        assert!(
            status.settle_retry_after(&[round]).is_none(),
            "no output yet"
        );
        let text = openbitfun_events::AgenticEvent::TextChunk {
            session_id: "s".into(),
            turn_id: "t1".into(),
            round_id: "r".into(),
            attempt_id: None,
            attempt_index: None,
            text: "hi".into(),
        };
        assert!(matches!(
            status.settle_retry_after(&[text.clone()]),
            Some(NativeAgentNotification::RetryStatusChanged { retry: None, .. })
        ));
        assert!(status.settle_retry_after(&[text]).is_none(), "settles once");
    }

    #[test]
    fn older_payloads_without_new_fields_deserialize() {
        let snapshot: NativeRateLimitSnapshot = serde_json::from_value(json!({})).unwrap();
        assert!(snapshot.windows.is_empty());
        let retry: NativeRetryStatus = serde_json::from_value(json!({"attempt":1})).unwrap();
        assert_eq!(retry.attempt, Some(1));
    }
}
