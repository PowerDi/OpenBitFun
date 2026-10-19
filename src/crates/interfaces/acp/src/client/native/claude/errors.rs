//! Maps Claude CLI failures (spawn errors, stderr tails, `result` frames) to
//! user-facing turn errors. The raw text stays available as provider message.
use openbitfun_events::agentic::{AiErrorDetail, ErrorCategory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaudeFailure {
    pub message: String,
    pub category: ErrorCategory,
    pub raw: String,
}

impl ClaudeFailure {
    /// Guidance first, raw CLI text after it for diagnostics.
    pub(crate) fn display(&self) -> String {
        let raw = self.raw.trim();
        if raw.is_empty() || raw == self.message {
            self.message.clone()
        } else {
            format!("{}

{raw}", self.message)
        }
    }

    pub(crate) fn detail(&self) -> AiErrorDetail {
        let hints = match self.category {
            // Model settings do not apply to the external CLI's own login.
            ErrorCategory::Auth | ErrorCategory::Unknown | ErrorCategory::InvalidRequest => {
                vec!["copy_diagnostics".to_string()]
            }
            _ => Vec::new(),
        };
        AiErrorDetail {
            category: self.category.clone(),
            provider: Some("Claude Code".to_string()),
            provider_code: None,
            provider_message: Some(self.raw.clone()).filter(|raw| !raw.trim().is_empty()),
            request_id: None,
            http_status: None,
            retryable: None,
            action_hints: hints,
            recovery_constraint: None,
        }
    }
}

/// Classify a failure. `subtype` is the `result` frame subtype when known;
/// `assistant_error` is the `error` field Claude puts on assistant frames.
pub(crate) fn classify(
    raw: &str,
    subtype: Option<&str>,
    assistant_error: Option<&str>,
) -> ClaudeFailure {
    let lower = raw.to_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
    let launch_failed = lower.contains("launch failed");
    let (category, message) = if has(&[
        "command not found",
        "not recognized as an internal or external command",
    ]) || (launch_failed
        && has(&[
            "not found",
            "os error 2",
            "no such file",
            "cannot find the file",
        ]))
    {
        (
            ErrorCategory::Unknown,
            "Claude Code CLI was not found. Install Claude Code (`npm i -g @anthropic-ai/claude-code` or the native installer) and make sure `claude` is on PATH.",
        )
    } else if matches!(
        assistant_error,
        Some("authentication_failed" | "oauth_org_not_allowed")
    ) || has(&[
        "not logged in",
        "please run /login",
        "invalid api key",
        "authentication_error",
        "oauth token has expired",
        "api error: 401",
        "authentication failed",
    ]) {
        (
            ErrorCategory::Auth,
            "Claude Code is not signed in or its credentials are invalid. Run `claude` in a terminal and complete `/login`, then retry.",
        )
    } else if subtype == Some("error_max_turns") {
        (
            ErrorCategory::ModelError,
            "Claude Code stopped after reaching its maximum number of turns. Send a follow-up message to continue.",
        )
    } else if subtype == Some("error_max_budget_usd") {
        (
            ErrorCategory::ProviderQuota,
            "Claude Code stopped after reaching its configured spending budget.",
        )
    } else if has(&["credit balance is too low", "usage limit", "limit reached"]) {
        (
            ErrorCategory::ProviderQuota,
            "Your Claude usage or credit limit has been reached. Wait for the limit to reset or check your plan.",
        )
    } else if has(&["rate_limit_error", "api error: 429", "rate limit"]) {
        (
            ErrorCategory::RateLimit,
            "Claude API rate limit reached. Wait a moment and retry.",
        )
    } else if has(&["overloaded", "api error: 529", "api error: 503", "api error: 500"]) {
        (
            ErrorCategory::ProviderUnavailable,
            "The Claude API is temporarily overloaded or unavailable. Retry shortly.",
        )
    } else if has(&["prompt is too long", "context window", "context_length"]) {
        (
            ErrorCategory::ContextOverflow,
            "The conversation exceeds Claude's context window. Run /compact or start a new chat.",
        )
    } else if has(&[
        "econnrefused",
        "enotfound",
        "etimedout",
        "connection error",
        "network",
        "fetch failed",
    ]) {
        (
            ErrorCategory::Network,
            "Claude Code could not reach the Claude API. Check your network or proxy settings.",
        )
    } else if has(&["api error: 400", "invalid_request_error"]) {
        (
            ErrorCategory::InvalidRequest,
            "The Claude API rejected the request.",
        )
    } else if has(&["process exited", "output stream closed", "supervisor ended"]) {
        (
            ErrorCategory::Unknown,
            "The Claude Code process exited unexpectedly. Retry to resume the session.",
        )
    } else {
        return ClaudeFailure {
            message: first_line(raw).unwrap_or("Claude Code turn failed").to_string(),
            category: ErrorCategory::Unknown,
            raw: raw.to_string(),
        };
    };
    ClaudeFailure {
        message: message.to_string(),
        category,
        raw: raw.to_string(),
    }
}

fn first_line(raw: &str) -> Option<&str> {
    raw.lines().map(str::trim).find(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_common_cli_failures() {
        let cases = [
            (
                "Claude native launch failed: program not found",
                None,
                None,
                ErrorCategory::Unknown,
                "not found",
            ),
            (
                "Claude control request failed\nInvalid API key · Please run /login",
                None,
                None,
                ErrorCategory::Auth,
                "/login",
            ),
            (
                "anything",
                None,
                Some("authentication_failed"),
                ErrorCategory::Auth,
                "/login",
            ),
            (
                "error_max_turns",
                Some("error_max_turns"),
                None,
                ErrorCategory::ModelError,
                "maximum number of turns",
            ),
            (
                "API Error: 529 {\"type\":\"overloaded_error\"}",
                Some("success"),
                None,
                ErrorCategory::ProviderUnavailable,
                "overloaded",
            ),
        ];
        for (raw, subtype, assistant, category, needle) in cases {
            let failure = classify(raw, subtype, assistant);
            assert_eq!(failure.category, category, "{raw}");
            assert!(failure.message.contains(needle), "{}", failure.message);
            assert_eq!(failure.raw, raw);
        }
        let unknown = classify("\nsomething odd\nmore", None, None);
        assert_eq!(unknown.message, "something odd");
    }
}
