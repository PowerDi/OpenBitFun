//! Maps Codex failures (spawn errors, stderr tails, `TurnError`) to user-facing
//! turn errors. The raw text stays available as the provider message.
use openbitfun_events::agentic::{AiErrorDetail, ErrorCategory};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexFailure {
    pub message: String,
    pub category: ErrorCategory,
    pub raw: String,
    /// `codexErrorInfo` tag when Codex classified the error itself.
    pub code: Option<String>,
}

impl CodexFailure {
    pub(crate) fn display(&self) -> String {
        let raw = self.raw.trim();
        if raw.is_empty() || raw == self.message {
            self.message.clone()
        } else {
            format!("{}\n\n{raw}", self.message)
        }
    }

    pub(crate) fn detail(&self) -> AiErrorDetail {
        let hints = match self.category {
            ErrorCategory::Auth | ErrorCategory::Unknown | ErrorCategory::InvalidRequest => {
                vec!["copy_diagnostics".to_string()]
            }
            _ => Vec::new(),
        };
        AiErrorDetail {
            category: self.category.clone(),
            provider: Some("Codex".to_string()),
            provider_code: self.code.clone(),
            provider_message: Some(self.raw.clone()).filter(|raw| !raw.trim().is_empty()),
            request_id: None,
            http_status: None,
            retryable: None,
            action_hints: hints,
            recovery_constraint: None,
        }
    }
}

/// Tag of a `CodexErrorInfo` value: a string variant or the single key of an
/// object variant (`{"httpConnectionFailed": {...}}`).
pub(crate) fn error_info_tag(info: &Value) -> Option<String> {
    match info {
        Value::String(tag) => Some(tag.clone()),
        Value::Object(map) if map.len() == 1 => map.keys().next().cloned(),
        _ => None,
    }
}

/// Classify a failure from raw text and an optional `codexErrorInfo` tag.
pub(crate) fn classify(raw: &str, code: Option<&str>) -> CodexFailure {
    let lower = raw.to_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
    let failure = |message: &str, category| CodexFailure {
        message: message.to_string(),
        category,
        raw: raw.to_string(),
        code: code.map(str::to_string),
    };
    if let Some(line) = raw
        .lines()
        .find(|line| line.contains(super::super::MISSING_WORKSPACE_PREFIX))
    {
        return CodexFailure {
            message: line.trim().to_string(),
            ..failure("", ErrorCategory::InvalidRequest)
        };
    }
    if lower.contains("older than the minimum supported version") {
        let first = raw.lines().next().unwrap_or(raw).trim();
        return failure(first, ErrorCategory::InvalidRequest);
    }
    match code {
        Some("unauthorized") => return failure(LOGIN_MESSAGE, ErrorCategory::Auth),
        Some("usageLimitExceeded" | "sessionBudgetExceeded") => {
            return failure(
                "Codex usage limit reached. Wait for the limit to reset or check your plan, then retry.",
                ErrorCategory::ProviderQuota,
            )
        }
        Some("rateLimitExceeded") => {
            return failure(
                "Codex is rate limited. Wait a moment, then retry.",
                ErrorCategory::RateLimit,
            )
        }
        Some("contextWindowExceeded") => {
            return failure(
                "The conversation exceeds the model context window. Compact it or start a new session.",
                ErrorCategory::ContextOverflow,
            )
        }
        Some("serverOverloaded" | "internalServerError" | "flexUnavailable") => {
            return failure(
                "The Codex service is temporarily unavailable. Retry shortly.",
                ErrorCategory::ProviderUnavailable,
            )
        }
        Some(
            "httpConnectionFailed"
            | "responseStreamConnectionFailed"
            | "responseStreamDisconnected"
            | "responseTooManyFailedAttempts",
        ) => {
            return failure(
                "Codex lost its connection to the model service. Check the network, then retry.",
                ErrorCategory::Network,
            )
        }
        Some("cyberPolicy" | "misalignmentPolicyViolation") => {
            return failure(
                "The request was blocked by the Codex usage policy.",
                ErrorCategory::ContentPolicy,
            )
        }
        Some("badRequest") => {
            return failure("Codex rejected the request.", ErrorCategory::InvalidRequest)
        }
        _ => {}
    }
    if lower.contains("launch failed")
        && has(&[
            "not found",
            "cannot find",
            "no such file",
            "os error 2",
            "program not found",
        ])
    {
        return failure(
            "Codex CLI was not found. Install it with `npm install -g @openai/codex`, then retry.",
            ErrorCategory::InvalidRequest,
        );
    }
    if has(&[
        "not logged in",
        "codex login",
        "401 unauthorized",
        "invalid api key",
        "authentication",
        "refresh token",
    ]) {
        return failure(LOGIN_MESSAGE, ErrorCategory::Auth);
    }
    if has(&["usage limit", "quota"]) {
        return failure(
            "Codex usage limit reached. Wait for the limit to reset or check your plan, then retry.",
            ErrorCategory::ProviderQuota,
        );
    }
    if has(&["rate limit", "429"]) {
        return failure(
            "Codex is rate limited. Wait a moment, then retry.",
            ErrorCategory::RateLimit,
        );
    }
    if has(&["context window", "context_length_exceeded"]) {
        return failure(
            "The conversation exceeds the model context window. Compact it or start a new session.",
            ErrorCategory::ContextOverflow,
        );
    }
    if has(&["timed out", "timeout"]) {
        return failure(
            "Codex did not respond in time. Retry.",
            ErrorCategory::Timeout,
        );
    }
    let first = raw
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("Codex turn failed");
    failure(first, ErrorCategory::Unknown)
}

const LOGIN_MESSAGE: &str =
    "Codex is not signed in. Run `codex login` in a terminal on the agent host, then retry.";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_codex_error_info_and_text() {
        assert_eq!(
            classify("401", Some("unauthorized")).category,
            ErrorCategory::Auth
        );
        assert_eq!(
            classify(
                "x",
                error_info_tag(&json!({"httpConnectionFailed":{"httpStatusCode":502}})).as_deref()
            )
            .category,
            ErrorCategory::Network
        );
        assert_eq!(
            classify("You've hit your usage limit", None).category,
            ErrorCategory::ProviderQuota
        );
        let missing = classify("Codex native launch failed: program not found", None);
        assert!(missing.message.contains("npm install -g @openai/codex"));
        let auth = classify("Error: Not logged in. Run codex login", None);
        assert_eq!(auth.category, ErrorCategory::Auth);
        assert!(auth.display().contains("codex login"));
    }
}
