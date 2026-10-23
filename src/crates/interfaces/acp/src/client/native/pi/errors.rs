//! Maps Pi RPC failures (spawn errors, stderr tails, assistant `errorMessage`,
//! failed command responses) to user-facing turn errors.
use openbitfun_events::agentic::{AiErrorDetail, ErrorCategory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PiFailure {
    pub message: String,
    pub category: ErrorCategory,
    pub raw: String,
}

impl PiFailure {
    /// Guidance first, raw Pi text after it for diagnostics.
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
            provider: Some("Pi".to_string()),
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

pub(crate) fn classify(raw: &str) -> PiFailure {
    let lower = raw.to_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
    if let Some(line) = raw
        .lines()
        .find(|line| line.contains(super::super::MISSING_WORKSPACE_PREFIX))
    {
        return PiFailure {
            message: line.trim().to_string(),
            category: ErrorCategory::InvalidRequest,
            raw: raw.to_string(),
        };
    }
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
        ])) {
        (
            ErrorCategory::Unknown,
            format!(
                "Pi CLI was not found. Install it with `{}` and make sure `pi` is on PATH.",
                super::cli::NPM_INSTALL_COMMAND
            ),
        )
    } else if has(&[
        "no api key",
        "no model selected",
        "not logged in",
        "invalid api key",
        "authentication",
        "unauthorized",
        "401",
        "oauth",
    ]) {
        (
            ErrorCategory::Auth,
            "Pi has no usable model credentials. Run `pi` in a terminal and sign in with `/login` (or set the provider API key), then retry.".to_string(),
        )
    } else if has(&["insufficient_quota", "credit balance", "quota", "billing"]) {
        (
            ErrorCategory::ProviderQuota,
            "The model provider reports a quota or billing limit. Check your provider plan, then retry.".to_string(),
        )
    } else if has(&["rate limit", "rate_limit", "429", "too many requests"]) {
        (
            ErrorCategory::RateLimit,
            "The model provider rate limit was reached. Wait a moment and retry.".to_string(),
        )
    } else if has(&["overloaded", "529", "503", "502", "500", "unavailable"]) {
        (
            ErrorCategory::ProviderUnavailable,
            "The model provider is temporarily unavailable. Retry in a moment.".to_string(),
        )
    } else if has(&[
        "context window",
        "context length",
        "too many tokens",
        "prompt is too long",
    ]) {
        (
            ErrorCategory::ContextOverflow,
            "The conversation exceeds the model context window. Run /compact or start a new session.".to_string(),
        )
    } else {
        (
            ErrorCategory::Unknown,
            first_line(raw).unwrap_or("Pi turn failed").to_string(),
        )
    };
    PiFailure {
        message,
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
    fn classifies_pi_failures_with_login_guidance() {
        let auth = classify("No API key found for \"anthropic\".\n\nUse /login");
        assert_eq!(auth.category, ErrorCategory::Auth);
        assert!(auth.message.contains("/login"));
        assert!(auth.display().contains("No API key found"));
        assert_eq!(classify("No model selected.").category, ErrorCategory::Auth);
        assert_eq!(
            classify("429 Too Many Requests").category,
            ErrorCategory::RateLimit
        );
        assert_eq!(
            classify("529 overloaded").category,
            ErrorCategory::ProviderUnavailable
        );
        let missing = classify("Pi native launch failed: program not found");
        assert!(missing
            .message
            .contains("npm i -g @earendil-works/pi-coding-agent"));
        let other = classify("\n  something odd\nmore");
        assert_eq!(other.message, "something odd");
    }
}
