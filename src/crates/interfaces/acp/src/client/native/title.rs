//! First-turn session titles for native agent sessions.
//!
//! Native sessions never pass through the Runtime coordinator, so they reuse
//! its detached title resolution: the configured title task model (an
//! `Inherit` selection resolves to `primary`), else the truncated message.
//! The model call runs on this host and sees only the first user message.
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use openbitfun_core::agentic::session::{
    is_ai_session_title_generation_enabled, ResolvedSessionTitle, SessionManager,
};
use openbitfun_core::service::session::SessionMetadata;
use openbitfun_core::util::errors::OpenBitFunResult;

use crate::client::session_persistence::AcpSessionPersistence;

/// Same bound the coordinator uses for first-turn titles.
pub(crate) const TITLE_MAX_LENGTH: usize = 20;

pub(crate) type TitleFuture = Pin<Box<dyn Future<Output = ResolvedSessionTitle> + Send>>;
/// Resolves a title for a first user message; replaceable in tests.
pub(crate) type TitleResolver = Arc<dyn Fn(String) -> TitleFuture + Send + Sync>;

pub(crate) fn default_resolver() -> TitleResolver {
    Arc::new(|message: String| {
        Box::pin(async move {
            let allow_ai = is_ai_session_title_generation_enabled().await;
            SessionManager::resolve_detached_session_title(
                &message,
                Some(TITLE_MAX_LENGTH),
                allow_ai,
            )
            .await
        })
    })
}

/// The message to title a first turn by, or `None` when it should not be
/// titled: blank (image-only) input and slash commands such as `/compact`.
pub(crate) fn title_source(input: &str, original: Option<&str>) -> Option<String> {
    let text = original.unwrap_or(input).trim();
    (!text.is_empty() && !text.starts_with('/')).then(|| text.to_string())
}

/// Apply the generated title unless the user renamed the session. Only the
/// `titleUserEdited` flag marks a rename: the client persists its placeholder
/// title (the truncated first message) while the title model runs, so a
/// changed name alone is not a rename.
pub(crate) fn apply_generated_title(metadata: &mut SessionMetadata, title: &str) -> bool {
    if user_edited(metadata) {
        return false;
    }
    metadata.session_name = title.to_string();
    if let Some(custom) = metadata
        .custom_metadata
        .as_mut()
        .and_then(serde_json::Value::as_object_mut)
    {
        // The generated text replaces a localized default title descriptor.
        custom.insert("titleSource".into(), "text".into());
        custom.remove("titleKey");
        custom.remove("titleParams");
    }
    true
}

pub(crate) fn user_edited(metadata: &SessionMetadata) -> bool {
    metadata
        .custom_metadata
        .as_ref()
        .and_then(|custom| custom.get("titleUserEdited"))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

impl AcpSessionPersistence {
    /// Persist a generated title unless the user renamed the session.
    pub(crate) async fn set_generated_title(
        &self,
        storage: &Path,
        session_id: &str,
        title: &str,
    ) -> OpenBitFunResult<bool> {
        let mut applied = false;
        self.update_metadata(storage, session_id, |metadata| {
            applied = apply_generated_title(metadata, title);
            Ok(())
        })
        .await?;
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metadata(name: &str, custom: serde_json::Value) -> SessionMetadata {
        let mut metadata = SessionMetadata::new(
            "s1".into(),
            name.into(),
            "acp:claude-native".into(),
            "primary".into(),
        );
        metadata.custom_metadata = Some(custom);
        metadata
    }

    #[test]
    fn generated_title_replaces_any_title_except_a_user_rename() {
        let mut fresh = metadata(
            "New Session",
            json!({"titleSource":"i18n", "titleKey":"flow-chat:session.new", "keep":1}),
        );
        assert!(apply_generated_title(&mut fresh, "Fix login"));
        assert_eq!(fresh.session_name, "Fix login");
        let custom = fresh.custom_metadata.unwrap();
        assert_eq!(custom["titleSource"], "text");
        assert!(custom.get("titleKey").is_none());
        assert_eq!(custom["keep"], 1);

        // Regression: the client persists its placeholder title while the
        // title model runs; the generated title still replaces it.
        let mut placeholder = metadata("fix the login...", json!({}));
        assert!(apply_generated_title(&mut placeholder, "Fix login"));
        assert_eq!(placeholder.session_name, "Fix login");

        // A user rename is never replaced.
        let mut edited = metadata("Mine", json!({"titleUserEdited": true}));
        assert!(!apply_generated_title(&mut edited, "Fix login"));
        assert_eq!(edited.session_name, "Mine");
    }

    #[test]
    fn slash_commands_and_blank_input_are_not_titled() {
        assert_eq!(title_source("/compact", None), None);
        assert_eq!(title_source("  ", None), None);
        assert_eq!(
            title_source("expanded", Some(" fix the build ")),
            Some("fix the build".into())
        );
    }

    #[tokio::test]
    async fn disabled_ai_resolves_the_truncated_fallback() {
        let resolved = SessionManager::resolve_detached_session_title(
            "Refactor the session persistence layer",
            Some(TITLE_MAX_LENGTH),
            false,
        )
        .await;
        assert_eq!(resolved.method.as_str(), "fallback");
        assert!(resolved.title.chars().count() <= TITLE_MAX_LENGTH + 3);
        assert!(resolved.title.starts_with("Refactor the"));
    }
}
