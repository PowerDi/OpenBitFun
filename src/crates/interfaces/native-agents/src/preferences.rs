//! Per-client defaults and tolerant reads of the original Claude preferences.
use std::collections::BTreeMap;
use std::path::PathBuf;

use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

pub(crate) const METADATA_KEY: &str = "nativeAgentPreferences";
const LEGACY_METADATA_KEY: &str = "claudeNativePreferences";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeAgentPreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
}

impl NativeAgentPreferences {
    pub(crate) fn from_metadata(custom: &Value, client_id: &str) -> Self {
        custom
            .get(METADATA_KEY)
            .or_else(|| {
                (client_id == super::claude::CLIENT_ID)
                    .then(|| custom.get(LEGACY_METADATA_KEY))
                    .flatten()
            })
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default()
    }
}

pub(crate) struct NativeAgentPreferencesStore {
    path: PathBuf,
    legacy_path: PathBuf,
    save: Mutex<()>,
}

impl NativeAgentPreferencesStore {
    pub(crate) fn new(config_dir: &std::path::Path) -> Self {
        Self {
            path: config_dir.join("native_agent_preferences.json"),
            legacy_path: config_dir.join("claude_native_preferences.json"),
            save: Mutex::new(()),
        }
    }

    async fn load(&self) -> BTreeMap<String, NativeAgentPreferences> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let legacy = tokio::fs::read(&self.legacy_path)
                    .await
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok());
                legacy
                    .map(|preferences| {
                        BTreeMap::from([(super::claude::CLIENT_ID.to_string(), preferences)])
                    })
                    .unwrap_or_default()
            }
            Err(_) => BTreeMap::new(),
        }
    }

    pub(crate) async fn get(&self, client_id: &str) -> NativeAgentPreferences {
        self.load().await.remove(client_id).unwrap_or_default()
    }

    pub(crate) async fn update(
        &self,
        client_id: &str,
        update: &impl Fn(&mut NativeAgentPreferences),
    ) -> OpenBitFunResult<()> {
        let _guard = self.save.lock().await;
        let mut all = self.load().await;
        update(all.entry(client_id.to_string()).or_default());
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| OpenBitFunError::service(error.to_string()))?;
        }
        let bytes = serde_json::to_vec_pretty(&all)
            .map_err(|error| OpenBitFunError::service(error.to_string()))?;
        tokio::fs::write(&self.path, bytes)
            .await
            .map_err(|error| OpenBitFunError::service(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn legacy_preferences_migrate_on_save_and_remain_client_scoped() {
        let dir = std::env::temp_dir().join(format!("native-preferences-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = NativeAgentPreferencesStore::new(&dir);
        let legacy = br#"{"model":"sonnet","permissionMode":"plan","thinking":"high"}"#;
        tokio::fs::write(&store.legacy_path, legacy).await.unwrap();
        let expected: NativeAgentPreferences = serde_json::from_slice(legacy).unwrap();
        assert_eq!(store.get(super::super::claude::CLIENT_ID).await, expected);
        assert!(!store.path.exists());
        assert_eq!(
            store.get("other-native").await,
            NativeAgentPreferences::default()
        );
        let metadata = json!({"claudeNativePreferences": expected, "futureField": true});
        assert_eq!(
            NativeAgentPreferences::from_metadata(&metadata, "claude-native"),
            expected
        );
        let mut round_trip: Value =
            serde_json::from_slice(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        round_trip[METADATA_KEY] = json!({"model": "opus"});
        assert_eq!(
            NativeAgentPreferences::from_metadata(&round_trip, "claude-native")
                .model
                .as_deref(),
            Some("opus")
        );
        assert_eq!(round_trip["futureField"], true);
        store
            .update("other-native", &|p| p.model = Some("other-model".into()))
            .await
            .unwrap();
        assert_eq!(store.get("claude-native").await, expected);
        assert_eq!(
            store.get("other-native").await.model.as_deref(),
            Some("other-model")
        );
        assert_eq!(tokio::fs::read(&store.legacy_path).await.unwrap(), legacy);
        // Once the new file exists, the legacy defaults must never reappear.
        tokio::fs::write(&store.path, b"{}").await.unwrap();
        assert_eq!(
            store.get("claude-native").await,
            NativeAgentPreferences::default()
        );
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
