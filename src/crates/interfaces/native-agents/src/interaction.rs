//! Host answers to native approvals and questions.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitAcpPermissionResponseRequest {
    pub permission_id: String,
    pub approve: bool,
    #[serde(default)]
    pub option_id: Option<String>,
    /// Native question answers (question index or text -> answer value).
    #[serde(default)]
    pub answers: Option<serde_json::Value>,
    /// Optional free-text feedback sent with a rejection.
    #[serde(default)]
    pub feedback: Option<String>,
}
