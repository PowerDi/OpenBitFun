//! Native agent drivers share lifecycle dispatch, not an ACP connection.
pub mod capabilities;
pub mod claude;
mod events;
pub(crate) mod preferences;
mod process;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use openbitfun_core::agentic::events::EventQueue;
use openbitfun_core::util::errors::OpenBitFunResult;

use super::session_persistence::AcpSessionPersistence;
use super::{
    AcpAvailableCommand, AcpSessionConfigValue, AcpSessionOptions,
    SubmitAcpPermissionResponseRequest,
};
pub use capabilities::{NativeAgentCapabilities, NativeSubagentCapabilities};
pub(crate) use preferences::NativeAgentPreferencesStore;

/// Prepared host resources. Provider identity and resume policy belong to the driver.
pub struct NativeAgentOpenContext {
    pub session_id: String,
    pub workspace_path: String,
    pub remote_connection_id: Option<String>,
    pub session_storage_path: PathBuf,
    pub queue: Arc<EventQueue>,
    pub(in crate::client) persistence: Arc<AcpSessionPersistence>,
}

#[async_trait]
pub trait NativeAgentDriver: Send + Sync {
    fn client_id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    /// Label used by the existing session validation errors.
    fn session_label(&self) -> &'static str;
    fn command(&self) -> &'static str;
    fn local_executable(&self) -> PathBuf;
    fn capabilities(&self) -> NativeAgentCapabilities;
    async fn open(
        &self,
        context: NativeAgentOpenContext,
    ) -> OpenBitFunResult<Arc<dyn NativeAgentSession>>;
}

#[async_trait]
pub trait NativeAgentSession: Send + Sync {
    fn client_id(&self) -> &'static str;
    fn options(&self) -> AcpSessionOptions;
    async fn warm_up(self: Arc<Self>) -> OpenBitFunResult<()>;
    async fn start_turn(
        self: Arc<Self>,
        turn_id: String,
        input: String,
        original: Option<String>,
    ) -> OpenBitFunResult<()>;
    async fn cancel(&self) -> OpenBitFunResult<()>;
    async fn set_model(&self, id: &str) -> OpenBitFunResult<AcpSessionOptions>;
    async fn set_config_option(
        &self,
        id: &str,
        value: AcpSessionConfigValue,
    ) -> OpenBitFunResult<AcpSessionOptions>;
    /// False leaves the permission available to the other native/ACP sessions.
    async fn respond_permission(
        &self,
        request: &SubmitAcpPermissionResponseRequest,
    ) -> OpenBitFunResult<bool>;
    fn commands(&self) -> Vec<AcpAvailableCommand>;
    fn matches_scope(&self, path: &str, remote: Option<&str>) -> bool;
    async fn shutdown(&self);
}

static CLAUDE: claude::ClaudeNativeDriver = claude::ClaudeNativeDriver;
static DRIVERS: [&'static dyn NativeAgentDriver; 1] = [&CLAUDE];

pub fn drivers() -> &'static [&'static dyn NativeAgentDriver] {
    &DRIVERS
}

pub fn native_driver(client_id: &str) -> Option<&'static dyn NativeAgentDriver> {
    drivers()
        .iter()
        .copied()
        .find(|driver| driver.client_id() == client_id)
}

pub fn is_native_client(client_id: &str) -> bool {
    native_driver(client_id).is_some()
}
