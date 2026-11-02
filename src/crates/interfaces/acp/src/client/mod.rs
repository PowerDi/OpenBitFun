mod builtin_clients;
mod config;
mod dsh_profile;
mod external_session_port;
mod manager;
mod prompt;
mod remote_capability_store;
mod remote_session;
mod requirements;
mod stream;
mod tool;
mod tool_card_bridge;
mod transport;

/// Native agent drivers live in `openbitfun-native-agents`; this path keeps
/// existing host imports (`openbitfun_acp::client::native::*`) working.
pub use openbitfun_native_agents as native;

pub use config::{
    AcpClientConfig, AcpClientConfigFile, AcpClientInfo, AcpClientPermissionMode,
    AcpClientRequirementProbe, AcpClientStatus, AcpClientSubagentConfig, AcpRequirementProbeItem,
    NativeCliInstallMethod, NativeCliStatus, RemoteAcpClientRequirementSnapshot,
};
pub use external_session_port::NativeExternalSessionPort;
pub use manager::{AcpClientService, SetAcpSessionConfigOptionRequest, SetAcpSessionModelRequest};
pub use openbitfun_native_agents::session_options::{
    AcpAvailableCommand, AcpPlanEntry, AcpSessionConfigKind, AcpSessionConfigOption,
    AcpSessionConfigSelectOption, AcpSessionConfigValue, AcpSessionContextUsage,
    AcpSessionModelOption, AcpSessionOptions,
};
pub use openbitfun_native_agents::CreateAcpFlowSessionRecordResponse;
pub use stream::AcpClientStreamEvent;
