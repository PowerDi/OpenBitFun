//! External-session port: lets the runtime scheduler drive native agent
//! sessions on any host (desktop, mobile and IM bots through Remote Connect,
//! CLI) instead of only through the desktop command.
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use openbitfun_core::runtime_ports::{
    AgentInputAttachment, ExternalSessionPort, ExternalSessionScope, ExternalTurnOutcomeSink,
    ExternalTurnStartRequest, PortError, PortErrorKind, PortResult,
};
use openbitfun_native_agents::notifications::global_notification_sink;
use openbitfun_native_agents::{NativeImageInput, NativeTurnInput};

use super::AcpClientService;

/// Native drivers behind the runtime scheduler. Generic ACP clients keep
/// their own prompt path for now and are reported unsupported here.
pub struct NativeExternalSessionPort {
    service: Arc<AcpClientService>,
}

impl NativeExternalSessionPort {
    pub fn new(service: Arc<AcpClientService>) -> Self {
        Self { service }
    }

    async fn session(
        &self,
        scope: &ExternalSessionScope,
    ) -> PortResult<Arc<dyn openbitfun_native_agents::NativeAgentSession>> {
        if scope.remote_connection_id.is_none() {
            if let Some(message) =
                openbitfun_native_agents::missing_local_workspace(&scope.workspace_path, true)
            {
                return Err(PortError::new(PortErrorKind::NotFound, message));
            }
        }
        self.service
            .prepare_native_session(
                &scope.client_id,
                &scope.session_id,
                &scope.workspace_path,
                scope.remote_connection_id.as_deref(),
                &PathBuf::from(&scope.session_storage_path),
                Some(global_notification_sink(
                    &scope.session_id,
                    &scope.client_id,
                )),
            )
            .await
            .map_err(port_error)
    }
}

fn port_error(error: openbitfun_core::util::errors::OpenBitFunError) -> PortError {
    use openbitfun_core::util::errors::OpenBitFunError;
    let kind = match &error {
        OpenBitFunError::Validation(_) => PortErrorKind::InvalidRequest,
        OpenBitFunError::NotFound(_) => PortErrorKind::NotFound,
        OpenBitFunError::Cancelled(_) => PortErrorKind::Cancelled,
        OpenBitFunError::Timeout(_) => PortErrorKind::Timeout,
        _ => PortErrorKind::Backend,
    };
    PortError::new(kind, error.to_string())
}

/// Inline images for the agent's stdin; the scheduler already resolved
/// file attachments into data URLs.
fn native_input(
    text: String,
    attachments: Vec<AgentInputAttachment>,
    images_supported: bool,
    label: &str,
) -> PortResult<NativeTurnInput> {
    if !attachments.is_empty() && !images_supported {
        return Err(PortError::new(
            PortErrorKind::InvalidRequest,
            format!("{label} does not accept image attachments"),
        ));
    }
    let images = attachments
        .into_iter()
        .map(|attachment| {
            let field = |key: &str| {
                attachment
                    .metadata
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            };
            let mime = field("mimeType").unwrap_or_else(|| "image/png".to_string());
            let data_url = field("dataUrl").ok_or_else(|| {
                PortError::new(
                    PortErrorKind::InvalidRequest,
                    "Image attachment has no data",
                )
            })?;
            NativeImageInput::from_data_url(&mime, &data_url)
                .map_err(|error| PortError::new(PortErrorKind::InvalidRequest, error.to_string()))
        })
        .collect::<PortResult<Vec<_>>>()?;
    Ok(NativeTurnInput {
        text,
        images,
        metadata: None,
    })
}

#[async_trait]
impl ExternalSessionPort for NativeExternalSessionPort {
    fn supports_client(&self, client_id: &str) -> bool {
        self.service.is_native_client(client_id)
    }

    async fn start_turn(
        &self,
        request: ExternalTurnStartRequest,
        on_outcome: ExternalTurnOutcomeSink,
    ) -> PortResult<()> {
        let driver = self
            .service
            .native_driver(&request.scope.client_id)
            .ok_or_else(|| {
                PortError::new(
                    PortErrorKind::NotAvailable,
                    format!("Unknown native agent: {}", request.scope.client_id),
                )
            })?;
        let input = native_input(
            request.message,
            request.attachments,
            driver.capabilities().input.images,
            driver.session_label(),
        )?;
        let input = NativeTurnInput {
            metadata: request.metadata,
            ..input
        };
        let session = self.session(&request.scope).await?;
        let session_id = request.scope.session_id.clone();
        let turn_id = request.turn_id.clone();
        openbitfun_native_agents::watch_external_turn(&session_id, &turn_id, on_outcome);
        match session
            .start_turn(turn_id.clone(), input, request.display_message)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                // A start that already projected the failed turn settled the
                // waiter; otherwise the scheduler reports this error itself.
                openbitfun_native_agents::unwatch_external_turn(&session_id, &turn_id);
                Err(port_error(error))
            }
        }
    }

    async fn steer_turn(
        &self,
        scope: ExternalSessionScope,
        turn_id: String,
        message: String,
        display_message: Option<String>,
        attachments: Vec<AgentInputAttachment>,
    ) -> PortResult<String> {
        let driver = self
            .service
            .native_driver(&scope.client_id)
            .ok_or_else(|| {
                PortError::new(
                    PortErrorKind::NotAvailable,
                    format!("Unknown native agent: {}", scope.client_id),
                )
            })?;
        if !driver.capabilities().input.steering {
            return Err(PortError::new(
                PortErrorKind::NotAvailable,
                format!("{} does not support steering", driver.session_label()),
            ));
        }
        let input = native_input(
            message,
            attachments,
            driver.capabilities().input.images,
            driver.session_label(),
        )?;
        let session = self.session(&scope).await?;
        session
            .steer(&turn_id, input, display_message)
            .await
            .map_err(port_error)
    }

    async fn cancel_turn(&self, session_id: &str) -> PortResult<()> {
        self.service
            .cancel_native_session(session_id)
            .await
            .map_err(port_error)
    }
}
