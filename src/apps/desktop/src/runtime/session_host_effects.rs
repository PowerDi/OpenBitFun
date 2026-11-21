use std::sync::Arc;

use async_trait::async_trait;
use openbitfun_agent_runtime::sdk::{SessionEventJournal, SessionEventProjectionSnapshot};

use super::session_application::{DesktopSessionExecutionSnapshot, DesktopSessionHostEffects};

pub(super) struct AcpJournalAdapter(pub Arc<SessionEventJournal>);

impl openbitfun_acp::client::AcpProjectionJournal for AcpJournalAdapter {
    fn record(
        &self,
        event: &openbitfun_events::AgenticEvent,
    ) -> Option<openbitfun_acp::client::AcpEventCursor> {
        self.0
            .record(event)
            .map(|cursor| openbitfun_acp::client::AcpEventCursor {
                stream_id: cursor.stream_id,
                cursor: cursor.cursor,
            })
    }

    fn snapshot(&self, session: &str) -> openbitfun_acp::client::AcpEventProjectionSnapshot {
        let snapshot = self.0.snapshot(session);
        openbitfun_acp::client::AcpEventProjectionSnapshot {
            session_id: snapshot.session_id,
            stream_id: snapshot.stream_id,
            cursor: snapshot.cursor,
            active_turn_id: snapshot.active_turn_id,
            events: snapshot.events,
        }
    }
}

pub(super) struct ProductionDesktopSessionHostEffects {
    acp_client_service: Option<Arc<openbitfun_acp::AcpClientService>>,
}

impl ProductionDesktopSessionHostEffects {
    pub(super) fn new(acp_client_service: Option<Arc<openbitfun_acp::AcpClientService>>) -> Self {
        Self { acp_client_service }
    }
}

#[async_trait]
impl DesktopSessionHostEffects for ProductionDesktopSessionHostEffects {
    fn supports_acp_projection(&self) -> bool {
        self.acp_client_service.is_some()
    }

    fn uses_native_execution(&self, agent_type: &str) -> bool {
        agent_type
            .strip_prefix("acp:")
            .is_some_and(|client_id| openbitfun_acp::client::native::is_native_client(client_id))
    }

    fn acp_execution_snapshot(&self, session_id: &str) -> Option<DesktopSessionExecutionSnapshot> {
        let snapshot = self
            .acp_client_service
            .as_ref()?
            .execution_snapshot(session_id)?;
        let projection = snapshot.projection;
        Some(DesktopSessionExecutionSnapshot {
            running: snapshot.running,
            projection: SessionEventProjectionSnapshot {
                session_id: projection.session_id,
                stream_id: projection.stream_id,
                cursor: projection.cursor,
                active_turn_id: projection.active_turn_id,
                events: projection.events,
            },
        })
    }

    async fn release_session(&self, session_id: &str) {
        if let Some(service) = self.acp_client_service.as_ref() {
            service.release_openbitfun_session(session_id).await;
        }
    }
}
