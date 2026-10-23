import { agentAPI } from '@/infrastructure/api/service-api/AgentAPI';
import { ACPClientAPI } from '@/infrastructure/api/service-api/ACPClientAPI';
import { getActiveSurfaceScope } from '@/infrastructure/peer-device/deviceSurface';
import { createLogger } from '@/shared/utils/logger';
import type { SteeringImage } from '../types/flow-chat';
import { FlowChatStore } from '../store/FlowChatStore';
import { acpAgentTypeFromSession, acpClientIdFromAgentType } from '../utils/acpSession';
import { insertSteeringItemIfAbsent } from './flow-chat-manager/EventHandlerModule';

const log = createLogger('SteeringSubmission');

/** Legacy send-now transport, shared by queued messages and inline follow-ups. */
export async function submitSteeringMessage(
  request: Parameters<typeof agentAPI.steerDialogTurn>[0],
  images?: SteeringImage[],
): Promise<void> {
  const surfaceScope = getActiveSurfaceScope();
  surfaceScope.assertCurrent('submit steering message');
  // External agents run outside the OpenBitFun scheduler; their driver owns
  // mid-turn delivery (callers gate on `input.steering`).
  const session = FlowChatStore.getInstance().getState().sessions.get(request.sessionId);
  const acpClientId = acpClientIdFromAgentType(acpAgentTypeFromSession(session));
  const response = acpClientId
    ? await ACPClientAPI.steerDialogTurn({
        sessionId: request.sessionId,
        clientId: acpClientId,
        dialogTurnId: request.dialogTurnId,
        content: request.content,
        displayContent: request.displayContent,
        imageContexts: request.imageContexts,
      })
    : await agentAPI.steerDialogTurn(request);
  surfaceScope.assertCurrent('accept steering message');
  if (!response.success || !response.steeringId) {
    throw new Error('Host did not acknowledge the steering message');
  }
  try {
    insertSteeringItemIfAbsent({
      sessionId: request.sessionId,
      turnId: request.dialogTurnId,
      steeringId: response.steeringId,
      content: request.displayContent ?? request.content,
      images,
      status: 'pending',
    });
  } catch (error) {
    // The host already accepted this message. A projection failure must not
    // invite a duplicate submission; the authoritative event can render it.
    log.warn('Optimistic steering render failed', { error });
  }
}
