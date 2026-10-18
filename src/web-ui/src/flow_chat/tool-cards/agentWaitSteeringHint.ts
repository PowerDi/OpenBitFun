import type { Session, ToolCardProps, ToolCardDisplayContext } from '../types/flow-chat';
import { externalAgentSupports, resolveExternalAgentCapabilities, type ExternalAgentCapabilities } from '../utils/externalAgentCapabilities';

export const RUNNING_STATUSES = new Set(['pending', 'preparing', 'running', 'streaming', 'receiving']);

export function shouldShowAgentWaitSteeringHint(
  status: ToolCardProps['toolItem']['status'],
  displayContext: ToolCardDisplayContext | undefined,
  session: Session | null | undefined,
  caps: ExternalAgentCapabilities | null = resolveExternalAgentCapabilities(session),
): boolean {
  if (!RUNNING_STATUSES.has(status) || displayContext === 'subagent-projection' || !session) {
    return false;
  }

  return session.sessionKind !== 'subagent'
    && !session.parentToolCallId
    && !session.isHistorical
    && externalAgentSupports(caps, 'input.steering');
}
