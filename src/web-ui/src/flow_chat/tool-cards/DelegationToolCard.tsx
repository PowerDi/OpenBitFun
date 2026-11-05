import React, { useCallback, useSyncExternalStore } from 'react';
import { useI18n } from '@/infrastructure/i18n/hooks/useI18n';
import { getActiveSurfaceScope } from '@/infrastructure/peer-device/deviceSurface';
import { notificationService } from '@/shared/notification-system';
import { ExternalAgentIcon } from '@/shared/components/ExternalAgentIcon';
import {
  AgentControlToolCard as AgentControlToolCardView,
  SessionMessageToolCard as SessionMessageToolCardView,
  type ToolCardInteraction,
} from '@openbitfun/ui/flow-chat';
import { flowChatStore } from '../store/FlowChatStore';
import { openBtwSessionInAuxPane } from '../services/btwSessionPane';
import { useSessionNavStatus } from '../hooks/useSessionNavStatus';
import type { SessionNavStatus } from '../utils/sessionNavStatus';
import { sessionLineageLifecycleForSession, type SessionLineageLifecycle } from '../utils/sessionLineage';
import type { FlowToolItem, ToolCardProps } from '../types/flow-chat';
import { getToolCardStatus } from './toolCardStatus';
import { interactionOutcome, toolString } from './toolInteractionModel';
import { useCurrentToolSessionParticipant } from './useToolSessionParticipant';
import {
  DELEGATION_LIFECYCLE_LABEL_KEYS,
  delegationAgentName,
  delegationStatusLabelKey,
  delegationToolKind,
  readDelegationCall,
  type DelegationCall,
} from './delegationToolModel';

type Translate = (key: string, options?: Record<string, unknown>) => string;

function subscribeToFlowChatStore(listener: () => void): () => void {
  return flowChatStore.subscribe(() => listener());
}

/** Title and hydrated lifecycle of the child, when the store has it. */
function useChildSnapshot(childId: string): { title: string; lifecycle?: SessionLineageLifecycle } {
  const read = useCallback(() => {
    const session = childId ? flowChatStore.getState().sessions.get(childId) : undefined;
    if (!session) return '["",""]';
    return JSON.stringify([
      session.title?.trim() ?? '',
      session.dialogTurns.length ? sessionLineageLifecycleForSession(session) : '',
    ]);
  }, [childId]);
  const snapshot = useSyncExternalStore(subscribeToFlowChatStore, read, read);
  const [title, lifecycle] = JSON.parse(snapshot) as [string, SessionLineageLifecycle | ''];
  return { title, lifecycle: lifecycle || undefined };
}

/**
 * States from the session list's status source; it also covers unopened
 * sessions. A finished, unread result is not a state: it reads as completed
 * either way, and the dot disappears once the user opens it.
 */
function liveLifecycleFromNav(status: SessionNavStatus): SessionLineageLifecycle | undefined {
  switch (status.kind) {
    case 'running': return 'running';
    case 'approval': case 'input': return 'waiting';
    case 'error': return 'error';
    case 'stopped': return 'cancelled';
    default: return undefined;
  }
}

/**
 * Open the delegated child next to its delegator. The child is an ordinary
 * persisted session: load its own record first so the side pane never invents
 * a transient side-chat session for it.
 */
function openDelegatedSession(childId: string, parentSessionId: string, title: string, t: Translate): void {
  const scope = getActiveSurfaceScope();
  const fail = () => { if (scope.isCurrent()) notificationService.error(t('toolCards.interaction.sessionUnavailable')); };
  const sessions = flowChatStore.getState().sessions;
  const parent = sessions.get(parentSessionId);
  if (!parent || parent.config?.dispatchTarget || parent.config?.dispatchJobId) {
    fail();
    return;
  }
  const open = () => {
    if (!scope.isCurrent()) return;
    const child = flowChatStore.getState().sessions.get(childId);
    if (!child) {
      fail();
      return;
    }
    openBtwSessionInAuxPane({
      childSessionId: childId,
      parentSessionId,
      workspaceId: child.workspaceId ?? child.config?.workspaceId,
      workspacePath: parent.workspacePath,
      sessionTitle: title,
      remoteConnectionId: parent.remoteConnectionId,
      remoteSshHost: parent.remoteSshHost,
    });
  };
  if (sessions.has(childId)) {
    open();
    return;
  }
  // Children are stored with their delegator's workspace.
  const workspaceId = parent.workspaceId ?? parent.config?.workspaceId;
  if (!workspaceId) {
    fail();
    return;
  }
  void flowChatStore.ensurePersistedSessionMetadata(childId, workspaceId)
    .then(found => (found ? open() : fail()))
    .catch(fail);
}

/**
 * The child as the card shows it: live states from the session list, else
 * the outcome of its own transcript once hydrated.
 */
function useDelegationTarget(call: DelegationCall, parentSessionId: string | undefined) {
  const { t } = useI18n('flow-chat');
  const child = useChildSnapshot(call.sessionId);
  const navStatus = useSessionNavStatus(call.sessionId);
  // Before the call answers, the requested name stands in for the title.
  const title = child.title || toolString(call.result.title) || toolString(call.input.task_name)
    || t('toolCards.delegation.untitled');
  const lifecycle = liveLifecycleFromNav(navStatus) ?? child.lifecycle;
  const onOpen = call.sessionId && parentSessionId
    ? (event: React.MouseEvent<HTMLButtonElement>) => {
      event.stopPropagation();
      openDelegatedSession(call.sessionId, parentSessionId, title, t);
    }
    : undefined;
  return { title, lifecycle, queued: navStatus.kind === 'queued', onOpen };
}

const AgentIcon: React.FC<{ agent: string; size: number }> = ({ agent, size }) => (
  agent ? <ExternalAgentIcon clientId={agent} size={size} /> : null
);

/** `delegate_start`: the same identity card as a subagent launch. */
const DelegationStartCard: React.FC<ToolCardProps & { call: DelegationCall }> = ({ toolItem, sessionId, call }) => {
  const { t, currentLanguage } = useI18n('flow-chat');
  const status = getToolCardStatus(toolItem);
  const target = useDelegationTarget(call, sessionId);
  const agentName = call.agent ? delegationAgentName(call.agent) : t('toolCards.interaction.unknownAgent');
  const failed = status === 'error';
  const lifecycle = failed ? undefined : target.lifecycle;
  const displayStatus: FlowToolItem['status'] = failed ? 'error'
    : lifecycle === 'running' || lifecycle === 'finishing' ? 'running'
      : lifecycle === 'waiting' ? 'waiting'
        : lifecycle === 'error' ? 'error'
          : lifecycle === 'cancelled' ? 'cancelled'
            : lifecycle === 'completed' ? 'completed' : status;
  const statusLabel = failed ? t('toolCards.default.failed')
    : target.queued ? t('toolCards.default.queued')
      : lifecycle && lifecycle !== 'completed' ? t(DELEGATION_LIFECYCLE_LABEL_KEYS[lifecycle]) : undefined;
  const statusTone = lifecycle === 'running' || lifecycle === 'finishing' ? 'success'
    : lifecycle === 'waiting' ? 'warning'
      : failed || lifecycle === 'error' || lifecycle === 'cancelled' ? 'danger' : 'neutral';
  const task = toolString(call.input.task);

  return (
    <div data-openbitfun-adapter="delegation-tool-card" data-tool-card-id={toolItem.id ?? toolItem.toolCall?.id ?? ''}>
      <AgentControlToolCardView
        lang={currentLanguage}
        status={displayStatus}
        agentName={target.title}
        summary={task || target.title}
        avatar={<AgentIcon agent={call.agent} size={24} />}
        // The agent's CLI chooses the model; the child session shows it when opened.
        preview={{
          avatar: <AgentIcon agent={call.agent} size={24} />,
          agentType: agentName,
          labels: {
            agentType: t('toolCards.delegation.previewAgent'),
            model: t('toolCards.delegation.previewModel'),
            description: t('toolCards.delegation.previewTask'),
          },
        }}
        statusLabel={statusLabel}
        statusTone={statusTone}
        isFailed={failed}
        onOpenAgent={target.onOpen}
        openAgentLabel={t('toolCards.delegation.open')}
      />
    </div>
  );
};

function relationOperation(kind: DelegationCall['kind']): ToolCardInteraction['operation'] {
  switch (kind) {
    case 'send': return 'send';
    case 'cancel': return 'interrupt';
    default: return 'receive';
  }
}

function childStatusLabel(value: unknown, t: Translate): string {
  const key = delegationStatusLabelKey(value);
  return key ? t(key) : toolString(value);
}

function relationSummary(call: DelegationCall, status: FlowToolItem['status'], t: Translate): string {
  const result = call.result;
  switch (call.kind) {
    case 'send':
      return interactionOutcome(status, t, result.status === 'queued'
        ? t('toolCards.delegation.queued') : t('toolCards.delegation.sent'), t('toolCards.delegation.sending'));
    case 'read':
      return interactionOutcome(status, t,
        t('toolCards.delegation.readDone', { status: childStatusLabel(result.status, t) }), t('toolCards.delegation.reading'));
    case 'wait':
      return interactionOutcome(status, t, result.timedOut === true
        ? t('toolCards.delegation.waitTimedOut')
        : t('toolCards.delegation.waitStopped', { status: childStatusLabel(result.status, t) }), t('toolCards.delegation.waiting'));
    case 'cancel':
      return interactionOutcome(status, t, result.cancelled === true
        ? t('toolCards.delegation.cancelled') : t('toolCards.delegation.nothingToCancel'), t('toolCards.delegation.cancelling'));
    default:
      return interactionOutcome(status, t, result.state === 'alreadyTerminal'
        ? t('toolCards.delegation.alreadyStopped', { status: childStatusLabel(result.status, t) })
        : t('toolCards.delegation.watching'), t('toolCards.delegation.watchRequesting'));
  }
}

/** Follow-up calls on a delegated session: one relation row each, no new card. */
const DelegationRelationRow: React.FC<ToolCardProps & { call: DelegationCall }> = ({ toolItem, sessionId, call }) => {
  const { t } = useI18n('flow-chat');
  const status = getToolCardStatus(toolItem);
  const source = useCurrentToolSessionParticipant(sessionId, t);
  const target = useDelegationTarget(call, sessionId);
  const summary = relationSummary(call, status, t);
  const resultText = toolString(call.result.result && typeof call.result.result === 'object'
    ? (call.result.result as Record<string, unknown>).text : undefined);
  const message = call.kind === 'send' ? toolString(call.input.message) : resultText;
  const error = toolItem.toolResult?.error || toolString(call.result.error);

  return (
    <div data-openbitfun-adapter="delegation-tool-card" data-tool-card-id={toolItem.id ?? toolItem.toolCall?.id ?? ''}>
      <SessionMessageToolCardView
        status={status}
        interaction={{
          operation: relationOperation(call.kind),
          source,
          target: {
            id: call.sessionId || undefined,
            label: target.title,
            kind: 'session',
            avatar: <AgentIcon agent={call.agent} size={16} />,
            openLabel: t('toolCards.delegation.open'),
            onOpen: target.onOpen,
          },
        }}
        summary={summary}
        resultLabel={t('toolCards.interaction.inspectResult')}
        message={message || undefined}
        messageLabel={message ? summary : undefined}
        error={error || (status === 'error' ? t('toolCards.default.failed') : undefined)}
      />
    </div>
  );
};

export const DelegationToolCard: React.FC<ToolCardProps> = props => {
  const kind = delegationToolKind(props.toolItem.toolName);
  if (!kind) return null;
  const call = readDelegationCall(props.toolItem, kind);
  return kind === 'start'
    ? <DelegationStartCard {...props} call={call} />
    : <DelegationRelationRow {...props} call={call} />;
};
