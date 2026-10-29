import { create } from 'zustand';
import {
  ACPClientAPI,
  type AcpRateLimitSnapshot,
  type AcpRetryStatus,
} from '@/infrastructure/api/service-api/ACPClientAPI';
import { i18nService } from '@/infrastructure/i18n';
import {
  clearLabeledRuntimeStatus,
  showLabeledRuntimeStatus,
} from './flow-chat-manager/RuntimeStatusModule';
import type { FlowChatContext } from './flow-chat-manager/types';
import { getRuntimeStatus } from '../store/runtimeStatusStore';

// Runtime presentation only: native agent retry and rate-limit status.
const RETRY_SOURCE = 'native-retry';
const RATE_LIMIT_SOURCE = 'native-rate-limit';
const HIGH_USAGE_PERCENT = 80;

/** Latest rate-limit snapshot each native session reported. */
export const useAcpRateLimitStore = create<{ bySessionId: Map<string, AcpRateLimitSnapshot> }>(() => ({
  bySessionId: new Map(),
}));

export function setAcpRateLimits(sessionId: string, snapshot: AcpRateLimitSnapshot): void {
  useAcpRateLimitStore.setState(({ bySessionId }) => ({
    bySessionId: new Map(bySessionId).set(sessionId, snapshot),
  }));
}

/** Seed from a session options snapshot only when no live update has arrived yet. */
export function hydrateAcpRateLimits(sessionId: string, snapshot: AcpRateLimitSnapshot | undefined): void {
  if (!snapshot || useAcpRateLimitStore.getState().bySessionId.has(sessionId)) return;
  setAcpRateLimits(sessionId, snapshot);
}

export function formatRetryStatusLabel(retry: AcpRetryStatus): string {
  const details: string[] = [];
  if (retry.attempt != null) {
    details.push(retry.maxAttempts != null
      ? i18nService.t('flow-chat:runtimeStatus.retryAttemptOf', {
        attempt: i18nService.formatNumber(retry.attempt),
        max: i18nService.formatNumber(retry.maxAttempts),
      })
      : i18nService.t('flow-chat:runtimeStatus.retryAttempt', {
        attempt: i18nService.formatNumber(retry.attempt),
      }));
  }
  if (retry.delayMs != null && retry.delayMs > 0) {
    details.push(i18nService.t('flow-chat:runtimeStatus.retryDelay', {
      seconds: i18nService.formatNumber(Math.ceil(retry.delayMs / 1000)),
    }));
  }
  return details.length
    ? i18nService.t('flow-chat:runtimeStatus.retryingWithDetails', {
      details: details.join(i18nService.t('flow-chat:runtimeStatus.detailSeparator')),
    })
    : i18nService.t('flow-chat:runtimeStatus.retrying');
}

/** Warning label when a limit blocks requests or a window is close to it. */
export function formatRateLimitLabel(snapshot: AcpRateLimitSnapshot): string | null {
  const windows = snapshot.windows ?? [];
  if (snapshot.limitReached || windows.some(window => window.status === 'rejected')) {
    return i18nService.t('flow-chat:runtimeStatus.rateLimitReached');
  }
  const used = Math.max(-1, ...windows.map(window => window.usedPercent ?? -1));
  if (used >= HIGH_USAGE_PERCENT) {
    return i18nService.t('flow-chat:runtimeStatus.rateLimitHighUsage', {
      percent: i18nService.formatNumber(Math.round(used)),
    });
  }
  if (windows.some(window => window.status === 'allowed_warning')) {
    return i18nService.t('flow-chat:runtimeStatus.rateLimitWarning');
  }
  return null;
}

function runningTurnId(context: FlowChatContext, sessionId: string): string | undefined {
  const turns = context.flowChatStore.getState().sessions.get(sessionId)?.dialogTurns;
  const turn = turns?.[turns.length - 1];
  return turn && !['completed', 'cancelled', 'error'].includes(turn.status) ? turn.id : undefined;
}

export function initializeAcpRuntimeStatusState(context: FlowChatContext): () => void {
  const unlistenRetry = ACPClientAPI.onRetryStatusChanged((event) => {
    if (!event?.sessionId || !event.turnId) return;
    if (!event.retry) {
      clearLabeledRuntimeStatus(event.sessionId, RETRY_SOURCE);
      return;
    }
    showLabeledRuntimeStatus(
      context,
      event.sessionId,
      event.turnId,
      formatRetryStatusLabel(event.retry),
      RETRY_SOURCE,
    );
  });
  const unlistenRateLimits = ACPClientAPI.onRateLimitsUpdated((event) => {
    if (!event?.sessionId || !event.rateLimits) return;
    setAcpRateLimits(event.sessionId, event.rateLimits);
    const label = formatRateLimitLabel(event.rateLimits);
    const turnId = runningTurnId(context, event.sessionId);
    if (!label || !turnId) return;
    // A visible retry status already explains the wait.
    if (getRuntimeStatus(event.sessionId)?.source === RETRY_SOURCE) return;
    showLabeledRuntimeStatus(context, event.sessionId, turnId, label, RATE_LIMIT_SOURCE);
  });
  return () => {
    unlistenRetry();
    unlistenRateLimits();
    useAcpRateLimitStore.setState({ bySessionId: new Map() });
  };
}
