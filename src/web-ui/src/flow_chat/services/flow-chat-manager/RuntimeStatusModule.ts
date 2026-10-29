/**
 * Session-scoped runtime wait status.
 *
 * Runtime status is transient UI state. It never enters model round items, so
 * showing or hiding it cannot change the virtualized conversation structure.
 */

import {
  clearRuntimeStatusState,
  getRuntimeStatus,
  showRuntimeStatus,
} from '../../store/runtimeStatusStore';
import type { DialogTurn, FlowChatContext } from './types';

export const MODEL_RESPONSE_STATUS_DELAY_MS = 1000;

interface RuntimeStatusOptions {
  delayMs?: number;
}

interface ClearRuntimeStatusOptions {
  roundId?: string;
}

function runtimeStatusKey(sessionId: string, turnId: string, roundId: string): string {
  return `${sessionId}:${turnId}:${roundId}`;
}

function getDialogTurn(context: FlowChatContext, sessionId: string, turnId: string): DialogTurn | undefined {
  return context.flowChatStore
    .getState()
    .sessions
    .get(sessionId)
    ?.dialogTurns
    .find(turn => turn.id === turnId);
}

function hasVisibleOutputForRound(turn: DialogTurn, roundId: string): boolean {
  return turn.modelRounds.find(candidate => candidate.id === roundId)?.items.length !== 0;
}

export function scheduleModelResponseStatus(
  context: FlowChatContext,
  sessionId: string,
  turnId: string,
  roundId: string,
  options: RuntimeStatusOptions = {},
): void {
  const key = runtimeStatusKey(sessionId, turnId, roundId);
  const sessionTimerPrefix = `${sessionId}:`;
  for (const [pendingKey, pendingTimer] of context.runtimeStatusTimers.entries()) {
    if (!pendingKey.startsWith(sessionTimerPrefix)) {
      continue;
    }
    clearTimeout(pendingTimer);
    context.runtimeStatusTimers.delete(pendingKey);
  }

  // A new round owns the session slot immediately, but remains visually hidden
  // until the delay elapses.
  clearRuntimeStatusState({ sessionId });

  const delayMs = options.delayMs ?? MODEL_RESPONSE_STATUS_DELAY_MS;
  const timer = setTimeout(() => {
    context.runtimeStatusTimers.delete(key);

    const turn = getDialogTurn(context, sessionId, turnId);
    if (!turn || hasVisibleOutputForRound(turn, roundId)) {
      return;
    }

    showRuntimeStatus({
      sessionId,
      turnId,
      roundId,
    });
  }, delayMs);

  context.runtimeStatusTimers.set(key, timer);
}

/**
 * Show a labeled status on the latest round of a running turn now, replacing a
 * pending unlabeled model-response status of the session.
 */
export function showLabeledRuntimeStatus(
  context: FlowChatContext,
  sessionId: string,
  turnId: string,
  label: string,
  source: string,
): boolean {
  const turn = getDialogTurn(context, sessionId, turnId);
  if (!turn || ['completed', 'cancelled', 'error'].includes(turn.status)) {
    return false;
  }
  const sessionTimerPrefix = `${sessionId}:`;
  for (const [pendingKey, pendingTimer] of context.runtimeStatusTimers.entries()) {
    if (pendingKey.startsWith(sessionTimerPrefix)) {
      clearTimeout(pendingTimer);
      context.runtimeStatusTimers.delete(pendingKey);
    }
  }
  // Before the first round opens, use a turn-scoped round id; the next round
  // start replaces it.
  const roundId = turn.modelRounds[turn.modelRounds.length - 1]?.id ?? `${turnId}:status`;
  showRuntimeStatus({ sessionId, turnId, roundId, label, source });
  return true;
}

/** Clear the session status only while it is still the label `source` showed. */
export function clearLabeledRuntimeStatus(sessionId: string, source: string): void {
  const current = getRuntimeStatus(sessionId);
  if (current?.source === source) {
    clearRuntimeStatusState({ sessionId, turnId: current.turnId, roundId: current.roundId });
  }
}

export function clearRuntimeStatus(
  context: FlowChatContext,
  sessionId: string,
  turnId: string,
  options: ClearRuntimeStatusOptions = {},
): void {
  const timerPrefix = `${sessionId}:${turnId}:`;
  const targetTimerKey = options.roundId
    ? runtimeStatusKey(sessionId, turnId, options.roundId)
    : null;
  for (const [key, timer] of context.runtimeStatusTimers.entries()) {
    if (targetTimerKey ? key !== targetTimerKey : !key.startsWith(timerPrefix)) {
      continue;
    }
    clearTimeout(timer);
    context.runtimeStatusTimers.delete(key);
  }

  clearRuntimeStatusState({
    sessionId,
    turnId,
    roundId: options.roundId,
  });
}
