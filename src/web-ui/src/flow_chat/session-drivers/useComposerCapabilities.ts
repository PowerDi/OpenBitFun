/**
 * Composer-facing session capabilities.
 *
 * One derivation for every affordance the chat input used to gate with
 * scattered "is this a dispatch session" branches. UI code reads semantic
 * flags; only this module knows which driver implies which affordance.
 *
 * Uses the driver resolver (three signals) rather than the bound dispatch
 * target alone, so a projection whose config has not bound yet is already
 * treated as remote instead of briefly offering local-only affordances.
 */

import { useMemo } from 'react';
import { useRuntimeStatusStore } from '../store/runtimeStatusStore';
import type { Session } from '../types/flow-chat';
import { externalAgentSupports, resolveExternalAgentCapabilities, type ExternalAgentCapabilities } from '../utils/externalAgentCapabilities';
import { useExternalAgentCapabilities } from '../hooks/useExternalAgentCapabilities';
import { resolveSessionDriverId, type SessionDriverId } from './resolve';
import { deriveSessionConversationCapability, type SessionConversationCapability } from './conversationCapability';

export const DISPATCH_TRANSFER_ROUND_PREFIX = 'dispatch-transfer:';

/** Renderer-side slash commands the composer can offer for a session. */
export type ComposerSlashOp = 'btw' | 'compact' | 'goal' | 'usage' | 'init' | 'review';

const LOCAL_SLASH_OPS: ReadonlySet<ComposerSlashOp> = new Set([
  'btw', 'compact', 'goal', 'usage', 'init', 'review',
]);
const LOCAL_SLASH_OPS_WITHOUT_THREAD_GOAL: ReadonlySet<ComposerSlashOp> = new Set([
  'btw', 'compact', 'usage', 'init', 'review',
]);
/** Ops a detached target serves via its durable turn mailbox / query verb. */
const DISPATCH_SLASH_OPS: ReadonlySet<ComposerSlashOp> = new Set(['compact', 'usage']);
const EXTERNAL_SLASH_CAPABILITIES = {
  btw: 'sideChat',
  compact: 'compact',
  goal: 'threadGoal',
  usage: 'contextUsage',
  init: 'hostCommands',
  review: 'hostCommands',
} as const;

export interface ComposerCapabilities {
  conversation: SessionConversationCapability;
  driverId: SessionDriverId;
  /**
   * Raw flag for residual plumbing (reload-context support, picker guards).
   * Prefer the semantic flags below for new gates.
   */
  dispatchTransport: boolean;
  /** Renderer-side slash-command pickers (full local command surface). */
  localSlashCommands: boolean;
  /** Individual slash commands this session can execute when typed. */
  ops: ReadonlySet<ComposerSlashOp>;
  usageReport: boolean;
  threadGoal: boolean;
  /** The submission is being transferred to the target; composer is inert. */
  transferInFlight: boolean;
  /** Approval-policy and model choices are no longer editable. */
  submissionOptionsLocked: boolean;
  /** Approval control edits this session's policy, not the global config. */
  sessionScopedApproval: boolean;
  /** Worktree chip is forced-locked: execution uses a managed baseline. */
  worktreeBaselineLocked: boolean;
  /** Model choice comes from the target's probed model list. */
  targetModelSelection: boolean;
}

export interface ComposerCapabilityInput {
  sessionId: string | null | undefined;
  session: Session | undefined;
  /** Host mask: miniapp / quick-input hosts hide dispatch affordances. */
  hostMasksDispatch: boolean;
  /** Relationship input: /btw child sessions have no thread goal of their own. */
  displayAsChild: boolean;
}

export function sessionSupportsThreadGoal(
  session: Pick<Session, 'config' | 'mode'> | undefined,
  caps: ExternalAgentCapabilities | null = resolveExternalAgentCapabilities(session),
): boolean {
  return externalAgentSupports(caps, 'threadGoal');
}

export function useComposerCapabilities(input: ComposerCapabilityInput): ComposerCapabilities {
  const { sessionId, session, hostMasksDispatch, displayAsChild } = input;
  const driverId = resolveSessionDriverId(sessionId ?? '', session);
  const dispatchTransport = !hostMasksDispatch && driverId === 'dispatch';
  const externalCaps = useExternalAgentCapabilities(sessionId) ?? resolveExternalAgentCapabilities(session);
  const threadGoalSupported = sessionSupportsThreadGoal(session, externalCaps);
  const ops = useMemo(() => {
    if (dispatchTransport) return DISPATCH_SLASH_OPS;
    const localOps = threadGoalSupported ? LOCAL_SLASH_OPS : LOCAL_SLASH_OPS_WITHOUT_THREAD_GOAL;
    if (externalCaps === null) return localOps;
    return new Set([...localOps].filter(op =>
      externalAgentSupports(externalCaps, EXTERNAL_SLASH_CAPABILITIES[op])));
  }, [dispatchTransport, externalCaps, threadGoalSupported]);

  const transferInFlight = useRuntimeStatusStore(state => {
    const status = sessionId ? state.bySessionId.get(sessionId) : undefined;
    return dispatchTransport
      && status?.roundId.startsWith(DISPATCH_TRANSFER_ROUND_PREFIX) === true;
  });

  // Options are editable whenever no turn is in flight: before the first
  // submit and between turns. Protocol v4 carries them per follow-up turn.
  const dispatchJobState = session?.config.dispatchJobState;
  const submissionOptionsLocked =
    dispatchTransport
    && (
      transferInFlight
      || dispatchJobState === 'queued'
      || dispatchJobState === 'running'
    );

  return {
    conversation: deriveSessionConversationCapability(session, driverId),
    driverId,
    dispatchTransport,
    localSlashCommands: !dispatchTransport && externalAgentSupports(externalCaps, 'hostCommands'),
    ops,
    usageReport: true,
    threadGoal: threadGoalSupported && !displayAsChild && !dispatchTransport,
    transferInFlight,
    submissionOptionsLocked,
    sessionScopedApproval: dispatchTransport,
    worktreeBaselineLocked: dispatchTransport,
    targetModelSelection: dispatchTransport,
  };
}
