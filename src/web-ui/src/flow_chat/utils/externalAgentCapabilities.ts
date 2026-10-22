/**
 * External capability -> OpenBitFun affordance:
 * history.rollbackLastTurn: user-message rollback; history.fork: native session checkpoint fork;
 * history.importTranscripts: import the external agent's own transcript history.
 * input.images: image picker; input.steering: send-now / AgentWait hints;
 * input.queue: host queue; voiceHistory: shared voice transcript history;
 * sideChat: /btw side chat and excerpt Ask.
 * commands: external agent's own slash commands in the command picker (reserved);
 * hostCommands: OpenBitFun prompt commands/delegation, actions, /init, /review,
 * reload-context and boost entry;
 * compact: /compact;
 * contextUsage: /usage; threadGoal: /goal; skills: skill picker;
 * mcpSelection: MCP sources/prompts; worktree: worktree and workspace binding;
 * dispatch: dispatch target picker. Model/thinking/permission controls keep their
 * existing external option routing. Local Runtime recovery, event/history
 * persistence and ACP-provided commands/plan/context remain transport contracts.
 * backgroundTasks: external-agent background tasks in the header background list
 * (with Stop). plan is a reserved fact: there is no matching blanket ACP feature
 * restriction to replace at that surface.
 */
import {
  ACPClientAPI,
  type AcpClientInfo,
  type AcpSessionOptions,
  type NativeAgentCapabilities,
} from '@/infrastructure/api/service-api/ACPClientAPI';
import type { Session } from '../types/flow-chat';
import { acpAgentTypeFromSession, acpClientIdFromAgentType } from './acpSession';

export type ExternalAgentCapabilities = NativeAgentCapabilities;
type CapabilityKey = {
  [K in keyof ExternalAgentCapabilities]: ExternalAgentCapabilities[K] extends boolean ? K : never;
}[keyof ExternalAgentCapabilities]
  | 'history.rollbackLastTurn' | 'history.fork' | 'history.importTranscripts'
  | 'input.images' | 'input.steering' | 'input.queue'
  | 'subagents.observe' | 'subagents.readTranscript' | 'subagents.interact';

type ExternalSession = Pick<Session, 'config' | 'mode'> & Partial<Pick<Session, 'sessionId' | 'remoteConnectionId'>>;
const normalizedCapabilities = new WeakMap<NativeAgentCapabilities, ExternalAgentCapabilities>();
function normalize(raw?: NativeAgentCapabilities): ExternalAgentCapabilities {
  const cached = raw && normalizedCapabilities.get(raw);
  if (cached) return cached;
  const caps: ExternalAgentCapabilities = {
    selectModel: raw?.selectModel === true,
    permissionMode: raw?.permissionMode === true,
    thinking: raw?.thinking === true,
    commands: raw?.commands === true,
    hostCommands: raw?.hostCommands === true,
    voiceHistory: raw?.voiceHistory === true,
    sideChat: raw?.sideChat === true,
    subagents: {
      observe: raw?.subagents?.observe === true,
      readTranscript: raw?.subagents?.readTranscript === true,
      interact: raw?.subagents?.interact === true,
    },
    autonomousTurns: raw?.autonomousTurns === true,
    history: {
      rollbackLastTurn: raw?.history?.rollbackLastTurn === true,
      fork: raw?.history?.fork === true,
      importTranscripts: raw?.history?.importTranscripts === true,
    },
    input: {
      images: raw?.input?.images === true,
      steering: raw?.input?.steering === true,
      queue: raw?.input?.queue === true,
    },
    compact: raw?.compact === true,
    plan: raw?.plan === true,
    contextUsage: raw?.contextUsage === true,
    backgroundTasks: raw?.backgroundTasks === true,
    threadGoal: raw?.threadGoal === true,
    worktree: raw?.worktree === true,
    dispatch: raw?.dispatch === true,
    mcpSelection: raw?.mcpSelection === true,
    skills: raw?.skills === true,
  };
  if (raw) normalizedCapabilities.set(raw, caps);
  return caps;
}
const UNKNOWN_CAPABILITIES = normalize();

/** Session advertisements take precedence; old/unknown hosts advertise no support. */
export function resolveExternalAgentCapabilities(
  session: ExternalSession | string | null | undefined,
  clientInfos?: readonly AcpClientInfo[],
  sessionOptions?: AcpSessionOptions | null,
): ExternalAgentCapabilities | null {
  const agentType = typeof session === 'string' ? session : acpAgentTypeFromSession(session);
  const clientId = acpClientIdFromAgentType(agentType);
  if (!clientId) return null;
  const cachedOptions = typeof session === 'object' && session?.sessionId
    ? ACPClientAPI.getCachedSessionOptions?.({
        sessionId: session.sessionId,
        clientId,
        remoteConnectionId: session.remoteConnectionId ?? session.config.remoteConnectionId,
      })
    : undefined;
  const raw = (sessionOptions ?? cachedOptions)?.capabilities
    ?? (clientInfos ?? ACPClientAPI.getCachedCapabilityClients?.() ?? []).find(client => client.id === clientId)?.capabilities;
  return raw ? normalize(raw) : UNKNOWN_CAPABILITIES;
}

/** Native OpenBitFun (null) is unrestricted by this external-agent contract. */
export function externalAgentSupports(caps: ExternalAgentCapabilities | null, key: CapabilityKey): boolean {
  if (caps === null) return true;
  const [group, field] = key.split('.');
  const value = caps[group as keyof ExternalAgentCapabilities];
  return field ? (value as unknown as Record<string, boolean>)[field] === true : value === true;
}
