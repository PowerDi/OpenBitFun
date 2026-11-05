import { parseMcpToolName } from '@/infrastructure/mcp/toolName';
import type { FlowToolItem } from '../types/flow-chat';
import { readInteractionInput, readToolRecord, toolString } from './toolInteractionModel';

/**
 * Native agent delegation tools (`native-agents::delegation`). Every driver
 * reports them as `mcp__openbitfun__<tool>` with the result `{ result: <JSON text> }`.
 */
export const DELEGATION_TOOL_SERVER = 'openbitfun';

export type DelegationToolKind = 'start' | 'send' | 'read' | 'wait' | 'cancel' | 'watch';

const KINDS: Record<string, DelegationToolKind> = {
  delegate_start: 'start',
  session_send: 'send',
  session_read: 'read',
  session_wait: 'wait',
  session_cancel: 'cancel',
  session_watch: 'watch',
};

/** Tools with a delegation card; `agent_inspect` and `session_list` keep the MCP card. */
export function delegationToolKind(toolName: string): DelegationToolKind | undefined {
  const parsed = parseMcpToolName(toolName);
  return parsed?.serverId === DELEGATION_TOOL_SERVER ? KINDS[parsed.toolName] : undefined;
}

/** Product names of the native agents; brand names are not translated. */
const AGENT_NAMES: Record<string, string> = {
  'claude-native': 'Claude Code',
  'codex-native': 'Codex',
  'pi-native': 'Pi',
};

export function delegationAgentName(clientId: string): string {
  return AGENT_NAMES[clientId] ?? clientId;
}

/**
 * Child status words of the delegation tools and notices
 * (`delegation/snapshot.rs`), as session lineage lifecycles.
 */
export const DELEGATION_STATUS_LIFECYCLES = {
  running: 'running',
  completed: 'completed',
  failed: 'error',
  cancelled: 'cancelled',
  idle: 'idle',
} as const;

export type DelegationLifecycle =
  (typeof DELEGATION_STATUS_LIFECYCLES)[keyof typeof DELEGATION_STATUS_LIFECYCLES] | 'finishing' | 'waiting';

export const DELEGATION_LIFECYCLE_LABEL_KEYS: Record<DelegationLifecycle, string> = {
  running: 'flowChatHeader.agentTreeStatus.running',
  finishing: 'flowChatHeader.agentTreeStatus.running',
  waiting: 'flowChatHeader.agentTreeStatus.waiting',
  completed: 'flowChatHeader.agentTreeStatus.completed',
  cancelled: 'flowChatHeader.agentTreeStatus.cancelled',
  error: 'flowChatHeader.agentTreeStatus.error',
  idle: 'flowChatHeader.agentTreeStatus.idle',
};

/** i18n key (flow-chat) of a child status word, when it is one. */
export function delegationStatusLabelKey(status: unknown): string | undefined {
  const lifecycle = DELEGATION_STATUS_LIFECYCLES[toolString(status) as keyof typeof DELEGATION_STATUS_LIFECYCLES];
  return lifecycle ? DELEGATION_LIFECYCLE_LABEL_KEYS[lifecycle] : undefined;
}

export interface DelegationCall {
  kind: DelegationToolKind;
  input: Record<string, unknown>;
  /** The tool's JSON answer; empty until the call completes. */
  result: Record<string, unknown>;
  sessionId: string;
  agent: string;
}

export function readDelegationCall(item: FlowToolItem, kind: DelegationToolKind): DelegationCall {
  const input = readInteractionInput(item);
  const result = readToolRecord(readToolRecord(item.toolResult?.result).result);
  return {
    kind,
    input,
    result,
    // Every answer names the child; follow-ups know it from their input already.
    sessionId: toolString(result.sessionId) || toolString(input.session_id),
    agent: toolString(result.agent) || toolString(input.agent),
  };
}
