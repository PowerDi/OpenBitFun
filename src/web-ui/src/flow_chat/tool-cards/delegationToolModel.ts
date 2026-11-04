import { parseMcpToolName } from '@/infrastructure/mcp/toolName';
import type { FlowToolItem } from '../types/flow-chat';
import { readInteractionInput, readToolRecord, toolString } from './toolInteractionModel';

/**
 * Native agent delegation tools (`native-agents::delegation`). Every driver
 * reports them as `mcp__openbitfun__<tool>`; the result is the tool's JSON text.
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

/** Native child session ids are `acp_<client>_<uuid>`. */
export function delegationAgentFromSessionId(sessionId: string): string {
  return Object.keys(AGENT_NAMES).find(clientId => sessionId.startsWith(`acp_${clientId}_`)) ?? '';
}

/** Drivers wrap the JSON text as `{ result: text }`; accept the bare record too. */
export function delegationResult(item: FlowToolItem): Record<string, unknown> {
  const record = readToolRecord(item.toolResult?.result);
  if (toolString(record.sessionId) || toolString(record.state) || record.timedOut !== undefined) return record;
  return readToolRecord(record.result);
}

export interface DelegationCall {
  kind: DelegationToolKind;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
  sessionId: string;
  agent: string;
}

export function readDelegationCall(item: FlowToolItem, kind: DelegationToolKind): DelegationCall {
  const input = readInteractionInput(item);
  const result = delegationResult(item);
  const sessionId = toolString(result.sessionId) || toolString(input.session_id);
  const agent = toolString(result.agent) || toolString(input.agent) || delegationAgentFromSessionId(sessionId);
  return { kind, input, result, sessionId, agent };
}

/** Session title the child got: the explicit name, else the task's first line. */
export function delegationTaskTitle(input: Record<string, unknown>): string {
  const name = toolString(input.task_name);
  if (name) return name;
  const line = toolString(input.task).split('\n').find(value => value.trim()) ?? '';
  return [...line.trim()].slice(0, 40).join('');
}
