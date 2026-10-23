import { describe, expect, it } from 'vitest';

import { acpAgentTypeFromSession, acpClientIdFromAgentType, isAcpFlowSession } from './acpSession';

describe('acpSession utilities', () => {
  it('resolves ACP agent type from session config first', () => {
    expect(
      acpAgentTypeFromSession({
        config: { agentType: 'acp:opencode' },
        mode: 'Standard',
      } as any)
    ).toBe('acp:opencode');
  });

  it('falls back to ACP mode for older or partial session state', () => {
    expect(
      acpAgentTypeFromSession({
        config: { agentType: 'Standard' },
        mode: 'acp:opencode',
      } as any)
    ).toBe('acp:opencode');
  });

  it('maps sessions of retired ACP presets to their native drivers', () => {
    expect(acpClientIdFromAgentType('acp:claude-code')).toBe('claude-native');
    expect(acpClientIdFromAgentType('acp:codex')).toBe('codex-native');
    expect(acpAgentTypeFromSession({ config: { agentType: 'acp:codex' }, mode: 'Standard' } as any))
      .toBe('acp:codex-native');
    expect(acpClientIdFromAgentType('acp:omp')).toBe('omp');
  });

  it('does not classify normal sessions as ACP sessions', () => {
    const session = {
      config: { agentType: 'Standard' },
      mode: 'Plan',
    } as any;

    expect(acpAgentTypeFromSession(session)).toBeNull();
    expect(isAcpFlowSession(session)).toBe(false);
  });
});
