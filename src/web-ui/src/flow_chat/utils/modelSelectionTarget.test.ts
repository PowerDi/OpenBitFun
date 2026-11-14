import { describe, expect, it } from 'vitest';
import { isBtwParentBackedSession, isBtwSessionDraft } from './modelSelectionTarget';

describe('BTW session target classification', () => {
  it.each(['claude-native', 'codex-native', 'pi-native'])('uses the %s fork itself for model and permission updates, including before its first question', clientId => {
    const child = { sessionKind: 'btw', parentSessionId: 'parent', isTransient: true,
      dialogTurns: [], mode: `acp:${clientId}`, config: { agentType: `acp:${clientId}` } };
    expect(isBtwSessionDraft(child)).toBe(false);
    expect(isBtwParentBackedSession(child)).toBe(false);
  });

  it('keeps a transient BTW child parent-backed after a provisional turn exists', () => {
    const child = {
      sessionKind: 'btw',
      parentSessionId: 'parent-1',
      isTransient: true,
      isHistorical: false,
      dialogTurns: [{ id: 'provisional-turn' }],
    };

    expect(isBtwSessionDraft(child)).toBe(false);
    expect(isBtwParentBackedSession(child)).toBe(true);
  });

  it.each([
    ['a historical BTW child', { sessionKind: 'btw', parentSessionId: 'parent-1', isTransient: true, isHistorical: true }],
    ['a durable child', { sessionKind: 'btw', parentSessionId: 'parent-1', isTransient: false, isHistorical: false }],
    ['a BTW child without a parent', { sessionKind: 'btw', isTransient: true, isHistorical: false }],
  ])('does not mark %s as parent-backed', (_label, session) => {
    expect(isBtwParentBackedSession(session)).toBe(false);
  });
});
