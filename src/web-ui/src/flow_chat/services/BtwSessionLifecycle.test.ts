import { beforeEach, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ sessions: new Map<string, any>(), cancel: vi.fn(), btwCancel: vi.fn(), delete: vi.fn(), discard: vi.fn(), fork: vi.fn(), metadata: vi.fn(), reloadTitle: vi.fn() }));
vi.mock('@/infrastructure/api', () => ({ agentAPI: { cancelSession: state.cancel, deleteSession: state.delete }, btwAPI: { cancel: state.btwCancel }, sessionAPI: { forkSession: state.fork } }));
vi.mock('../store/FlowChatStore', () => ({ flowChatStore: { getState: () => ({ sessions: state.sessions }), ensurePersistedSessionMetadata: state.metadata,
  setState: (update: (previous: any) => any) => { state.sessions = update({ sessions: state.sessions }).sessions; },
} }));
vi.mock('./FlowChatManager', () => ({ flowChatManager: { discardLocalSession: state.discard, reloadSessionTitle: state.reloadTitle } }));

import { canSaveBtwSessionAsFork, discardBtwSession, saveBtwSessionAsFork, trackBtwSessionSubmission } from './BtwSessionLifecycle';
import { sessionComposerStore } from '../store/sessionComposerStore';
import { clearAgentCanvasForPeerSwitch, switchAgentCanvasScope, useAgentCanvasStore } from '@/app/components/panels/content-canvas/stores';
import { pendingQueueManager } from './flow-chat-manager/PendingQueueModule';
import { drainPendingQueue } from './flow-chat-manager/MessageModule';
import type { FlowChatContext } from './flow-chat-manager/types';

describe('BTW session lifetime', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    clearAgentCanvasForPeerSwitch();
    state.sessions.clear();
    pendingQueueManager.clear('child');
    state.cancel.mockResolvedValue({ cancelled: true });
    state.delete.mockResolvedValue(undefined);
    state.discard.mockImplementation((id: string) => { state.sessions.delete(id); return [id]; });
    state.fork.mockResolvedValue({ sessionId: 'saved-fork' });
    state.reloadTitle.mockResolvedValue(undefined);
    state.metadata.mockImplementation(async (id: string) => {
      state.sessions.set(id, { sessionId: id, sessionKind: 'normal', isTransient: false });
      return true;
    });
    state.sessions.set('child', {
      sessionId: 'child', sessionKind: 'btw', workspaceId: 'worktree', projectWorkspaceId: 'project',
      config: { executionTarget: { kind: 'managedWorktree' } }, dialogTurns: [], isTransient: true, agentBackedTransient: true,
    });
  });

  it('promotes a native child in place without any host write, fork, or delete', async () => {
    Object.assign(state.sessions.get('child'), { mode: 'acp:codex-native', config: { agentType: 'acp:codex-native', executionTarget: { kind: 'managedWorktree' } },
      parentSessionId: 'parent', btwOrigin: { requestId: 'native-fork' } });
    sessionComposerStore.getState().setValue('child', 'follow up');
    await saveBtwSessionAsFork('child');
    expect(state.sessions.get('child')).toMatchObject({ sessionKind: 'normal', isTransient: false });
    expect(state.sessions.get('child').btwOrigin).toBeUndefined();
    expect(state.sessions.get('child').parentSessionId).toBeUndefined();
    expect(state.reloadTitle).toHaveBeenCalledWith('child');
    expect(state.fork).not.toHaveBeenCalled();
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.discard).not.toHaveBeenCalled();
    expect(sessionComposerStore.getState().getDraft('child').value).toBe('follow up');
  });

  it('still saves a native side chat when the title reload fails', async () => {
    Object.assign(state.sessions.get('child'), { mode: 'acp:codex-native', btwOrigin: { requestId: 'native-fork' } });
    state.reloadTitle.mockRejectedValueOnce(new Error('host offline'));
    await saveBtwSessionAsFork('child');
    expect(state.sessions.get('child').sessionKind).toBe('normal');
    expect(state.delete).not.toHaveBeenCalled();
  });

  it('deletes an unsent native excerpt fork even without a submitted question', async () => {
    Object.assign(state.sessions.get('child'), { mode: 'acp:codex-native' });
    await discardBtwSession('child');
    expect(state.cancel).toHaveBeenCalledWith('child');
    expect(state.delete).toHaveBeenCalledWith('child', 'project');
    expect(state.fork).not.toHaveBeenCalled();
  });

  it('offers saving only after a question has been submitted', () => {
    expect(canSaveBtwSessionAsFork('child')).toBe(false);
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    expect(canSaveBtwSessionAsFork('child')).toBe(true);
  });

  it('saves a durable fork before deletion and carries the unfinished composer to it', async () => {
    state.sessions.get('child').dialogTurns = [{ id: 'turn' }];
    sessionComposerStore.getState().setValue('child', 'next question');
    await saveBtwSessionAsFork('child');
    expect(state.fork).toHaveBeenCalledWith('child', 'turn', 'project');
    expect(state.metadata).toHaveBeenCalledWith('saved-fork', 'project');
    expect(state.cancel.mock.invocationCallOrder[0]).toBeLessThan(state.fork.mock.invocationCallOrder[0]);
    expect(state.metadata.mock.invocationCallOrder[0]).toBeLessThan(state.delete.mock.invocationCallOrder[0]);
    expect(state.sessions.get('saved-fork')).toMatchObject({ sessionKind: 'normal', isTransient: false });
    expect(state.sessions.has('child')).toBe(false);
    expect(sessionComposerStore.getState().getDraft('saved-fork').value).toBe('next question');
  });

  it('retains the source when saving fails', async () => {
    state.sessions.get('child').dialogTurns = [{ id: 'turn' }];
    state.fork.mockRejectedValueOnce(new Error('host offline'));
    await expect(saveBtwSessionAsFork('child')).rejects.toThrow('host offline');
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(true);
  });

  it('uses the settled host turn when its creation event has not reached the controller yet', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    state.cancel.mockResolvedValueOnce({ cancelled: true, dialogTurnId: 'host-turn' });
    await saveBtwSessionAsFork('child');
    expect(state.fork).toHaveBeenCalledWith('child', 'host-turn', 'project');
    expect(state.sessions.has('child')).toBe(false);
  });

  it('reuses the saved fork after cleanup fails rather than creating a duplicate on retry', async () => {
    state.sessions.get('child').dialogTurns = [{ id: 'turn' }];
    state.delete.mockRejectedValueOnce(new Error('cleanup failed'));
    await expect(saveBtwSessionAsFork('child')).rejects.toThrow('cleanup failed');
    expect(state.sessions.has('child')).toBe(true);
    expect(state.sessions.has('saved-fork')).toBe(true);
    await saveBtwSessionAsFork('child');
    expect(state.fork).toHaveBeenCalledTimes(1);
    expect(state.sessions.has('child')).toBe(false);
  });

  it('keeps the source until saved metadata is available and reuses the fork when publishing is retried', async () => {
    state.sessions.get('child').dialogTurns = [{ id: 'turn' }];
    state.metadata.mockResolvedValueOnce(false);
    await expect(saveBtwSessionAsFork('child')).rejects.toThrow('metadata is unavailable');
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(true);
    await saveBtwSessionAsFork('child');
    expect(state.fork).toHaveBeenCalledTimes(1);
    expect(state.sessions.has('child')).toBe(false);
  });

  it('waits for first-question admission before saving and prevents a new submission during save', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    let accept!: () => void;
    const submit = trackBtwSessionSubmission('child', () => new Promise<void>(resolve => {
      accept = () => { state.sessions.get('child').dialogTurns = [{ id: 'accepted-turn' }]; resolve(); };
    }));
    await Promise.resolve();
    const saving = saveBtwSessionAsFork('child');
    expect(state.cancel).not.toHaveBeenCalled();
    await expect(trackBtwSessionSubmission('child', async () => undefined)).rejects.toThrow('closing');
    accept();
    await submit;
    await saving;
    expect(state.fork).toHaveBeenCalledWith('child', 'accepted-turn', 'project');
  });

  it('discards an empty draft and its composer without creating a host session', async () => {
    sessionComposerStore.getState().setValue('child', 'unfinished question');
    await discardBtwSession('child');
    expect(state.cancel).not.toHaveBeenCalled();
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(false);
    expect(sessionComposerStore.getState().getDraft('child').value).toBe('');
  });

  it('stops the runtime before deleting it from the owning project', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    await discardBtwSession('child');
    expect(state.cancel).toHaveBeenCalledWith('child');
    expect(state.delete).toHaveBeenCalledWith('child', 'project');
    expect(state.cancel.mock.invocationCallOrder[0]).toBeLessThan(state.delete.mock.invocationCallOrder[0]);
    expect(state.delete.mock.invocationCallOrder[0]).toBeLessThan(state.discard.mock.invocationCallOrder[0]);
  });

  it('stops the turn through btw_cancel on a peer host that predates cancel_session', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    state.cancel.mockRejectedValueOnce(new Error(
      "command 'cancel_session' is not supported on CLI peer host: the CLI peer host has no handler for this command",
    ));
    state.btwCancel.mockResolvedValueOnce(undefined);
    await discardBtwSession('child');
    expect(state.btwCancel).toHaveBeenCalledWith({ requestId: 'request' });
    expect(state.btwCancel.mock.invocationCallOrder[0]).toBeLessThan(state.delete.mock.invocationCallOrder[0]);
    expect(state.delete).toHaveBeenCalledWith('child', 'project');
    expect(state.sessions.has('child')).toBe(false);
  });

  it('never uses ordinary BTW cancellation as proof that a native fork stopped', async () => {
    Object.assign(state.sessions.get('child'), { mode: 'acp:codex-native', btwOrigin: { requestId: 'native-fork' } });
    state.cancel.mockRejectedValueOnce(new Error(
      "command 'cancel_session' is not supported on CLI peer host: the CLI peer host has no handler for this command",
    ));
    await expect(discardBtwSession('child')).rejects.toThrow('not supported');
    expect(state.btwCancel).not.toHaveBeenCalled();
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(true);
  });

  it('keeps the tab when cancellation fails for any other reason', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    state.cancel.mockRejectedValueOnce(new Error('host offline'));
    await expect(discardBtwSession('child')).rejects.toThrow('host offline');
    expect(state.btwCancel).not.toHaveBeenCalled();
    expect(state.delete).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(true);
  });

  it('retains local contents when the host cannot confirm deletion', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    state.delete.mockRejectedValueOnce(new Error('host offline'));
    await expect(discardBtwSession('child')).rejects.toThrow('host offline');
    expect(state.discard).not.toHaveBeenCalled();
    expect(state.sessions.has('child')).toBe(true);
    await discardBtwSession('child');
    expect(state.sessions.has('child')).toBe(false);
  });

  it('waits for an initial fork and deduplicates closure while rejecting new submissions', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    let accept!: () => void;
    const fork = trackBtwSessionSubmission('child', () => new Promise<void>(resolve => { accept = resolve; }));
    await Promise.resolve();
    const closing = discardBtwSession('child');
    expect(discardBtwSession('child')).toBe(closing);
    expect(state.cancel).not.toHaveBeenCalled();
    pendingQueueManager.enqueue({ sessionId: 'child', content: 'queued follow-up' });
    // Closing must fence automatic drain before cancellation publishes IDLE.
    await drainPendingQueue({} as FlowChatContext, 'child');
    expect(pendingQueueManager.list('child')).toHaveLength(1);
    await expect(trackBtwSessionSubmission('child', async () => undefined)).rejects.toThrow('closing');
    accept();
    await fork;
    await closing;
    expect(state.delete).toHaveBeenCalledTimes(1);
    expect(pendingQueueManager.list('child')).toEqual([]);
  });

  it('clears drafts even when the host deletion event removed the projection before acknowledgement', async () => {
    state.sessions.get('child').btwOrigin = { requestId: 'request' };
    sessionComposerStore.getState().setValue('child', 'unsent');
    state.delete.mockImplementationOnce(async () => { state.sessions.delete('child'); });
    state.discard.mockReturnValueOnce([]);
    await discardBtwSession('child');
    expect(sessionComposerStore.getState().getDraft('child').value).toBe('');
  });

  it('deletes legacy persisted BTW on explicit closure and preserves other child kinds', async () => {
    Object.assign(state.sessions.get('child'), { isTransient: false, isHistorical: true });
    state.sessions.set('review', { ...state.sessions.get('child'), sessionId: 'review', sessionKind: 'review' });
    await discardBtwSession('review');
    expect(state.delete).not.toHaveBeenCalled();
    await discardBtwSession('child');
    expect(state.delete).toHaveBeenCalledWith('child', 'project');
    expect(state.sessions.has('review')).toBe(true);
  });

  it('removes the discarded tab from suspended canvases and old reopen history', async () => {
    switchAgentCanvasScope('parent');
    const canvas = useAgentCanvasStore.getState();
    canvas.addTab({ type: 'btw-session', title: 'legacy', data: { childSessionId: 'child' } });
    const tabId = useAgentCanvasStore.getState().primaryGroup.tabs[0].id;
    canvas.closeTab(tabId, 'primary');
    canvas.reopenClosedTab();
    switchAgentCanvasScope('other-parent');
    await discardBtwSession('child');
    switchAgentCanvasScope('parent');
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toEqual([]);
    expect(useAgentCanvasStore.getState().closedTabs).toEqual([]);
  });

  it('retains an open temporary tab when ordinary canvas snapshots exceed the cache budget', () => {
    switchAgentCanvasScope('parent');
    useAgentCanvasStore.getState().addTab({
      type: 'btw-session', title: 'temporary', data: { childSessionId: 'child' },
      metadata: { discardSessionOnClose: true },
    });
    for (let index = 0; index < 28; index++) {
      switchAgentCanvasScope(`parent-${index}`);
      useAgentCanvasStore.getState().addTab({ type: 'text-viewer', title: 'file' });
    }
    switchAgentCanvasScope('parent');
    expect(useAgentCanvasStore.getState().primaryGroup.tabs.map(tab => tab.title)).toEqual(['temporary']);
  });
});
