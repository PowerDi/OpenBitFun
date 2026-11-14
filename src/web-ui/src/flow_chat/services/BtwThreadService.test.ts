import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockCreateSession = vi.fn();
const mockAskStream = vi.fn();
const mockAddExternalSession = vi.fn();
const mockUpdateSessionRelationship = vi.fn();
const mockUpdateSessionBtwOrigin = vi.fn();
const mockAddBtwThreadMarker = vi.fn();
const mockUpdateSessionModelName = vi.fn();
const mockUpdateSessionReasoningPreset = vi.fn();
const mockEnsureBackendSession = vi.fn();
const mockUpdateSessionModelPool = vi.fn();
const mockNativeFork = vi.fn();
const mockNativeSend = vi.fn();

vi.mock('@/infrastructure/api/service-api/ACPClientAPI', () => ({ ACPClientAPI: {
  forkSession: (...args: unknown[]) => mockNativeFork(...args),
  getCachedSessionOptions: () => undefined,
  getCachedCapabilityClients: () => [{ id: 'codex-native', capabilities: {
    sideChat: true, history: { fork: true }, input: { queue: true },
  } }],
} }));

const sessions = new Map<string, any>();

vi.mock('@/infrastructure/api', () => ({
  agentAPI: {
    createSession: (...args: any[]) => mockCreateSession(...args),
  },
  btwAPI: {
    askStream: (...args: any[]) => mockAskStream(...args),
  },
}));

vi.mock('../store/FlowChatStore', () => ({
  flowChatStore: {
    getState: () => ({ sessions }),
    updateSessionModelPool: (...args: any[]) => mockUpdateSessionModelPool(...args),
    addExternalSession: (...args: any[]) => mockAddExternalSession(...args),
    updateSessionRelationship: (...args: any[]) => mockUpdateSessionRelationship(...args),
    updateSessionBtwOrigin: (...args: any[]) => mockUpdateSessionBtwOrigin(...args),
    addBtwThreadMarker: (...args: any[]) => mockAddBtwThreadMarker(...args),
    updateSessionModelName: (...args: any[]) => mockUpdateSessionModelName(...args),
    updateSessionReasoningPreset: (...args: any[]) => mockUpdateSessionReasoningPreset(...args),
  },
}));

vi.mock('../state-machine', () => ({
  stateMachineManager: {
    get: () => ({
      getContext: () => ({
        currentDialogTurnId: 'turn-parent-1',
      }),
    }),
  },
}));

vi.mock('./FlowChatManager', () => ({
  flowChatManager: {
    sendMessage: (...args: unknown[]) => mockNativeSend(...args),
    discardLocalSession: vi.fn(),
    ensureBackendSession: (...args: any[]) => mockEnsureBackendSession(...args),
  },
}));

vi.mock('@/shared/notification-system', () => ({
  notificationService: {
    warning: vi.fn(),
  },
}));

import {
  createBtwChildSession,
  createBtwSessionPlaceholder,
  createBtwSession,
  sendMessageToBtwSession,
  startBtwThread,
} from './BtwThreadService';

describe('BtwThreadService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessions.clear();
    sessions.set('parent-1', {
      sessionId: 'parent-1',
      mode: 'Standard',
      workspaceId: 'workspace-1',
      workspacePath: '/workspace',
      remoteConnectionId: 'remote-1',
      remoteSshHost: 'host-1',
      config: {
        modelName: 'primary',
        reasoningPreset: 'high',
      },
      dialogTurns: [
        {
          id: 'turn-parent-1',
        },
      ],
    });
    mockAskStream.mockResolvedValue({ ok: true });
    mockCreateSession.mockResolvedValue({
      sessionId: 'child-1',
    });
  });

  function nativeParent() {
    const parent = sessions.get('parent-1');
    parent.mode = 'acp:codex-native';
    parent.config.agentType = parent.mode;
    parent.dialogTurns = [
      { id: 'completed', status: 'completed', modelRounds: [] },
      { id: 'running', status: 'streaming', modelRounds: [] },
    ];
    return parent;
  }

  it('forks native context at the last completed turn and retains remote identity', async () => {
    const parent = nativeParent();
    mockNativeFork.mockResolvedValueOnce({ sessionId: 'native-child' });
    await createBtwSession({ parentSessionId: 'parent-1', childSessionName: 'Why?', parentDialogTurnId: 'older-excerpt' });
    expect(mockNativeFork).toHaveBeenCalledWith(expect.objectContaining({ sourceSessionId: 'parent-1',
      clientId: 'codex-native', turnId: 'completed', workspaceId: 'workspace-1',
      sideChat: { requestId: expect.any(String), sessionName: 'Why?' } }));
    expect(mockAddExternalSession).toHaveBeenCalledWith('native-child', 'Why?', 'acp:codex-native', '/workspace',
      expect.objectContaining({ sessionKind: 'btw', isTransient: true, agentBackedTransient: true }), 'remote-1', 'host-1');
    expect(parent.dialogTurns.at(-1).id).toBe('running');
    expect(mockCreateSession).not.toHaveBeenCalled();
    expect(mockAskStream).not.toHaveBeenCalled();
  });

  it('sends native questions through the ordinary agent path without overriding model or permissions', async () => {
    nativeParent();
    sessions.set('native-child', { ...sessions.get('parent-1'), sessionId: 'native-child', sessionKind: 'btw',
      parentSessionId: 'parent-1', dialogTurns: [], isTransient: true, agentBackedTransient: true });
    await sendMessageToBtwSession({ parentSessionId: 'parent-1', childSessionId: 'native-child', question: 'Why?',
      modelId: 'must-not-override', initialModelSelection: { modelId: 'other' }, userMessageMetadata: { excerpt: 'quoted' } });
    expect(mockNativeSend).toHaveBeenCalledWith('Why?', 'native-child', 'Why?', undefined, undefined,
      { userMessageMetadata: { excerpt: 'quoted' } });
    expect(mockAskStream).not.toHaveBeenCalled();
    expect(mockUpdateSessionModelName).not.toHaveBeenCalled();
  });

  it('refuses missing completed context, steered boundaries, failed forks and parent identities without local fallback', async () => {
    const parent = nativeParent();
    parent.dialogTurns = [{ id: 'running', status: 'streaming', modelRounds: [] }];
    await expect(createBtwSession({ parentSessionId: 'parent-1', childSessionName: 'Why?' })).rejects.toThrow('completed turn');
    parent.dialogTurns = [{ id: 'done', status: 'completed', modelRounds: [{ items: [{ type: 'user-steering' }] }] }];
    await expect(createBtwSession({ parentSessionId: 'parent-1', childSessionName: 'Why?' })).rejects.toThrow('steered input');
    parent.dialogTurns[0].modelRounds = [];
    mockNativeFork.mockRejectedValueOnce(new Error('checkpoint refused'));
    await expect(startBtwThread({ parentSessionId: 'parent-1', workspacePath: '/workspace', question: 'Why?' })).rejects.toThrow('checkpoint refused');
    mockNativeFork.mockResolvedValueOnce({ sessionId: 'parent-1' });
    await expect(createBtwSession({ parentSessionId: 'parent-1', childSessionName: 'Why?' })).rejects.toThrow('parent session');
    expect(mockAddExternalSession).not.toHaveBeenCalled();
    expect(mockAskStream).not.toHaveBeenCalled();
    expect(mockEnsureBackendSession).not.toHaveBeenCalled();
  });

  it('passes structured relationship metadata to backend-created review sessions', async () => {
    const deepReviewRunManifest = {
      reviewers: [],
    };

    await createBtwChildSession({
      parentSessionId: 'parent-1',
      workspacePath: '/workspace',
      childSessionName: 'Deep review',
      sessionKind: 'deep_review',
      agentType: 'DeepReview',
      requestId: 'review-request-1',
      deepReviewRunManifest,
    });

    expect(mockCreateSession).toHaveBeenCalledWith(
      expect.objectContaining({
        sessionName: 'Deep review',
        agentType: 'DeepReview',
        sessionId: 'review_child_review-request-1',
        workspaceId: 'workspace-1',
        workspacePath: '/workspace',
        remoteConnectionId: 'remote-1',
        remoteSshHost: 'host-1',
        relationship: {
          kind: 'deep_review',
          parentSessionId: 'parent-1',
          parentRequestId: 'review-request-1',
          parentDialogTurnId: 'turn-parent-1',
          parentTurnIndex: 1,
        },
        deepReviewRunManifest,
      }),
    );
  });

  it('persists the narrow target envelope for standard review follow-up turns', async () => {
    const reviewTargetEvidence = {
      version: 1 as const,
      source: 'workspace' as const,
      fingerprint: '0123456789abcdef',
      completeness: 'partial' as const,
      workspaceBinding: 'matching_dirty' as const,
      files: [],
      limitations: ['mutable_workspace_snapshot'],
    };

    await createBtwChildSession({
      parentSessionId: 'parent-1',
      workspacePath: '/workspace',
      childSessionName: 'Review',
      sessionKind: 'review',
      agentType: 'CodeReview',
      requestId: 'review-target-1',
      reviewTargetEvidence,
    });

    expect(mockCreateSession).toHaveBeenCalledWith(expect.objectContaining({
      reviewTargetEvidence,
    }));
  });

  it('creates a temporary agent-backed /btw placeholder with its parent anchor and inherited pool', () => {
    const binding = { poolId: 'pool-a', modelId: 'member-a', generation: 1 };
    Object.assign(sessions.get('parent-1').config, { modelPoolId: 'pool-a', modelPoolBinding: binding });
    const result = createBtwSessionPlaceholder({
      parentSessionId: 'parent-1',
      workspacePath: '/workspace',
      childSessionName: 'Side question',
    });

    expect(result.childSessionId).toMatch(/^btw_session_/);
    expect(result.parentDialogTurnId).toBe('turn-parent-1');
    expect(result.parentTurnIndex).toBe(1);
    expect(mockAddExternalSession).toHaveBeenCalledWith(
      result.childSessionId,
      'Side question',
      'Standard',
      '/workspace',
      expect.objectContaining({
        parentSessionId: 'parent-1',
        sessionKind: 'btw',
        isTransient: true,
        agentBackedTransient: true,
        btwOrigin: {
          parentSessionId: 'parent-1',
          parentDialogTurnId: 'turn-parent-1',
          parentTurnIndex: 1,
        },
      }),
      'remote-1',
      'host-1',
    );
    expect(mockUpdateSessionRelationship).toHaveBeenCalledWith(result.childSessionId, {
      parentSessionId: 'parent-1',
      sessionKind: 'btw',
    });
    expect(mockUpdateSessionModelPool).toHaveBeenCalledWith(result.childSessionId, 'pool-a', binding);
  });

  it('retains pending selection on failure and preserves the accepted binding on success', async () => {
    const binding = { poolId: 'pool-b', modelId: 'member-b', generation: 2 };
    sessions.set('btw-child', { sessionId: 'btw-child', sessionKind: 'btw',
      config: { modelPoolId: 'pool-b', modelPoolSelectionPending: true, modelPoolBinding: binding } });
    const params = { parentSessionId: 'parent-1', childSessionId: 'btw-child', question: 'Why?',
      initialModelTarget: { selection: { kind: 'pool' as const, pool_id: 'pool-b' } } };
    mockAskStream.mockRejectedValueOnce(new Error('offline'));
    await expect(sendMessageToBtwSession(params)).rejects.toThrow('offline');
    expect(mockUpdateSessionModelPool).not.toHaveBeenCalled();
    await sendMessageToBtwSession(params);
    expect(mockAskStream).toHaveBeenLastCalledWith(expect.objectContaining({ initialModelTarget: params.initialModelTarget }));
    expect(mockAskStream.mock.calls.at(-1)?.[0]).not.toHaveProperty('modelId');
    expect(mockUpdateSessionModelPool).toHaveBeenCalledWith('btw-child', 'pool-b', binding);
  });

  it('stores an absolute parent Turn index for a partial restored tail', () => {
    sessions.set('parent-1', {
      ...sessions.get('parent-1'),
      isPartial: true,
      loadedTurnCount: 3,
      totalTurnCount: 100,
      dialogTurns: [
        { id: 'turn-98' },
        { id: 'turn-99' },
        { id: 'turn-parent-1' },
      ],
    });

    const result = createBtwSessionPlaceholder({
      parentSessionId: 'parent-1',
      workspacePath: '/workspace',
      childSessionName: 'Side question',
    });

    expect(result.parentDialogTurnId).toBe('turn-parent-1');
    expect(result.parentTurnIndex).toBe(100);
  });

  it('anchors a selected historical Turn and inherits the parent model', () => {
    sessions.get('parent-1').dialogTurns.unshift({ id: 'selected-turn' });
    const result = createBtwSessionPlaceholder({ parentSessionId: 'parent-1',
      childSessionName: 'Side question', parentDialogTurnId: 'selected-turn' });
    expect(result.parentDialogTurnId).toBe('selected-turn');
    expect(result.parentTurnIndex).toBe(1);
    expect(mockUpdateSessionModelName).toHaveBeenCalledWith(result.childSessionId, 'primary');
    expect(mockUpdateSessionReasoningPreset).toHaveBeenCalledWith(result.childSessionId, 'high');
    expect(mockUpdateSessionModelName).toHaveBeenCalledWith(result.childSessionId, 'primary');
  });

  it('retains quote metadata and stable request identity on a failed initial submission retry', async () => {
    sessions.set('btw-child', {
      sessionId: 'btw-child', title: 'Side question', sessionKind: 'btw',
      btwOrigin: { parentDialogTurnId: 'selected-turn' }, config: {},
    });
    const userMessageMetadata = { composerPresentation: { version: 1, segments: [{ kind: 'text', text: 'quote' }] },
      sessionReferences: [{ sessionId: 'reference' }] };
    const params = { parentSessionId: 'parent-1', childSessionId: 'btw-child', question: 'Quote and question',
      requestId: 'stable-request', userMessageMetadata,
      initialModelSelection: { modelId: 'primary', reasoningPreset: 'high' } };
    mockAskStream.mockRejectedValueOnce(new Error('network unavailable'));
    await expect(sendMessageToBtwSession(params)).rejects.toThrow('network unavailable');
    await sendMessageToBtwSession(params);
    expect(mockAskStream).toHaveBeenLastCalledWith(expect.objectContaining({
      requestId: 'stable-request', question: 'Quote and question', userMessageMetadata, parentDialogTurnId: 'selected-turn',
      initialModelSelection: { modelId: 'primary', reasoningPreset: 'high' },
    }));
    expect(sessions.has('btw-child')).toBe(true);
  });

  it('rejects an unavailable side draft before binding its first request or calling the transport', async () => {
    sessions.set('archived-child', {
      sessionId: 'archived-child', sessionKind: 'btw', persistedStatus: 'archived', config: {},
    });
    await expect(sendMessageToBtwSession({ parentSessionId: 'parent-1', childSessionId: 'archived-child', question: 'Follow up' }))
      .rejects.toMatchObject({ reason: 'archived' });
    expect(mockUpdateSessionBtwOrigin).not.toHaveBeenCalled();
    expect(mockAskStream).not.toHaveBeenCalled();
  });

  it('passes image contexts and parent turn metadata through to the desktop /btw API', async () => {
    sessions.set('btw-child', {
      sessionId: 'btw-child',
      title: 'Side question',
      isTransient: false,
      sessionKind: 'btw',
      config: { modelName: 'fast' },
      btwOrigin: {
        parentSessionId: 'parent-1',
        parentDialogTurnId: 'turn-parent-1',
        parentTurnIndex: 1,
      },
    });

    await sendMessageToBtwSession({
      parentSessionId: 'parent-1',
      childSessionId: 'btw-child',
      question: 'What is in this image?',
      imagePayload: {
        imageContexts: [
          {
            id: 'img-1',
            image_path: 'C:/tmp/clip.png',
            mime_type: 'image/png',
            metadata: { name: 'clip.png' },
          },
        ],
        imageDisplayData: [
          {
            id: 'img-1',
            name: 'clip.png',
            imagePath: 'C:/tmp/clip.png',
            mimeType: 'image/png',
          },
        ],
      },
    });

    expect(mockAskStream).toHaveBeenCalledWith(
      expect.objectContaining({
        sessionId: 'parent-1',
        childSessionId: 'btw-child',
        question: 'What is in this image?',
        parentDialogTurnId: 'turn-parent-1',
        parentTurnIndex: 1,
        imageContexts: [
          expect.objectContaining({
            id: 'img-1',
            image_path: 'C:/tmp/clip.png',
            mime_type: 'image/png',
          }),
        ],
      }),
    );
    expect(mockAskStream.mock.calls[0][0]).not.toHaveProperty('modelId');
    expect(mockUpdateSessionBtwOrigin).toHaveBeenCalledWith(
      'btw-child',
      expect.objectContaining({
        requestId: expect.any(String),
        parentSessionId: 'parent-1',
        parentDialogTurnId: 'turn-parent-1',
        parentTurnIndex: 1,
      }),
      'btw',
    );
  });

  it('discards the local placeholder when the first /btw request fails', async () => {
    const error = new Error('backend refused');
    mockAskStream.mockRejectedValueOnce(error);
    mockAddExternalSession.mockImplementationOnce((sessionId, title, mode, workspacePath, meta) => {
      sessions.set(sessionId, {
        sessionId,
        title,
        mode,
        workspacePath,
        config: {},
        sessionKind: meta?.sessionKind,
        parentSessionId: meta?.parentSessionId,
        btwOrigin: meta?.btwOrigin,
        isTransient: meta?.isTransient,
        agentBackedTransient: meta?.agentBackedTransient,
      });
    });

    await expect(startBtwThread({
      parentSessionId: 'parent-1',
      question: 'Will this send?',
      workspacePath: '/workspace',
    })).rejects.toThrow('backend refused');

    const childSessionId = mockAddExternalSession.mock.calls[0][0];
    const { flowChatManager } = await import('./FlowChatManager');
    expect(flowChatManager.discardLocalSession).toHaveBeenCalledWith(childSessionId);
  });

  it('restores the parent coordinator session before starting a temporary /btw thread', async () => {
    sessions.set('parent-1', {
      ...sessions.get('parent-1'),
      isHistorical: false,
      historyState: 'ready',
      contextRestoreState: 'pending',
    });
    mockAddExternalSession.mockImplementationOnce((sessionId, title, mode, workspacePath, meta) => {
      sessions.set(sessionId, {
        sessionId,
        title,
        mode,
        workspacePath,
        config: {},
        sessionKind: meta?.sessionKind,
        parentSessionId: meta?.parentSessionId,
        btwOrigin: meta?.btwOrigin,
        isTransient: meta?.isTransient,
        agentBackedTransient: meta?.agentBackedTransient,
      });
    });

    await startBtwThread({
      parentSessionId: 'parent-1',
      question: 'Restore the parent first',
      workspacePath: '/workspace',
    });

    expect(mockEnsureBackendSession).toHaveBeenCalledWith('parent-1');
    expect(mockAskStream).toHaveBeenCalledWith(expect.objectContaining({
      sessionId: 'parent-1',
    }));
  });

  it('uses an explicit model override for /btw sends when provided', async () => {
    sessions.set('btw-child', {
      sessionId: 'btw-child',
      title: 'Side question',
      isTransient: false,
      sessionKind: 'btw',
      config: { modelName: 'parent-multimodal-model' },
      dialogTurns: [],
    });

    await sendMessageToBtwSession({
      parentSessionId: 'parent-1',
      childSessionId: 'btw-child',
      question: 'What is in this image?',
      modelId: 'parent-multimodal-model',
      imagePayload: {
        imageContexts: [
          {
            id: 'img-1',
            image_path: '/tmp/clip.png',
            mime_type: 'image/png',
          },
        ],
        imageDisplayData: [
          {
            id: 'img-1',
            name: 'clip.png',
            imagePath: '/tmp/clip.png',
            mimeType: 'image/png',
          },
        ],
      },
    });

    expect(mockAskStream).toHaveBeenCalledWith(
      expect.objectContaining({
        modelId: 'parent-multimodal-model',
      }),
    );
    expect(mockUpdateSessionModelName).toHaveBeenCalledWith(
      'btw-child',
      'parent-multimodal-model',
    );
  });
});
