import { expect, it, vi } from 'vitest';
import { api } from '@/infrastructure/api/service-api/ApiClient';
import { activateSurface, getActiveSurfaceScope } from '@/infrastructure/peer-device/deviceSurface';
import { ACPClientAPI, type AcpClientInfo, type AcpSessionOptions } from '@/infrastructure/api/service-api/ACPClientAPI';
import { externalAgentSupports, resolveExternalAgentCapabilities } from './externalAgentCapabilities';

it('resolves native, unknown, legacy and session-specific external advertisements conservatively', async () => {
  expect(resolveExternalAgentCapabilities('Standard')).toBeNull();
  expect(resolveExternalAgentCapabilities({ config: {}, mode: 'Claw' })).toBeNull();
  expect(externalAgentSupports(null, 'input.images')).toBe(true);
  const unknown = resolveExternalAgentCapabilities('acp:unknown', [])!;
  const unknownFlags = Object.values(unknown)
    .flatMap(value => typeof value === 'object' ? Object.values(value) : [value]);
  expect(unknownFlags.every(value => value === false)).toBe(true);

  // An older host knows only the original fields; nested additions may also be partial.
  const legacy = JSON.parse('{"selectModel":true,"permissionMode":true,"thinking":true,"subagents":{"observe":true,"readTranscript":true},"autonomousTurns":true}');
  const clients = [{ id: 'claude-native', capabilities: legacy }] as AcpClientInfo[];
  const session = { config: {}, mode: 'acp:claude-native' };
  const caps = resolveExternalAgentCapabilities(session, clients)!;
  expect(caps.selectModel).toBe(true);
  expect(caps.permissionMode).toBe(true);
  expect(caps.hostCommands).toBe(false);
  expect(caps.voiceHistory).toBe(false);
  expect(caps.sideChat).toBe(false);
  expect(caps.input).toEqual({ images: false, steering: false, queue: false });
  expect(caps.history).toEqual({ rollbackLastTurn: false, fork: false, importTranscripts: false });
  expect(externalAgentSupports(caps, 'threadGoal')).toBe(false);
  expect(externalAgentSupports(caps, 'subagents.readTranscript')).toBe(true);
  expect(externalAgentSupports(caps, 'subagents.interact')).toBe(false);
  expect(resolveExternalAgentCapabilities('acp:other', clients)).toEqual(unknown);
  expect(resolveExternalAgentCapabilities('acp:', clients)).toBeNull();
  const options: AcpSessionOptions = {
    availableModels: [], configOptions: [],
    capabilities: { ...unknown, compact: true, input: { ...unknown.input, images: true } },
  };
  const overridden = resolveExternalAgentCapabilities(session, clients, options);
  expect(externalAgentSupports(overridden, 'compact')).toBe(true);
  expect(externalAgentSupports(overridden, 'input.images')).toBe(true);
  expect(externalAgentSupports(overridden, 'permissionMode')).toBe(false);
  expect(resolveExternalAgentCapabilities({ config: { agentType: 'acp:other' }, mode: 'acp:claude-native' }, clients))
    .toEqual(unknown);
  expect(JSON.parse(JSON.stringify(caps))).toEqual(caps);

  const originalSurface = getActiveSurfaceScope().surfaceId;
  const invoke = vi.spyOn(api, 'invoke').mockResolvedValueOnce(clients).mockResolvedValueOnce(options);
  const notify = vi.fn();
  const unsubscribe = ACPClientAPI.subscribeCapabilities(notify);
  try {
    activateSurface('capabilities-test-host');
    await ACPClientAPI.getClients();
    const loadedSession = { ...session, sessionId: 'same-session', remoteConnectionId: 'ssh-host' };
    expect(resolveExternalAgentCapabilities(loadedSession)?.selectModel).toBe(true);
    await ACPClientAPI.getSessionOptions({ sessionId: 'same-session', clientId: 'claude-native', remoteConnectionId: 'ssh-host' });
    expect(externalAgentSupports(resolveExternalAgentCapabilities(loadedSession), 'input.images')).toBe(true);
    expect(externalAgentSupports(resolveExternalAgentCapabilities({ ...loadedSession, remoteConnectionId: 'other-ssh-host' }), 'input.images'))
      .toBe(false);
    expect(invoke).toHaveBeenCalledTimes(2);
    notify.mockClear();
    activateSurface('capabilities-other-host');
    expect(notify).toHaveBeenCalled();
    expect(resolveExternalAgentCapabilities(loadedSession)).toEqual(unknown);
    expect(invoke).toHaveBeenCalledTimes(2);
  } finally {
    unsubscribe();
    invoke.mockRestore();
    activateSurface(originalSurface);
  }
});

it.each([
  ['hostCommands', 'commands'],
  ['voiceHistory', 'history.importTranscripts'],
  ['sideChat', 'history.fork'],
] as const)('keeps %s independent from %s', (hostKey, nativeKey) => {
  const unknown = resolveExternalAgentCapabilities('acp:unknown', [])!;
  const nativeOnly = resolveExternalAgentCapabilities('acp:unknown', [], {
    availableModels: [], configOptions: [],
    capabilities: {
      ...unknown, commands: true,
      history: { ...unknown.history, fork: true, importTranscripts: true },
    },
  });
  expect(externalAgentSupports(nativeOnly, nativeKey)).toBe(true);
  expect(externalAgentSupports(nativeOnly, hostKey)).toBe(false);
  const hostOnly = resolveExternalAgentCapabilities('acp:unknown', [], {
    availableModels: [], configOptions: [],
    capabilities: { ...unknown, [hostKey]: true },
  });
  expect(externalAgentSupports(hostOnly, hostKey)).toBe(true);
  expect(externalAgentSupports(hostOnly, nativeKey)).toBe(false);
  expect(externalAgentSupports(null, hostKey)).toBe(true);
});
