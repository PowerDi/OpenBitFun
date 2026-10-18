import { resolveExternalAgentCapabilities } from './externalAgentCapabilities';
import { describe, expect, it } from 'vitest';
import { parseReloadCommand, supportsLocalReloadContext } from './reloadCommand';

describe('parseReloadCommand', () => {
  it.each([
    ['/reload', 'all'],
    ['  /RELOAD  ', 'all'],
    ['/reload skills', 'skills'],
    ['/reload Instructions', 'instructions'],
    ['/reload-skills', 'skills'],
  ] as const)('parses %s as %s', (input, target) => {
    expect(parseReloadCommand(input)).toEqual({ kind: 'reload', target });
  });

  it('keeps unrelated input outside the local command path', () => {
    expect(parseReloadCommand('/reloader')).toBeNull();
    expect(parseReloadCommand('please /reload')).toBeNull();
  });

  it.each([
    '/reload mcp',
    '/reload skills instructions',
    '/reload\nskills\ninstructions',
    '/reload-skills instructions',
  ])('rejects unsupported form %s', input => {
    expect(parseReloadCommand(input)).toEqual({ kind: 'invalid' });
  });
});

describe('supportsLocalReloadContext', () => {
  it.each([
    { commands: true, hostCommands: false, expected: false },
    { commands: false, hostCommands: true, expected: true },
  ])('uses hostCommands=$hostCommands independently of commands=$commands', ({ commands, hostCommands, expected }) => {
    const unknown = resolveExternalAgentCapabilities('acp:unknown')!;
    expect(supportsLocalReloadContext({
      desktopRuntime: true,
      externalCapabilities: { ...unknown, commands, hostCommands },
      dispatchTransport: false,
    })).toBe(expected);
  });

  it('allows only local Desktop sessions', () => {
    expect(supportsLocalReloadContext({
      desktopRuntime: true,
      externalCapabilities: null,
      dispatchTransport: false,
    })).toBe(true);
    expect(supportsLocalReloadContext({
      desktopRuntime: false,
      externalCapabilities: null,
      dispatchTransport: false,
    })).toBe(false);
    expect(supportsLocalReloadContext({
      desktopRuntime: true,
      externalCapabilities: resolveExternalAgentCapabilities('acp:unknown'),
      dispatchTransport: false,
    })).toBe(false);
    expect(supportsLocalReloadContext({
      desktopRuntime: true,
      externalCapabilities: null,
      dispatchTransport: true,
    })).toBe(false);
  });
});
