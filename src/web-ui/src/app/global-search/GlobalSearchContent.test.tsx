// @vitest-environment jsdom
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { activateSurface, resetDeviceSurfaceForTest } from '@/infrastructure/peer-device/deviceSurface';
import { GlobalSearchContent } from './GlobalSearchRoot';
import type { GlobalSearchSnapshot } from './types';

globalThis.IS_REACT_ACT_ENVIRONMENT = true;
const fixtures = vi.hoisted(() => ({
  t: (key: string) => key,
  workspaces: [],
  providers: [],
  activate: vi.fn(),
  search: vi.fn(),
  setActiveWorkspace: vi.fn(),
  selectAssistantWorkspace: vi.fn(),
  openAssistant: vi.fn(),
}));
vi.mock('@/infrastructure/i18n', () => ({ useI18n: () => ({ t: fixtures.t }) }));
vi.mock('@/infrastructure/hooks/useShortcut', () => ({ useShortcut: vi.fn() }));
vi.mock('@/infrastructure/contexts/WorkspaceContext', () => ({ useWorkspaceContext: () => ({
  currentWorkspace: null, openedWorkspacesList: fixtures.workspaces, setActiveWorkspace: fixtures.setActiveWorkspace,
}) }));
vi.mock('@/app/scenes/my-agent/myAgentStore', () => ({ useMyAgentStore: () => fixtures.selectAssistantWorkspace }));
vi.mock('@/app/scenes/profile/nurseryStore', () => ({ useNurseryStore: () => fixtures.openAssistant }));
vi.mock('@/shared/notification-system', () => ({ notificationService: { error: vi.fn() } }));
vi.mock('./globalSearchActivator', () => ({ activateGlobalSearchTarget: fixtures.activate }));
vi.mock('./federatedSearchEngine', () => ({ runFederatedSearch: fixtures.search }));
vi.mock('./globalSearchRegistry', () => ({ globalSearchRegistry: {
  subscribe: () => () => {}, getSnapshot: () => fixtures.providers,
} }));
vi.mock('./globalSearchShortcut', () => ({
  GLOBAL_SEARCH_SHORTCUT: 'mod+k', getGlobalSearchShortcutLabel: () => 'Cmd+K',
  splitGlobalSearchShortcutLabel: () => ({ modifier: 'command', key: 'K' }), subscribeGlobalSearchShortcut: () => () => {},
}));

const snapshot: GlobalSearchSnapshot = {
  providerStatus: {}, diagnostics: [], isSearching: false, truncated: false,
  items: [
    { id: 'terminal', providerId: 'actions', group: 'actions', title: 'Open terminal', score: 90, target: { kind: 'action', actionId: 'surface.terminal.open' } },
    { id: 'project', providerId: 'actions', group: 'actions', title: 'Open project', score: 80, target: { kind: 'action', actionId: 'project.open' } },
  ],
};

describe('GlobalSearchContent accessibility', () => {
  let container: HTMLDivElement;
  let root: Root;
  beforeEach(() => {
    resetDeviceSurfaceForTest();
    vi.useFakeTimers();
    fixtures.activate.mockReset();
    fixtures.search.mockReset();
    fixtures.search.mockImplementation((_providers, _request, _signal, onUpdate) => onUpdate(snapshot));
    HTMLElement.prototype.scrollIntoView = vi.fn();
    vi.stubGlobal('CSS', { escape: (value: string) => value });
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });
  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });
  it('retains named native actions and tracks keyboard selection in the controlled grid', async () => {
    act(() => root.render(<GlobalSearchContent autoFocus />));
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    const input = container.querySelector<HTMLInputElement>('[role="combobox"]')!;
    const grid = document.getElementById(input.getAttribute('aria-controls')!)!;
    expect(input.getAttribute('aria-haspopup')).toBe('grid');
    expect(grid.getAttribute('role')).toBe('grid');
    const buttons = grid.querySelectorAll('[role="gridcell"] button:not([role])');
    expect([...buttons].map(button => button.getAttribute('aria-label'))).toEqual(['Open terminal', 'Open project']);
    act(() => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true })));
    const selected = document.getElementById(input.getAttribute('aria-activedescendant')!)!;
    expect(selected.getAttribute('role')).toBe('gridcell');
    expect(selected.getAttribute('aria-selected')).toBe('true');
    expect(selected.textContent).toContain('Open terminal');
    expect(document.activeElement).toBe(input);
    await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
    expect(fixtures.activate).toHaveBeenCalledWith(snapshot.items[0].target, expect.anything());
  });
  it('shows source diagnostics and retries without clearing the query or healthy results', async () => {
    const failure = { ...snapshot, diagnostics: [{
      providerId: 'files', code: 'workspace_unavailable', message: 'Connection timed out',
      workspaceName: 'Remote project', workspacePath: '/srv/project', hostLabel: 'build-host',
    }] };
    fixtures.search.mockImplementationOnce((_providers, _request, _signal, onUpdate) => onUpdate(failure));
    fixtures.search.mockImplementationOnce((_providers, _request, _signal, onUpdate, retry) =>
      onUpdate({ ...retry.previous, isSearching: true }));
    act(() => root.render(<GlobalSearchContent initialQuery="needle" />));
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    const details = container.querySelector('details')!;
    expect(details.textContent).toContain('Connection timed out');
    expect(details.textContent).toContain('/srv/project');
    expect(details.textContent).toContain('build-host');
    act(() => details.querySelector<HTMLButtonElement>('button')!.click());
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    expect(fixtures.search.mock.calls[1][4]).toEqual({ previous: failure, providerIds: ['files'] });
    expect(container.querySelector('input')?.value).toBe('needle');
    expect(container.textContent).toContain('Open terminal');
    expect(details.querySelector('button')?.disabled).toBe(true);
  });

  it('discards old results and ignores late completions after a device switch', async () => {
    act(() => root.render(<GlobalSearchContent />));
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    const [, , signal, publishOld] = fixtures.search.mock.calls[0];
    expect(container.textContent).toContain('Open terminal');
    act(() => activateSurface('peer-b'));
    expect(signal.aborted).toBe(true);
    expect(container.textContent).not.toContain('Open terminal');
    act(() => publishOld(snapshot));
    expect(container.textContent).not.toContain('Open terminal');
  });

});
