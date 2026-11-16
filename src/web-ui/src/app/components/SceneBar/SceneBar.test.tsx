// @vitest-environment jsdom

import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { InteractionMotion } from '@/shared/utils/motionPreference';
import type { SceneTab, SceneTabId } from './types';
import SceneBar from './SceneBar';
import { useSceneStore, resetSceneStoreForTest } from '../../stores/sceneStore';
import { useContentResourceStore } from '../../workbench/contentResourceStore';
import { writeSessionTabDrag } from '../../workbench/canvasTabTransfer';
import { clearAgentCanvasForPeerSwitch, switchAgentCanvasScope, useAgentCanvasStore } from '../panels/content-canvas/stores';
import { activateSurface } from '@/infrastructure/peer-device/deviceSurface';
import { useDeviceDirectory } from '@/infrastructure/account/deviceDirectory';
import { useContextMenuStore } from '@/shared/context-menu-system/store/ContextMenuStore';

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

const sceneBarStyles = readFileSync(
  resolve(process.cwd(), 'src/app/components/SceneBar/SceneBar.scss'),
  'utf8',
);

const sceneHarness = vi.hoisted(() => ({
  state: {
    openTabs: [
      { id: 'session' as const, lastUsed: 1 },
      { id: 'settings' as const, lastUsed: 2 },
      { id: 'terminal' as const, lastUsed: 3 },
      { id: 'git' as const, lastUsed: 4 },
    ] as SceneTab[],
    activeTabId: 'session' as SceneTabId,
    pendingTabId: null as SceneTabId | null,
    navigationMotion: 'instant' as InteractionMotion,
    sessionTitle: undefined as string | undefined,
    tabDefs: [
      { id: 'session' as const, label: 'Session', Icon: () => null, pinned: true, closable: true, singleton: true, defaultOpen: false },
      { id: 'settings' as const, label: 'Settings', pinned: false, singleton: true, defaultOpen: false },
      { id: 'terminal' as const, label: 'Terminal', pinned: false, singleton: true, defaultOpen: false },
      { id: 'git' as const, label: 'Git', pinned: false, singleton: true, defaultOpen: false },
    ],
  },
  activateScene: vi.fn(),
  closeScene: vi.fn(),
}));

vi.mock('../../hooks/useSceneManager', () => ({
  useSceneManager: () => ({
    ...sceneHarness.state,
    activateScene: sceneHarness.activateScene,
    closeScene: sceneHarness.closeScene,
  }),
}));

vi.mock('../../hooks/useSessionTabLabels', () => ({
  useSessionTabLabels: (tabs: SceneTab[]) => Object.fromEntries(tabs.map(tab => [tab.id,
    { title: tab.id === 'session' ? sceneHarness.state.sessionTitle ?? '' : tab.session?.sessionId ?? '' },
  ])),
}));

vi.mock('@/infrastructure/i18n/hooks/useI18n', () => ({
  useI18n: () => ({ t: (key: string) => key }),
}));

vi.mock('@/infrastructure/runtime', async importOriginal => ({
  ...await importOriginal<typeof import('@/infrastructure/runtime')>(),
  supportsNativeWindowDragging: () => false,
}));

vi.mock('@/shared/utils/logger', () => ({
  createLogger: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
}));

vi.mock('@openbitfun/ui', async importOriginal => ({
  ...await importOriginal<typeof import('@openbitfun/ui')>(),
  Tooltip: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock('@/app/components/WindowControls', () => ({
  WindowControls: () => null,
}));

describe('SceneBar overflow navigation', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    activateSurface('local');
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    sceneHarness.state.activeTabId = 'session';
    sceneHarness.state.pendingTabId = null;
    sceneHarness.state.navigationMotion = 'instant';
    sceneHarness.state.sessionTitle = undefined;
    for (const tab of sceneHarness.state.openTabs) { delete tab.session; delete tab.sessionHistory; }
    sceneHarness.activateScene.mockReset();
    sceneHarness.closeScene.mockReset();
    clearAgentCanvasForPeerSwitch();
    useContentResourceStore.setState({ resources: {} });
    useContextMenuStore.getState().reset();
    window.getSelection()?.removeAllRanges();
    resetSceneStoreForTest();
  });

  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    useContextMenuStore.getState().reset();
    window.getSelection()?.removeAllRanges();
    activateSurface('local');
    vi.restoreAllMocks();
  });

  function renderSceneBar() {
    act(() => root.render(<SceneBar />));
  }

  it('activates a session tab on click', () => {
    sceneHarness.state.activeTabId = 'git';
    renderSceneBar();
    const trigger = container.querySelector<HTMLButtonElement>('[role="tab"][data-openbitfun-value="session"]')!;
    act(() => trigger.click());
    expect(sceneHarness.activateScene).toHaveBeenCalledWith('session');
    expect(trigger.hasAttribute('aria-haspopup')).toBe(false);
    expect(container.querySelector('[data-openbitfun-part="tabDevice"]')).toBeNull();
  });

  it('brings a grown last tab fully into view so its close action stays reachable', () => {
    renderSceneBar();
    const scroller = container.querySelector<HTMLElement>('[data-scene-bar-part="tabs"]')!;
    const item = container.querySelector<HTMLElement>('[data-scene-tab-id="git"]')!;
    const close = item.querySelector('[data-scene-bar-part="closeTab"]');
    expect(close?.hasAttribute('title')).toBe(false);
    scroller.scrollBy = vi.fn();
    vi.spyOn(scroller, 'getBoundingClientRect').mockReturnValue({ right: 400 } as DOMRect);
    vi.spyOn(item, 'getBoundingClientRect').mockReturnValue({ right: 430 } as DOMRect);
    const hovered = vi.spyOn(item, 'matches').mockReturnValue(false);
    const grown = () => {
      const event = new Event('transitionend', { bubbles: true });
      Object.defineProperty(event, 'propertyName', { value: 'padding-inline-end' });
      act(() => { item.firstElementChild!.dispatchEvent(event); });
    };
    grown();
    expect(scroller.scrollBy).not.toHaveBeenCalled();
    hovered.mockReturnValue(true);
    grown();
    expect(scroller.scrollBy).toHaveBeenCalledWith(expect.objectContaining({ left: 32 }));
  });

  it('groups tabs by device and names each device once', () => {
    const [session, settings, terminal] = sceneHarness.state.openTabs;
    session.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote' };
    terminal.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote-2' };
    settings.session = { surfaceId: 'local', workspaceKey: 'workspace-b', sessionId: 'here' };
    useDeviceDirectory.setState({
      devices: [
        { device_id: 'peer-tab-device', device_name: 'Studio' } as never,
        { device_id: 'me', device_name: 'MacBook' } as never,
      ],
      localId: 'me',
    });
    try {
      renderSceneBar();
      const labels = Array.from(container.querySelectorAll('[data-openbitfun-part="tabDevice"]'));
      expect(labels.map(label => label.textContent)).toEqual(['Studio', 'MacBook']);
      expect(labels[1].hasAttribute('data-current')).toBe(true);
      // Studio's tabs are adjacent; the scene tab joins the rendered device.
      const order = Array.from(container.querySelectorAll('[role="tab"]'))
        .map(tab => tab.getAttribute('data-openbitfun-value'));
      expect(order).toEqual(['session', 'terminal', 'settings', 'git']);
      const describedBy = (value: string) => container
        .querySelector(`[role="tab"][data-openbitfun-value="${value}"]`)?.getAttribute('aria-describedby');
      expect(describedBy('terminal')).toBe(labels[0].id);
      expect(describedBy('git')).toBe(labels[1].id);
    } finally {
      useDeviceDirectory.setState({ devices: [], localId: null });
    }
  });

  function startSessionTabDrag() {
    const store = useAgentCanvasStore.getState();
    store.addTab({ type: 'code-editor', title: 'a.ts', data: { filePath: '/project/a.ts' },
      metadata: { resourceScope: { surfaceId: 'local', workspacePath: '/project' } } }, 'active');
    const tab = useAgentCanvasStore.getState().primaryGroup.tabs[0];
    store.startDrag(tab.id, 'primary');
    const data = new Map<string, string>();
    const transfer = { get types() { return [...data.keys()]; },
      setData: (type: string, value: string) => { data.set(type, value); },
      getData: (type: string) => data.get(type) ?? '',
    } as DataTransfer;
    writeSessionTabDrag(transfer, tab.id, 'primary');
    document.dispatchEvent(dragEvent('dragstart', transfer));
    return transfer;
  }

  function dragEvent(type: string, transfer: DataTransfer, clientX = 0) {
    const event = new MouseEvent(type, { bubbles: true, cancelable: true, clientX });
    Object.defineProperty(event, 'dataTransfer', { value: transfer });
    return event;
  }

  it('advertises the destination at dragstart, then distinguishes hover from availability', () => {
    renderSceneBar();
    const bar = container.querySelector<HTMLElement>('.openbitfun-scene-bar')!;
    expect(bar.dataset.canvasDropState).toBeUndefined();
    let transfer!: DataTransfer;
    act(() => { transfer = startSessionTabDrag(); });
    expect(bar.dataset.canvasDropState).toBe('available');
    const hint = container.querySelector<HTMLElement>('[data-openbitfun-part="dropHint"]')!;
    expect(hint.querySelector('[aria-hidden="false"]')?.textContent).toBe('workbench.dragToPopOut');
    expect(container.querySelector('[data-canvas-drop-position]')).toBeNull();
    expect(useContentResourceStore.getState().resources).toEqual({});

    act(() => bar.dispatchEvent(dragEvent('dragover', transfer)));
    expect(bar.dataset.canvasDropState).toBe('active');
    expect(hint.querySelector('[aria-hidden="false"]')?.textContent).toBe('workbench.releaseToPopOut');
    act(() => bar.dispatchEvent(dragEvent('dragleave', transfer)));
    expect(bar.dataset.canvasDropState).toBe('available');
    expect(hint.querySelector('[aria-hidden="false"]')?.textContent).toBe('workbench.dragToPopOut');
    expect(container.querySelector('[data-canvas-drop-position]')).toBeNull();
    act(() => window.dispatchEvent(new Event('dragend')));
    expect(bar.dataset.canvasDropState).toBeUndefined();
    expect(container.querySelector('[data-openbitfun-part="dropHint"]')).toBeNull();
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(1);
  });

  it('clears its offer after another drop target stops event bubbling', async () => {
    renderSceneBar();
    let transfer!: DataTransfer;
    act(() => { transfer = startSessionTabDrag(); });
    const elsewhere = document.createElement('div');
    document.body.appendChild(elsewhere);
    elsewhere.addEventListener('drop', event => event.stopPropagation());
    await act(async () => {
      elsewhere.dispatchEvent(dragEvent('drop', transfer));
      await new Promise(resolve => setTimeout(resolve, 0));
    });
    expect(container.querySelector('[data-openbitfun-part="dropHint"]')).toBeNull();
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(1);
    expect(useContentResourceStore.getState().resources).toEqual({});
    elsewhere.remove();
  });

  it.each(['end', 'remove', 'scope-switch', 'device-switch'])('withdraws availability when the source is invalidated: %s', action => {
    renderSceneBar();
    act(() => { startSessionTabDrag(); });
    expect(container.querySelector('[data-openbitfun-part="dropHint"]')).not.toBeNull();
    act(() => {
      const store = useAgentCanvasStore.getState();
      if (action === 'end') store.endDrag();
      if (action === 'remove') store.detachTab(store.draggingTabId!, 'primary');
      if (action === 'scope-switch') switchAgentCanvasScope('another-session');
      if (action === 'device-switch') activateSurface('peer');
    });
    expect(container.querySelector('[data-openbitfun-part="dropHint"]')).toBeNull();
    expect(container.querySelector('[data-canvas-drop-state]')).toBeNull();
  });

  it.each(['Files', 'application/x-openbitfun-scene'])('does not offer pop-out for another drag format: %s', type => {
    renderSceneBar();
    const transfer = { types: [type] } as unknown as DataTransfer;
    act(() => document.dispatchEvent(dragEvent('dragstart', transfer)));
    expect(container.querySelector('[data-openbitfun-part="dropHint"]')).toBeNull();
  });

  it('accepts a session tab over an existing top tab and transfers it at the indicated position', () => {
    useSceneStore.getState().openScene('terminal');
    useSceneStore.getState().openScene('git');
    const transfer = startSessionTabDrag();
    renderSceneBar();
    const destination = container.querySelector<HTMLElement>('[data-scene-tab-id="git"]')!;
    vi.spyOn(destination, 'getBoundingClientRect').mockReturnValue({ left: 100, width: 100 } as DOMRect);
    const over = dragEvent('dragover', transfer, 120);
    act(() => destination.dispatchEvent(over));
    expect(over.defaultPrevented).toBe(true);
    expect(destination.dataset.canvasDropPosition).toBe('before');
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(1);
    act(() => destination.dispatchEvent(dragEvent('drop', transfer, 120)));
    const id = useSceneStore.getState().activeTabId;
    expect(id).toMatch(/^content:/);
    expect(useSceneStore.getState().openTabs.map(tab => tab.id)).toEqual(['terminal', id, 'git']);
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(0);
    expect(container.querySelector('[data-canvas-drop-target="true"]')).toBeNull();
  });

  it('accepts the empty area of the top bar and clears a canceled drag without moving content', () => {
    const transfer = startSessionTabDrag();
    renderSceneBar();
    const bar = container.querySelector<HTMLElement>('.openbitfun-scene-bar')!;
    act(() => bar.dispatchEvent(dragEvent('dragover', transfer)));
    expect(bar.dataset.canvasDropTarget).toBe('true');
    act(() => window.dispatchEvent(new Event('dragend')));
    expect(bar.dataset.canvasDropTarget).toBeUndefined();
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(1);
    expect(useContentResourceStore.getState().resources).toEqual({});
    act(() => bar.dispatchEvent(dragEvent('drop', transfer)));
    expect(useSceneStore.getState().activeTabId).toMatch(/^content:/);
    expect(useAgentCanvasStore.getState().primaryGroup.tabs).toHaveLength(0);
  });

  it('ignores external file drags', () => {
    renderSceneBar();
    const bar = container.querySelector<HTMLElement>('.openbitfun-scene-bar')!;
    const over = dragEvent('dragover', { types: ['Files'] } as unknown as DataTransfer);
    act(() => bar.dispatchEvent(over));
    expect(over.defaultPrevented).toBe(false);
    expect(bar.dataset.canvasDropTarget).toBeUndefined();
    expect(useContentResourceStore.getState().resources).toEqual({});
  });

  function setOverflowMetrics(tabs: HTMLElement, region: HTMLElement) {
    Object.defineProperty(region, 'clientWidth', { configurable: true, value: 240 });
    Object.defineProperty(tabs, 'clientWidth', { configurable: true, value: 180 });
    Object.defineProperty(tabs, 'scrollWidth', { configurable: true, value: 620 });
    Object.defineProperty(tabs, 'scrollLeft', { configurable: true, value: 0, writable: true });
  }

  it('uses the real session title as the single label and keeps Settings static', () => {
    sceneHarness.state.sessionTitle = 'Investigate top tabs';
    renderSceneBar();

    const sessionTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="session"]')!;
    const settingsTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="settings"]')!;

    expect(sessionTab.querySelector('[data-openbitfun-part="label"]')?.textContent)
      .toBe('Investigate top tabs');
    expect(settingsTab.querySelector('[data-openbitfun-part="label"]')?.textContent)
      .toBe('Settings');
    expect(sessionTab.querySelector('[data-openbitfun-part="icon"]')).toBeNull();
    expect(sessionTab.closest('[data-openbitfun-part="item"]')?.getAttribute('data-has-icon'))
      .toBe('false');
    expect(sessionTab.closest('[data-openbitfun-part="item"]')?.hasAttribute('data-overflow-trigger'))
      .toBe(true);
    expect(sessionTab.querySelector('[data-openbitfun-part="label"]')?.getAttribute('data-overflow-behavior'))
      .toBe('marquee');
    expect(container.querySelector('[data-scene-bar-part="tabs"]')?.getAttribute('data-size'))
      .toBe('sm');
    expect(container.querySelector('.openbitfun-scene-bar__tab-subtitle')).toBeNull();
    expect(container.querySelector('.openbitfun-scene-bar__tab-separator')).toBeNull();
  });

  it('delegates arrow and Home/End navigation to TabGroup', () => {
    renderSceneBar();
    const sessionTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="session"]');
    const settingsTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="settings"]');
    expect(sessionTab).not.toBeNull();
    expect(settingsTab).not.toBeNull();

    sessionTab!.focus();
    act(() => {
      sessionTab!.dispatchEvent(new KeyboardEvent('keydown', {
        key: 'ArrowRight',
        bubbles: true,
        cancelable: true,
      }));
    });

    expect(sceneHarness.activateScene).toHaveBeenCalledWith('settings');
    expect(document.activeElement).toBe(settingsTab);

    act(() => {
      settingsTab!.dispatchEvent(new KeyboardEvent('keydown', {
        key: 'End',
        bubbles: true,
        cancelable: true,
      }));
    });
    expect(sceneHarness.activateScene).toHaveBeenLastCalledWith('git');
  });

  it('keeps the tab and rolling label mounted when its workspace selects another session', () => {
    const sessionSlot = sceneHarness.state.openTabs[0];
    sessionSlot.session = { surfaceId: 'local', workspaceKey: 'workspace-a', sessionId: 'a' };
    sceneHarness.state.sessionTitle = 'First title';
    renderSceneBar();
    const tab = container.querySelector<HTMLButtonElement>('[role="tab"][data-openbitfun-value="session"]')!;
    const label = tab.querySelector('[data-openbitfun-component="rolling-text"]');
    expect(label).not.toBeNull();
    tab.focus();

    sessionSlot.session = { ...sessionSlot.session, sessionId: 'b' };
    sceneHarness.state.sessionTitle = 'Second title';
    renderSceneBar();
    expect(container.querySelector('[role="tab"][data-openbitfun-value="session"]')).toBe(tab);
    expect(tab.querySelector('[data-openbitfun-component="rolling-text"]')).toBe(label);
    expect(document.activeElement).toBe(tab);
    expect(tab.textContent).toBe('Second title');
    expect(tab.textContent).not.toContain('workspace-a');
  });

  it('marks pending navigation and lets the user return to the displayed tab', () => {
    sceneHarness.state.activeTabId = 'settings';
    sceneHarness.state.pendingTabId = 'session';
    renderSceneBar();
    expect(container.querySelector('[data-scene-bar-part="tabs"]')?.getAttribute('aria-busy')).toBe('true');
    const settings = container.querySelector<HTMLButtonElement>('[role="tab"][data-openbitfun-value="settings"]')!;
    act(() => settings.click());
    expect(sceneHarness.activateScene).toHaveBeenCalledWith('settings');
  });

  it('activates a retained background tab from the idle shell state', () => {
    const previousTabs = sceneHarness.state.openTabs;
    sceneHarness.state.activeTabId = null;
    sceneHarness.state.openTabs = [
      { id: 'settings', lastUsed: 1 },
      { id: 'terminal', lastUsed: 2 },
    ];
    try {
      renderSceneBar();

      const settings = container.querySelector<HTMLButtonElement>('[role="tab"][data-openbitfun-value="settings"]')!;
      act(() => settings.click());

      expect(sceneHarness.activateScene).toHaveBeenCalledWith('settings');
    } finally {
      sceneHarness.state.openTabs = previousTabs;
    }
  });

  it('exposes overflow controls and translates a vertical wheel into horizontal movement', () => {
    renderSceneBar();
    const region = container.querySelector<HTMLElement>('[data-openbitfun-component="scene-bar"][data-openbitfun-part="tabs"]')!;
    const tabs = container.querySelector<HTMLElement>('[data-scene-bar-part="tabs"]')!;
    setOverflowMetrics(tabs, region);

    act(() => tabs.dispatchEvent(new Event('scroll')));

    expect(region.dataset.overflow).toBe('true');
    expect(container.querySelector('[data-openbitfun-part="scrollPrevious"]')).not.toBeNull();
    expect(container.querySelector('[data-openbitfun-part="scrollNext"]')).not.toBeNull();

    act(() => {
      tabs.dispatchEvent(new WheelEvent('wheel', {
        deltaY: 72,
        bubbles: true,
        cancelable: true,
      }));
    });

    expect(tabs.scrollLeft).toBe(72);
    expect(container.querySelector<HTMLButtonElement>('[data-openbitfun-part="scrollPrevious"]')?.disabled).toBe(false);
  });

  it('removes unavailable edge controls from layout and paints the overflow fade directly', () => {
    expect(sceneBarStyles).toMatch(/&:disabled\s*\{\s*display:\s*none;/);
    expect(sceneBarStyles).toContain('background: linear-gradient(');
    expect(sceneBarStyles).not.toContain('mask-image: linear-gradient(');
    expect(sceneBarStyles).not.toContain('backdrop-filter: var(--openbitfun-effect-blur-subtle)');
  });

  it('scrolls a newly active off-screen tab into view', () => {
    renderSceneBar();
    const region = container.querySelector<HTMLElement>('[data-openbitfun-component="scene-bar"][data-openbitfun-part="tabs"]')!;
    const tabs = container.querySelector<HTMLElement>('[data-scene-bar-part="tabs"]')!;
    const gitTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="git"]')!;
    const gitItem = gitTab.closest<HTMLElement>('[data-openbitfun-part="item"]')!;
    setOverflowMetrics(tabs, region);
    expect(gitItem.parentElement?.dataset.sceneTabId).toBe('git');
    // The real positioned wrapper owns offsetLeft; it is zero for the inner item.
    Object.defineProperty(gitItem, 'offsetLeft', { configurable: true, value: 0 });
    vi.spyOn(tabs, 'getBoundingClientRect').mockReturnValue({ left: 100, width: 180 } as DOMRect);
    vi.spyOn(gitItem, 'getBoundingClientRect').mockReturnValue({ left: 520, width: 100 } as DOMRect);
    const scrollTo = vi.fn(({ left }: ScrollToOptions) => {
      tabs.scrollLeft = left ?? tabs.scrollLeft;
    });
    Object.defineProperty(tabs, 'scrollTo', { configurable: true, value: scrollTo });

    sceneHarness.state.activeTabId = 'git';
    sceneHarness.state.navigationMotion = 'pointer';
    act(() => root.render(<SceneBar />));

    expect(scrollTo).toHaveBeenCalledWith({ left: 340, behavior: 'smooth' });
  });

  it('renders close actions from closeability metadata, including the leading session tab', () => {
    renderSceneBar();
    const sessionTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="session"]')!;
    const sessionItem = sessionTab.closest<HTMLElement>('[data-openbitfun-part="item"]')!;
    const sessionCloseButton = sessionItem.querySelector<HTMLButtonElement>('[data-scene-bar-part="closeTab"]')!;
    const settingsTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="settings"]')!;
    const settingsItem = settingsTab.closest<HTMLElement>('[data-openbitfun-part="item"]')!;
    const closeButton = settingsItem.querySelector<HTMLButtonElement>('[data-scene-bar-part="closeTab"]')!;

    expect(sessionCloseButton).not.toBeNull();
    act(() => sessionCloseButton.click());
    expect(sceneHarness.closeScene).toHaveBeenCalledWith('session');

    expect(settingsTab.contains(closeButton)).toBe(false);
    act(() => closeButton.click());
    expect(sceneHarness.closeScene).toHaveBeenCalledWith('settings');
  });

  it('keeps close targets out of press transforms so pointer hit testing stays stable', () => {
    renderSceneBar();
    const closeButtons = container.querySelectorAll<HTMLButtonElement>('[data-scene-bar-part="closeTab"]');

    expect(closeButtons.length).toBeGreaterThan(0);
    for (const closeButton of closeButtons) {
      expect(closeButton.dataset.motion).toBe('none');
    }
  });

  it('supports standard middle-click and Delete-key close interactions for session', () => {
    renderSceneBar();
    const sessionTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="session"]')!;

    act(() => {
      sessionTab.dispatchEvent(new MouseEvent('auxclick', {
        button: 1,
        bubbles: true,
        cancelable: true,
      }));
    });
    expect(sceneHarness.closeScene).toHaveBeenCalledWith('session');

    sceneHarness.closeScene.mockReset();
    act(() => {
      sessionTab.dispatchEvent(new KeyboardEvent('keydown', {
        key: 'Delete',
        bubbles: true,
        cancelable: true,
      }));
    });
    expect(sceneHarness.closeScene).toHaveBeenCalledWith('session');
  });

  it('prevents text selection on tabs and device labels in styles', () => {
    const barRule = sceneBarStyles.match(/\.openbitfun-scene-bar\s*\{([^}]*)\}/s)?.[1] ?? '';
    const deviceGroupRule = sceneBarStyles.match(/&__device-group\s*\{([^}]*)\}/s)?.[1] ?? '';

    expect(barRule).toMatch(/-webkit-user-select:\s*none;/);
    expect(barRule).toMatch(/user-select:\s*none;/);
    expect(deviceGroupRule).toMatch(/cursor:\s*default;/);
    expect(deviceGroupRule).toMatch(/-webkit-user-select:\s*none;/);
    expect(deviceGroupRule).toMatch(/user-select:\s*none;/);
    expect(deviceGroupRule).not.toMatch(/pointer-events:\s*none;/);
  });

  it('clears tab text selection on contextmenu while preserving external selections', () => {
    sceneHarness.state.sessionTitle = 'OpenBitFun ACP能力';
    renderSceneBar();

    const sessionTab = container.querySelector<HTMLElement>('[role="tab"][data-openbitfun-value="session"]')!;
    const labelText = sessionTab.querySelector('[data-openbitfun-part="label"]')?.firstChild;
    expect(labelText).not.toBeNull();

    const selection = window.getSelection()!;
    const tabRange = document.createRange();
    tabRange.selectNodeContents(sessionTab.querySelector('[data-openbitfun-part="label"]')!);
    selection.removeAllRanges();
    selection.addRange(tabRange);
    expect(selection.isCollapsed).toBe(false);

    act(() => {
      sessionTab.dispatchEvent(new MouseEvent('contextmenu', {
        bubbles: true,
        cancelable: true,
        clientX: 40,
        clientY: 16,
      }));
    });

    expect(selection.rangeCount).toBe(0);
    expect(useContextMenuStore.getState().visible).toBe(true);

    const external = document.createElement('div');
    external.textContent = 'External transcript text';
    document.body.appendChild(external);
    try {
      const externalRange = document.createRange();
      externalRange.selectNodeContents(external);
      selection.removeAllRanges();
      selection.addRange(externalRange);
      expect(selection.toString()).toBe('External transcript text');

      act(() => {
        sessionTab.dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 44,
          clientY: 18,
        }));
      });

      expect(selection.rangeCount).toBe(1);
      expect(selection.toString()).toBe('External transcript text');
    } finally {
      selection.removeAllRanges();
      external.remove();
    }
  });

  it('opens a device-group context menu to close all closable tabs under that device', async () => {
    const [session, settings, terminal] = sceneHarness.state.openTabs;
    session.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote' };
    terminal.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote-2' };
    settings.session = { surfaceId: 'local', workspaceKey: 'workspace-b', sessionId: 'here' };
    useDeviceDirectory.setState({
      devices: [
        { device_id: 'peer-tab-device', device_name: 'Studio' } as never,
        { device_id: 'me', device_name: 'MacBook' } as never,
      ],
      localId: 'me',
    });

    const originalDefs = sceneHarness.state.tabDefs;
    try {
      sceneHarness.state.activeTabId = 'settings';
      renderSceneBar();
      const labels = Array.from(container.querySelectorAll<HTMLElement>('[data-openbitfun-part="tabDevice"]'));
      expect(labels.map(label => label.textContent)).toEqual(['Studio', 'MacBook']);

      act(() => {
        labels[0].dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 20,
          clientY: 12,
        }));
      });

      let menu = useContextMenuStore.getState();
      expect(menu.visible).toBe(true);
      expect(menu.items).toHaveLength(1);
      expect(menu.items[0]).toMatchObject({
        id: 'close-device-tabs',
        label: 'sceneBar.closeDeviceTabs',
        disabled: false,
      });

      await act(async () => {
        menu.items[0].onClick?.(menu.context!);
        await Promise.resolve();
      });
      expect(sceneHarness.closeScene.mock.calls.map(call => call[0])).toEqual(['session', 'terminal']);

      sceneHarness.closeScene.mockReset();
      act(() => {
        labels[1].dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 220,
          clientY: 12,
        }));
      });
      menu = useContextMenuStore.getState();
      await act(async () => {
        menu.items[0].onClick?.(menu.context!);
        await Promise.resolve();
      });
      expect(sceneHarness.closeScene.mock.calls.map(call => call[0])).toEqual(['git', 'settings']);

      sceneHarness.closeScene.mockReset();
      sceneHarness.state.tabDefs = originalDefs.map(def =>
        def.id === 'settings' || def.id === 'git' ? { ...def, closable: false } : def,
      );
      renderSceneBar();
      const updatedLabels = Array.from(container.querySelectorAll<HTMLElement>('[data-openbitfun-part="tabDevice"]'));
      act(() => {
        updatedLabels[1].dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 220,
          clientY: 12,
        }));
      });
      menu = useContextMenuStore.getState();
      expect(menu.items[0]?.disabled).toBe(true);

      sceneHarness.state.tabDefs = originalDefs.map(def =>
        def.id === 'settings' ? { ...def, closable: false } : def,
      );
      renderSceneBar();
      const partialLabels = Array.from(container.querySelectorAll<HTMLElement>('[data-openbitfun-part="tabDevice"]'));
      act(() => {
        partialLabels[1].dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 220,
          clientY: 12,
        }));
      });
      menu = useContextMenuStore.getState();
      expect(menu.items[0]?.disabled).toBe(false);
      await act(async () => {
        menu.items[0].onClick?.(menu.context!);
        await Promise.resolve();
      });
      expect(sceneHarness.closeScene.mock.calls.map(call => call[0])).toEqual(['git']);
    } finally {
      sceneHarness.state.tabDefs = originalDefs;
      act(() => { useDeviceDirectory.setState({ devices: [], localId: null }); });
    }
  });

  it('dismisses the device-group context menu and skips closing when the active surface switches', async () => {
    const [session, settings, terminal] = sceneHarness.state.openTabs;
    session.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote' };
    terminal.session = { surfaceId: 'peer-tab-device', workspaceKey: 'workspace-a', sessionId: 'remote-2' };
    settings.session = { surfaceId: 'local', workspaceKey: 'workspace-b', sessionId: 'here' };
    useDeviceDirectory.setState({
      devices: [
        { device_id: 'peer-tab-device', device_name: 'Studio' } as never,
        { device_id: 'me', device_name: 'MacBook' } as never,
      ],
      localId: 'me',
    });

    try {
      renderSceneBar();
      const labels = Array.from(container.querySelectorAll<HTMLElement>('[data-openbitfun-part="tabDevice"]'));
      act(() => {
        labels[0].dispatchEvent(new MouseEvent('contextmenu', {
          bubbles: true,
          cancelable: true,
          clientX: 20,
          clientY: 12,
        }));
      });
      const menu = useContextMenuStore.getState();
      expect(menu.visible).toBe(true);

      act(() => {
        activateSurface('peer-tab-device');
      });
      expect(useContextMenuStore.getState().visible).toBe(false);

      await act(async () => {
        menu.items[0].onClick?.(menu.context!);
        await Promise.resolve();
      });
      expect(sceneHarness.closeScene).not.toHaveBeenCalled();
    } finally {
      act(() => { useDeviceDirectory.setState({ devices: [], localId: null }); });
    }
  });
});
