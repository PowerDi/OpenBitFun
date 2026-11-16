// @vitest-environment jsdom
import { resolve } from 'node:path';
import { compile } from 'sass';
import { describe, expect, it } from 'vitest';

const css = compile(resolve(__dirname, 'VirtualMessageList.scss')).css;

describe('FlowChat history paging status interaction', () => {
  it.each(['idle', 'loading', 'error'] as const)('exposes pointer interaction only for the %s recovery state', state => {
    const style = document.createElement('style');
    style.textContent = css;
    const boundary = document.createElement('div');
    boundary.className = 'virtual-message-list__history-paging-sentinel';
    boundary.dataset.historyPagingSentinel = state;
    document.head.append(style);
    document.body.append(boundary);
    try {
      const computed = getComputedStyle(boundary);
      expect(computed.visibility).toBe(state === 'idle' ? 'hidden' : 'visible');
      // Programmatic button.click() bypasses pointer hit testing; the actual
      // compiled stylesheet must make the recovery container reachable too.
      expect(computed.pointerEvents).toBe(state === 'error' ? 'auto' : 'none');
    } finally {
      boundary.remove();
      style.remove();
    }
  });
});
