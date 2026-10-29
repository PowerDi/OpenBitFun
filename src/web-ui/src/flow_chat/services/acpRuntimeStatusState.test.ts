import { describe, expect, it, vi } from 'vitest';

vi.mock('@/infrastructure/i18n', () => ({
  i18nService: {
    t: (key: string, params?: Record<string, unknown>) =>
      params ? `${key}:${JSON.stringify(params)}` : key,
    formatNumber: (value: number) => String(value),
  },
}));

import { formatRateLimitLabel, formatRetryStatusLabel } from './acpRuntimeStatusState';
import { clearLabeledRuntimeStatus } from './flow-chat-manager/RuntimeStatusModule';
import { getRuntimeStatus, showRuntimeStatus } from '../store/runtimeStatusStore';
import { readNativeTaskProgress } from '../tool-cards/TaskToolDisplay';

describe('native runtime status labels', () => {
  it('describes retries with the details the agent reported', () => {
    expect(formatRetryStatusLabel({})).toBe('flow-chat:runtimeStatus.retrying');
    const label = formatRetryStatusLabel({ attempt: 2, maxAttempts: 10, delayMs: 4500 });
    expect(label).toContain('flow-chat:runtimeStatus.retryingWithDetails');
    expect(label).toContain('retryAttemptOf');
    expect(label).toContain('\\"seconds\\":\\"5\\"');
  });

  it('warns only when a limit is reached or close', () => {
    const window = (usedPercent: number, status?: string) => ({ kind: 'primary', usedPercent, status });
    expect(formatRateLimitLabel({ windows: [window(20, 'allowed')], updatedAtMs: 1 })).toBeNull();
    expect(formatRateLimitLabel({ windows: [window(85)], updatedAtMs: 1 }))
      .toContain('rateLimitHighUsage');
    expect(formatRateLimitLabel({ windows: [window(10, 'allowed_warning')], updatedAtMs: 1 }))
      .toBe('flow-chat:runtimeStatus.rateLimitWarning');
    expect(formatRateLimitLabel({ windows: [], limitReached: 'rate_limit_reached', updatedAtMs: 1 }))
      .toBe('flow-chat:runtimeStatus.rateLimitReached');
  });

  it('clears only the label its source showed', () => {
    showRuntimeStatus({ sessionId: 's', turnId: 't', roundId: 'r', label: 'x', source: 'other' });
    clearLabeledRuntimeStatus('s', 'native-retry');
    expect(getRuntimeStatus('s')?.label).toBe('x');
    showRuntimeStatus({ sessionId: 's', turnId: 't', roundId: 'r', label: 'y', source: 'native-retry' });
    clearLabeledRuntimeStatus('s', 'native-retry');
    expect(getRuntimeStatus('s')).toBeUndefined();
  });

  it('reads native Task progress and ignores plain progress text', () => {
    expect(readNativeTaskProgress('running...')).toBeNull();
    expect(readNativeTaskProgress(JSON.stringify({
      kind: 'native_task_progress', lastToolName: 'Grep', toolUses: 3, totalTokens: null,
    }))).toEqual({ lastToolName: 'Grep', toolUses: 3, totalTokens: undefined, durationMs: undefined });
  });
});
