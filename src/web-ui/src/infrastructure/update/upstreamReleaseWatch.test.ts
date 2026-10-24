import { describe, expect, it, vi } from 'vitest';
import {
  UPSTREAM_WATCH_INTERVAL_MS,
  claimUpstreamNotice,
  fetchUpstreamLatestRelease,
  getUpstreamWatchRepo,
  isUpstreamCoreNewer,
  recordUpstreamChecked,
  shouldCheckUpstream,
} from './upstreamReleaseWatch';

function memoryStorage(): Storage {
  const data = new Map<string, string>();
  return {
    get length() { return data.size; },
    clear: () => data.clear(),
    getItem: key => data.get(key) ?? null,
    key: index => [...data.keys()][index] ?? null,
    removeItem: key => { data.delete(key); },
    setItem: (key, value) => { data.set(key, value); },
  };
}

describe('upstream release watch', () => {
  it('is inert unless a valid owner/repo is compiled in', () => {
    expect(getUpstreamWatchRepo(undefined)).toBeNull();
    expect(getUpstreamWatchRepo('')).toBeNull();
    expect(getUpstreamWatchRepo('https://evil.example/x')).toBeNull();
    expect(getUpstreamWatchRepo(' GCWing/OpenBitFun ')).toBe('GCWing/OpenBitFun');
  });

  it('compares version cores so fork betas of the same core are not flagged', () => {
    expect(isUpstreamCoreNewer('1.0.7', '1.0.7-beta.3')).toBe(false);
    expect(isUpstreamCoreNewer('1.0.8', '1.0.7-beta.3')).toBe(true);
    expect(isUpstreamCoreNewer('1.0.6', '1.0.7-beta.1')).toBe(false);
    expect(isUpstreamCoreNewer('garbage', '1.0.7')).toBe(false);
  });

  it('degrades to null on network, HTTP, and malformed responses', async () => {
    const failing = vi.fn().mockRejectedValue(new TypeError('offline'));
    expect(await fetchUpstreamLatestRelease('GCWing/OpenBitFun', failing)).toBeNull();
    const limited = vi.fn().mockResolvedValue(new Response('{}', { status: 403 }));
    expect(await fetchUpstreamLatestRelease('GCWing/OpenBitFun', limited)).toBeNull();
    const malformed = vi.fn().mockResolvedValue(new Response('{"tag_name":42}'));
    expect(await fetchUpstreamLatestRelease('GCWing/OpenBitFun', malformed)).toBeNull();
  });

  it('reads the latest release tag and keeps only GitHub release links', async () => {
    const ok = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tag_name: 'v1.0.8', html_url: 'javascript:alert(1)',
    })));
    expect(await fetchUpstreamLatestRelease('GCWing/OpenBitFun', ok)).toEqual({
      version: '1.0.8', url: 'https://github.com/GCWing/OpenBitFun/releases/latest',
    });
    expect(ok).toHaveBeenCalledWith(
      'https://api.github.com/repos/GCWing/OpenBitFun/releases/latest', expect.anything(),
    );
  });

  it('checks at most daily and announces each upstream version once', () => {
    const storage = memoryStorage();
    expect(shouldCheckUpstream(1_000, storage)).toBe(true);
    recordUpstreamChecked(1_000, storage);
    expect(shouldCheckUpstream(1_000 + UPSTREAM_WATCH_INTERVAL_MS - 1, storage)).toBe(false);
    expect(shouldCheckUpstream(1_000 + UPSTREAM_WATCH_INTERVAL_MS, storage)).toBe(true);
    expect(claimUpstreamNotice('1.0.8', storage)).toBe(true);
    expect(claimUpstreamNotice('1.0.8', storage)).toBe(false);
    expect(claimUpstreamNotice('1.0.9', storage)).toBe(true);
  });
});
