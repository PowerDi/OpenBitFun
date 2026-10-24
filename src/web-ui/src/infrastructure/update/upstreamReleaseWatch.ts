import coerce from 'semver/functions/coerce';
import gt from 'semver/functions/gt';

/**
 * Fork-only upstream release watch.
 *
 * Fork release builds set `VITE_OPENBITFUN_UPSTREAM_WATCH_REPO` (for example
 * `GCWing/OpenBitFun`). Upstream builds leave it empty, so this module is inert
 * there. The watch only informs; it never downloads or installs upstream builds.
 */
const REPO_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const NOTIFIED_KEY = 'openbitfun:upstream-watch:notified';
const CHECKED_AT_KEY = 'openbitfun:upstream-watch:checked-at';
export const UPSTREAM_WATCH_INTERVAL_MS = 24 * 60 * 60 * 1000;

export interface UpstreamRelease {
  version: string;
  url: string;
}

export function getUpstreamWatchRepo(
  value: unknown = import.meta.env.VITE_OPENBITFUN_UPSTREAM_WATCH_REPO,
): string | null {
  const repo = typeof value === 'string' ? value.trim() : '';
  return REPO_PATTERN.test(repo) ? repo : null;
}

/** Compare version cores only: 1.0.8 is newer than 1.0.7-beta.3, 1.0.7 is not. */
export function isUpstreamCoreNewer(upstream: string, installed: string): boolean {
  const upstreamCore = coerce(upstream);
  const installedCore = coerce(installed);
  return Boolean(upstreamCore && installedCore && gt(upstreamCore.version, installedCore.version));
}

/** Latest non-prerelease upstream release, or null on any failure. */
export async function fetchUpstreamLatestRelease(
  repo: string,
  fetchImpl: typeof fetch = fetch,
): Promise<UpstreamRelease | null> {
  try {
    const response = await fetchImpl(`https://api.github.com/repos/${repo}/releases/latest`, {
      headers: { Accept: 'application/vnd.github+json' },
    });
    if (!response.ok) return null;
    const body = await response.json() as { tag_name?: unknown; html_url?: unknown };
    const tag = typeof body.tag_name === 'string' ? body.tag_name : '';
    const version = coerce(tag)?.version;
    if (!version) return null;
    const url = typeof body.html_url === 'string' && body.html_url.startsWith('https://github.com/')
      ? body.html_url
      : `https://github.com/${repo}/releases/latest`;
    return { version, url };
  } catch {
    return null;
  }
}

export function shouldCheckUpstream(now: number, storage: Storage = localStorage): boolean {
  const stored = storage.getItem(CHECKED_AT_KEY);
  if (stored === null) return true;
  const last = Number(stored);
  // A corrupt or future timestamp must not suppress the watch forever.
  return !Number.isFinite(last) || last > now || now - last >= UPSTREAM_WATCH_INTERVAL_MS;
}

export function recordUpstreamChecked(now: number, storage: Storage = localStorage): void {
  storage.setItem(CHECKED_AT_KEY, String(now));
}

/** Each upstream version is announced once per device. */
export function claimUpstreamNotice(version: string, storage: Storage = localStorage): boolean {
  if (storage.getItem(NOTIFIED_KEY) === version) return false;
  storage.setItem(NOTIFIED_KEY, version);
  return true;
}
