import { useEffect, useRef } from 'react';
import { useI18n } from '@/infrastructure/i18n';
import { systemAPI } from '@/infrastructure/api/service-api/SystemAPI';
import { notificationService } from '@/shared/notification-system/services/NotificationService';
import { createLogger } from '@/shared/utils/logger';
import { canAutoCheckForAppUpdates } from './tauriEnv';
import {
  UPSTREAM_WATCH_INTERVAL_MS,
  claimUpstreamNotice,
  fetchUpstreamLatestRelease,
  getUpstreamWatchRepo,
  isUpstreamCoreNewer,
  recordUpstreamChecked,
  shouldCheckUpstream,
} from './upstreamReleaseWatch';

const log = createLogger('UpstreamReleaseWatch');
const FIRST_CHECK_DELAY_MS = 30_000;

/**
 * Fork builds only: tells the maintainer when upstream ships a newer version
 * core so they can merge it. Renders nothing and never installs anything.
 */
export function UpstreamReleaseNotice(): null {
  const { t } = useI18n('common');
  const repo = getUpstreamWatchRepo();
  const tRef = useRef(t);
  tRef.current = t;

  useEffect(() => {
    if (!repo || !canAutoCheckForAppUpdates()) return;
    let cancelled = false;
    const check = async () => {
      const now = Date.now();
      if (cancelled || navigator.onLine === false || !shouldCheckUpstream(now)) return;
      recordUpstreamChecked(now);
      const [installed, upstream] = await Promise.all([
        systemAPI.getLocalAppVersion().catch(() => null),
        fetchUpstreamLatestRelease(repo),
      ]);
      // Offline, rate-limited, or malformed responses degrade silently.
      if (cancelled || !installed || !upstream) return;
      if (!isUpstreamCoreNewer(upstream.version, installed)) return;
      if (!claimUpstreamNotice(upstream.version)) return;
      log.info('Upstream release is newer than this fork build', { upstream: upstream.version, installed });
      const t = tRef.current;
      notificationService.info(t('update.upstreamAvailableMessage', { version: upstream.version, current: installed }), {
        title: t('update.upstreamAvailableTitle'),
        duration: 0,
        actions: [{
          label: t('update.upstreamViewRelease'),
          onClick: () => { void systemAPI.openExternal(upstream.url).catch(() => undefined); },
        }],
      });
    };
    const first = setTimeout(() => void check(), FIRST_CHECK_DELAY_MS);
    const interval = setInterval(() => void check(), UPSTREAM_WATCH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearTimeout(first);
      clearInterval(interval);
    };
  }, [repo]);

  return null;
}
