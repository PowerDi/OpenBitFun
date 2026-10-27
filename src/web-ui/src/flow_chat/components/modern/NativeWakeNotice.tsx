import { BellRing } from 'lucide-react';
import { useI18n } from '@/infrastructure/i18n';
import './NativeWakeNotice.scss';

interface NativeWakeTask {
  description?: string;
  status?: string;
}

interface NativeWakeMetadata extends NativeWakeTask {
  source?: string;
  taskType?: string | null;
  subagentType?: string | null;
  threads?: NativeWakeTask[];
}

/**
 * A native CLI (Claude Code, Codex) resumed the agent on its own after
 * background work finished. The turn has no user prompt, so it renders as a
 * system notice rather than a user bubble.
 */
export function NativeWakeNotice({ wake, fallback }: { wake: NativeWakeMetadata; fallback: string }) {
  const { t } = useI18n('flow-chat');
  let title: string;
  let detail: string;
  if (wake.source === 'subagent_thread') {
    const threads = wake.threads ?? [];
    title = t(threads.length > 1 ? 'nativeWake.agentsFinished' : 'nativeWake.agentFinished');
    detail = threads
      .map(thread => (thread.status && thread.status !== 'completed'
        ? `${thread.description ?? ''} (${t('shared:statuses.failed')})`
        : thread.description ?? ''))
      .filter(Boolean)
      .join('; ');
  } else if (wake.source === 'background_task') {
    const isAgent = wake.taskType === 'local_agent' || Boolean(wake.subagentType);
    const failed = wake.status !== 'completed';
    title = t(isAgent
      ? (failed ? 'nativeWake.agentFailed' : 'nativeWake.agentFinished')
      : (failed ? 'nativeWake.taskFailed' : 'nativeWake.taskFinished'));
    detail = wake.description?.trim() ?? '';
  } else {
    title = fallback;
    detail = '';
  }
  return (
    <div className="native-wake-notice" role="note"
      data-openbitfun-product-component="native-wake-notice" data-openbitfun-product-part="root">
      <BellRing className="native-wake-notice__icon" size={12} aria-hidden="true" />
      <span className="native-wake-notice__title">{title}</span>
      {detail ? <span className="native-wake-notice__detail" title={detail}>{detail}</span> : null}
    </div>
  );
}
