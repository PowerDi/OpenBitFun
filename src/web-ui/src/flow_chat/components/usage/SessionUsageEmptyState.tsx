import { Empty } from '@openbitfun/ui';
import { useTranslation } from 'react-i18next';

export function SessionUsageEmptyState() {
  const { t } = useTranslation('flow-chat');
  return (
    <Empty
      data-testid="session-usage-empty"
      role="status"
      title={t('usage.empty.session')}
      description={t('usage.empty.sessionDescription')}
    />
  );
}
