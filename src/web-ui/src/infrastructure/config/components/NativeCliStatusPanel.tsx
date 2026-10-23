import { Button, OverflowText, Spinner, StatusPill, type StatusPillTone } from '@openbitfun/ui';
import React, { useCallback, useState } from 'react';
import { useI18n } from '@/infrastructure/i18n';
import { useNotification } from '@/shared/notification-system';
import { createLogger } from '@/shared/utils/logger';
import {
  ACPClientAPI,
  type AcpClientRequirementProbe,
  type NativeCliInstallMethod,
} from '../../api/service-api/ACPClientAPI';
import { systemAPI } from '../../api/service-api/SystemAPI';

const log = createLogger('NativeCliStatusPanel');

const INSTALL_METHOD_KEYS: Record<NativeCliInstallMethod, string> = {
  native: 'nativeCli.method.native',
  npm: 'nativeCli.method.npm',
  homebrew: 'nativeCli.method.homebrew',
  winget: 'nativeCli.method.winget',
  unknown: 'nativeCli.method.unknown',
};

export interface NativeCliStatusPanelProps {
  clientId: string;
  name: string;
  probe?: AcpClientRequirementProbe;
  checking: boolean;
  /** Set for an SSH host; the CLI update is not offered there. */
  remoteConnectionId?: string;
  onRefresh: () => void;
}

/**
 * Install/version/update state of a native driver's CLI. Installation is
 * shown as the official command to run by hand; the app never pipes an
 * installer script into a shell.
 */
export const NativeCliStatusPanel: React.FC<NativeCliStatusPanelProps> = ({
  clientId,
  name,
  probe,
  checking,
  remoteConnectionId,
  onRefresh,
}) => {
  const { t } = useI18n('settings/acp-agents');
  const { error: notifyError, success: notifySuccess } = useNotification();
  const [updating, setUpdating] = useState(false);
  const status = probe?.nativeCli;
  const installed = probe?.tool.installed === true;

  const copyInstallCommand = useCallback(async () => {
    if (!status?.installCommand) return;
    try {
      await navigator.clipboard.writeText(status.installCommand);
      notifySuccess(t('nativeCli.copied'));
    } catch (error) {
      log.warn('Failed to copy install command', error);
      notifyError(t('nativeCli.copyFailed'));
    }
  }, [notifyError, notifySuccess, status?.installCommand, t]);

  const openDocs = useCallback(() => {
    if (!status?.docsUrl) return;
    void systemAPI.openExternal(status.docsUrl).catch((error) => {
      log.warn('Failed to open native CLI docs', error);
      notifyError(error instanceof Error ? error.message : String(error), {
        title: t('notifications.openLinkFailed'),
      });
    });
  }, [notifyError, status?.docsUrl, t]);

  const runUpdate = useCallback(async () => {
    setUpdating(true);
    try {
      await ACPClientAPI.updateNativeClientCli({ clientId });
      notifySuccess(t('nativeCli.updateSuccess', { name }));
    } catch (error) {
      log.error('Native CLI update failed', { clientId, error });
      notifyError(error instanceof Error ? error.message : String(error), {
        title: t('nativeCli.updateFailed', { name }),
      });
    } finally {
      setUpdating(false);
      onRefresh();
    }
  }, [clientId, name, notifyError, notifySuccess, onRefresh, t]);

  let tone: StatusPillTone = 'neutral';
  let label = t('registry.checking');
  if (!checking && probe) {
    if (!installed) {
      tone = 'danger';
      label = t('nativeCli.notInstalled');
    } else if (status?.tooOld) {
      tone = 'warning';
      label = t('nativeCli.tooOld');
    } else {
      tone = 'success';
      label = t('registry.ready');
    }
  }

  return (
    <div
      className="openbitfun-acp-agents__registry-row"
      data-openbitfun-component="acp-agents-config"
      data-openbitfun-part="nativeCli"
      data-native-client-id={clientId}
    >
      <div className="openbitfun-acp-agents__registry-main">
        <div className="openbitfun-acp-agents__registry-copy">
          <OverflowText className="openbitfun-acp-agents__registry-name">{name}</OverflowText>
          {checking && !probe ? (
            <Spinner size="sm" />
          ) : installed ? (
            <>
              <p className="openbitfun-acp-agents__registry-description">
                {t('nativeCli.installedSummary', {
                  version: status?.version || probe?.tool.version || t('nativeCli.versionUnknown'),
                  method: t(INSTALL_METHOD_KEYS[status?.installMethod ?? 'unknown']),
                })}
              </p>
              {probe?.tool.path ? (
                <p className="openbitfun-acp-agents__registry-description openbitfun-acp-agents__registry-command">
                  {probe.tool.path}
                </p>
              ) : null}
              {status?.tooOld ? (
                <p className="openbitfun-acp-agents__registry-description" role="alert">
                  {t('nativeCli.tooOldDetail', {
                    version: status.version ?? '',
                    minVersion: status.minVersion,
                  })}
                </p>
              ) : null}
              <p className="openbitfun-acp-agents__registry-description">
                {remoteConnectionId ? t('nativeCli.remoteUpdateUnsupported') : t('nativeCli.updateNote')}
              </p>
            </>
          ) : probe ? (
            <>
              <p className="openbitfun-acp-agents__registry-description">
                {t(remoteConnectionId ? 'nativeCli.installHintRemote' : 'nativeCli.installHint')}
              </p>
              {status?.installCommand ? (
                <code className="openbitfun-acp-agents__registry-description openbitfun-acp-agents__registry-command">
                  {status.installCommand}
                </code>
              ) : null}
            </>
          ) : null}
        </div>
      </div>
      <div className="openbitfun-acp-agents__status-cell">
        <StatusPill tone={tone} aria-label={label}>
          {label}
        </StatusPill>
      </div>
      <div className="openbitfun-acp-agents__confirmation-cell">
        {probe && !installed && status?.installCommand ? (
          <Button variant="outline" size="sm" onClick={() => void copyInstallCommand()}>
            {t('nativeCli.copyCommand')}
          </Button>
        ) : null}
        {probe && !installed && status?.docsUrl ? (
          <Button variant="quiet" size="sm" onClick={openDocs}>
            {t('nativeCli.docs')}
          </Button>
        ) : null}
        {installed && status?.updateSupported && !remoteConnectionId ? (
          <Button
            variant={status.tooOld ? 'primary' : 'outline'}
            size="sm"
            disabled={updating}
            onClick={() => void runUpdate()}
          >
            {updating ? t('nativeCli.updating') : t('nativeCli.update')}
          </Button>
        ) : null}
      </div>
    </div>
  );
};

export default NativeCliStatusPanel;
