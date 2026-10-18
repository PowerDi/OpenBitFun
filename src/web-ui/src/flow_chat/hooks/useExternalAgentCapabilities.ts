import { useCallback, useSyncExternalStore } from 'react';
import { ACPClientAPI } from '@/infrastructure/api/service-api/ACPClientAPI';
import { onSurfaceActivated } from '@/infrastructure/peer-device/deviceSurface';
import { FlowChatStore } from '../store/FlowChatStore';
import { resolveExternalAgentCapabilities } from '../utils/externalAgentCapabilities';

/** Reads already-loaded client/session advertisements without fetching them. */
export function useExternalAgentCapabilities(sessionId: string | null | undefined) {
  useSyncExternalStore(ACPClientAPI.subscribeCapabilities, ACPClientAPI.getCapabilityRevision, ACPClientAPI.getCapabilityRevision);
  const subscribe = useCallback((notify: () => void) => {
    let unsubscribeStore = FlowChatStore.getInstance().subscribe(notify);
    const unsubscribeSurface = onSurfaceActivated(() => {
      unsubscribeStore();
      unsubscribeStore = FlowChatStore.getInstance().subscribe(notify);
      notify();
    });
    return () => { unsubscribeStore(); unsubscribeSurface(); };
  }, []);
  const getSession = useCallback(() => sessionId
    ? FlowChatStore.getInstance().getState().sessions.get(sessionId)
    : undefined, [sessionId]);
  const session = useSyncExternalStore(subscribe, getSession, getSession);
  return resolveExternalAgentCapabilities(session);
}
