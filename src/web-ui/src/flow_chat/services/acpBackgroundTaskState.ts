import { create } from 'zustand';
import {
  ACPClientAPI,
  type AcpBackgroundTask,
} from '@/infrastructure/api/service-api/ACPClientAPI';

// Runtime presentation only: the latest task list each native session published.
export const useAcpBackgroundTaskStore = create<{ tasks: Map<string, AcpBackgroundTask[]> }>(() => ({
  tasks: new Map(),
}));

export function setAcpBackgroundTasks(sessionId: string, tasks: AcpBackgroundTask[]): void {
  useAcpBackgroundTaskStore.setState(({ tasks: current }) => ({
    tasks: new Map(current).set(sessionId, tasks),
  }));
}

/** Seed from a session options snapshot only when no live update has arrived yet. */
export function hydrateAcpBackgroundTasks(sessionId: string, tasks: AcpBackgroundTask[] | undefined): void {
  if (!tasks || useAcpBackgroundTaskStore.getState().tasks.has(sessionId)) return;
  setAcpBackgroundTasks(sessionId, tasks);
}

export function isAcpBackgroundTaskRunning(task: AcpBackgroundTask): boolean {
  return task.status === 'running' || task.status === 'pending' || task.status === 'paused';
}

export function initializeAcpBackgroundTaskState(): () => void {
  const unlisten = ACPClientAPI.onBackgroundTasksUpdated((event) => {
    if (event?.sessionId && Array.isArray(event.tasks)) {
      setAcpBackgroundTasks(event.sessionId, event.tasks);
    }
  });
  return () => {
    unlisten();
    useAcpBackgroundTaskStore.setState({ tasks: new Map() });
  };
}
