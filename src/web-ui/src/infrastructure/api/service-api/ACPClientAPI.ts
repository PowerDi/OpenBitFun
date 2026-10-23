import { api } from './ApiClient';
import { getActiveSurfaceScope, onSurfaceActivated } from '@/infrastructure/peer-device/deviceSurface';
import type { ImageContextData as ImageInputContextData } from './ImageContextTypes';

export type AcpClientPermissionMode = 'ask' | 'allow_once' | 'reject_once';
export type AcpClientStatus = 'configured' | 'starting' | 'running' | 'stopped' | 'failed';

export interface AcpClientSubagentConfig {
  enabled: boolean;
  description?: string;
  bestFor?: string;
}

export interface NativeAgentCapabilities {
  selectModel: boolean;
  permissionMode: boolean;
  thinking: boolean;
  commands: boolean;
  hostCommands: boolean;
  voiceHistory: boolean;
  sideChat: boolean;
  subagents: {
    observe: boolean;
    readTranscript: boolean;
    interact: boolean;
  };
  autonomousTurns: boolean;
  history: {
    rollbackLastTurn: boolean;
    fork: boolean;
    importTranscripts: boolean;
  };
  input: {
    images: boolean;
    steering: boolean;
    queue: boolean;
  };
  compact: boolean;
  plan: boolean;
  contextUsage: boolean;
  backgroundTasks: boolean;
  threadGoal: boolean;
  worktree: boolean;
  dispatch: boolean;
  mcpSelection: boolean;
  skills: boolean;
}

export interface AcpClientInfo {
  capabilities?: NativeAgentCapabilities;
  id: string;
  name: string;
  command: string;
  args: string[];
  enabled: boolean;
  readonly: boolean;
  /** Missing on older hosts; absence preserves the historical enabled behavior. */
  subagent?: AcpClientSubagentConfig;
  permissionMode: AcpClientPermissionMode;
  status: AcpClientStatus;
  toolName: string;
  sessionCount: number;
}

export interface AcpRequirementProbeItem {
  name: string;
  installed: boolean;
  version?: string;
  path?: string;
  error?: string;
}

export type NativeCliInstallMethod = 'native' | 'npm' | 'homebrew' | 'winget' | 'unknown';

/** Install/version/update facts for a native driver's CLI; absent on older hosts. */
export interface NativeCliStatus {
  installMethod: NativeCliInstallMethod;
  version?: string | null;
  minVersion: string;
  tooOld: boolean;
  updateSupported: boolean;
  installCommand?: string | null;
  docsUrl?: string | null;
}

export interface AcpClientRequirementProbe {
  id: string;
  tool: AcpRequirementProbeItem;
  adapter?: AcpRequirementProbeItem;
  runnable: boolean;
  notes: string[];
  nativeCli?: NativeCliStatus;
}

export interface AcpClientIdRequest {
  clientId: string;
  remoteConnectionId?: string;
}

export interface UpdateAcpClientSubagentConfigRequest extends AcpClientSubagentConfig {
  clientId: string;
}

export interface CreateAcpFlowSessionRequest {
  clientId: string;
  sessionName?: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
}

export interface CreateAcpFlowSessionResponse {
  sessionId: string;
  sessionName: string;
  agentType: string;
}

export interface StartAcpDialogTurnRequest {
  sessionId: string;
  clientId: string;
  userInput: string;
  originalUserInput?: string;
  turnId: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath?: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
  timeoutSeconds?: number;
  imageContexts?: ImageInputContextData[];
  userMessageMetadata?: Record<string, unknown>;
}

export interface CancelAcpDialogTurnRequest {
  sessionId: string;
  clientId: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath?: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
}

export interface RollbackAcpSessionTurnRequest {
  sessionId: string;
  clientId: string;
  /** Latest turn of the session; the host refuses any other turn. */
  turnId: string;
  workspaceId: string;
}

export interface RollbackAcpSessionTurnResponse {
  filesChanged: string[];
  filesRewound: boolean;
  /** Why workspace files were left untouched, when they were. */
  note?: string | null;
}

export interface ForkAcpSessionRequest {
  sourceSessionId: string;
  clientId: string;
  turnId: string;
  workspaceId: string;
}

export interface GetAcpSessionOptionsRequest {
  sessionId: string;
  clientId: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath?: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
}

export interface SetAcpSessionModelRequest {
  sessionId: string;
  clientId: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath?: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
  modelId: string;
}

export type AcpSessionConfigValue =
  | { type: 'select'; value: string }
  | { type: 'boolean'; value: boolean };

export interface SetAcpSessionConfigOptionRequest {
  sessionId: string;
  clientId: string;
  /** Workspace identity; authoritative when present. Paths below are IO projections. */
  workspaceId?: string;
  workspacePath?: string;
  remoteConnectionId?: string;
  remoteSshHost?: string;
  configId: string;
  value: AcpSessionConfigValue;
}

export interface AcpSessionModelOption {
  id: string;
  name: string;
  providerName?: string;
  description?: string;
}

export interface AcpSessionConfigSelectOption {
  value: string;
  name: string;
  description?: string;
}

interface AcpSessionConfigOptionBase {
  id: string;
  name: string;
  description?: string;
  category?: 'mode' | 'model' | 'thought_level' | string;
}

export type AcpSessionConfigOption = AcpSessionConfigOptionBase & (
  | {
      type: 'select';
      currentValue: string;
      options: AcpSessionConfigSelectOption[];
    }
  | {
      type: 'boolean';
      currentValue: boolean;
    }
);

export interface AcpContextUsage {
  used: number;
  size: number;
  cost?: {
    amount: number;
    currency: string;
  };
}

export interface AcpSessionOptions {
  capabilities?: NativeAgentCapabilities;
  currentModelId?: string;
  availableModels: AcpSessionModelOption[];
  modelConfigId?: string;
  configOptions: AcpSessionConfigOption[];
  contextUsage?: AcpContextUsage;
  /** Native agents advertising `backgroundTasks`; omitted when empty. */
  backgroundTasks?: AcpBackgroundTask[];
}

/** One background task run by an external agent (background shell, subagent, ...). */
export interface AcpBackgroundTask {
  id: string;
  /** Agent-specific kind, e.g. Claude `local_bash` / `local_agent`. */
  taskType?: string | null;
  subagentType?: string | null;
  description: string;
  toolUseId?: string | null;
  /** `running`, `completed`, `failed`, `killed` (or another agent status). */
  status: string;
  startedAtMs: number;
  endedAtMs?: number | null;
  summary?: string | null;
  outputFile?: string | null;
}

export interface AcpBackgroundTasksUpdatedEvent {
  sessionId: string;
  clientId: string;
  tasks: AcpBackgroundTask[];
}

export interface StopAcpSessionTaskRequest {
  sessionId: string;
  clientId: string;
  taskId: string;
}

export interface SteerAcpDialogTurnRequest {
  sessionId: string;
  clientId: string;
  dialogTurnId: string;
  content: string;
  displayContent?: string;
  imageContexts?: unknown[];
}

export interface SteerAcpDialogTurnResponse {
  success: boolean;
  steeringId: string;
}

export interface AcpAvailableCommand {
  name: string;
  description: string;
  inputHint?: string;
}

export interface AcpAvailableCommandsUpdatedEvent {
  sessionId: string;
  clientId: string;
  commands: AcpAvailableCommand[];
}

export interface AcpPlanEntry {
  content: string;
  priority: 'high' | 'medium' | 'low' | string;
  status: 'pending' | 'in_progress' | 'completed' | string;
}

export interface AcpPlanUpdatedEvent {
  sessionId: string;
  turnId: string;
  clientId: string;
  entries: AcpPlanEntry[];
}

export interface AcpSessionOptionsChangedEvent {
  sessionId: string;
  clientId: string;
}

export interface SubmitAcpPermissionResponseRequest {
  permissionId: string;
  approve: boolean;
  optionId?: string;
  /** Native question answers keyed by question index (or question text). */
  answers?: Record<string, string | string[]>;
  /** Optional free-text feedback sent with a rejection. */
  feedback?: string;
}

export interface AcpPermissionOption {
  optionId: string;
  name: string;
  kind: 'allow_once' | 'allow_session' | 'allow_always' | 'reject_once' | 'reject_always';
}

export interface AcpPermissionRequestEvent {
  permissionId: string;
  sessionId: string;
  toolCall?: {
    toolCallId?: string;
    title?: string;
    rawInput?: unknown;
    content?: unknown;
  };
  options?: AcpPermissionOption[];
}

const LOCAL_REQUIREMENT_CACHE_KEY = '__local__';
const CLIENT_LIST_CACHE_TTL_MS = 1000;
const requirementProbeCache = new Map<string, AcpClientRequirementProbe[]>();
const requirementProbeInFlight = new Map<string, Promise<AcpClientRequirementProbe[]>>();

export class ACPClientAPI {
  private static cacheScope = getActiveSurfaceScope();
  private static capabilityRevision = 0;
  private static capabilityListeners = new Set<() => void>();
  private static unsubscribeCapabilitySurface: (() => void) | null = null;
  private static capabilityClients: readonly AcpClientInfo[] = [];
  private static sessionOptionsCache = new Map<string, AcpSessionOptions>();

  private static sessionOptionsKey(request: Pick<GetAcpSessionOptionsRequest, 'sessionId' | 'clientId' | 'remoteConnectionId'>): string {
    return JSON.stringify([request.sessionId, request.clientId, request.remoteConnectionId ?? '']);
  }

  private static publishCapabilities(): void {
    ACPClientAPI.capabilityRevision += 1;
    ACPClientAPI.capabilityListeners.forEach(listener => listener());
  }

  /** Observes existing API responses; subscribing never issues a host request. */
  static subscribeCapabilities = (listener: () => void): (() => void) => {
    ACPClientAPI.capabilityListeners.add(listener);
    ACPClientAPI.unsubscribeCapabilitySurface ??= onSurfaceActivated(() => {
      ACPClientAPI.prepareCacheScope();
      ACPClientAPI.publishCapabilities();
    });
    return () => {
      ACPClientAPI.capabilityListeners.delete(listener);
      if (ACPClientAPI.capabilityListeners.size === 0) {
        ACPClientAPI.unsubscribeCapabilitySurface?.();
        ACPClientAPI.unsubscribeCapabilitySurface = null;
      }
    };
  };

  static getCapabilityRevision = (): number => {
    ACPClientAPI.prepareCacheScope();
    return ACPClientAPI.capabilityRevision;
  };

  static getCachedCapabilityClients(): readonly AcpClientInfo[] {
    ACPClientAPI.prepareCacheScope();
    return ACPClientAPI.capabilityClients;
  }

  static getCachedSessionOptions(request: Pick<GetAcpSessionOptionsRequest, 'sessionId' | 'clientId' | 'remoteConnectionId'>): AcpSessionOptions | undefined {
    ACPClientAPI.prepareCacheScope();
    return ACPClientAPI.sessionOptionsCache.get(ACPClientAPI.sessionOptionsKey(request));
  }

  private static async cacheSessionOptions(
    command: string,
    request: GetAcpSessionOptionsRequest,
  ): Promise<AcpSessionOptions> {
    const scope = ACPClientAPI.prepareCacheScope();
    const options = await api.invoke<AcpSessionOptions>(command, { request });
    scope.assertCurrent('cache ACP session capabilities');
    ACPClientAPI.sessionOptionsCache.set(ACPClientAPI.sessionOptionsKey(request), options);
    ACPClientAPI.publishCapabilities();
    return options;
  }

  private static clientListCache: {
    clients: AcpClientInfo[];
    expiresAt: number;
  } | null = null;

  private static clientListInFlight: Promise<AcpClientInfo[]> | null = null;

  private static invalidateClientListCache(): void {
    ACPClientAPI.clientListCache = null;
    ACPClientAPI.clientListInFlight = null;
  }

  private static invalidateRequirementProbeCache(): void {
    requirementProbeCache.clear();
    requirementProbeInFlight.clear();
  }

  private static prepareCacheScope() {
    const scope = getActiveSurfaceScope();
    if (ACPClientAPI.cacheScope.epoch !== scope.epoch) {
      ACPClientAPI.invalidateClientListCache();
      ACPClientAPI.invalidateRequirementProbeCache();
      ACPClientAPI.capabilityClients = [];
      ACPClientAPI.sessionOptionsCache.clear();
      ACPClientAPI.capabilityRevision += 1;
      ACPClientAPI.cacheScope = scope;
    }
    return scope;
  }

  static async initializeClients(): Promise<void> {
    const scope = getActiveSurfaceScope();
    await api.invoke('initialize_acp_clients');
    scope.assertCurrent('publish ACP client changes');
    ACPClientAPI.invalidateClientListCache();
    ACPClientAPI.invalidateRequirementProbeCache();
    window.dispatchEvent(new Event('openbitfun:acp-clients-changed'));
  }

  static async getClients(): Promise<AcpClientInfo[]> {
    const scope = ACPClientAPI.prepareCacheScope();
    const now = Date.now();
    if (ACPClientAPI.clientListCache && ACPClientAPI.clientListCache.expiresAt > now) {
      return ACPClientAPI.clientListCache.clients;
    }
    if (ACPClientAPI.clientListInFlight) {
      return ACPClientAPI.clientListInFlight;
    }

    const inFlight = api.invoke<AcpClientInfo[]>('get_acp_clients')
      .then((clients) => {
        scope.assertCurrent('cache ACP clients');
        ACPClientAPI.clientListCache = {
          clients,
          expiresAt: Date.now() + CLIENT_LIST_CACHE_TTL_MS,
        };
        ACPClientAPI.capabilityClients = clients;
        ACPClientAPI.publishCapabilities();
        return clients;
      })
      .finally(() => {
        if (ACPClientAPI.clientListInFlight === inFlight) {
          ACPClientAPI.clientListInFlight = null;
        }
      });

    ACPClientAPI.clientListInFlight = inFlight;
    return inFlight;
  }

  static async probeClientRequirements(
    options: { force?: boolean; remoteConnectionId?: string } = {}
  ): Promise<AcpClientRequirementProbe[]> {
    const scope = ACPClientAPI.prepareCacheScope();
    const cacheKey = options.remoteConnectionId || LOCAL_REQUIREMENT_CACHE_KEY;
    if (!options.force && requirementProbeCache.has(cacheKey)) {
      return requirementProbeCache.get(cacheKey) ?? [];
    }
    if (!options.force && requirementProbeInFlight.has(cacheKey)) {
      return requirementProbeInFlight.get(cacheKey)!;
    }

    const request = options.remoteConnectionId
      ? { remoteConnectionId: options.remoteConnectionId, forceRefresh: options.force === true }
      : {};

    const inFlight = api.invoke<AcpClientRequirementProbe[]>('probe_acp_client_requirements', { request })
      .then((probes) => {
        scope.assertCurrent('cache ACP client requirements');
        requirementProbeCache.set(cacheKey, probes);
        window.dispatchEvent(new Event('openbitfun:acp-requirements-changed'));
        return probes;
      })
      .finally(() => {
        if (requirementProbeInFlight.get(cacheKey) === inFlight) {
          requirementProbeInFlight.delete(cacheKey);
        }
      });

    requirementProbeInFlight.set(cacheKey, inFlight);
    return inFlight;
  }

  static async predownloadClientAdapter(request: AcpClientIdRequest): Promise<void> {
    const scope = getActiveSurfaceScope();
    await api.invoke('predownload_acp_client_adapter', { request });
    scope.assertCurrent('publish ACP client requirements');
    ACPClientAPI.invalidateClientListCache();
    ACPClientAPI.invalidateRequirementProbeCache();
    window.dispatchEvent(new Event('openbitfun:acp-requirements-changed'));
  }

  static async installClientCli(request: AcpClientIdRequest): Promise<void> {
    const scope = getActiveSurfaceScope();
    await api.invoke('install_acp_client_cli', { request });
    scope.assertCurrent('publish ACP client requirements');
    ACPClientAPI.invalidateClientListCache();
    ACPClientAPI.invalidateRequirementProbeCache();
    window.dispatchEvent(new Event('openbitfun:acp-requirements-changed'));
  }

  /** Runs the native agent's own CLI update locally; resolves to the CLI output. */
  static async updateNativeClientCli(request: AcpClientIdRequest): Promise<string> {
    const scope = getActiveSurfaceScope();
    const output = await api.invoke<string>('update_native_agent_cli', { request });
    scope.assertCurrent('publish ACP client requirements');
    ACPClientAPI.invalidateRequirementProbeCache();
    window.dispatchEvent(new Event('openbitfun:acp-requirements-changed'));
    return output;
  }

  static async stopClient(request: AcpClientIdRequest): Promise<void> {
    const scope = getActiveSurfaceScope();
    await api.invoke('stop_acp_client', { request });
    scope.assertCurrent('publish ACP client changes');
    ACPClientAPI.invalidateClientListCache();
    window.dispatchEvent(new Event('openbitfun:acp-clients-changed'));
  }

  static async loadJsonConfig(): Promise<string> {
    return api.invoke('load_acp_json_config');
  }

  static async saveJsonConfig(jsonConfig: string): Promise<void> {
    const scope = getActiveSurfaceScope();
    await api.invoke('save_acp_json_config', { jsonConfig });
    scope.assertCurrent('publish ACP client changes');
    ACPClientAPI.invalidateClientListCache();
    ACPClientAPI.invalidateRequirementProbeCache();
    window.dispatchEvent(new Event('openbitfun:acp-clients-changed'));
  }

  static async updateClientSubagentConfig(
    request: UpdateAcpClientSubagentConfigRequest
  ): Promise<void> {
    const scope = getActiveSurfaceScope();
    const jsonConfig = await ACPClientAPI.loadJsonConfig();
    scope.assertCurrent('update ACP client configuration');
    const rawConfig = JSON.parse(jsonConfig) as unknown;
    if (!rawConfig || typeof rawConfig !== 'object' || Array.isArray(rawConfig)) {
      throw new Error('ACP client configuration is invalid');
    }

    const config = rawConfig as Record<string, unknown>;
    const rawClients = config.acpClients;
    if (!rawClients || typeof rawClients !== 'object' || Array.isArray(rawClients)) {
      throw new Error('ACP client configuration has no client registry');
    }

    const clients = rawClients as Record<string, unknown>;
    const rawClient = clients[request.clientId];
    if (!rawClient || typeof rawClient !== 'object' || Array.isArray(rawClient)) {
      throw new Error(`ACP client '${request.clientId}' is not configured`);
    }

    const description = request.description?.trim();
    const bestFor = request.bestFor?.trim();
    const client = rawClient as Record<string, unknown>;
    client.subagent = {
      enabled: request.enabled,
      ...(description ? { description } : {}),
      ...(bestFor ? { bestFor } : {}),
    };

    await ACPClientAPI.saveJsonConfig(JSON.stringify(config, null, 2));
  }

  static async submitPermissionResponse(
    request: SubmitAcpPermissionResponseRequest
  ): Promise<void> {
    return api.invoke('submit_acp_permission_response', { request });
  }

  static async createFlowSession(
    request: CreateAcpFlowSessionRequest
  ): Promise<CreateAcpFlowSessionResponse> {
    const scope = getActiveSurfaceScope();
    const response = await api.invoke<CreateAcpFlowSessionResponse>('create_acp_flow_session', { request });
    scope.assertCurrent('publish ACP client changes');
    ACPClientAPI.invalidateClientListCache();
    window.dispatchEvent(new Event('openbitfun:acp-clients-changed'));
    return response;
  }

  static async startDialogTurn(request: StartAcpDialogTurnRequest): Promise<void> {
    return api.invoke('start_acp_dialog_turn', { request });
  }

  static async cancelDialogTurn(request: CancelAcpDialogTurnRequest): Promise<void> {
    return api.invoke('cancel_acp_dialog_turn', { request });
  }

  /** Native agents advertising `history.rollbackLastTurn` only. */
  static async rollbackSessionTurn(
    request: RollbackAcpSessionTurnRequest,
  ): Promise<RollbackAcpSessionTurnResponse> {
    return api.invoke('rollback_acp_session_turn', { request });
  }

  /** Native agents advertising `backgroundTasks` only. */
  static async stopSessionTask(request: StopAcpSessionTaskRequest): Promise<void> {
    return api.invoke('stop_acp_session_task', { request });
  }

  /** Native agents advertising `input.steering` only. */
  static async steerDialogTurn(request: SteerAcpDialogTurnRequest): Promise<SteerAcpDialogTurnResponse> {
    return api.invoke('steer_acp_dialog_turn', { request });
  }

  /** Native agents advertising `history.fork` only. */
  static async forkSession(request: ForkAcpSessionRequest): Promise<CreateAcpFlowSessionResponse> {
    return api.invoke('fork_acp_session', { request });
  }

  /** Native agents advertising `history.importTranscripts` only; local workspaces. */
  static async continueImportedSession(request: ForkAcpSessionRequest): Promise<CreateAcpFlowSessionResponse> {
    return api.invoke('continue_imported_acp_session', { request });
  }

  static async getSessionOptions(
    request: GetAcpSessionOptionsRequest
  ): Promise<AcpSessionOptions> {
    return ACPClientAPI.cacheSessionOptions('get_acp_session_options', request);
  }

  static async getSessionCommands(
    request: GetAcpSessionOptionsRequest
  ): Promise<AcpAvailableCommand[]> {
    return api.invoke('get_acp_session_commands', { request });
  }

  static async setSessionModel(
    request: SetAcpSessionModelRequest
  ): Promise<AcpSessionOptions> {
    return ACPClientAPI.cacheSessionOptions('set_acp_session_model', request);
  }

  static async setSessionConfigOption(
    request: SetAcpSessionConfigOptionRequest
  ): Promise<AcpSessionOptions> {
    return ACPClientAPI.cacheSessionOptions('set_acp_session_config_option', request);
  }

  static onAvailableCommandsUpdated(
    callback: (event: AcpAvailableCommandsUpdatedEvent) => void
  ): () => void {
    return api.listen<AcpAvailableCommandsUpdatedEvent>(
      'agentic://acp-available-commands-updated',
      callback
    );
  }

  static onPlanUpdated(callback: (event: AcpPlanUpdatedEvent) => void): () => void {
    return api.listen<AcpPlanUpdatedEvent>('agentic://acp-plan-updated', callback);
  }

  static onBackgroundTasksUpdated(
    callback: (event: AcpBackgroundTasksUpdatedEvent) => void
  ): () => void {
    return api.listen<AcpBackgroundTasksUpdatedEvent>(
      'agentic://acp-background-tasks-updated',
      callback
    );
  }

  static onSessionOptionsChanged(
    callback: (event: AcpSessionOptionsChangedEvent) => void
  ): () => void {
    return api.listen<AcpSessionOptionsChangedEvent>(
      'agentic://acp-session-options-changed',
      callback
    );
  }
}

export default ACPClientAPI;
