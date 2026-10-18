/** Brand logos for external agent clients, keyed by ACP client id. */
const EXTERNAL_AGENT_ICON_SOURCES: Record<string, string> = {
  'claude-native': '/assets/ecosystem-compatibility/claude-code.svg',
  'claude-code': '/assets/ecosystem-compatibility/claude-code.svg',
  codex: '/assets/ecosystem-compatibility/codex.svg',
  dsh: '/assets/ecosystem-compatibility/deepseek-harness.svg',
  opencode: '/assets/ecosystem-compatibility/opencode.svg',
};

export function externalAgentIconSource(clientId: string | null | undefined): string | null {
  return clientId ? EXTERNAL_AGENT_ICON_SOURCES[clientId] ?? null : null;
}

export function ExternalAgentIcon({ clientId, size = 14, className }: {
  clientId: string;
  size?: number;
  className?: string;
}) {
  const source = externalAgentIconSource(clientId);
  if (!source) return null;
  return (
    <img
      className={className}
      src={source}
      alt=""
      aria-hidden="true"
      width={size}
      height={size}
      draggable={false}
      data-external-agent={clientId}
    />
  );
}
