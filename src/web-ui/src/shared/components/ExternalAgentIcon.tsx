/** Brand logos for external agent clients, keyed by ACP client id. */
const EXTERNAL_AGENT_ICON_SOURCES: Record<string, string> = {
  'claude-native': '/assets/ecosystem-compatibility/claude-code.svg',
  'codex-native': '/assets/ecosystem-compatibility/codex-mark.svg',
  'pi-native': '/assets/ecosystem-compatibility/pi.svg',
  'claude-code': '/assets/ecosystem-compatibility/claude-code.svg',
  codex: '/assets/ecosystem-compatibility/codex-mark.svg',
  dsh: '/assets/ecosystem-compatibility/deepseek-harness.svg',
  opencode: '/assets/ecosystem-compatibility/opencode.svg',
};

/** Single-color marks drawn in the surrounding text color through a mask. */
const MONOCHROME_SOURCES = new Set(['/assets/ecosystem-compatibility/codex-mark.svg']);

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
  if (MONOCHROME_SOURCES.has(source)) return (
    <span
      className={className}
      aria-hidden="true"
      data-external-agent={clientId}
      style={{
        display: 'inline-block',
        width: size,
        height: size,
        backgroundColor: 'currentColor',
        maskImage: `url("${source}")`,
        maskSize: 'contain',
        maskPosition: 'center',
        maskRepeat: 'no-repeat',
      }}
    />
  );
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
