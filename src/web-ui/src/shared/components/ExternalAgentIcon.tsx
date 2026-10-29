import { ProductBrandIcon } from '@/shared/ui/ProductBrandIcon';
import { getProductBrandIcon } from '@/shared/ui/productBrandIcons';

/** ACP client id -> shared product icon id. Native drivers reuse their product's mark. */
const EXTERNAL_AGENT_PRODUCT_IDS: Record<string, string> = {
  'claude-native': 'claude-code',
  'codex-native': 'codex',
  'claude-code': 'claude-code',
  codex: 'codex',
  dsh: 'dsh',
  opencode: 'opencode',
};

/**
 * The shared catalog's Pi asset is the dark app tile; session rows use the
 * tile-free glyph so the mark follows the interface theme like the Codex symbol.
 */
const PI_MARK_SOURCE = '/assets/ecosystem-compatibility/pi-mark.svg';
const PI_CLIENT_IDS = new Set(['pi-native']);

export function externalAgentIconSource(clientId: string | null | undefined): string | null {
  if (!clientId) return null;
  if (PI_CLIENT_IDS.has(clientId)) return PI_MARK_SOURCE;
  const productId = EXTERNAL_AGENT_PRODUCT_IDS[clientId];
  return productId ? getProductBrandIcon(productId)?.src ?? null : null;
}

export function ExternalAgentIcon({ clientId, size = 14, className }: {
  clientId: string;
  size?: number;
  className?: string;
}) {
  if (PI_CLIENT_IDS.has(clientId)) return (
    <span
      className={className}
      aria-hidden="true"
      data-external-agent={clientId}
      style={{
        display: 'inline-block',
        width: size,
        height: size,
        backgroundColor: 'currentColor',
        maskImage: `url("${PI_MARK_SOURCE}")`,
        maskSize: 'contain',
        maskPosition: 'center',
        maskRepeat: 'no-repeat',
      }}
    />
  );
  const productId = EXTERNAL_AGENT_PRODUCT_IDS[clientId];
  if (!productId || !getProductBrandIcon(productId)) return null;
  return (
    <span
      className={className}
      aria-hidden="true"
      data-external-agent={clientId}
      style={{ display: 'inline-flex', width: size, height: size }}
    >
      <ProductBrandIcon productId={productId} size={size} />
    </span>
  );
}
