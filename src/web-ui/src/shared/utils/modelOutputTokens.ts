const AUTOMATIC_MAX_OUTPUT_TOKEN_TIERS = [64_000, 32_000, 24_000, 16_000, 8_000] as const;
const MAX_OUTPUT_64K_MIN_CONTEXT_WINDOW = 400_000;

/** Mirrors automatic_max_output_tokens in config-contracts. */
export function automaticMaxOutputTokens(contextWindow: number): number {
  const quarterContext = Math.floor(contextWindow / 4);
  return AUTOMATIC_MAX_OUTPUT_TOKEN_TIERS.find(tier =>
    tier <= quarterContext && (tier !== 64_000 || contextWindow >= MAX_OUTPUT_64K_MIN_CONTEXT_WINDOW),
  ) ?? quarterContext;
}
