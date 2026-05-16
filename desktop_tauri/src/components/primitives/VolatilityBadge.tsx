export type VolatilityTier = "low" | "medium" | "high";

interface Contributor {
  name: string;
  contribution: number;
}

interface VolatilityBadgeProps {
  tier: VolatilityTier;
  coefficient: number;
  contributors?: Contributor[];
  reason?: string;
}

const TIER_LABEL: Record<VolatilityTier, string> = {
  low: "Low",
  medium: "Medium",
  high: "High",
};

export function VolatilityBadge({
  tier,
  coefficient,
  contributors,
  reason,
}: VolatilityBadgeProps) {
  if (reason === "insufficient_features") {
    return (
      <span
        className="volatility-badge volatility-unknown tabular"
        title="Limited data available to score this prop"
        aria-label="Limited data volatility"
      >
        Limited data
      </span>
    );
  }

  const top = (contributors ?? []).slice(0, 3);
  const tooltip = top.length
    ? top
        .map((c) => `${c.name}: ${(c.contribution * 100).toFixed(0)}%`)
        .join("  •  ")
    : `Volatility ${coefficient.toFixed(2)}`;
  const label = TIER_LABEL[tier];

  return (
    <span
      className={`volatility-badge volatility-${tier} tabular`}
      title={tooltip}
      aria-label={`${label} volatility, coefficient ${coefficient.toFixed(2)}`}
    >
      {label}
    </span>
  );
}
