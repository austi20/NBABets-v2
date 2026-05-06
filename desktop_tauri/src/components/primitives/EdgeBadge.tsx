type EdgeTone = "positive" | "caution" | "negative";

type EdgeBadgeProps = {
  edge: number;
};

function edgeTone(edge: number): EdgeTone {
  if (edge >= 5) {
    return "positive";
  }
  if (edge >= 1) {
    return "caution";
  }
  return "negative";
}

export function EdgeBadge({ edge }: EdgeBadgeProps) {
  const tone = edgeTone(edge);
  const sign = edge >= 0 ? "+" : "";
  const ariaLabel =
    tone === "positive"
      ? `Positive edge ${sign}${edge.toFixed(1)} percent`
      : tone === "caution"
        ? `Caution edge ${sign}${edge.toFixed(1)} percent`
        : `Negative edge ${sign}${edge.toFixed(1)} percent`;

  return (
    <span className={`edge-badge edge-${tone} tabular`} aria-label={ariaLabel}>
      {sign}
      {edge.toFixed(1)}%
    </span>
  );
}
