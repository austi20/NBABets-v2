type ConfidenceBarProps = {
  value: number;
  max?: number;
};

export function ConfidenceBar({ value, max = 100 }: ConfidenceBarProps) {
  const safeMax = max <= 0 ? 100 : max;
  const clamped = Math.max(0, Math.min(value, safeMax));
  const pct = (clamped / safeMax) * 100;
  const tone = pct >= 66 ? "positive" : pct >= 40 ? "caution" : "negative";

  return (
    <div
      className="confidence-bar"
      role="progressbar"
      aria-label="Confidence"
      aria-valuemin={0}
      aria-valuemax={safeMax}
      aria-valuenow={clamped}
    >
      <div className={`confidence-fill confidence-${tone}`} style={{ width: `${pct}%` }} />
    </div>
  );
}
