type FreshnessBadgeProps = {
  fetchedAt: string | Date | null;
  cautionAfterMinutes?: number;
};

function formatAgeMs(ageMs: number): string {
  const minute = 60_000;
  const hour = 60 * minute;
  if (ageMs < minute) {
    return "just now";
  }
  if (ageMs < hour) {
    return `${Math.floor(ageMs / minute)}m ago`;
  }
  return `${Math.floor(ageMs / hour)}h ago`;
}

export function FreshnessBadge({ fetchedAt, cautionAfterMinutes = 5 }: FreshnessBadgeProps) {
  if (!fetchedAt) {
    return <span className="freshness-badge freshness-caution">Updated --</span>;
  }

  const at = fetchedAt instanceof Date ? fetchedAt : new Date(fetchedAt);
  const ageMs = Math.max(0, Date.now() - at.getTime());
  const cautionMs = cautionAfterMinutes * 60_000;
  const tone = ageMs > cautionMs ? "caution" : "positive";
  return (
    <span className={`freshness-badge freshness-${tone}`} aria-label={`Data updated ${formatAgeMs(ageMs)}`}>
      Updated {formatAgeMs(ageMs)}
    </span>
  );
}
