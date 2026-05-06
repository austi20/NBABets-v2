type InjuryStatus = "Q" | "D" | "O" | "Healthy";

type InjuryPillProps = {
  status: InjuryStatus;
};

const STATUS_META: Record<InjuryStatus, { tone: "caution" | "negative" | "positive"; label: string }> = {
  Q: { tone: "caution", label: "Questionable" },
  D: { tone: "negative", label: "Doubtful" },
  O: { tone: "negative", label: "Out" },
  Healthy: { tone: "positive", label: "Healthy" },
};

export function InjuryPill({ status }: InjuryPillProps) {
  const meta = STATUS_META[status];
  return (
    <span className={`injury-pill injury-${meta.tone}`} aria-label={`Injury status ${meta.label}`}>
      {status}
    </span>
  );
}
