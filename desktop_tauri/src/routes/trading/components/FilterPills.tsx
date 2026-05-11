// desktop_tauri/src/routes/trading/components/FilterPills.tsx
import { useTradingStore } from "../store";

const PILLS: { id: "all" | "queued" | "excluded" | "blocked"; label: string }[] = [
  { id: "all", label: "All" },
  { id: "queued", label: "Queued" },
  { id: "excluded", label: "Excluded" },
  { id: "blocked", label: "Blocked" },
];

export function FilterPills() {
  const filter = useTradingStore((s) => s.filter);
  const setFilter = useTradingStore((s) => s.setFilter);
  const snapshot = useTradingStore((s) => s.snapshot);
  const counts = {
    all: snapshot?.picks.length ?? 0,
    queued: snapshot?.picks.filter((p) => p.state === "queued").length ?? 0,
    excluded: snapshot?.kpis.picks.excluded ?? 0,
    blocked: snapshot?.kpis.picks.blocked ?? 0,
  };
  return (
    <div style={{ display: "flex", gap: 6, marginBottom: 10, alignItems: "center", flexWrap: "wrap" }}>
      {PILLS.map((pill) => (
        <button
          key={pill.id}
          type="button"
          className={`filter-pill ${filter === pill.id ? "active" : ""}`}
          onClick={() => setFilter(pill.id)}
        >
          {pill.label} · {counts[pill.id]}
        </button>
      ))}
    </div>
  );
}
