// desktop_tauri/src/routes/trading/components/BulkActions.tsx
import { tradingActions } from "../api/actions";

export function BulkActions() {
  return (
    <div style={{ display: "flex", gap: 6 }}>
      <button type="button" className="btn-trading ghost" onClick={() => void tradingActions.bulk("select_all_hittable")}>
        Select all hittable
      </button>
      <button type="button" className="btn-trading ghost" onClick={() => void tradingActions.bulk("deselect_all")}>
        Deselect all
      </button>
      <button type="button" className="btn-trading ghost" onClick={() => void tradingActions.bulk("top_n", 5)}>
        Top 5
      </button>
    </div>
  );
}
