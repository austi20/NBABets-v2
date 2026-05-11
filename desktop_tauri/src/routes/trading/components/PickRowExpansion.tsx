// desktop_tauri/src/routes/trading/components/PickRowExpansion.tsx
import type { PickRow } from "../api/types";

type Props = { pick: PickRow };

export function PickRowExpansion({ pick }: Props) {
  return (
    <tr>
      <td />
      <td colSpan={6} style={{ background: "var(--trading-surface-alt)", padding: 12 }}>
        <div className="mono" style={{ fontSize: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <div>
            <div className="micro">Kalshi quote</div>
            <div style={{ marginTop: 4 }}>
              ticker: {pick.kalshi.ticker ?? "—"}<br />
              yes bid: {pick.kalshi.yes_bid?.toFixed(3) ?? "—"}<br />
              yes ask: {pick.kalshi.yes_ask?.toFixed(3) ?? "—"}<br />
              spread: {pick.kalshi.spread?.toFixed(3) ?? "—"}<br />
              last quote: {pick.kalshi.last_quote_at ?? "—"}
            </div>
          </div>
          <div>
            <div className="micro">Model</div>
            <div style={{ marginTop: 4 }}>
              model prob: {(pick.model_prob * 100).toFixed(1)}%<br />
              market prob: {pick.market_prob !== null ? `${(pick.market_prob * 100).toFixed(1)}%` : "—"}<br />
              edge: +{pick.edge_bps}bp<br />
              rank: #{pick.rank + 1}
            </div>
          </div>
        </div>
        {pick.blocker_reason ? (
          <p style={{ marginTop: 12, color: "var(--trading-accent-danger)", fontSize: 11 }}>
            ⊘ Blocked: {pick.blocker_reason}
          </p>
        ) : null}
      </td>
    </tr>
  );
}
