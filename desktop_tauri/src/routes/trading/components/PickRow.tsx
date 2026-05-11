// desktop_tauri/src/routes/trading/components/PickRow.tsx
import { tradingActions } from "../api/actions";
import { useTradingStore } from "../store";
import { usePulseOnChange } from "../hooks/usePulseOnChange";
import type { PickRow as PickRowType } from "../api/types";

type Props = { pick: PickRowType };

function PulseCell({ value, format }: { value: number; format: (v: number) => string }) {
  const pulse = usePulseOnChange(value);
  const cls = pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return <span className={cls}>{format(value)}</span>;
}

const fmtPct = (v: number) => `${(v * 100).toFixed(0)}%`;
const fmtBp = (v: number) => `${v >= 0 ? "+" : ""}${v}bp`;
const fmtUsd = (v: number) => `$${v.toFixed(2)}`;
const fmtUsdSigned = (v: number) => `${v >= 0 ? "+" : ""}$${v.toFixed(2)}`;

export function PickRow({ pick }: Props) {
  const toggleExpand = useTradingStore((s) => s.toggleExpand);

  const onBulletClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (pick.state === "blocked") return;
    await tradingActions.togglePick(pick.candidate_id, !pick.selected);
  };

  const onRowClick = () => toggleExpand(pick.candidate_id);

  const bulletGlyph = pick.state === "blocked" ? "⊘" : pick.selected ? "●" : "○";
  const bulletCls = `pick-bullet ${pick.state === "blocked" ? "blocked" : pick.selected ? "included" : "excluded"}`;
  const rowCls = pick.state === "blocked" ? "blocked" : pick.state === "excluded" ? "excluded" : "";

  return (
    <tr
      className={rowCls}
      onClick={onRowClick}
      tabIndex={0}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onRowClick(); } }}
    >
      <td onClick={(e) => void onBulletClick(e)} style={{ width: 18 }}>
        <span
          className={bulletCls}
          title={pick.state === "blocked" ? (pick.blocker_reason ?? "blocked") : ""}
        >
          {bulletGlyph}
        </span>
      </td>
      <td>
        {pick.prop_label}
        {pick.game_label ? (
          <span style={{ color: "var(--trading-fg-subtle)", fontSize: 9, marginLeft: 8 }}>· {pick.game_label}</span>
        ) : null}
      </td>
      <td style={{ color: "var(--trading-accent-pnl)" }}>
        <PulseCell value={pick.hit_pct} format={fmtPct} />
      </td>
      <td style={{ color: "var(--trading-accent-picks)" }}>
        <PulseCell value={pick.edge_bps} format={fmtBp} />
      </td>
      <td>{pick.alloc > 0 ? <PulseCell value={pick.alloc} format={fmtUsd} /> : "--"}</td>
      <td style={{ color: pick.est_profit >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>
        {pick.est_profit !== 0 ? <PulseCell value={pick.est_profit} format={fmtUsdSigned} /> : "--"}
      </td>
      <td>
        {pick.state === "blocked" ? (
          <span style={{ color: "var(--trading-accent-danger)", fontSize: 9 }}>{pick.blocker_reason}</span>
        ) : pick.state === "queued" ? (
          <span style={{ color: "var(--trading-accent-budget)" }}>queued</span>
        ) : (
          <span style={{ color: "var(--trading-fg-subtle)" }}>{pick.state}</span>
        )}
      </td>
    </tr>
  );
}
