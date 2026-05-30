// desktop_tauri/src/routes/trading/format.ts
// Shared display formatters for the trading view. Signed helpers derive the
// +/- prefix from the value sign so negatives render "-32bp" / "-$5.00"
// instead of the double-signed "+-32bp" / "+-$5.00".

export const fmtPct = (v: number): string => `${(v * 100).toFixed(0)}%`;

export const fmtUsd = (v: number): string => `$${v.toFixed(2)}`;

// Placeholder for non-finite values (NaN/Infinity) on money screens — matches
// the "—" dash convention used by FillsFeed/PositionsTable for missing fields.
const PLACEHOLDER = "—";

export const fmtBp = (v: number): string =>
  Number.isFinite(v) ? `${v >= 0 ? "+" : "-"}${Math.abs(Math.round(v))}bp` : PLACEHOLDER;

export const fmtDollarSigned = (v: number): string =>
  Number.isFinite(v) ? `${v >= 0 ? "+" : "-"}$${Math.abs(v).toFixed(2)}` : PLACEHOLDER;
