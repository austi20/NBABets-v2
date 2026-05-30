// desktop_tauri/src/routes/trading/format.ts
// Shared display formatters for the trading view. Signed helpers derive the
// +/- prefix from the value sign so negatives render "-32bp" / "-$5.00"
// instead of the double-signed "+-32bp" / "+-$5.00".

export const fmtPct = (v: number): string => `${(v * 100).toFixed(0)}%`;

export const fmtUsd = (v: number): string => `$${v.toFixed(2)}`;

export const fmtBp = (v: number): string => `${v >= 0 ? "+" : "-"}${Math.abs(v)}bp`;

export const fmtDollarSigned = (v: number): string => `${v >= 0 ? "+" : "-"}$${Math.abs(v).toFixed(2)}`;
