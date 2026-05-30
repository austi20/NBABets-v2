// desktop_tauri/src/routes/trading/components/StreamStatusBanner.tsx
// Surfaces SSE health on the trading money screen. Silent SSE death (or a
// drifted snapshot the ingestion guard rejected) would otherwise leave stale
// numbers on screen with no signal to the operator.
import { useTradingStore } from "../store";

export function StreamStatusBanner() {
  const streamConnected = useTradingStore((s) => s.streamConnected);
  const streamError = useTradingStore((s) => s.streamError);

  if (streamConnected && !streamError) {
    return null;
  }

  const message = !streamConnected
    ? "disconnected — retrying"
    : (streamError?.message ?? "stream issue");

  return (
    <div
      role="status"
      style={{
        position: "fixed",
        bottom: 12,
        right: 12,
        padding: "4px 10px",
        background: "var(--trading-surface)",
        border: "1px solid var(--trading-accent-danger)",
        borderRadius: 12,
        fontSize: 10,
        color: "var(--trading-accent-danger)",
      }}
    >
      ● {message}
    </div>
  );
}
