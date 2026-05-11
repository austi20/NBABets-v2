// desktop_tauri/src/routes/trading/components/ControlBar.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { getAppToken, tradingSnapshotUrl } from "../../../api/client";

function resolveBase(): string {
  const url = new URL(tradingSnapshotUrl());
  return url.origin;
}

function authHeaders(): Record<string, string> {
  const token = getAppToken();
  return token ? { "X-App-Token": token } : {};
}

export function ControlBar() {
  const snapshot = useTradingStore((s) => s.snapshot);
  const setLimitsOpen = useTradingStore((s) => s.setLimitsModalOpen);
  const [modeBusy, setModeBusy] = useState(false);
  const [loopBusy, setLoopBusy] = useState(false);

  if (!snapshot) return <div className="trading-tile" style={{ marginBottom: 18, height: 44 }} />;

  const { control } = snapshot;

  const setMode = async (mode: "observe" | "supervised-live") => {
    setModeBusy(true);
    try {
      await fetch(`${resolveBase()}/api/trading/brain/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ mode, resolve_markets: true, build_pack: true }),
      });
    } finally {
      setModeBusy(false);
    }
  };

  const startLoop = async () => {
    setLoopBusy(true);
    try {
      await fetch(`${resolveBase()}/api/trading/loop/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
      });
    } finally {
      setLoopBusy(false);
    }
  };

  const triggerKill = async () => {
    try {
      await fetch(`${resolveBase()}/api/trading/kill-switch`, {
        method: "POST",
        headers: authHeaders(),
      });
    } catch {
      // kill-switch failure is visible via SSE snapshot update or disconnect chip
    }
  };

  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 12px", background: "var(--trading-surface)", borderRadius: 6, marginBottom: 18 }}>
      <div style={{ display: "flex", gap: 6 }}>
        <button
          type="button"
          className={`btn-trading ${control.mode === "supervised-live" ? "primary" : "ghost"}`}
          disabled={modeBusy}
          onClick={() => void setMode("supervised-live")}
        >
          ● Live
        </button>
        <button
          type="button"
          className={`btn-trading ${control.mode === "observe" ? "primary" : "ghost"}`}
          disabled={modeBusy}
          onClick={() => void setMode("observe")}
        >
          ○ Watch
        </button>
        <button type="button" className="btn-trading ghost" onClick={() => setLimitsOpen(true)}>
          ⚙ Limits
        </button>
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <button
          type="button"
          className="btn-trading primary"
          disabled={!control.can_start || loopBusy}
          onClick={() => void startLoop()}
        >
          ▶ {control.start_label}
        </button>
        <button
          type="button"
          className="btn-trading danger"
          disabled={control.kill_switch_active}
          onClick={() => void triggerKill()}
        >
          ⏻ {control.kill_switch_active ? "Stopped" : "Kill Switch"}
        </button>
      </div>
    </div>
  );
}
