// desktop_tauri/src/routes/trading/components/ControlBar.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { getAppToken, tradingSnapshotUrl } from "../../../api/client";
import { ConfirmTypedDialog } from "../../../components/primitives/ConfirmTypedDialog";

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
  const [pendingAction, setPendingAction] = useState<(() => void) | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  if (!snapshot) return <div className="trading-tile" style={{ marginBottom: 18, height: 44 }} />;

  const { control } = snapshot;

  const setMode = async (mode: "observe" | "supervised-live") => {
    setModeBusy(true);
    setFetchError(null);
    try {
      const res = await fetch(`${resolveBase()}/api/trading/brain/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ mode, resolve_markets: true, build_pack: true }),
      });
      if (!res.ok) {
        setFetchError(`Mode change failed (${res.status})`);
      }
    } catch (err) {
      setFetchError(err instanceof Error ? err.message : "Mode change failed");
    } finally {
      setModeBusy(false);
    }
  };

  const startLoop = async () => {
    setLoopBusy(true);
    setFetchError(null);
    try {
      const res = await fetch(`${resolveBase()}/api/trading/loop/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
      });
      if (!res.ok) {
        setFetchError(`Loop start failed (${res.status})`);
      }
    } catch (err) {
      setFetchError(err instanceof Error ? err.message : "Loop start failed");
    } finally {
      setLoopBusy(false);
    }
  };

  const triggerKill = async () => {
    setFetchError(null);
    try {
      const res = await fetch(`${resolveBase()}/api/trading/kill-switch`, {
        method: "POST",
        headers: authHeaders(),
      });
      if (!res.ok) {
        setFetchError(`Kill switch failed (${res.status})`);
      }
    } catch (err) {
      setFetchError(err instanceof Error ? err.message : "Kill switch failed");
    }
  };

  const arm = (action: () => void) => setPendingAction(() => action);

  return (
    <>
      <ConfirmTypedDialog
        open={pendingAction !== null}
        phrase="LIVE"
        body="Type LIVE to confirm this action."
        onConfirm={() => {
          pendingAction?.();
          setPendingAction(null);
        }}
        onCancel={() => setPendingAction(null)}
      />
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 12px", background: "var(--trading-surface)", borderRadius: 6, marginBottom: 18 }}>
        {fetchError && (
          <span style={{ color: "var(--color-danger, red)", fontSize: 12, marginRight: 8 }}>{fetchError}</span>
        )}
        <div style={{ display: "flex", gap: 6 }}>
          <button
            type="button"
            className={`btn-trading ${control.mode === "supervised-live" ? "primary" : "ghost"}`}
            disabled={modeBusy}
            onClick={() => arm(() => void setMode("supervised-live"))}
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
            onClick={() => arm(() => void startLoop())}
          >
            ▶ {control.start_label}
          </button>
          <button
            type="button"
            className="btn-trading danger"
            disabled={control.kill_switch_active}
            onClick={() => arm(() => void triggerKill())}
          >
            ⏻ {control.kill_switch_active ? "Stopped" : "Kill Switch"}
          </button>
        </div>
      </div>
    </>
  );
}
