// desktop_tauri/src/routes/trading/components/LimitsModal.tsx
import { useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";

type LimitsForm = {
  max_open_notional: string;
  daily_loss_cap: string;
  reject_cooldown_seconds: string;
  per_order_cap_override: string;
};

const EMPTY: LimitsForm = {
  max_open_notional: "",
  daily_loss_cap: "",
  reject_cooldown_seconds: "",
  per_order_cap_override: "",
};

const overlayStyle: React.CSSProperties = {
  position: "fixed",
  inset: 0,
  background: "rgba(0,0,0,0.55)",
  zIndex: 50,
};

const contentStyle: React.CSSProperties = {
  position: "fixed",
  top: "50%",
  left: "50%",
  transform: "translate(-50%, -50%)",
  zIndex: 51,
  background: "var(--trading-bg)",
  border: "1px solid var(--trading-border)",
  borderRadius: 6,
  padding: 24,
  width: 360,
  maxWidth: "90vw",
  color: "var(--trading-fg)",
  fontFamily: "inherit",
};

const fieldStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginBottom: 14,
};

const labelStyle: React.CSSProperties = {
  fontSize: 10,
  color: "var(--trading-fg-muted)",
  textTransform: "uppercase",
  letterSpacing: "0.06em",
};

const inputStyle: React.CSSProperties = {
  background: "var(--trading-surface)",
  border: "1px solid var(--trading-border)",
  borderRadius: 4,
  color: "var(--trading-fg)",
  fontSize: 12,
  padding: "6px 8px",
  fontFamily: "inherit",
  width: "100%",
  boxSizing: "border-box",
};

const btnPrimary: React.CSSProperties = {
  background: "var(--trading-accent)",
  border: "none",
  borderRadius: 4,
  color: "#000",
  fontSize: 11,
  fontWeight: 700,
  padding: "7px 16px",
  cursor: "pointer",
  fontFamily: "inherit",
};

const btnSecondary: React.CSSProperties = {
  background: "transparent",
  border: "1px solid var(--trading-border)",
  borderRadius: 4,
  color: "var(--trading-fg-muted)",
  fontSize: 11,
  padding: "7px 16px",
  cursor: "pointer",
  fontFamily: "inherit",
};

export function LimitsModal() {
  const open = useTradingStore((s) => s.limitsModalOpen);
  const setOpen = useTradingStore((s) => s.setLimitsModalOpen);
  const [form, setForm] = useState<LimitsForm>(EMPTY);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      void tradingActions.readLimits().then((data) => {
        setForm({
          max_open_notional: String(data.max_open_notional ?? ""),
          daily_loss_cap: String(data.daily_loss_cap ?? ""),
          reject_cooldown_seconds: String(data.reject_cooldown_seconds ?? ""),
          per_order_cap_override:
            data.per_order_cap_override !== null && data.per_order_cap_override !== undefined
              ? String(data.per_order_cap_override)
              : "",
        });
      });
    } else {
      setForm(EMPTY);
      setError(null);
    }
  }, [open]);

  const refreshWallet = async () => {
    try {
      const { balance } = await tradingActions.fetchWallet();
      setForm((f) => ({ ...f, max_open_notional: String(balance) }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "wallet fetch failed");
    }
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await tradingActions.updateLimits({
        max_open_notional: form.max_open_notional ? Number(form.max_open_notional) : undefined,
        daily_loss_cap: form.daily_loss_cap ? Number(form.daily_loss_cap) : undefined,
        reject_cooldown_seconds: form.reject_cooldown_seconds ? Number(form.reject_cooldown_seconds) : undefined,
        per_order_cap_override: form.per_order_cap_override ? Number(form.per_order_cap_override) : undefined,
      });
      setOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "save failed");
    } finally {
      setSaving(false);
    }
  };

  const field = (key: keyof LimitsForm, label: string) => (
    <div style={fieldStyle}>
      <label style={labelStyle}>{label}</label>
      <input
        style={inputStyle}
        type="number"
        value={form[key]}
        onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
      />
    </div>
  );

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay style={overlayStyle} />
        <Dialog.Content style={contentStyle}>
          <Dialog.Title style={{ fontSize: 13, fontWeight: 700, marginBottom: 18 }}>
            Risk Limits
          </Dialog.Title>

          {field("max_open_notional", "Max Open Notional ($)")}
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 14 }}>
            <button type="button" style={{ ...btnSecondary, fontSize: 10 }} onClick={() => void refreshWallet()}>
              Sync from wallet
            </button>
          </div>

          {field("daily_loss_cap", "Daily Loss Cap ($)")}
          {field("reject_cooldown_seconds", "Reject Cooldown (s)")}
          {field("per_order_cap_override", "Per-Order Cap Override ($, optional)")}

          {error && (
            <p style={{ color: "var(--trading-accent-danger)", fontSize: 10, marginBottom: 12 }}>{error}</p>
          )}

          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 8 }}>
            <Dialog.Close asChild>
              <button type="button" style={btnSecondary}>Cancel</button>
            </Dialog.Close>
            <button type="button" style={btnPrimary} disabled={saving} onClick={() => void save()}>
              {saving ? "Saving..." : "Save"}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
