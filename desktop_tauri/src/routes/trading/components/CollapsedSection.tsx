// desktop_tauri/src/routes/trading/components/CollapsedSection.tsx
import { useState } from "react";
import type { ReactNode } from "react";

type Props = {
  title: string;
  count: number;
  hideWhenEmpty?: boolean;
  rightLabel?: string;
  children: ReactNode;
};

export function CollapsedSection({ title, count, hideWhenEmpty, rightLabel, children }: Props) {
  const [open, setOpen] = useState(false);
  if (hideWhenEmpty && count === 0) return null;
  return (
    <div style={{ background: "var(--trading-surface-alt)", border: "1px solid var(--trading-border)", borderRadius: 4, padding: "8px 12px", marginBottom: 6 }}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center", width: "100%", background: "transparent", border: "none", color: "var(--trading-fg-muted)", fontSize: 10, fontFamily: "inherit", cursor: "pointer", padding: 0 }}
      >
        <span>
          {open ? "▾" : "▸"} {title} <span style={{ opacity: 0.5 }}>· {count}</span>
        </span>
        <span style={{ opacity: 0.5 }}>{rightLabel ?? (open ? "collapse" : "expand")}</span>
      </button>
      {open ? <div style={{ marginTop: 8 }}>{children}</div> : null}
    </div>
  );
}
