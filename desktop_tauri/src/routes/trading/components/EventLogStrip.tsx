// desktop_tauri/src/routes/trading/components/EventLogStrip.tsx
import { useEffect, useRef } from "react";
import { useTradingStore, selectEventLog } from "../store";

export function EventLogStrip() {
  const log = useTradingStore(selectEventLog);
  const containerRef = useRef<HTMLDivElement>(null);
  const userScrolledRef = useRef(false);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    if (!userScrolledRef.current) {
      el.scrollTop = 0; // newest at top
    }
  }, [log.length]);

  return (
    <section style={{ marginTop: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>Event Log</div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>errors · warnings · info</span>
      </div>
      <div
        ref={containerRef}
        className="event-log"
        onScroll={() => {
          const el = containerRef.current;
          if (!el) return;
          userScrolledRef.current = el.scrollTop > 16;
        }}
      >
        {[...log].reverse().map((line) => (
          <div key={line.cursor} className={`log-${line.level}`}>
            [{line.timestamp.slice(11, 19)}] {line.level}  {line.message}
          </div>
        ))}
      </div>
    </section>
  );
}
