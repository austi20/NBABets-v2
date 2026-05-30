// desktop_tauri/src/routes/trading/hooks/useTradingStream.ts
import { useEffect, useRef } from "react";
import { z } from "zod";
import { getAppToken, tradingSnapshotUrl, tradingStreamUrl } from "../../../api/client";
import { useTradingStore } from "../store";
import type { TradingLiveSnapshot } from "../api/types";

const BACKOFF_STEPS_MS = [1_000, 2_000, 4_000, 8_000];

// The live snapshot reaches the money screens via SSE/poll, NOT apiFetch, so the
// client zod layer never runs on it. Validate the probability and money fields
// the formatters consume (KpiTileStrip, PicksTable, BetSlipSidebar, FillsFeed)
// at the ingestion boundary so a drifted backend can't paint NaN/out-of-range
// numbers onto a live-money view. Kept minimal; passthrough preserves the rest.
const finiteMoney = z.number().finite();
const probability01 = z.number().min(0).max(1);

const snapshotSchema = z
  .object({
    kpis: z
      .object({
        pnl: z
          .object({
            daily_pnl: finiteMoney,
            realized: finiteMoney,
            unrealized: finiteMoney,
            loss_cap: finiteMoney,
            loss_progress: probability01,
          })
          .passthrough(),
        budget: z
          .object({
            max_open_notional: finiteMoney,
            allocated: finiteMoney,
            free: finiteMoney,
            usage_progress: probability01,
          })
          .passthrough(),
        picks: z
          .object({ est_total_profit: finiteMoney })
          .passthrough(),
      })
      .passthrough(),
    picks: z.array(
      z
        .object({
          hit_pct: finiteMoney,
          edge_bps: finiteMoney,
          model_prob: finiteMoney,
          alloc: finiteMoney,
          est_profit: finiteMoney,
        })
        .passthrough(),
    ),
    fills: z.array(z.object({ realized_pnl: finiteMoney }).passthrough()),
  })
  .passthrough();

// Returns a validated snapshot, or null if the payload drifted out of shape.
function parseSnapshot(raw: unknown): TradingLiveSnapshot | null {
  const result = snapshotSchema.safeParse(raw);
  return result.success ? (raw as TradingLiveSnapshot) : null;
}

export function useTradingStream(): void {
  const applySnapshot = useTradingStore((s) => s.applySnapshot);
  const setStreamConnected = useTradingStore((s) => s.setStreamConnected);
  const setStreamError = useTradingStore((s) => s.setStreamError);
  const backoffIndex = useRef(0);
  const pollTimerRef = useRef<number | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const sourceRef = useRef<EventSource | null>(null);
  const epochRef = useRef(0);

  useEffect(() => {
    let cancelled = false;

    const stopPolling = () => {
      if (pollTimerRef.current !== null) {
        window.clearTimeout(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };

    const pollFallback = async () => {
      try {
        const token = getAppToken();
        const headers: Record<string, string> = {};
        if (token) headers["X-App-Token"] = token;
        const res = await fetch(tradingSnapshotUrl(), { headers });
        if (!res.ok) throw new Error(`${res.status}`);
        const data = parseSnapshot(await res.json());
        if (cancelled) return;
        if (data === null) {
          setStreamError({ message: "Received an invalid trading snapshot; ignoring", at: Date.now() });
          throw new Error("invalid snapshot shape");
        }
        applySnapshot(data);
      } catch {
        // swallow — next poll will retry
      }
      if (cancelled) return;
      pollTimerRef.current = window.setTimeout(pollFallback, 2_000);
    };

    const connect = () => {
      if (cancelled) return;
      const epoch = ++epochRef.current;
      const url = tradingStreamUrl();
      const source = new EventSource(url);
      sourceRef.current = source;

      source.addEventListener("open", () => {
        if (cancelled || epochRef.current !== epoch) return;
        setStreamConnected(true);
        setStreamError(null);
        backoffIndex.current = 0;
        stopPolling();
      });

      source.addEventListener("snapshot", (event) => {
        if (cancelled || epochRef.current !== epoch) return;
        try {
          const data = parseSnapshot(JSON.parse((event as MessageEvent<string>).data));
          if (data === null) {
            setStreamError({ message: "Received an invalid trading snapshot; ignoring", at: Date.now() });
            return;
          }
          applySnapshot(data);
        } catch {
          // ignore malformed JSON
        }
      });

      source.onerror = () => {
        if (cancelled || epochRef.current !== epoch) return;
        setStreamConnected(false);
        setStreamError({
          message: "Trading stream disconnected; falling back to polling",
          at: Date.now(),
        });
        source.close();
        if (pollTimerRef.current === null) {
          pollTimerRef.current = window.setTimeout(pollFallback, 0);
        }
        const delay = BACKOFF_STEPS_MS[Math.min(backoffIndex.current, BACKOFF_STEPS_MS.length - 1)];
        backoffIndex.current = Math.min(backoffIndex.current + 1, BACKOFF_STEPS_MS.length - 1);
        reconnectTimerRef.current = window.setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      cancelled = true;
      sourceRef.current?.close();
      sourceRef.current = null;
      stopPolling();
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
  }, [applySnapshot, setStreamConnected, setStreamError]);
}
