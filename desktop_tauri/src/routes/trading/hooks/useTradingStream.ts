// desktop_tauri/src/routes/trading/hooks/useTradingStream.ts
import { useEffect, useRef } from "react";
import { getAppToken, tradingSnapshotUrl, tradingStreamUrl } from "../../../api/client";
import { useTradingStore } from "../store";
import type { TradingLiveSnapshot } from "../api/types";

const BACKOFF_STEPS_MS = [1_000, 2_000, 4_000, 8_000];

export function useTradingStream(): void {
  const applySnapshot = useTradingStore((s) => s.applySnapshot);
  const setStreamConnected = useTradingStore((s) => s.setStreamConnected);
  const backoffIndex = useRef(0);
  const pollTimerRef = useRef<number | null>(null);
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
        const data = (await res.json()) as TradingLiveSnapshot;
        if (cancelled) return;
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
        backoffIndex.current = 0;
        stopPolling();
      });

      source.addEventListener("snapshot", (event) => {
        if (cancelled || epochRef.current !== epoch) return;
        try {
          const data = JSON.parse((event as MessageEvent<string>).data) as TradingLiveSnapshot;
          applySnapshot(data);
        } catch {
          // ignore malformed
        }
      });

      source.onerror = () => {
        if (cancelled || epochRef.current !== epoch) return;
        setStreamConnected(false);
        source.close();
        if (pollTimerRef.current === null) {
          pollTimerRef.current = window.setTimeout(pollFallback, 0);
        }
        const delay = BACKOFF_STEPS_MS[Math.min(backoffIndex.current, BACKOFF_STEPS_MS.length - 1)];
        backoffIndex.current = Math.min(backoffIndex.current + 1, BACKOFF_STEPS_MS.length - 1);
        window.setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      cancelled = true;
      sourceRef.current?.close();
      sourceRef.current = null;
      stopPolling();
    };
  }, [applySnapshot, setStreamConnected]);
}
