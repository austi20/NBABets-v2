import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, startupStreamUrl, type StartupSnapshot } from "../api/client";

type StartupSnapshotState = {
  snapshot: StartupSnapshot | null;
  isLoading: boolean;
  streamConnected: boolean;
  errorMessage: string | null;
  refreshStartup: () => Promise<void>;
};

const POLL_INTERVAL_MS = 1_000;

export function useStartupSnapshot(): StartupSnapshotState {
  const [snapshot, setSnapshot] = useState<StartupSnapshot | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [streamConnected, setStreamConnected] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [streamEpoch, setStreamEpoch] = useState(0);
  const startupKickoffRef = useRef(false);
  const pollTimerRef = useRef<number | null>(null);
  const snapshotRef = useRef<StartupSnapshot | null>(null);

  useEffect(() => {
    snapshotRef.current = snapshot;
  }, [snapshot]);

  const stopPolling = useCallback(() => {
    if (pollTimerRef.current !== null) {
      window.clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }, []);

  const startPolling = useCallback(() => {
    if (pollTimerRef.current !== null) {
      return;
    }
    pollTimerRef.current = window.setInterval(async () => {
      try {
        const next = await api.startupSnapshot();
        setSnapshot(next);
        setErrorMessage(null);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unable to poll startup snapshot";
        setErrorMessage(message);
      }
    }, POLL_INTERVAL_MS);
  }, []);

  const reconnectStream = useCallback(() => {
    setStreamEpoch((prev) => prev + 1);
  }, []);

  const refreshStartup = useCallback(async () => {
    await api.runStartup();
    reconnectStream();
  }, [reconnectStream]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const next = await api.startupSnapshot();
        if (cancelled) {
          return;
        }
        setSnapshot(next);
        setErrorMessage(null);
        if (!next.completed && !startupKickoffRef.current) {
          startupKickoffRef.current = true;
          await api.runStartup();
        }
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Unable to load startup snapshot";
        setErrorMessage(message);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const source = new EventSource(startupStreamUrl());

    source.addEventListener("open", () => {
      setStreamConnected(true);
      setErrorMessage(null);
      stopPolling();
    });

    source.addEventListener("snapshot", (event) => {
      try {
        const parsed = JSON.parse((event as MessageEvent<string>).data) as StartupSnapshot;
        setSnapshot(parsed);
      } catch {
        setErrorMessage("Received malformed startup snapshot event");
      }
    });

    source.onerror = () => {
      setStreamConnected(false);
      const current = snapshotRef.current;
      if (!current?.completed && !current?.failed) {
        startPolling();
      }
    };

    return () => {
      source.close();
      setStreamConnected(false);
      stopPolling();
    };
  }, [startPolling, stopPolling, streamEpoch]);

  return useMemo(
    () => ({
      snapshot,
      isLoading,
      streamConnected,
      errorMessage,
      refreshStartup,
    }),
    [errorMessage, isLoading, refreshStartup, snapshot, streamConnected],
  );
}
