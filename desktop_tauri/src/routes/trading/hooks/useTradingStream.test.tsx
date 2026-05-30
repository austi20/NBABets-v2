import { renderHook, act } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useTradingStream } from "./useTradingStream";
import { useTradingStore } from "../store";

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  onerror: ((this: EventSource, ev: Event) => unknown) | null = null;
  listeners = new Map<string, ((ev: Event) => void)[]>();
  closed = false;
  constructor(public url: string) {
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, cb: (ev: Event) => void) {
    const list = this.listeners.get(type) ?? [];
    list.push(cb);
    this.listeners.set(type, list);
  }
  close() {
    this.closed = true;
  }
  emitError() {
    this.onerror?.call(this as unknown as EventSource, new Event("error"));
  }
  emit(type: string, ev: Event) {
    for (const cb of this.listeners.get(type) ?? []) {
      cb(ev);
    }
  }
}

function validSnapshot() {
  return {
    observed_at: "2026-05-30T00:00:00Z",
    kpis: {
      pnl: { daily_pnl: 0, realized: 0, unrealized: 0, loss_cap: 100, loss_progress: 0.1 },
      budget: { max_open_notional: 250, allocated: 50, free: 200, usage_progress: 0.2 },
      picks: { available: 3, selected: 1, excluded: 0, blocked: 0, est_total_profit: 5 },
      system: { status: "ready", mode: "observe", gates_passed: 1, gates_total: 1, ws_connected: true, summary: "ok" },
    },
    picks: [],
    fills: [],
  };
}

describe("useTradingStream error surfacing", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    vi.useFakeTimers();
    vi.stubGlobal("EventSource", FakeEventSource as unknown as typeof EventSource);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) }));
    useTradingStore.setState({ streamConnected: true, streamError: null });
  });

  afterEach(() => {
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("sets streamConnected=false and records a structured error on SSE failure", () => {
    renderHook(() => useTradingStream());
    expect(FakeEventSource.instances).toHaveLength(1);

    act(() => {
      FakeEventSource.instances[0].emitError();
    });

    const state = useTradingStore.getState();
    expect(state.streamConnected).toBe(false);
    expect(state.streamError).not.toBeNull();
    expect(state.streamError?.message).toMatch(/stream/i);
    expect(typeof state.streamError?.at).toBe("number");
  });

  it("applies a valid snapshot from the SSE stream", () => {
    useTradingStore.setState({ snapshot: null });
    renderHook(() => useTradingStream());
    act(() => {
      const ev = new MessageEvent("snapshot", { data: JSON.stringify(validSnapshot()) });
      FakeEventSource.instances[0].emit("snapshot", ev);
    });
    expect(useTradingStore.getState().snapshot).not.toBeNull();
  });

  it("records a streamError and skips applying a malformed snapshot", () => {
    useTradingStore.setState({ snapshot: null, streamError: null });
    renderHook(() => useTradingStream());
    const bad = validSnapshot();
    bad.kpis.budget.max_open_notional = Number.NaN;
    act(() => {
      const ev = new MessageEvent("snapshot", { data: JSON.stringify(bad) });
      FakeEventSource.instances[0].emit("snapshot", ev);
    });
    const state = useTradingStore.getState();
    expect(state.snapshot).toBeNull();
    expect(state.streamError).not.toBeNull();
    expect(state.streamError?.message).toMatch(/snapshot/i);
  });
});
