import { renderHook, act, waitFor } from "@testing-library/react";
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
});
