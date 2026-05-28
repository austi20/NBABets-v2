import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { ControlBar } from "../ControlBar";

// Stub the store — returns a snapshot with controlled state
vi.mock("../../store", () => ({
  useTradingStore: vi.fn((selector: (s: unknown) => unknown) => {
    const state = {
      snapshot: {
        control: {
          mode: "observe",
          loop_state: "idle",
          can_start: true,
          start_label: "Start Loop",
          kill_switch_active: false,
        },
      },
      setLimitsModalOpen: vi.fn(),
    };
    return selector(state);
  }),
}));

// Path relative to this test file: __tests__/ up 4 levels = src/, then api/client
vi.mock("../../../../api/client", () => ({
  getAppToken: () => null,
  tradingSnapshotUrl: () => "http://localhost:8765/api/trading/snapshot-live",
}));

const mockFetch = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });

describe("ControlBar live arming", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // jsdom exposes fetch on globalThis; override for test isolation
    vi.stubGlobal("fetch", mockFetch);
  });

  it("does not call fetch immediately when user clicks Live button", () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /live/i }));
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("shows the confirmation dialog after clicking Live", () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /live/i }));
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("does not call fetch when wrong text is typed and Confirm clicked", () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /live/i }));
    fireEvent.change(screen.getByPlaceholderText(/LIVE/i), { target: { value: "live" } });
    // Button is disabled; dispatching click directly still hits the if-guard
    screen.getByRole("button", { name: /confirm/i }).dispatchEvent(
      new MouseEvent("click", { bubbles: true }),
    );
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("calls fetch after operator types LIVE and confirms", async () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /live/i }));
    fireEvent.change(screen.getByPlaceholderText(/LIVE/i), { target: { value: "LIVE" } });
    // After state settles, confirm button is enabled and onClick fires fetch
    await waitFor(() => {
      const btn = screen.getByRole("button", { name: /confirm/i });
      expect(btn).not.toBeDisabled();
    });
    fireEvent.click(screen.getByRole("button", { name: /confirm/i }));
    expect(mockFetch).toHaveBeenCalledOnce();
  });

  it("does not call fetch immediately when user clicks Start Loop", () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /start loop/i }));
    expect(mockFetch).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("does not call fetch immediately when user clicks Kill Switch", () => {
    render(<ControlBar />);
    fireEvent.click(screen.getByRole("button", { name: /kill switch/i }));
    expect(mockFetch).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });
});
