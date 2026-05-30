import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { KpiTileStrip } from "../KpiTileStrip";

const updateLimits = vi.fn().mockResolvedValue({});

vi.mock("../../api/actions", () => ({
  tradingActions: {
    updateLimits: (...args: unknown[]) => updateLimits(...args),
  },
}));

const SNAPSHOT = {
  kpis: {
    pnl: { daily_pnl: 0, loss_progress: 0, loss_cap: 100, realized: 0, unrealized: 0 },
    budget: { allocated: 50, max_open_notional: 250, free: 200, usage_progress: 0.2 },
    picks: { selected: 1, available: 3, est_total_profit: 5, excluded: 0, blocked: 0 },
    system: { status: "ready", summary: "ok" },
  },
};

vi.mock("../../store", () => ({
  useTradingStore: vi.fn((selector: (s: unknown) => unknown) =>
    selector({ snapshot: SNAPSHOT }),
  ),
}));

describe("KpiTileStrip budget input", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("is controlled and seeds from server budget on edit entry", () => {
    render(<KpiTileStrip />);
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    const input = screen.getByRole("spinbutton") as HTMLInputElement;
    expect(input.value).toBe("250.00");
  });

  it("does not resubmit a stale draft after reopening edit without typing", async () => {
    render(<KpiTileStrip />);
    // First edit: type a value and submit.
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    fireEvent.change(screen.getByRole("spinbutton"), { target: { value: "999" } });
    fireEvent.blur(screen.getByRole("spinbutton"));
    expect(updateLimits).toHaveBeenCalledWith({ max_open_notional: 999 });
    // Wait for the async submit to settle and the input to close.
    await waitFor(() => expect(screen.queryByRole("spinbutton")).toBeNull());
    updateLimits.mockClear();

    // Reopen edit and blur without typing — must NOT resubmit the old 999 draft.
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    expect((screen.getByRole("spinbutton") as HTMLInputElement).value).toBe("250.00");
    fireEvent.blur(screen.getByRole("spinbutton"));
    expect(updateLimits).not.toHaveBeenCalled();
  });

  it("submits the typed value on blur", () => {
    render(<KpiTileStrip />);
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    const input = screen.getByRole("spinbutton");
    fireEvent.change(input, { target: { value: "300" } });
    fireEvent.blur(input);
    expect(updateLimits).toHaveBeenCalledWith({ max_open_notional: 300 });
  });

  it("still submits when a mid-edit snapshot changes the server value to the typed value", () => {
    render(<KpiTileStrip />);
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    const input = screen.getByRole("spinbutton");
    fireEvent.change(input, { target: { value: "400" } });
    // Simulate a snapshot arriving mid-edit that sets the server value to the
    // exact value the user typed — must not swallow the legitimate submit.
    SNAPSHOT.kpis.budget.max_open_notional = 400;
    fireEvent.blur(input);
    SNAPSHOT.kpis.budget.max_open_notional = 250;
    expect(updateLimits).toHaveBeenCalledWith({ max_open_notional: 400 });
  });
});
