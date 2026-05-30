import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { ThresholdsRow } from "../ThresholdsRow";

const readThresholds = vi.fn();
const setThresholds = vi.fn().mockResolvedValue({});

vi.mock("../../api/actions", () => ({
  tradingActions: {
    readThresholds: () => readThresholds(),
    setThresholds: (...args: unknown[]) => setThresholds(...args),
  },
}));

let openState = true;
vi.mock("../../store", () => ({
  useTradingStore: vi.fn((selector: (s: unknown) => unknown) =>
    selector({ thresholdsOpen: openState, setThresholdsOpen: vi.fn() }),
  ),
}));

describe("ThresholdsRow server seeding", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    openState = true;
    readThresholds.mockResolvedValue({ min_hit_pct: 0.62, min_edge_bps: 120 });
  });

  it("fetches current thresholds when open", async () => {
    render(<ThresholdsRow />);
    await waitFor(() => expect(readThresholds).toHaveBeenCalledOnce());
  });

  it("pre-fills inputs from server state rather than hardcoded defaults", async () => {
    render(<ThresholdsRow />);
    await waitFor(() => {
      const inputs = screen.getAllByRole("spinbutton") as HTMLInputElement[];
      // min hit % shown as 62 (0.62 * 100), not the hardcoded 55
      expect(inputs[0].value).toBe("62");
      // min edge bps shown as 120, not the hardcoded 50
      expect(inputs[1].value).toBe("120");
    });
  });
});
