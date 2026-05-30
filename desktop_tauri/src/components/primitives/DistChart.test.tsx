import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DistChart } from "./DistChart";

describe("DistChart finite guards", () => {
  it("renders an unavailable state when projectedMean is NaN", () => {
    render(<DistChart projectedMean={NaN} projectedVariance={4} line={20} />);
    expect(screen.getByText(/unavailable/i)).toBeInTheDocument();
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument();
  });

  it("renders an unavailable state when projectedVariance is NaN", () => {
    render(<DistChart projectedMean={20} projectedVariance={NaN} line={20} />);
    expect(screen.getByText(/unavailable/i)).toBeInTheDocument();
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument();
  });

  it("renders an unavailable state when line is NaN", () => {
    render(<DistChart projectedMean={20} projectedVariance={4} line={NaN} />);
    expect(screen.getByText(/unavailable/i)).toBeInTheDocument();
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument();
  });

  it("renders projected probabilities for finite inputs", () => {
    render(<DistChart projectedMean={20} projectedVariance={9} line={18} />);
    expect(screen.getByText(/Projected over/i)).toBeInTheDocument();
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument();
  });
});
