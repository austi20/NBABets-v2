import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ConfidenceBar } from "./ConfidenceBar";

describe("ConfidenceBar", () => {
  it("uses positive class for high confidence", () => {
    const { container } = render(<ConfidenceBar value={85} max={100} />);
    expect(screen.getByRole("progressbar", { name: "Confidence" })).toBeInTheDocument();
    expect(container.querySelector(".confidence-fill")).toHaveClass("confidence-positive");
  });

  it("uses caution class for medium confidence", () => {
    const { container } = render(<ConfidenceBar value={52} max={100} />);
    expect(container.querySelector(".confidence-fill")).toHaveClass("confidence-caution");
  });

  it("uses negative class for low confidence", () => {
    const { container } = render(<ConfidenceBar value={20} max={100} />);
    expect(container.querySelector(".confidence-fill")).toHaveClass("confidence-negative");
  });
});
