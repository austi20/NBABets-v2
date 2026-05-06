import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { EdgeBadge } from "./EdgeBadge";

describe("EdgeBadge", () => {
  it("renders positive tone with aria label", () => {
    render(<EdgeBadge edge={8.2} />);
    const badge = screen.getByLabelText("Positive edge +8.2 percent");
    expect(badge).toHaveClass("edge-positive");
    expect(badge).toHaveTextContent("+8.2%");
  });

  it("renders caution tone with aria label", () => {
    render(<EdgeBadge edge={2.4} />);
    const badge = screen.getByLabelText("Caution edge +2.4 percent");
    expect(badge).toHaveClass("edge-caution");
  });

  it("renders negative tone with aria label", () => {
    render(<EdgeBadge edge={-0.7} />);
    const badge = screen.getByLabelText("Negative edge -0.7 percent");
    expect(badge).toHaveClass("edge-negative");
  });
});
