import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { InjuryPill } from "./InjuryPill";

describe("InjuryPill", () => {
  it("renders caution style for questionable", () => {
    render(<InjuryPill status="Q" />);
    const pill = screen.getByLabelText("Injury status Questionable");
    expect(pill).toHaveClass("injury-caution");
    expect(pill).toHaveTextContent("Q");
  });

  it("renders negative style for doubtful", () => {
    render(<InjuryPill status="D" />);
    const pill = screen.getByLabelText("Injury status Doubtful");
    expect(pill).toHaveClass("injury-negative");
  });

  it("renders positive style for healthy", () => {
    render(<InjuryPill status="Healthy" />);
    const pill = screen.getByLabelText("Injury status Healthy");
    expect(pill).toHaveClass("injury-positive");
  });
});
