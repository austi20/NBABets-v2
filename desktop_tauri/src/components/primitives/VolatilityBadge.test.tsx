import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { VolatilityBadge } from "./VolatilityBadge";

describe("VolatilityBadge", () => {
  it("renders the tier label and class for high", () => {
    render(<VolatilityBadge tier="high" coefficient={0.78} />);
    const badge = screen.getByText(/^high$/i);
    expect(badge).toBeInTheDocument();
    expect(badge.className).toMatch(/volatility-high/);
  });

  it("renders the medium tier with medium class", () => {
    render(<VolatilityBadge tier="medium" coefficient={0.5} />);
    const badge = screen.getByText(/^medium$/i);
    expect(badge.className).toMatch(/volatility-medium/);
  });

  it("renders the low tier with low class", () => {
    render(<VolatilityBadge tier="low" coefficient={0.1} />);
    const badge = screen.getByText(/^low$/i);
    expect(badge.className).toMatch(/volatility-low/);
  });

  it("shows top contributors in the tooltip when supplied", () => {
    render(
      <VolatilityBadge
        tier="high"
        coefficient={0.78}
        contributors={[
          { name: "stat_cv", contribution: 0.28 },
          { name: "archetype_risk", contribution: 0.21 },
          { name: "minutes_instability", contribution: 0.18 },
        ]}
      />,
    );
    const badge = screen.getByText(/^high$/i);
    expect(badge.getAttribute("title")).toContain("stat_cv");
    expect(badge.getAttribute("title")).toContain("archetype_risk");
  });

  it("renders a 'Limited data' badge when reason is insufficient_features", () => {
    render(
      <VolatilityBadge
        tier="medium"
        coefficient={0.5}
        reason="insufficient_features"
      />,
    );
    const badge = screen.getByText(/limited data/i);
    expect(badge).toBeInTheDocument();
    expect(badge.className).toMatch(/volatility-unknown/);
  });
});
