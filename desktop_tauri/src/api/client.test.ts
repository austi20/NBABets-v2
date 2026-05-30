import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./client";

function buildOpportunity(overrides: Record<string, unknown> = {}) {
  return {
    rank: 1,
    game_id: 1,
    player_id: 2,
    player_name: "Player",
    player_icon: "",
    market_key: "points",
    consensus_line: 20,
    projected_mean: 21,
    recommended_side: "over",
    hit_probability: 0.6,
    likelihood_score: 0.6,
    calibrated_over_probability: 0.6,
    sportsbooks_summary: "",
    top_features: [],
    quotes: [],
    projected_variance: null,
    confidence_interval_low: null,
    confidence_interval_high: null,
    predicted_at: null,
    data_sufficiency_tier: "ok",
    data_confidence_score: 1,
    player_team_abbreviation: null,
    player_position: null,
    game_label: null,
    game_start_time: null,
    percentile_25: 18,
    percentile_75: 24,
    dnp_risk: 0.1,
    boom_probability: 0.2,
    bust_probability: 0.1,
    availability_branches: 1,
    volatility_coefficient: 0.1,
    volatility_tier: "low",
    adjusted_over_probability: null,
    ...overrides,
  };
}

const VALID_INSIGHT = {
  best_quote: {
    no_vig_market_probability: 0.55,
  },
};

function buildPropResponse(opportunityOverrides: Record<string, unknown> = {}) {
  return {
    items: [{ opportunity: buildOpportunity(opportunityOverrides), insight: VALID_INSIGHT }],
    total: 1,
    page: 1,
    page_size: 50,
  };
}

function mockFetchOnce(payload: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: async () => payload }),
  );
}

describe("api.props probability validation", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("resolves when probability fields are valid", async () => {
    mockFetchOnce(buildPropResponse());
    await expect(api.props()).resolves.toBeDefined();
  });

  it("rejects when hit_probability is missing", async () => {
    const opp = buildOpportunity();
    delete (opp as Record<string, unknown>).hit_probability;
    mockFetchOnce({ items: [{ opportunity: opp, insight: VALID_INSIGHT }], total: 1, page: 1, page_size: 50 });
    await expect(api.props()).rejects.toThrow();
  });

  it("rejects when hit_probability is out of [0,1]", async () => {
    mockFetchOnce(buildPropResponse({ hit_probability: 1.4 }));
    await expect(api.props()).rejects.toThrow();
  });

  it("rejects when hit_probability is the wrong type", async () => {
    mockFetchOnce(buildPropResponse({ hit_probability: "0.6" }));
    await expect(api.props()).rejects.toThrow();
  });
});
