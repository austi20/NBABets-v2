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

function buildParlay(overrides: Record<string, unknown> = {}) {
  return {
    rank: 1,
    game_id: 1,
    matchup: "A @ B",
    sportsbook_key: "dk",
    sportsbook_name: "DK",
    sportsbook_icon: "",
    leg_count: 2,
    game_count: 1,
    game_ids: [1],
    game_labels: ["A @ B"],
    joint_probability: 0.4,
    combined_decimal_odds: 2.5,
    combined_american_odds: 150,
    expected_profit_per_unit: 0.1,
    implied_probability: 0.4,
    edge: 0.05,
    all_legs_live: true,
    verification_status: "ok",
    odds_source_provider: "kalshi",
    correlation_penalty: 0,
    average_leg_hit_probability: 0.6,
    weakest_leg_hit_probability: 0.5,
    legs: [],
    ...overrides,
  };
}

function buildParlaySection(parlayOverrides: Record<string, unknown> = {}) {
  return {
    sections: {
      dk: { "2": { "1": [{ parlay: buildParlay(parlayOverrides), insight: {} }] } },
    },
  };
}

describe("api.sameGameParlays probability validation", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("resolves when probability fields are valid", async () => {
    mockFetchOnce(buildParlaySection());
    await expect(api.sameGameParlays()).resolves.toBeDefined();
  });

  it("rejects when average_leg_hit_probability is out of [0,1]", async () => {
    mockFetchOnce(buildParlaySection({ average_leg_hit_probability: 1.5 }));
    await expect(api.sameGameParlays()).rejects.toThrow();
  });

  it("rejects when weakest_leg_hit_probability is the wrong type", async () => {
    mockFetchOnce(buildParlaySection({ weakest_leg_hit_probability: "0.5" }));
    await expect(api.sameGameParlays()).rejects.toThrow();
  });
});

describe("api.multiGameParlays probability validation", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("rejects when joint_probability is missing", async () => {
    const parlay = buildParlay();
    delete (parlay as Record<string, unknown>).joint_probability;
    mockFetchOnce({ sections: { dk: { "2": [{ parlay, insight: {} }] } } });
    await expect(api.multiGameParlays()).rejects.toThrow();
  });
});
