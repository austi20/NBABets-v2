import { describe, expect, it } from "vitest";
import { fmtBp, fmtDollarSigned } from "./format";

describe("fmtBp", () => {
  it("prefixes a + on positive values", () => {
    expect(fmtBp(32)).toBe("+32bp");
  });

  it("renders negatives as -32bp, never +-32bp", () => {
    expect(fmtBp(-32)).toBe("-32bp");
  });

  it("renders zero with a +", () => {
    expect(fmtBp(0)).toBe("+0bp");
  });

  it.each([NaN, Infinity, -Infinity])("renders a safe placeholder for %s", (value) => {
    expect(fmtBp(value)).toBe("—");
  });
});

describe("fmtDollarSigned", () => {
  it("renders positive dollars with a + prefix", () => {
    expect(fmtDollarSigned(5)).toBe("+$5.00");
  });

  it("renders negative dollars with a single - prefix, never +-$", () => {
    expect(fmtDollarSigned(-5)).toBe("-$5.00");
  });

  it("renders zero with a +", () => {
    expect(fmtDollarSigned(0)).toBe("+$0.00");
  });

  it.each([NaN, Infinity, -Infinity])("renders a safe placeholder for %s", (value) => {
    expect(fmtDollarSigned(value)).toBe("—");
  });
});
