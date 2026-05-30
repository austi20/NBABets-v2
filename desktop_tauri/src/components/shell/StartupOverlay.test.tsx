import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { StartupOverlay } from "./StartupOverlay";
import type { StartupSnapshot, StartupStep } from "../../api/client";

function makeStep(overrides: Partial<StartupStep>): StartupStep {
  return {
    key: "k",
    label: "Step",
    status: "pending",
    message: "",
    progress_fraction: 0,
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<StartupSnapshot>): StartupSnapshot {
  return {
    progress_percent: 0,
    eta_seconds: null,
    current_step: "",
    current_detail: "",
    database_message: "",
    board_date_message: "",
    completed: false,
    failed: false,
    error_message: null,
    steps: [],
    log_lines: [],
    ...overrides,
  };
}

describe("StartupOverlay current step contract", () => {
  it("marks the step whose label matches current_step as current", () => {
    // The backend sets snapshot.current_step to the running step's `label`
    // (see app/services/startup.py), so the overlay must match on label, not key.
    const steps = [
      makeStep({ key: "db", label: "Database", status: "complete" }),
      makeStep({ key: "ingest", label: "Ingesting players", status: "running" }),
    ];
    const snapshot = makeSnapshot({ steps, current_step: "Ingesting players" });

    const { container } = render(
      <StartupOverlay snapshot={snapshot} isLoading={false} streamConnected />,
    );

    const currentEls = container.querySelectorAll(".startup-step.current");
    expect(currentEls).toHaveLength(1);
    expect(currentEls[0].textContent).toContain("Ingesting players");
    expect(screen.getByText("Database").closest(".startup-step")).not.toHaveClass("current");
  });
});
