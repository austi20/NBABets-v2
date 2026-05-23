import { createRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { Route as rootRoute } from "./__root";

function SettingsPage() {
  const [logDir, setLogDir] = useState<string>("");

  useEffect(() => {
    void invoke<string>("get_log_directory")
      .then(setLogDir)
      .catch(() => setLogDir("Unavailable in browser-only dev mode"));
  }, []);

  const openLogDirectory = async () => {
    await invoke("open_log_directory");
  };

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold mb-1" style={{ color: "var(--color-fg-primary)" }}>
          Settings
        </h1>
        <p className="text-sm" style={{ color: "var(--color-fg-secondary)" }}>
          Theme, bankroll, diagnostics.
        </p>
      </div>

      <section
        className="rounded-lg border p-4 space-y-3"
        style={{
          borderColor: "var(--color-smoke)",
          backgroundColor: "var(--color-surface-1)",
        }}
      >
        <h2 className="text-sm font-semibold uppercase tracking-wide" style={{ color: "var(--color-fg-secondary)" }}>
          Diagnostics
        </h2>
        <p className="text-sm" style={{ color: "var(--color-fg-secondary)" }}>
          Log directory: <span style={{ color: "var(--color-fg-primary)" }}>{logDir || "Loading..."}</span>
        </p>
        <button
          type="button"
          className="px-3 py-2 rounded text-sm font-medium"
          style={{
            backgroundColor: "var(--color-surface-3)",
            color: "var(--color-fg-primary)",
            border: "1px solid var(--color-smoke)",
          }}
          onClick={() => {
            void openLogDirectory();
          }}
        >
          Open log directory
        </button>
      </section>
    </div>
  );
}

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
});
