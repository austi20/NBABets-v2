import { createRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { check } from "@tauri-apps/plugin-updater";
import { Route as rootRoute } from "./__root";

type UpdateStatus = "idle" | "checking" | "available" | "none" | "error";

type UpdateInfo = {
  currentVersion: string;
  version: string;
};

function SettingsPage() {
  const [logDir, setLogDir] = useState<string>("");
  const [updateStatus, setUpdateStatus] = useState<UpdateStatus>("idle");
  const [updateInfo, setUpdateInfo] = useState<UpdateInfo | null>(null);
  const [updateMessage, setUpdateMessage] = useState<string>("");

  useEffect(() => {
    void invoke<string>("get_log_directory")
      .then(setLogDir)
      .catch(() => setLogDir("Unavailable in browser-only dev mode"));
  }, []);

  const openLogDirectory = async () => {
    await invoke("open_log_directory");
  };

  const checkForUpdates = async () => {
    setUpdateStatus("checking");
    setUpdateMessage("");
    try {
      const update = await check();
      if (update) {
        setUpdateInfo({
          currentVersion: update.currentVersion,
          version: update.version,
        });
        setUpdateStatus("available");
        return;
      }
      setUpdateInfo(null);
      setUpdateStatus("none");
      setUpdateMessage("No updates available from the configured endpoint.");
    } catch (error) {
      setUpdateInfo(null);
      setUpdateStatus("error");
      setUpdateMessage(error instanceof Error ? error.message : "Update check failed");
    }
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

      <section
        className="rounded-lg border p-4 space-y-3"
        style={{
          borderColor: "var(--color-smoke)",
          backgroundColor: "var(--color-surface-1)",
        }}
      >
        <h2 className="text-sm font-semibold uppercase tracking-wide" style={{ color: "var(--color-fg-secondary)" }}>
          Auto Update
        </h2>
        <p className="text-sm" style={{ color: "var(--color-fg-secondary)" }}>
          Placeholder endpoint is wired; signing is intentionally deferred.
        </p>
        <button
          type="button"
          className="px-3 py-2 rounded text-sm font-medium"
          style={{
            backgroundColor: "var(--color-crimson)",
            color: "var(--color-fg-primary)",
          }}
          onClick={() => {
            void checkForUpdates();
          }}
          disabled={updateStatus === "checking"}
        >
          {updateStatus === "checking" ? "Checking..." : "Check for updates"}
        </button>
        {updateStatus === "available" && updateInfo ? (
          <p className="text-sm" style={{ color: "var(--color-fg-primary)" }}>
            Update available: {updateInfo.currentVersion} {"->"} {updateInfo.version}
          </p>
        ) : null}
        {updateMessage ? (
          <p
            className="text-sm"
            style={{
              color:
                updateStatus === "error"
                  ? "var(--state-negative)"
                  : "var(--color-fg-secondary)",
            }}
          >
            {updateMessage}
          </p>
        ) : null}
      </section>
    </div>
  );
}

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
});
