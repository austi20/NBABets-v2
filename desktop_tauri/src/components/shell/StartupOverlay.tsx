import type { StartupSnapshot } from "../../api/client";

type StartupOverlayProps = {
  snapshot: StartupSnapshot | null;
  isLoading: boolean;
  streamConnected: boolean;
};

function formatEta(etaSeconds: number | null): string {
  if (etaSeconds === null || etaSeconds < 0) {
    return "--";
  }
  const totalSeconds = Math.round(etaSeconds);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
}

export function StartupOverlay({ snapshot, isLoading, streamConnected }: StartupOverlayProps) {
  if (snapshot?.completed) {
    return null;
  }

  const progress = snapshot?.progress_percent ?? 0;
  const recentLogs = snapshot?.log_lines.slice(-10) ?? [];
  const steps = snapshot?.steps ?? [];

  return (
    <section className="startup-overlay" aria-live="polite">
      <div className="startup-card">
        <h2 className="startup-title">Booting startup pipeline</h2>
        <p className="startup-detail">
          {isLoading ? "Loading startup status..." : snapshot?.current_detail || "Preparing startup sequence"}
        </p>

        <div className="startup-progress-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}>
          <div className="startup-progress-fill" style={{ width: `${Math.max(0, Math.min(100, progress))}%` }} />
        </div>
        <div className="startup-meta">
          <span className="tabular">{progress.toFixed(1)}%</span>
          <span>ETA {formatEta(snapshot?.eta_seconds ?? null)}</span>
          <span>{streamConnected ? "Live stream" : "Polling fallback"}</span>
        </div>

        <div className="startup-grid">
          <div className="startup-steps">
            {steps.map((step) => {
              const isCurrent = step.label === snapshot?.current_step;
              return (
                <div key={step.key} className={`startup-step ${isCurrent ? "current" : ""}`}>
                  <span>{step.label}</span>
                  <small>{step.status.toUpperCase()}</small>
                </div>
              );
            })}
            {steps.length === 0 ? (
              <>
                <div className="startup-skeleton" />
                <div className="startup-skeleton" />
                <div className="startup-skeleton" />
              </>
            ) : null}
          </div>
          <pre className="startup-log">{recentLogs.length > 0 ? recentLogs.join("\n") : "[waiting for startup output]"}</pre>
        </div>
      </div>
    </section>
  );
}
