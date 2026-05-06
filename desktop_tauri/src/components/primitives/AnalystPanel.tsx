type AnalystPanelProps = {
  title?: string;
  lines: string[];
};

export function AnalystPanel({ title = "Analyst Stream", lines }: AnalystPanelProps) {
  return (
    <section className="analyst-panel" aria-live="polite">
      <header className="analyst-header">
        <h3>{title}</h3>
      </header>
      <pre className="analyst-log">{lines.length > 0 ? lines.join("\n") : "[waiting for analyst stream]"}</pre>
    </section>
  );
}
