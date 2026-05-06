import { MetricCard } from "../primitives/MetricCard";

type MetricStripProps = {
  board: string;
  games: string;
  props: string;
  books: string;
  alt: string;
  updated: string;
};

export function MetricStrip({ board, games, props, books, alt, updated }: MetricStripProps) {
  return (
    <section className="metric-strip" aria-label="Board metrics">
      <MetricCard label="Board" value={board} />
      <MetricCard label="Games" value={games} />
      <MetricCard label="Props" value={props} />
      <MetricCard label="Books" value={books} />
      <MetricCard label="Alt Lines" value={alt} />
      <MetricCard label="Updated" value={updated} />
    </section>
  );
}
