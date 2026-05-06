import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type DistChartProps = {
  projectedMean: number;
  projectedVariance: number | null;
  line: number;
};

type DistPoint = {
  value: number;
  density: number;
  cumulative: number;
};

const SAMPLE_COUNT = 100;
const MIN_STDDEV = 0.75;

export function DistChart({ projectedMean, projectedVariance, line }: DistChartProps) {
  const points = buildDistribution(projectedMean, projectedVariance, line);
  const underProbability = interpolateCdf(points, line);
  const overProbability = Math.max(0, Math.min(1, 1 - underProbability));

  return (
    <div className="dist-chart">
      <div className="dist-chart-meta">
        <span className="tabular">Under {line.toFixed(1)}: {(underProbability * 100).toFixed(1)}%</span>
        <span className="tabular">Over {line.toFixed(1)}: {(overProbability * 100).toFixed(1)}%</span>
      </div>
      <div className="dist-chart-canvas">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--color-smoke)" strokeOpacity={0.45} vertical={false} />
            <XAxis
              dataKey="value"
              stroke="var(--color-fg-muted)"
              tick={{ fill: "var(--color-fg-secondary)", fontSize: 11 }}
              tickFormatter={(value: number) => value.toFixed(1)}
            />
            <YAxis hide domain={[0, "auto"]} />
            <Tooltip
              contentStyle={{
                border: "1px solid var(--color-smoke)",
                borderRadius: "8px",
                background: "var(--color-surface-2)",
                color: "var(--color-fg-primary)",
              }}
              formatter={(value: number, name: string) =>
                name === "density" ? [`${(value * 100).toFixed(2)}%`, "Density"] : [value, name]
              }
              labelFormatter={(value: number) => `Projection ${value.toFixed(2)}`}
            />
            <Area
              type="monotone"
              dataKey="density"
              stroke="var(--color-crimson)"
              fill="var(--color-crimson)"
              fillOpacity={0.2}
              strokeWidth={2}
              isAnimationActive={false}
            />
            <ReferenceLine
              x={line}
              stroke="var(--color-caution)"
              strokeWidth={2}
              strokeDasharray="4 4"
              label={{ value: `Line ${line.toFixed(1)}`, fill: "var(--color-fg-secondary)", position: "top" }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function buildDistribution(projectedMean: number, projectedVariance: number | null, line: number): DistPoint[] {
  const stdDev = Math.max(Math.sqrt(Math.max(projectedVariance ?? MIN_STDDEV * MIN_STDDEV, 0)), MIN_STDDEV);
  const minValue = Math.min(projectedMean - stdDev * 4, line - stdDev * 2);
  const maxValue = Math.max(projectedMean + stdDev * 4, line + stdDev * 2);
  const step = (maxValue - minValue) / (SAMPLE_COUNT - 1);

  const unnormalized: Array<{ value: number; density: number }> = [];
  let densitySum = 0;
  for (let index = 0; index < SAMPLE_COUNT; index += 1) {
    const value = minValue + index * step;
    const density = gaussian(value, projectedMean, stdDev);
    unnormalized.push({ value, density });
    densitySum += density;
  }

  let cumulative = 0;
  return unnormalized.map((point) => {
    const normalizedDensity = densitySum > 0 ? point.density / densitySum : 0;
    cumulative += normalizedDensity;
    return {
      value: point.value,
      density: normalizedDensity,
      cumulative: Math.min(cumulative, 1),
    };
  });
}

function interpolateCdf(points: DistPoint[], line: number): number {
  if (points.length === 0) {
    return 0.5;
  }
  if (line <= points[0].value) {
    return points[0].cumulative;
  }
  if (line >= points[points.length - 1].value) {
    return points[points.length - 1].cumulative;
  }
  for (let index = 1; index < points.length; index += 1) {
    const left = points[index - 1];
    const right = points[index];
    if (line <= right.value) {
      const width = right.value - left.value;
      if (width <= 0) {
        return right.cumulative;
      }
      const weight = (line - left.value) / width;
      return left.cumulative + (right.cumulative - left.cumulative) * weight;
    }
  }
  return 0.5;
}

function gaussian(x: number, mean: number, stdDev: number): number {
  const z = (x - mean) / stdDev;
  const exponent = -(z * z) / 2;
  return Math.exp(exponent) / (stdDev * Math.sqrt(2 * Math.PI));
}
