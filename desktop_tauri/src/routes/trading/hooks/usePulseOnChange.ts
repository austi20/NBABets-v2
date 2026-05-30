// desktop_tauri/src/routes/trading/hooks/usePulseOnChange.ts
import { useEffect, useRef, useState } from "react";

export type PulseDirection = "up" | "down" | null;

export function usePulseOnChange(value: number): PulseDirection {
  const previousRef = useRef(value);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [pulse, setPulse] = useState<PulseDirection>(null);

  useEffect(() => {
    if (value > previousRef.current) setPulse("up");
    else if (value < previousRef.current) setPulse("down");
    previousRef.current = value;
    if (timerRef.current !== null) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => setPulse(null), 250);
    return () => {
      if (timerRef.current !== null) clearTimeout(timerRef.current);
    };
  }, [value]);

  return pulse;
}
