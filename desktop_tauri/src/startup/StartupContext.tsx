import { createContext, useContext } from "react";
import type { StartupSnapshot } from "../api/client";

export type StartupContextValue = {
  snapshot: StartupSnapshot | null;
  isLoading: boolean;
  streamConnected: boolean;
  errorMessage: string | null;
  refreshStartup: () => Promise<void>;
};

export const StartupContext = createContext<StartupContextValue | null>(null);

export function useStartupContext(): StartupContextValue {
  const context = useContext(StartupContext);
  if (!context) {
    throw new Error("useStartupContext must be used within StartupContext provider");
  }
  return context;
}
