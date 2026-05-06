import { Outlet } from "@tanstack/react-router";
import { useStartupSnapshot } from "../../hooks/useStartupSnapshot";
import { api } from "../../api/client";
import { StartupContext } from "../../startup/StartupContext";
import { NavRail } from "./NavRail";
import { StartupOverlay } from "./StartupOverlay";
import { useQuery } from "@tanstack/react-query";

export function AppShell() {
  const startup = useStartupSnapshot();
  const { data: tradingPnl } = useQuery({
    queryKey: ["trading", "pnl"],
    queryFn: api.tradingPnl,
    staleTime: 5_000,
    refetchInterval: 5_000,
    retry: false,
  });

  return (
    <StartupContext.Provider value={startup}>
      <div className="flex h-full overflow-hidden" style={{ backgroundColor: "var(--color-base)" }}>
        <StartupOverlay
          snapshot={startup.snapshot}
          isLoading={startup.isLoading}
          streamConnected={startup.streamConnected}
        />
        <NavRail />
        <div className="flex flex-col flex-1 overflow-hidden">
          {tradingPnl?.kill_switch_active ? (
            <div className="kill-switch-banner" role="status" aria-live="polite">
              Kill switch active: new trading intents are disabled for this session.
            </div>
          ) : null}
          <main className="flex-1 overflow-auto">
            <Outlet />
          </main>
        </div>
      </div>
    </StartupContext.Provider>
  );
}
