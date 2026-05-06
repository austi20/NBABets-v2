import { createRouter } from "@tanstack/react-router";
import { Route as rootRoute } from "./routes/__root";
import { Route as indexRoute } from "./routes/index";
import { Route as playersRoute } from "./routes/players";
import { Route as parlaysRoute } from "./routes/parlays";
import { Route as tradingRoute } from "./routes/trading";
import { Route as insightsRoute } from "./routes/insights";
import { Route as settingsRoute } from "./routes/settings";
import { Route as devComponentsRoute } from "./routes/dev.components";

const routeTree = rootRoute.addChildren([
  indexRoute,
  playersRoute,
  parlaysRoute,
  tradingRoute,
  insightsRoute,
  settingsRoute,
  devComponentsRoute,
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
