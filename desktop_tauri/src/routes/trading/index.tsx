// desktop_tauri/src/routes/trading/index.tsx
import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "../__root";
import "../../styles/trading.css";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPageV2,
});

function TradingPageV2() {
  return (
    <div className="trading-page-v2">
      <p className="micro">Trading terminal -- under construction</p>
    </div>
  );
}
