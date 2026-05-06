import { useState } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  LayoutDashboard,
  Search,
  Layers,
  BarChart2,
  Activity,
  Settings,
} from "lucide-react";

type NavItem = {
  to: string;
  icon: React.ReactNode;
  label: string;
};

const TOP_ITEMS: NavItem[] = [
  { to: "/", icon: <LayoutDashboard size={20} />, label: "Dashboard" },
  { to: "/players", icon: <Search size={20} />, label: "Players" },
  { to: "/parlays", icon: <Layers size={20} />, label: "Parlays" },
  { to: "/trading", icon: <BarChart2 size={20} />, label: "Trading" },
  { to: "/insights", icon: <Activity size={20} />, label: "Insights" },
];

export function NavRail() {
  const [expanded, setExpanded] = useState(false);
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;

  const isActive = (to: string) =>
    to === "/" ? currentPath === "/" : currentPath.startsWith(to);

  return (
    <nav
      className="flex flex-col h-full py-3 border-r transition-all duration-200 ease-out overflow-hidden shrink-0"
      style={{
        width: expanded ? "200px" : "48px",
        backgroundColor: "var(--color-surface-1)",
        borderColor: "var(--color-smoke)",
      }}
      onMouseEnter={() => setExpanded(true)}
      onMouseLeave={() => setExpanded(false)}
    >
      {/* Logo mark */}
      <div
        className="flex items-center h-10 mb-4 px-3 shrink-0"
        style={{ minWidth: "48px" }}
      >
        <span
          className="text-lg font-bold shrink-0"
          style={{ color: "var(--color-crimson)" }}
        >
          N
        </span>
        {expanded && (
          <span
            className="ml-1 text-sm font-semibold truncate"
            style={{ color: "var(--color-fg-primary)" }}
          >
            BA Props
          </span>
        )}
      </div>

      {/* Top nav items */}
      <div className="flex flex-col gap-1 flex-1 px-1">
        {TOP_ITEMS.map((item) => (
          <NavItem
            key={item.to}
            item={item}
            active={isActive(item.to)}
            expanded={expanded}
          />
        ))}
      </div>

      {/* Bottom-pinned settings */}
      <div className="px-1 pb-1">
        <NavItem
          item={{ to: "/settings", icon: <Settings size={20} />, label: "Settings" }}
          active={isActive("/settings")}
          expanded={expanded}
        />
      </div>
    </nav>
  );
}

function NavItem({
  item,
  active,
  expanded,
}: {
  item: NavItem;
  active: boolean;
  expanded: boolean;
}) {
  return (
    <Link
      to={item.to}
      aria-label={item.label}
      className="flex items-center gap-3 h-10 px-3 rounded-lg transition-colors duration-150 shrink-0 outline-none"
      style={{
        backgroundColor: active ? "var(--accent-glow)" : "transparent",
        color: active ? "var(--color-crimson)" : "var(--color-fg-secondary)",
        borderLeft: active ? `2px solid var(--color-crimson)` : "2px solid transparent",
      }}
      onMouseEnter={(e) => {
        if (!active) {
          (e.currentTarget as HTMLElement).style.backgroundColor =
            "var(--color-surface-3)";
          (e.currentTarget as HTMLElement).style.color =
            "var(--color-fg-primary)";
        }
      }}
      onMouseLeave={(e) => {
        if (!active) {
          (e.currentTarget as HTMLElement).style.backgroundColor = "transparent";
          (e.currentTarget as HTMLElement).style.color =
            "var(--color-fg-secondary)";
        }
      }}
    >
      <span className="shrink-0">{item.icon}</span>
      {expanded && (
        <span className="text-sm font-medium truncate">{item.label}</span>
      )}
    </Link>
  );
}
