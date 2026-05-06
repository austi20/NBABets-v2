import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/players",
  component: () => <PageStub title="Players" description="Player search and detail — coming in T3" />,
});

function PageStub({ title, description }: { title: string; description: string }) {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold mb-1" style={{ color: "var(--color-fg-primary)" }}>
        {title}
      </h1>
      <p className="text-sm" style={{ color: "var(--color-fg-secondary)" }}>
        {description}
      </p>
    </div>
  );
}
