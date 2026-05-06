import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { invoke } from "@tauri-apps/api/core";
import { attachConsole } from "@tauri-apps/plugin-log";
import { router } from "./router";
import "./theme.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 2,
    },
  },
});

type SidecarConfig = {
  api_base: string;
  app_token: string;
};

async function hydrateSidecarConfig() {
  try {
    const config = await invoke<SidecarConfig | null>("get_sidecar_config");
    if (!config) {
      return;
    }
    window.__NBA_API_BASE__ = config.api_base;
    window.__APP_TOKEN__ = config.app_token;
  } catch {
    // In browser-only dev runs there is no Tauri invoke bridge.
  }
}

async function bootstrap() {
  try {
    await attachConsole();
  } catch {
    // Browser-only dev runs do not have the Tauri plugin bridge.
  }
  await hydrateSidecarConfig();
  const root = document.getElementById("root");
  if (!root) throw new Error("Root element not found");
  createRoot(root).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </StrictMode>,
  );
}

void bootstrap();
