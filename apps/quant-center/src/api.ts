import type { CenterBootstrap, DatasetRecord, ExperimentRecord, NavigationConfig, ServiceStatus, StrategyRecord } from "./types";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export const centerApi = {
  bootstrap: () => request<CenterBootstrap>("/v1/center/bootstrap"),
  saveNavigation: (config: NavigationConfig) =>
    request<NavigationConfig>("/v1/center/navigation", {
      method: "PUT",
      body: JSON.stringify(config),
    }),
  strategies: () => request<StrategyRecord[]>("/v1/strategies"),
  experiments: () => request<ExperimentRecord[]>("/v1/experiments?limit=50"),
  datasets: () => request<DatasetRecord[]>("/v1/datasets"),
  services: () => request<ServiceStatus[]>("/v1/center/services"),
};
