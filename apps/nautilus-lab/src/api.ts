export interface Overview {
  mode: "research_only";
  live_trading_enabled: boolean;
  engine: { name: string; available: boolean; version: string | null; error: string | null };
  strategy_count: number;
  experiment_count: number;
  data_root: { configured: boolean; available: boolean };
  latest_experiment: Experiment | null;
}

export interface Strategy {
  strategy_id: string;
  name: string;
  version: string;
  status: string;
  market: string[];
  instruments: string[];
  engine: string;
}

export interface Experiment {
  experiment_id: string;
  created_at?: string;
  strategy_id?: string;
  status: string;
  engine?: { name: string; version: string };
  metrics: Record<string, number | null>;
  equity_curve: Array<{ timestamp: string; equity: number }>;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!response.ok) {
    let message = `请求失败（HTTP ${response.status}）`;
    try {
      const payload = await response.json() as { detail?: string };
      if (payload.detail) message = payload.detail;
    } catch {
      // Keep the status-based message for non-JSON gateway errors.
    }
    throw new Error(message);
  }
  return response.json() as Promise<T>;
}

export const api = {
  overview: () => request<Overview>("/v1/overview"),
  strategies: () => request<Strategy[]>("/v1/strategies"),
  experiments: () => request<Experiment[]>("/v1/experiments?limit=20"),
  runValidation: () => request<Experiment>("/v1/backtests/synthetic", { method: "POST" }),
};
