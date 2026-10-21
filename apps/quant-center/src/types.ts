export type NavigationMode = "services" | "custom";

export interface ServiceGroup {
  id: string;
  label: string;
  source: string;
}

export interface ServiceItem {
  id: string;
  label: string;
  description: string;
  group: string;
  source: string;
  icon: string;
  href: string;
}

export interface NavigationConfig {
  mode: NavigationMode;
  item_ids: string[];
}

export interface PlatformOverview {
  platform: string;
  mode: string;
  live_trading_enabled: boolean;
  engine: { name: string; available: boolean; version: string | null; error: string | null };
  strategy_count: number;
  experiment_count: number;
  data_root: { configured: boolean; available: boolean };
  latest_experiment: null | {
    experiment_id: string;
    created_at?: string;
    strategy_id?: string;
    status?: string;
    metrics?: Record<string, number>;
  };
  upstream_versions: Record<string, string>;
}

export interface CenterBootstrap {
  overview: PlatformOverview;
  catalog: {
    schema_version: string;
    groups: ServiceGroup[];
    items: ServiceItem[];
  };
  navigation: NavigationConfig;
  persistence: { configured: boolean; available: boolean };
}

export interface StrategyRecord {
  strategy_id: string;
  name: string;
  version: string;
  status: string;
  market: string[];
  instruments: string[];
  engine: string;
}

export interface ExperimentRecord {
  experiment_id: string;
  created_at?: string;
  strategy_id?: string;
  status?: string;
  engine?: string | { name: string; version?: string | null };
  metrics?: Record<string, number>;
}

export interface DatasetRecord {
  dataset_id: string;
  version: string;
  kind: string;
  market: string;
  provider: string;
  frequency: string;
  format: string;
  storage_uri: string;
  fetched_at: string;
  row_count: number | null;
  quality_status: string;
  revision_policy: string;
  path: string;
}

export interface ServiceStatus {
  id: string;
  label: string;
  source: string;
  state: "online" | "degraded" | "offline";
  latency_ms: number | null;
  checked_at: string;
  detail: string;
}
