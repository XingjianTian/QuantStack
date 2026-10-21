import type sample from "./brokerageSample.generated.json";

export type SampleScenario = "ready" | "blocked" | "stale";
type Snapshot = typeof sample.ready.snapshot;
type Proposal = typeof sample.ready.proposal;
export interface WorkbenchData {
  snapshot: Snapshot | null;
  proposal: Proposal | null;
  state: {
    current_proposal_state: string;
    proposal_freshness_reasons: string[];
    state: { last_attempt_status: string | null; last_success_at: string | null; complete: boolean };
  };
  operations: { sync_count: number; propose_count: number; last_sync_at: string | null; last_propose_at: string | null };
  funding: null | {
    currency: string;
    available_cash: string | null;
    buying_power: string | null;
    open_order_occupation: string;
    reservation_included: boolean | null;
    cash_after_open_orders: string | null;
    proposal_cash_use: string;
    proposal_fees: string;
    cash_reserve: string;
    remaining_cash_after_proposals: string | null;
    blocking_reasons: string[];
    order_ids: string[];
    uncertain_order_ids: string[];
  };
  calculations: {
    item_id: string; plan_id: string; layer_id: string | null; symbol: string;
    strategy_budget: string; spent_budget: string; remaining_budget: string;
    assumed_limit_price: string | null; quantity: string; gross_amount: string;
    estimated_fee: string; cash_reserve: string; maximum_cash_use: string;
    budget_before: string; budget_after_proposal: string; formula: string;
  }[];
  ledger: null | { entries: { evidence_id: string; source_transaction_id?: string | null; plan_id: string | null; layer_id: string | null; status: string; cost: string; fee: string }[] };
}

export async function sampleRequest(scenario: SampleScenario, action: "state" | "sync" | "propose", signal?: AbortSignal): Promise<WorkbenchData> {
  const profile = scenario === "blocked" ? "blocked" : "ready";
  const query = new URLSearchParams({ profile, stale: String(scenario === "stale") });
  const response = await fetch(`/brokerage-sample-api/v1/brokerage-sample/${action}${action === "state" ? `?${query}` : ""}`, {
    method: action === "state" ? "GET" : "POST",
    headers: action === "state" ? undefined : { "Content-Type": "application/json" },
    body: action === "state" ? undefined : JSON.stringify({ profile }),
    cache: "no-store", credentials: "same-origin", signal,
  });
  if (!response.ok) throw new Error("本地样本服务未完成请求，请确认服务已启动后重试。");
  const result = await response.json() as WorkbenchData;
  // Re-read at the stale scenario clock after a real mutation, never revive an old quote.
  if (scenario === "stale" && action !== "state") return sampleRequest(scenario, "state", signal);
  return result;
}
