import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import sample from "./brokerageSample.generated.json";

function view(synced = false, proposed = false, stale = false) {
  return { snapshot: synced ? sample.ready.snapshot : null, proposal: proposed && !stale ? sample.ready.proposal : null,
    state: { current_proposal_state: stale ? "stale" : proposed ? "current" : "absent", proposal_freshness_reasons: stale ? ["QUOTE_STALE:QXEN"] : [], state: { complete: synced, last_success_at: synced ? "2026-10-16T00:00:00Z" : null } },
    operations: { sync_count: Number(synced), propose_count: Number(proposed), last_sync_at: null, last_propose_at: null },
    funding: synced ? { available_cash: "490", buying_power: "980", open_order_occupation: "80", reservation_included: true, cash_after_open_orders: "490", proposal_cash_use: "302.80", proposal_fees: "2.80", cash_reserve: "20", remaining_cash_after_proposals: "187.20", order_ids: [], uncertain_order_ids: [] } : null,
    calculations: proposed ? [{ item_id: sample.ready.proposal.items[0].item_id, formula: "后端返回的预算与持仓上限计算依据" }] : [],
    ledger: proposed ? { entries: [{ evidence_id: "SAMPLE-EVIDENCE", plan_id: "sample-etf", layer_id: null, status: "attributed", cost: "30", fee: "0.50" }] } : null };
}
function openSample(results: unknown[], pathname = "/schwab") {
  window.history.replaceState({}, "", pathname);
  const fetch = vi.fn();
  for (const result of results) {
    if (result instanceof Error) fetch.mockRejectedValueOnce(result);
    else fetch.mockResolvedValueOnce({ ok: true, json: async () => result });
  }
  vi.stubGlobal("fetch", fetch); render(<App />); return fetch;
}
afterEach(() => { cleanup(); vi.unstubAllGlobals(); window.history.replaceState({}, "", "/"); });
describe("local fictional account workbench", () => {
  it.each(["/schwab", "/account-sample"])("opens %s as the active top-level Schwab tab without center bootstrap", async (pathname) => {
    const fetch = openSample([view()], pathname);
    expect(await screen.findByText("尚未同步样本")).toBeVisible();
    expect(screen.getByRole("heading", { name: "嘉信实盘" })).toBeVisible();
    const nav = screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" });
    expect(within(nav).getByRole("link", { name: "嘉信实盘" })).toHaveAttribute("aria-current", "page");
    expect(within(nav).getByRole("link", { name: "嘉信实盘" })).toHaveAttribute("href", "/schwab");
    expect(within(nav).getByRole("link", { name: "量化中心" })).not.toHaveAttribute("aria-current");
    expect(within(nav).getByRole("link", { name: "策略实施(NT)" })).toBeVisible();
    expect(screen.getByText(/当前先用虚构账户/)).toBeVisible();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(String(fetch.mock.calls[0][0])).toContain("brokerage-sample/state");
  });
  it("starts empty, syncs and recalculates through the API", async () => {
    const user = userEvent.setup(); const fetch = openSample([view(), view(true), view(true, true)]);
    expect(await screen.findByText("尚未同步样本")).toBeVisible();
    expect(screen.getByRole("button", { name: "重新生成清单" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "同步样本账户" }));
    expect(await screen.findByText("样本快照已保存")).toBeVisible();
    expect(screen.getByRole("table", { name: "样本交易流水" })).toBeVisible();
    expect(within(screen.getByRole("table", { name: "当前样本持仓" })).getByRole("columnheader", { name: "持仓总成本" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "重新生成清单" }));
    const table = await screen.findByRole("table", { name: "虚构账户拟买入建议" });
    expect(within(table).getByText("5 股")).toBeVisible(); expect(within(table).getByText("3 股")).toBeVisible();
    expect(screen.getByText("$302.80")).toBeVisible();
    await user.click(screen.getAllByText(/预算与数据依据/)[0]);
    expect(screen.getByText("后端返回的预算与持仓上限计算依据")).toBeVisible();
    await user.click(screen.getByText("预算归属与投入记录", { exact: true }));
    expect(screen.getByText("$30.00")).toBeVisible();
    expect(screen.getByText("费用 $0.50")).toBeVisible();
    expect(fetch.mock.calls.map(call => call[0])).toEqual(["/brokerage-sample-api/v1/brokerage-sample/state?profile=ready&stale=false", "/brokerage-sample-api/v1/brokerage-sample/sync", "/brokerage-sample-api/v1/brokerage-sample/propose"]);
    for (const call of fetch.mock.calls.slice(1)) expect(call[1]).toMatchObject({ method: "POST", body: '{"profile":"ready"}', credentials: "same-origin" });
    expect(screen.queryByRole("button", { name: /下单|提交|授权/ })).not.toBeInTheDocument();
  });
  it("rechecks stale state after recalculation without reviving suggestions", async () => {
    const user = userEvent.setup(); const fetch = openSample([view(true, true), view(true, true, true), view(true, true), view(true, true, true)]);
    await screen.findByRole("table", { name: "虚构账户拟买入建议" });
    await user.click(screen.getByRole("button", { name: "报价过期" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("QXEN 报价已过期");
    await user.click(screen.getByRole("button", { name: "重新生成清单" }));
    await screen.findByText("清单已重新计算生成；未扣减投入预算。");
    expect(screen.queryByRole("table", { name: "虚构账户拟买入建议" })).not.toBeInTheDocument();
    expect(fetch.mock.calls.at(-1)?.[0]).toContain("stale=true");
  });
  it("hides previous data on failure and restores persisted state on retry", async () => {
    const user = userEvent.setup(); openSample([view(true, true), new Error("offline"), view(true, true)]);
    await screen.findByRole("table", { name: "虚构账户拟买入建议" });
    await user.click(screen.getByRole("button", { name: "同步样本账户" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("已隐藏旧数据");
    expect(screen.queryByRole("table", { name: "虚构账户拟买入建议" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重试读取状态" }));
    expect(await screen.findByRole("table", { name: "虚构账户拟买入建议" })).toBeVisible();
  });
  it("does not fall back to fixed JSON when the service is unavailable", async () => {
    openSample([new Error("offline")]); expect(await screen.findByRole("alert")).toHaveTextContent("样本服务暂时不可用");
    expect(screen.queryByText("$500.00")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "同步样本账户" })).toBeDisabled();
  });
});
