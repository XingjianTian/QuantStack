import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";

const responses = [
  { mode: "research_only", live_trading_enabled: false, engine: { name: "NautilusTrader", available: true, version: "1.215.0", error: null }, strategy_count: 1, experiment_count: 0, data_root: { configured: true, available: true }, latest_experiment: null },
  [{ strategy_id: "sma_cross_demo", name: "双均线演示策略", version: "0.1.0", status: "research", market: ["US"], instruments: ["SPY"], engine: "nautilus_trader" }],
  [],
];

describe("独立 Nautilus 研究台", () => {
  beforeEach(() => {
    document.cookie = "quant-platform-theme=; Path=/; Max-Age=0";
    document.documentElement.removeAttribute("data-theme");
    let index = 0;
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(responses[index++]), { status: 200, headers: { "Content-Type": "application/json" } })));
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("显示统一顶部导航和研究安全边界", async () => {
    render(<App />);
    const platformNav = screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" });
    expect(platformNav).toBeInTheDocument();
    expect(within(platformNav).getByText("QuantStack 量子栈")).toBeVisible();
    expect(screen.getByRole("link", { name: "行情选股(SS)" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "策略实施(NT)" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("button", { name: "切换到浅色模式" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "AI 投研（VT）" })).toHaveAttribute("href", expect.stringContaining("vibe-trading"));
    expect(screen.getByText("仅限研究环境")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("双均线演示策略")).toBeInTheDocument());
    expect(screen.getByText("在线 · 1.215.0")).toBeInTheDocument();
  });

  it("通过统一按钮切换平台主题", () => {
    render(<App />);
    const platformNav = screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" });
    fireEvent.click(within(platformNav).getByRole("button", { name: "切换到浅色模式" }));

    expect(document.documentElement).toHaveAttribute("data-theme", "light");
    expect(within(platformNav).getByRole("button", { name: "切换到深色模式" })).toBeInTheDocument();
    expect(document.cookie).toContain("quant-platform-theme=light");
  });
});
