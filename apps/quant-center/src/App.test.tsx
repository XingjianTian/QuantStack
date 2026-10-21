import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import type { CenterBootstrap } from "./types";

const bootstrap: CenterBootstrap = {
  overview: {
    platform: "Personal Quant Platform",
    mode: "research_only",
    live_trading_enabled: false,
    engine: { name: "NautilusTrader", available: true, version: "1.215.0", error: null },
    strategy_count: 1,
    experiment_count: 2,
    data_root: { configured: true, available: true },
    latest_experiment: null,
    upstream_versions: {},
  },
  catalog: {
    schema_version: "1.0.0",
    groups: [
      { id: "center", label: "Quant Center", source: "quant-center" },
      { id: "screener", label: "选股与市场", source: "stock-screener" },
      { id: "research", label: "AI 投研", source: "vibe-trading" },
    ],
    items: [
      { id: "center-overview", label: "总览", description: "平台状态", group: "center", source: "quant-center", icon: "layout-dashboard", href: "/" },
      { id: "center-navigation", label: "导航定制", description: "定制入口", group: "center", source: "quant-center", icon: "panel-left", href: "/navigation" },
      { id: "screener-scan", label: "股票筛选", description: "条件扫描", group: "screener", source: "stock-screener", icon: "scan-search", href: "https://stock-screener.example/scan" },
      { id: "vibe-agent", label: "AI 研究助手", description: "辅助投研", group: "research", source: "vibe-trading", icon: "bot", href: "https://vibe.example/agent" },
    ],
  },
  navigation: { mode: "services", item_ids: ["center-overview", "screener-scan", "vibe-agent", "center-navigation"] },
  persistence: { configured: true, available: true },
};

function jsonResponse(payload: unknown) {
  return Promise.resolve(new Response(JSON.stringify(payload), { status: 200, headers: { "Content-Type": "application/json" } }));
}

describe("Quant Center", () => {
  beforeEach(() => {
    window.history.replaceState({}, "", "/");
    document.cookie = "quant-platform-theme=; Path=/; Max-Age=0";
    document.documentElement.removeAttribute("data-theme");
    vi.stubGlobal("fetch", vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith("/v1/center/bootstrap")) return jsonResponse(bootstrap);
      if (url.endsWith("/v1/center/navigation") && init?.method === "PUT") return jsonResponse(JSON.parse(String(init.body)));
      return jsonResponse([]);
    }));
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    window.localStorage.clear();
  });

  it("renders a research-only platform overview and source-aware navigation", async () => {
    render(<App />);
    expect(await screen.findByRole("heading", { name: "今天从研究开始。" })).toBeInTheDocument();
    const platformNav = screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" });
    expect(within(platformNav).getByRole("link", { name: "量化中心" })).toHaveAttribute("aria-current", "page");
    expect(within(platformNav).getByRole("link", { name: "AI 投研（VT）" })).toHaveAttribute("href", expect.stringContaining("vibe-trading"));
    expect(within(platformNav).getByRole("link", { name: "行情选股(SS)" })).toHaveAttribute("href", expect.stringContaining("stock-screener"));
    expect(within(platformNav).getByRole("link", { name: "行情选股(SS)" })).toHaveAttribute("href", expect.stringContaining("shell=1.4.0-zh.59"));
    expect(within(platformNav).getByRole("button", { name: "切换到浅色模式" })).toBeInTheDocument();
    expect(within(platformNav).getByRole("link", { name: "策略实施(NT)" })).toHaveAttribute("href", expect.stringContaining("nautilus"));
    expect(within(platformNav).getByRole("link", { name: "嘉信实盘" })).toHaveAttribute("href", "/schwab");
    expect(within(platformNav).getByRole("link", { name: "嘉信实盘" })).not.toHaveAttribute("aria-current");
    expect(screen.getByRole("link", { name: "打开 AI 研究助手" })).toHaveAttribute("href", "https://vibe.example/agent");
    expect(screen.queryByText("研究工作区")).not.toBeInTheDocument();
    expect(screen.getByText("实盘通道未启用")).toBeInTheDocument();
    expect(screen.getAllByTitle("stock-screener").length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: /股票筛选/ })).toHaveAttribute("href", expect.stringContaining("shell=1.4.0-zh.59"));
    expect(screen.getAllByTitle("vibe-trading").length).toBeGreaterThan(0);
  });

  it("keeps the platform identity visible and switches the shared theme", async () => {
    const user = userEvent.setup();
    render(<App />);
    const platformNav = screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" });
    expect(within(platformNav).getByText("QuantStack 量子栈")).toBeVisible();

    await user.click(within(platformNav).getByRole("button", { name: "切换到浅色模式" }));

    expect(document.documentElement).toHaveAttribute("data-theme", "light");
    expect(within(platformNav).getByRole("button", { name: "切换到深色模式" })).toBeInTheDocument();
    expect(document.cookie).toContain("quant-platform-theme=light");
  });

  it("renders a route-specific engineering backdrop", async () => {
    window.history.replaceState({}, "", "/services");
    const { container } = render(<App />);
    await screen.findByRole("heading", { name: "服务状态" });
    expect(container.querySelector('[data-field-variant="services"]')).toBeInTheDocument();
    expect(container.querySelector(".service-nodes")).toBeInTheDocument();
    expect(container.querySelector(".overview-radar")).not.toBeInTheDocument();
    expect(container.querySelector(".field-backdrop text")).not.toBeInTheDocument();
    expect(container.querySelector(".field-index")).not.toBeInTheDocument();
    expect(container.querySelector(".field-code")).not.toBeInTheDocument();
    const ownedCenter = container.querySelector(".service-source-owned");
    expect(ownedCenter).not.toBeNull();
    expect(within(ownedCenter as HTMLElement).getByRole("heading", { name: "自研量化中心" })).toBeVisible();
    expect(within(ownedCenter as HTMLElement).getByText("自有能力")).toBeVisible();
    expect(ownedCenter?.querySelector(".brand-symbol")).toBeInTheDocument();
  });

  it("groups navigation choices without repeating source badges per item", async () => {
    window.history.replaceState({}, "", "/navigation");
    const { container } = render(<App />);
    await screen.findByRole("heading", { name: "把常用页面放到手边" });
    expect(container.querySelector(".nav-catalog")).toHaveClass("content-surface");
    expect(container.querySelectorAll(".catalog-group-title .source-mark")).toHaveLength(2);
    expect(container.querySelectorAll(".catalog-item > .source-mark")).toHaveLength(0);
    expect(container.querySelector(".source-quant-center")).not.toBeInTheDocument();
  });

  it("switches to custom navigation and persists the mode", async () => {
    const user = userEvent.setup();
    render(<App />);
    await screen.findByRole("heading", { name: "今天从研究开始。" });
    await user.click(screen.getByRole("button", { name: "我的导航" }));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      "/api/v1/center/navigation",
      expect.objectContaining({ method: "PUT" }),
    ));
    expect(screen.getByRole("button", { name: "我的导航" })).toHaveClass("selected");
  });

  it("starts page content without a redundant internal header", async () => {
    const { container } = render(<App />);
    await screen.findByRole("heading", { name: "今天从研究开始。" });
    expect(screen.queryByRole("button", { name: "全局搜索" })).not.toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: "全局搜索" })).not.toBeInTheDocument();
    expect(container.querySelector(".topbar")).not.toBeInTheDocument();
    expect(container.querySelector(".breadcrumb")).not.toBeInTheDocument();
    expect(container.querySelector(".research-chip")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "打开导航" })).toBeInTheDocument();
  });

  it("keeps the platform navigation visible when experiments contain structured engine metadata", async () => {
    window.history.replaceState({}, "", "/experiments");
    vi.stubGlobal("fetch", vi.fn((url: string) => {
      if (url.endsWith("/v1/center/bootstrap")) return jsonResponse(bootstrap);
      if (url.includes("/v1/experiments")) return jsonResponse([{
        experiment_id: "exp-001",
        strategy_id: "sma-cross",
        status: "completed",
        engine: { name: "NautilusTrader", version: "1.215.0" },
      }]);
      return jsonResponse([]);
    }));

    render(<App />);

    expect(await screen.findByRole("heading", { name: "实验记录" })).toBeInTheDocument();
    expect(await screen.findByText("NautilusTrader 1.215.0")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "QuantStack 量子栈统一导航" })).toBeInTheDocument();
  });
});
