import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, Database, FlaskConical, Moon, Play, RefreshCw, ShieldCheck, Sun, Waves } from "lucide-react";
import { api, type Experiment, type Overview, type Strategy } from "./api";
import { usePlatformTheme } from "./platformTheme";

function serviceUrl(subdomain: string, path = "/") {
  const suffix = window.location.hostname.replace(/^nautilus\./, "");
  const port = window.location.port ? `:${window.location.port}` : "";
  return `${window.location.protocol}//${subdomain}.${suffix}${port}${path}`;
}

function PlatformNav() {
  const { theme, toggleTheme } = usePlatformTheme();
  const toggleLabel = theme === "dark" ? "切换到浅色模式" : "切换到深色模式";
  return (
    <nav className="platform-nav" data-surface={theme} aria-label="QuantStack 量子栈统一导航">
      <strong>QuantStack 量子栈</strong>
      <div className="platform-nav-links">
        <a href={serviceUrl("quant")}>量化中心</a>
        <a href={serviceUrl("vibe-trading", "/agent")}>AI 投研（VT）</a>
        <a href={serviceUrl("stock-screener")}>行情选股(SS)</a>
        <a href="/" aria-current="page">策略实施(NT)</a>
      </div>
      <button className="platform-theme-toggle" type="button" onClick={toggleTheme} aria-label={toggleLabel} title={toggleLabel}>
        {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
      </button>
    </nav>
  );
}

export function App() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [nextOverview, nextStrategies, nextExperiments] = await Promise.all([
        api.overview(), api.strategies(), api.experiments(),
      ]);
      setOverview(nextOverview);
      setStrategies(nextStrategies);
      setExperiments(nextExperiments);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Nautilus 服务暂时不可用");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  async function runValidation() {
    setRunning(true);
    setError(null);
    try {
      await api.runValidation();
      await load();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "验证回测运行失败");
    } finally {
      setRunning(false);
    }
  }

  const latest = experiments[0] ?? overview?.latest_experiment ?? null;

  return (
    <div className="app-shell">
      <PlatformNav />
      <main>
        <header className="hero">
          <div>
            <span className="eyebrow"><Waves size={15} /> 独立回测与策略实验服务</span>
            <h1>Nautilus 研究台</h1>
            <p>NautilusTrader 独立承担确定性回测、策略规格验证与实验留档，不再依附于 VT。</p>
          </div>
          <div className="actions">
            <button className="secondary" onClick={() => void load()} disabled={loading || running}><RefreshCw size={16} />刷新</button>
            <button className="primary" onClick={() => void runValidation()} disabled={running || !overview?.engine.available}><Play size={16} />{running ? "运行中…" : "运行验证回测"}</button>
          </div>
        </header>

        <section className="notice"><ShieldCheck size={20} /><div><strong>仅限研究环境</strong><span> 当前没有模拟盘、实盘账户、下单或资金接口。</span></div></section>
        {error && <section className="error" role="alert"><AlertTriangle size={18} /><div><strong>服务暂时不可用</strong><p>{error}</p></div></section>}

        {loading ? <div className="loading">正在加载 Nautilus 研究环境…</div> : (
          <>
            <StatusStrip overview={overview} />
            <div className="feature-grid">
              <BacktestPanel experiment={latest} running={running} />
              <PipelinePanel overview={overview} />
            </div>
            <StrategyTable strategies={strategies} />
            <ExperimentTable experiments={experiments} />
          </>
        )}
      </main>
    </div>
  );
}

function StatusStrip({ overview }: { overview: Overview | null }) {
  const items = [
    ["回测引擎", overview?.engine.available ? `在线 · ${overview.engine.version}` : "不可用", overview?.engine.available],
    ["策略规格", String(overview?.strategy_count ?? 0), true],
    ["实验记录", String(overview?.experiment_count ?? 0), true],
    ["NAS 数据根目录", overview?.data_root.available ? "已挂载" : "未挂载", overview?.data_root.available],
  ] as const;
  return <section className="status-strip">{items.map(([label, value, ok]) => <div key={label}><span><i className={ok ? "ok" : "warn"} />{label}</span><strong>{value}</strong></div>)}</section>;
}

function BacktestPanel({ experiment, running }: { experiment: Experiment | null; running: boolean }) {
  const metrics = experiment?.metrics ?? {};
  return <section className="panel"><div className="panel-head"><div><span>最新验证证据</span><h2>确定性回测</h2></div><code>{experiment?.experiment_id ?? "尚无运行记录"}</code></div>{running && <div className="progress"><i /></div>}<div className="metrics"><Metric label="最终权益" value={money(metrics.final_equity)} /><Metric label="总收益率" value={percent(metrics.total_return)} /><Metric label="最大回撤" value={percent(metrics.max_drawdown)} /><Metric label="成交次数" value={number(metrics.trade_count)} /></div><EquityCurve points={experiment?.equity_curve ?? []} /><footer><span>{experiment?.engine ? `${experiment.engine.name} ${experiment.engine.version}` : "等待首次运行"}</span><span>{dateTime(experiment?.created_at)}</span></footer></section>;
}

function EquityCurve({ points }: { points: Experiment["equity_curve"] }) {
  const polyline = useMemo(() => {
    if (points.length < 2) return "";
    const values = points.map((point) => point.equity);
    const min = Math.min(...values); const max = Math.max(...values);
    return values.map((value, index) => `${(index / (values.length - 1)) * 100},${92 - ((value - min) / Math.max(max - min, 1)) * 76}`).join(" ");
  }, [points]);
  return <div className="equity-curve">{polyline ? <svg viewBox="0 0 100 100" preserveAspectRatio="none" aria-label="最新权益曲线"><polyline points={polyline} fill="none" stroke="#38bdf8" strokeWidth="1.5" vectorEffect="non-scaling-stroke" /></svg> : <span>运行一次验证回测后显示权益曲线</span>}</div>;
}

function PipelinePanel({ overview }: { overview: Overview | null }) {
  const steps = [[FlaskConical, "策略契约", "strategy_spec.yaml"], [Activity, "执行引擎", `NautilusTrader ${overview?.engine.version ?? ""}`], [Database, "实验凭证", "NAS / experiments"]] as const;
  return <section className="panel pipeline"><div className="panel-head"><div><span>职责边界</span><h2>独立研究流水线</h2></div></div><p className="pipeline-intro">VT 产出研究想法和策略规格；本服务读取规格、执行回测，并将结果写入 Quant Stack 与 NAS。</p>{steps.map(([Icon, title, detail], index) => <div className="pipeline-step" key={title}><b><Icon size={16} /></b><div><strong>{title}</strong><code>{detail}</code></div>{index < steps.length - 1 && <i />}</div>)}</section>;
}

function StrategyTable({ strategies }: { strategies: Strategy[] }) {
  return <section className="table-panel"><header><div><span>策略契约</span><h2>Strategy Spec</h2></div><code>共 {strategies.length} 项</code></header><div className="table-scroll"><table><thead><tr><th>策略</th><th>状态</th><th>市场范围</th><th>执行引擎</th><th>版本</th></tr></thead><tbody>{strategies.map((strategy) => <tr key={strategy.strategy_id}><td><strong>{strategy.name}</strong><code>{strategy.strategy_id}</code></td><td><Status status={strategy.status} /></td><td>{strategy.instruments.join(", ") || strategy.market.join(", ") || "—"}</td><td>{strategy.engine === "nautilus_trader" ? "NautilusTrader" : strategy.engine}</td><td>v{strategy.version}</td></tr>)}</tbody></table></div>{strategies.length === 0 && <p className="empty">尚无策略规格</p>}</section>;
}

function ExperimentTable({ experiments }: { experiments: Experiment[] }) {
  return <section className="table-panel"><header><div><span>实验凭证</span><h2>近期实验记录</h2></div><code>NAS 持久化</code></header>{experiments.length ? <div className="experiment-list">{experiments.slice(0, 8).map((item) => <article key={item.experiment_id}><div><strong>{item.experiment_id}</strong><span>{item.strategy_id ?? "未知策略"} · {dateTime(item.created_at)}</span></div><Status status={item.status} /><Metric label="收益率" value={percent(item.metrics.total_return)} /><Metric label="成交" value={number(item.metrics.trade_count)} /></article>)}</div> : <p className="empty">尚无实验记录</p>}</section>;
}

function Status({ status }: { status: string }) { const labels: Record<string, string> = { research: "研究中", backtested: "已回测", completed: "已完成", failed: "失败", paper: "模拟盘", live: "实盘", retired: "已退役" }; return <span className={`status ${status}`}>{labels[status] ?? status}</span>; }
function Metric({ label, value }: { label: string; value: string }) { return <div className="metric"><span>{label}</span><strong>{value}</strong></div>; }
function money(value?: number | null) { return value == null ? "—" : new Intl.NumberFormat("zh-CN", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value); }
function percent(value?: number | null) { return value == null ? "—" : `${(value * 100).toFixed(2)}%`; }
function number(value?: number | null) { return value == null ? "—" : String(value); }
function dateTime(value?: string) { if (!value) return "—"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? value : date.toLocaleString("zh-CN"); }
