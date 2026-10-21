import {
  Activity,
  ArrowDown,
  ArrowUp,
  Atom,
  Bot,
  ChartCandlestick,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  CircleCheck,
  Database,
  ExternalLink,
  FileChartColumn,
  GitBranch,
  Grid3X3,
  History,
  Layers3,
  LayoutDashboard,
  Menu,
  Moon,
  PanelLeft,
  ScanSearch,
  ServerCog,
  ShipWheel,
  Sparkles,
  Sun,
  Waves,
  Workflow,
  X,
  type LucideIcon,
} from "lucide-react";
import { useEffect, useMemo, useState, type MouseEvent, type ReactNode } from "react";
import { centerApi } from "./api";
import { BrokerageSample, SampleSidebar } from "./BrokerageSample";
import type {
  CenterBootstrap,
  DatasetRecord,
  ExperimentRecord,
  NavigationConfig,
  NavigationMode,
  ServiceGroup,
  ServiceItem,
  ServiceStatus,
  StrategyRecord,
} from "./types";
import { usePlatformTheme } from "./platformTheme";

const bootstrapCacheKey = "quant-center.bootstrap.v1";

const iconMap: Record<string, LucideIcon> = {
  "layout-dashboard": LayoutDashboard,
  "git-branch": GitBranch,
  history: History,
  database: Database,
  activity: Activity,
  "panel-left": PanelLeft,
  "chart-candlestick": ChartCandlestick,
  "scan-search": ScanSearch,
  waves: Waves,
  "layers-3": Layers3,
  "server-cog": ServerCog,
  bot: Bot,
  "file-chart-column": FileChartColumn,
  atom: Atom,
  "grid-3x3": Grid3X3,
  workflow: Workflow,
  "ship-wheel": ShipWheel,
};

const sourceNames: Record<string, string> = {
  "quant-center": "QC",
  "stock-screener": "SS",
  "vibe-trading": "VT",
  "nautilus-trader": "NT",
};

function platformServiceUrl(subdomain: string, path = "/") {
  const port = window.location.port ? `:${window.location.port}` : "";
  const host = window.location.hostname;
  const suffix = host === "localhost" || host === "127.0.0.1" || host === "[::1]"
    ? "localhost"
    : host.replace(/^quant\./, "");
  return `${window.location.protocol}//${subdomain}.${suffix}${port}${path}`;
}

const stockScreenerShellVersion = "1.4.0-zh.59";

function withStockScreenerShell(href: string) {
  const url = new URL(href, window.location.origin);
  url.searchParams.set("shell", stockScreenerShellVersion);
  return url.toString();
}

function stockScreenerUrl(path = "/") {
  return withStockScreenerShell(platformServiceUrl("stock-screener", path));
}

function PlatformNav() {
  const pathname = usePathname();
  const isSchwab = pathname === "/schwab" || pathname === "/account-sample";
  const { theme, toggleTheme } = usePlatformTheme();
  const toggleLabel = theme === "dark" ? "切换到浅色模式" : "切换到深色模式";
  return (
    <nav className="platform-nav" data-surface={theme} aria-label="QuantStack 量子栈统一导航">
      <strong>QuantStack 量子栈</strong>
      <div className="platform-nav-links">
        <a href="/" aria-current={isSchwab ? undefined : "page"}>量化中心</a>
        <a href={platformServiceUrl("vibe-trading", "/agent")}>AI 投研（VT）</a>
        <a href={stockScreenerUrl()}>行情选股(SS)</a>
        <a href={platformServiceUrl("nautilus")}>策略实施(NT)</a>
        <a href="/schwab" aria-current={isSchwab ? "page" : undefined}>嘉信实盘</a>
      </div>
      <button className="platform-theme-toggle" type="button" onClick={toggleTheme} aria-label={toggleLabel} title={toggleLabel}>
        {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
      </button>
    </nav>
  );
}

function usePathname() {
  const [pathname, setPathname] = useState(window.location.pathname);
  useEffect(() => {
    const update = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", update);
    return () => window.removeEventListener("popstate", update);
  }, []);
  return pathname;
}

function InternalLink({ to, className, onClick, children }: {
  to: string;
  className?: string;
  onClick?: () => void;
  children: ReactNode;
}) {
  function navigate(event: MouseEvent<HTMLAnchorElement>) {
    onClick?.();
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    if (window.location.pathname !== to) {
      window.history.pushState({}, "", to);
      window.dispatchEvent(new PopStateEvent("popstate"));
    }
  }
  return <a href={to} className={className} onClick={navigate}>{children}</a>;
}

type ServiceTarget = Pick<ServiceItem, "id" | "href" | "source">;

function ServiceLink({ item, className, onClick, children }: {
  item: ServiceTarget;
  className?: string;
  onClick?: () => void;
  children: ReactNode;
}) {
  const href = item.source === "stock-screener"
    ? withStockScreenerShell(item.href)
    : item.href;
  if (href.startsWith("http")) {
    return <a href={href} className={className} onClick={onClick}>{children}</a>;
  }
  return <InternalLink to={href} className={className} onClick={onClick}>{children}</InternalLink>;
}

function SourceMark({ source, compact = false }: { source: string; compact?: boolean }) {
  if (source === "quant-center") return null;
  return (
    <span className={`source-mark source-${source}${compact ? " compact" : ""}`} title={source}>
      {sourceNames[source] ?? "EXT"}
    </span>
  );
}

function NavigationIcon({ item }: { item: ServiceItem }) {
  const Icon = iconMap[item.icon] ?? Sparkles;
  return <Icon size={17} strokeWidth={1.8} />;
}

function SidebarLink({ item, closeMobile }: { item: ServiceItem; closeMobile: () => void }) {
  const content = (
    <>
      <span className="nav-icon"><NavigationIcon item={item} /></span>
      <span className="nav-label">{item.label}</span>
      <SourceMark source={item.source} compact />
    </>
  );
  return (
    <ServiceLink className={`nav-link${window.location.pathname === item.href ? " active" : ""}`} item={item} onClick={closeMobile}>
      {content}
    </ServiceLink>
  );
}

function Sidebar({
  bootstrap,
  navigation,
  onModeChange,
  mobileOpen,
  closeMobile,
}: {
  bootstrap: CenterBootstrap;
  navigation: NavigationConfig;
  onModeChange: (mode: NavigationMode) => void;
  mobileOpen: boolean;
  closeMobile: () => void;
}) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({
    center: true,
    screener: true,
    research: true,
    execution: true,
  });
  const byGroup = useMemo(
    () => bootstrap.catalog.items.reduce<Record<string, ServiceItem[]>>((groups, item) => {
      (groups[item.group] ??= []).push(item);
      return groups;
    }, {}),
    [bootstrap.catalog.items],
  );
  const selected = new Set(navigation.item_ids);

  return (
    <aside className={`sidebar${mobileOpen ? " mobile-open" : ""}`}>
      <div className="brand-row">
        <div className="brand-symbol" aria-hidden="true"><span /><span /><span /></div>
        <div><strong>量化中心</strong><small>RESEARCH OS</small></div>
        <button className="mobile-close" onClick={closeMobile} aria-label="关闭导航"><X size={18} /></button>
      </div>

      <div className="nav-mode" aria-label="导航模式">
        <button className={navigation.mode === "services" ? "selected" : ""} onClick={() => onModeChange("services")}>服务目录</button>
        <button className={navigation.mode === "custom" ? "selected" : ""} onClick={() => onModeChange("custom")}>我的导航</button>
      </div>

      <nav className="sidebar-nav">
        {navigation.mode === "custom" ? (
          <div className="custom-links">
            {navigation.item_ids.map((id) => bootstrap.catalog.items.find((item) => item.id === id)).filter(Boolean).map((item) => (
              <SidebarLink key={item!.id} item={item!} closeMobile={closeMobile} />
            ))}
          </div>
        ) : (
          bootstrap.catalog.groups.map((group) => (
            <div className="nav-group" key={group.id}>
              <button className="nav-group-title" onClick={() => setExpanded((state) => ({ ...state, [group.id]: !state[group.id] }))}>
                <span>{group.label}</span>
                {expanded[group.id] ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
              </button>
              {expanded[group.id] && (byGroup[group.id] ?? []).map((item) => (
                <SidebarLink key={item.id} item={item} closeMobile={closeMobile} />
              ))}
            </div>
          ))
        )}
      </nav>

      <footer className="sidebar-foot">
        <span className="pulse-dot" />
        <div><strong>研究模式</strong><small>实盘通道未启用</small></div>
        <span>{selected.size}</span>
      </footer>
    </aside>
  );
}

function PageIntro({ eyebrow, title, copy, action }: { eyebrow: string; title: string; copy: string; action?: ReactNode }) {
  return (
    <div className="page-intro">
      <div><span className="eyebrow">{eyebrow}</span><h1>{title}</h1><p>{copy}</p></div>
      {action}
    </div>
  );
}

type FieldVariant = "overview" | "strategies" | "experiments" | "data" | "services" | "navigation";

const routeFields: Record<string, { variant: FieldVariant }> = {
  "/": { variant: "overview" },
  "/strategies": { variant: "strategies" },
  "/experiments": { variant: "experiments" },
  "/data": { variant: "data" },
  "/services": { variant: "services" },
  "/navigation": { variant: "navigation" },
};

function FieldDiagram({ variant }: { variant: FieldVariant }) {
  if (variant === "strategies") return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="diagram-lines strategy-lines">
        <path d="M116 280H270M270 280L410 138M270 280L410 280M270 280L410 422M548 138L700 214M548 280H700M548 422L700 346" />
        <path className="diagram-dash" d="M700 214V346M735 214V346" />
      </g>
      <g className="diagram-nodes strategy-nodes">
        <rect x="70" y="240" width="92" height="80"/><rect x="230" y="240" width="80" height="80"/>
        <rect x="410" y="101" width="138" height="74"/><rect x="410" y="243" width="138" height="74"/><rect x="410" y="385" width="138" height="74"/>
        <rect className="diagram-accent" x="700" y="176" width="78" height="76"/><rect x="700" y="308" width="78" height="76"/>
      </g>
    </svg>
  );
  if (variant === "experiments") return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="diagram-lines experiment-lines">
        <path d="M70 120H818M70 220H818M70 320H818M70 420H818" />
        <path className="diagram-dash" d="M190 76V464M430 76V464M690 76V464" />
        <path className="diagram-signal" d="M70 320L150 320L190 220L270 220L318 420L430 420L488 120L590 120L630 320L690 320L732 220L818 220" />
      </g>
      <g className="diagram-nodes experiment-nodes"><circle cx="190" cy="220" r="10"/><circle cx="430" cy="420" r="10"/><circle className="diagram-accent" cx="690" cy="320" r="13"/></g>
    </svg>
  );
  if (variant === "data") return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="diagram-lines data-lines"><path d="M85 180H270M490 180H700M270 180V370H490V180"/><path className="diagram-dash" d="M120 236H238M120 274H238M120 312H238M536 236H654M536 274H654M536 312H654"/></g>
      <g className="data-stack"><path d="M85 150L177 116L270 150L177 185Z"/><path d="M85 150V340L177 382L270 340V150"/><path d="M85 214L177 252L270 214M85 278L177 316L270 278"/></g>
      <g className="data-core"><rect className="diagram-accent" x="330" y="310" width="160" height="112"/><path d="M350 338H470M350 365H470M350 392H430"/></g>
      <g className="data-stack"><path d="M700 150L792 116L884 150L792 185Z"/><path d="M700 150V340L792 382L884 340V150"/><path d="M700 214L792 252L884 214M700 278L792 316L884 278"/></g>
    </svg>
  );
  if (variant === "services") return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="diagram-lines service-lines"><path d="M448 278L170 128M448 278L720 118M448 278L762 342M448 278L492 470M448 278L152 404"/><path className="diagram-dash" d="M170 128L720 118L762 342L492 470L152 404Z"/></g>
      <g className="service-nodes"><circle className="diagram-accent" cx="448" cy="278" r="72"/><circle cx="170" cy="128" r="44"/><circle cx="720" cy="118" r="44"/><circle cx="762" cy="342" r="44"/><circle cx="492" cy="470" r="44"/><circle cx="152" cy="404" r="44"/><circle className="service-pulse" cx="448" cy="278" r="102"/></g>
    </svg>
  );
  if (variant === "navigation") return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="navigation-map"><rect x="74" y="88" width="190" height="112"/><rect x="354" y="72" width="190" height="112"/><rect x="640" y="104" width="190" height="112"/><rect x="112" y="344" width="190" height="112"/><rect className="diagram-accent" x="390" y="316" width="190" height="112"/><rect x="676" y="358" width="150" height="94"/></g>
      <g className="diagram-lines navigation-lines"><path d="M264 144H354M544 128L640 160M735 216V272H485V316M390 372H302M580 372L676 405"/><path className="diagram-signal" d="M96 270H318L344 244L370 270H594"/></g>
    </svg>
  );
  return (
    <svg className="field-diagram" viewBox="0 0 900 560">
      <g className="overview-radar"><circle cx="610" cy="260" r="210"/><circle cx="610" cy="260" r="142"/><circle cx="610" cy="260" r="70"/><path d="M610 18V502M368 260H852M439 89L781 431M439 431L781 89"/><path className="diagram-signal" d="M610 260L720 116L804 260L676 348L514 414L450 244Z"/></g>
      <g className="diagram-nodes"><circle className="diagram-accent" cx="610" cy="260" r="12"/><circle cx="720" cy="116" r="9"/><circle cx="804" cy="260" r="9"/><circle cx="676" cy="348" r="9"/><circle cx="514" cy="414" r="9"/><circle cx="450" cy="244" r="9"/></g>
    </svg>
  );
}

function FieldBackdrop({ pathname }: { pathname: string }) {
  const field = routeFields[pathname] ?? { variant: "overview" as const };
  return (
    <div className="field-backdrop" data-field-variant={field.variant} aria-hidden="true">
      <div className="field-grid" />
      <FieldDiagram variant={field.variant} />
      <div className="field-scale" />
    </div>
  );
}

function StatusLine({ ok, label, detail }: { ok: boolean; label: string; detail: string }) {
  return (
    <div className="status-line">
      <span className={ok ? "status-icon ok" : "status-icon warn"}>{ok ? <CircleCheck size={17} /> : <CircleAlert size={17} />}</span>
      <div><strong>{label}</strong><small>{detail}</small></div>
    </div>
  );
}

function OverviewPage({ bootstrap }: { bootstrap: CenterBootstrap }) {
  const { overview } = bootstrap;
  const externalGroups = bootstrap.catalog.groups.filter((group) => group.source !== "quant-center");
  const vibeAgentUrl = bootstrap.catalog.items.find((item) => item.id === "vibe-agent")?.href ?? platformServiceUrl("vibe-trading", "/agent");
  return (
    <main className="page overview-page">
      <PageIntro
        eyebrow="PERSONAL QUANT PLATFORM"
        title="今天从研究开始。"
        copy="把市场观察、AI 投研、策略规格和可复现实验放进同一条工作流。"
        action={<a className="primary-action" href={vibeAgentUrl}><Bot size={16} />打开 AI 研究助手</a>}
      />

      <section className="signal-board">
        <div className="regime-panel">
          <div className="section-heading"><div><span>平台信号</span><h2>研究基础设施已就绪</h2></div><span className="freshness">刚刚同步</span></div>
          <svg className="signal-line" viewBox="0 0 760 180" role="img" aria-label="研究流程状态示意">
            <defs><linearGradient id="signal-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stopColor="#f1832c" stopOpacity=".28"/><stop offset="1" stopColor="#f1832c" stopOpacity="0"/></linearGradient></defs>
            <path className="grid-line" d="M0 35H760M0 90H760M0 145H760" />
            <path className="area-line" d="M0 138 C65 128 92 110 144 115 S235 148 300 112 S389 56 448 73 S535 130 594 87 S690 31 760 48 L760 180 L0 180Z" />
            <path className="stroke-line" d="M0 138 C65 128 92 110 144 115 S235 148 300 112 S389 56 448 73 S535 130 594 87 S690 31 760 48" />
            <circle cx="760" cy="48" r="5" />
          </svg>
          <div className="flow-labels"><span>数据接入</span><span>市场研究</span><span>策略规格</span><span>回测验证</span></div>
        </div>
        <div className="platform-kpis">
          <div><span>策略规格</span><strong>{overview.strategy_count}</strong><small>受版本控制</small></div>
          <div><span>实验记录</span><strong>{overview.experiment_count}</strong><small>保存在 NAS</small></div>
          <div><span>回测引擎</span><strong className={overview.engine.available ? "good" : "muted"}>{overview.engine.available ? "READY" : "OFFLINE"}</strong><small>{overview.engine.name} {overview.engine.version ?? ""}</small></div>
          <div><span>交易权限</span><strong className="safe">关闭</strong><small>研究阶段保护</small></div>
        </div>
      </section>

      <section className="workspace-grid">
        <div className="workspace-column">
          <div className="section-heading compact"><div><span>工作流</span><h2>从想法到可验证结果</h2></div></div>
          <div className="workflow-list">
            <div><span className="step-index">01</span><div><strong>提出研究问题</strong><small>在 VT 中探索市场与信号假设</small></div><a href={vibeAgentUrl}>进入 VT <ExternalLink size={14}/></a></div>
            <div><span className="step-index">02</span><div><strong>形成策略规格</strong><small>统一记录数据、规则、仓位与风险</small></div><InternalLink to="/strategies">查看策略</InternalLink></div>
            <div><span className="step-index">03</span><div><strong>运行可复现实验</strong><small>锁定代码、数据版本和回测结果</small></div><InternalLink to="/experiments">查看实验</InternalLink></div>
          </div>
        </div>
        <div className="workspace-column service-column">
          <div className="section-heading compact"><div><span>已接入服务</span><h2>专业工具保持独立升级</h2></div><InternalLink to="/services">全部服务</InternalLink></div>
          <div className="service-strips">
            {externalGroups.map((group) => {
              const first = bootstrap.catalog.items.find((item) => item.group === group.id);
              return first ? <ServiceLink key={group.id} item={first}><SourceMark source={group.source}/><div><strong>{group.label}</strong><small>{first.description}</small></div><ChevronRight size={15}/></ServiceLink> : null;
            })}
          </div>
        </div>
      </section>

      <section className="integrity-strip">
        <StatusLine ok={overview.data_root.available} label="NAS 数据根目录" detail={overview.data_root.available ? "已挂载，可写入实验与配置" : "尚未挂载"} />
        <StatusLine ok={bootstrap.persistence.available} label="导航配置" detail={bootstrap.persistence.available ? "跨浏览器保存在服务器" : "当前仅临时可用"} />
        <StatusLine ok={!overview.live_trading_enabled} label="交易安全边界" detail="无实盘订单入口或券商凭据" />
      </section>
    </main>
  );
}

function StrategiesPage() {
  const [records, setRecords] = useState<StrategyRecord[]>([]);
  const [error, setError] = useState("");
  useEffect(() => { centerApi.strategies().then(setRecords).catch((cause: Error) => setError(cause.message)); }, []);
  return (
    <main className="page strategies-page">
      <PageIntro eyebrow="STRATEGY CONTRACTS" title="策略库" copy="每一个策略都以统一规格描述，并沿研究、回测、模拟、实盘、退役推进。" />
      {error ? <div className="error-banner">策略接口暂时不可用：{error}</div> : (
        <div className="data-table content-surface"><div className="table-head"><span>策略</span><span>市场</span><span>引擎</span><span>版本</span><span>状态</span></div>{records.map((record) => <div className="table-row" key={record.strategy_id}><span><strong>{record.name}</strong><small>{record.strategy_id}</small></span><span>{record.market.join(" / ")}</span><span>{record.engine}</span><span>v{record.version}</span><span><em className={`state state-${record.status}`}>{record.status}</em></span></div>)}{records.length === 0 && <div className="table-empty">暂无策略规格</div>}</div>
      )}
    </main>
  );
}

function ExperimentsPage() {
  const [records, setRecords] = useState<ExperimentRecord[]>([]);
  const [error, setError] = useState("");
  useEffect(() => { centerApi.experiments().then(setRecords).catch((cause: Error) => setError(cause.message)); }, []);
  return (
    <main className="page experiments-page">
      <PageIntro eyebrow="REPRODUCIBLE RUNS" title="实验记录" copy="代码提交、数据版本、参数、指标和报告一起保存。" />
      {error ? <div className="error-banner">实验接口暂时不可用：{error}</div> : (
        <div className="timeline-list content-surface">{records.map((record) => {
          const engine = typeof record.engine === "string"
            ? record.engine
            : record.engine
              ? `${record.engine.name}${record.engine.version ? ` ${record.engine.version}` : ""}`
              : "引擎待记录";
          return <article key={record.experiment_id}><span className="timeline-dot"/><div><small>{record.created_at ? new Date(record.created_at).toLocaleString("zh-CN") : "时间未记录"}</small><h2>{record.strategy_id ?? "未命名策略"}</h2><p>{record.experiment_id}</p></div><div className="experiment-meta"><span>{engine}</span><em className={`state state-${record.status}`}>{record.status ?? "unknown"}</em></div></article>;
        })}{records.length === 0 && <div className="table-empty">暂无实验记录</div>}</div>
      )}
    </main>
  );
}

function DataPage({ bootstrap }: { bootstrap: CenterBootstrap }) {
  const [datasets, setDatasets] = useState<DatasetRecord[]>([]);
  const [error, setError] = useState("");
  useEffect(() => { centerApi.datasets().then(setDatasets).catch((cause: Error) => setError(cause.message)); }, []);
  return (
    <main className="page data-page">
      <PageIntro eyebrow="DATA PLANE" title="数据中心" copy="Git 只保存元数据；行情、财务、文本、因子和回测结果长期落在 NAS。" />
      <section className="data-topology">
        <div className="topology-node source"><small>数据源</small><strong>行情 · 财务 · 新闻</strong><span>provider adapters</span></div>
        <div className="topology-arrow">→</div>
        <div className="topology-node core"><small>NAS DATA LAKE</small><strong>{bootstrap.overview.data_root.available ? "已挂载" : "未连接"}</strong><span>raw · curated · features</span></div>
        <div className="topology-arrow">→</div>
        <div className="topology-node"><small>研究消费</small><strong>VT · Quant · Nautilus</strong><span>read through manifests</span></div>
      </section>
      <section className="dataset-catalog">
        <div className="section-heading compact"><div><span>数据目录</span><h2>已登记数据版本</h2></div><small>{datasets.length} 个 manifest</small></div>
        {error ? <div className="error-banner">数据目录暂时不可用：{error}</div> : <div className="dataset-table content-surface">
          <div className="dataset-row head"><span>数据集</span><span>市场 / 频率</span><span>提供商</span><span>版本</span><span>质量</span></div>
          {datasets.map((item) => <div className="dataset-row" key={`${item.dataset_id}-${item.version}`}><span><strong>{item.dataset_id}</strong><small>{item.storage_uri}</small></span><span>{item.market} · {item.frequency}</span><span>{item.provider}</span><span>{item.version}</span><span><em className={`quality quality-${item.quality_status}`}>{item.quality_status}</em></span></div>)}
          {datasets.length === 0 && <div className="table-empty">暂无已登记数据集</div>}
        </div>}
      </section>
      <section className="directory-map content-surface">
        {[
          ["raw", "原始不可变数据", "按市场 / 提供商 / 频率分区"],
          ["curated", "清洗标准数据", "统一代码、时区和复权策略"],
          ["features", "因子与衍生特征", "记录计算代码与输入版本"],
          ["experiments", "回测与研究结果", "实验清单、指标、图表和报告"],
          ["logs", "运行日志", "按服务与日期滚动保留"],
          ["backups", "私有备份", "加密、校验和恢复演练"],
        ].map((entry) => <div key={entry[0]}><Database size={18}/><span><strong>{entry[0]}/</strong><small>{entry[1]}</small></span><p>{entry[2]}</p></div>)}
      </section>
    </main>
  );
}

function ServicesPage({ bootstrap }: { bootstrap: CenterBootstrap }) {
  const [statuses, setStatuses] = useState<ServiceStatus[]>([]);
  const [refresh, setRefresh] = useState(0);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    setLoading(true);
    centerApi.services().then(setStatuses).catch(() => setStatuses([])).finally(() => setLoading(false));
  }, [refresh]);
  return (
    <main className="page services-page">
      <PageIntro eyebrow="SERVICE REGISTRY" title="服务状态" copy="中心只依赖稳定链接和契约；各项目继续独立发布、升级和回滚。" action={<button className="secondary-action" onClick={() => setRefresh((value) => value + 1)} disabled={loading}><Activity size={15}/>{loading ? "检测中…" : "重新检测"}</button>} />
      <div className="service-directory content-surface">{bootstrap.catalog.groups.map((group) => {
        const items = bootstrap.catalog.items.filter((item) => item.group === group.id);
        const status = statuses.find((item) => item.source === group.source);
        const isOwnedCenter = group.source === "quant-center";
        return <section key={group.id}><div className={`service-source${isOwnedCenter ? " service-source-owned" : ""}`}>{isOwnedCenter ? <div className="brand-symbol service-brand-symbol" aria-hidden="true"><span/><span/><span/></div> : <SourceMark source={group.source}/>}<div className="service-identity"><h2>{isOwnedCenter ? "自研量化中心" : group.label}</h2><p>{isOwnedCenter ? "自有能力" : `来源：${group.source}`}</p></div><span className="service-count">{items.length} 个入口</span><div className={`service-health ${status?.state ?? "checking"}`}><i/>{status ? `${status.state === "online" ? "在线" : status.state === "degraded" ? "异常" : "离线"}${status.latency_ms === null ? "" : ` · ${status.latency_ms}ms`}` : "等待检测"}</div></div><div className="service-links">{items.map((item) => <ServiceLink key={item.id} item={item}><NavigationIcon item={item}/><span><strong>{item.label}</strong><small>{item.description}</small></span><ChevronRight size={15}/></ServiceLink>)}</div></section>;
      })}</div>
    </main>
  );
}

function NavigationStudio({
  bootstrap,
  navigation,
  saveNavigation,
}: {
  bootstrap: CenterBootstrap;
  navigation: NavigationConfig;
  saveNavigation: (next: NavigationConfig) => Promise<void>;
}) {
  const validItemIds = useMemo(() => new Set(bootstrap.catalog.items.map((item) => item.id)), [bootstrap.catalog.items]);
  const [draft, setDraft] = useState(() => navigation.item_ids.filter((id) => validItemIds.has(id)));
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  useEffect(() => setDraft(navigation.item_ids.filter((id) => validItemIds.has(id))), [navigation.item_ids, validItemIds]);

  function toggle(id: string) {
    setSaved(false);
    setDraft((items) => items.includes(id) ? items.filter((item) => item !== id) : [...items, id]);
  }
  function move(id: string, direction: -1 | 1) {
    setSaved(false);
    setDraft((items) => {
      const index = items.indexOf(id);
      const target = index + direction;
      if (index < 0 || target < 0 || target >= items.length) return items;
      const next = [...items];
      [next[index], next[target]] = [next[target], next[index]];
      return next;
    });
  }
  async function save() {
    setSaving(true);
    await saveNavigation({ mode: "custom", item_ids: draft });
    setSaving(false);
    setSaved(true);
  }

  return (
    <main className="page navigation-page">
      <PageIntro eyebrow="NAVIGATION STUDIO" title="把常用页面放到手边" copy="选择来自不同项目的页面并调整顺序；来源标记始终保留。" action={<button className="primary-action" onClick={save} disabled={saving}>{saving ? "正在保存…" : saved ? "已保存到 NAS" : "保存并启用"}</button>} />
      <div className="nav-studio">
        <section className="nav-preview"><span className="eyebrow">实时预览</span><div className="preview-brand"><div className="brand-symbol mini"><span/><span/><span/></div><strong>量化中心</strong></div>{draft.map((id, index) => {
          const item = bootstrap.catalog.items.find((candidate) => candidate.id === id);
          if (!item) return null;
          return <div className="preview-item" key={id}><NavigationIcon item={item}/><span>{item.label}</span><SourceMark source={item.source} compact/><button onClick={() => move(id, -1)} disabled={index === 0} aria-label={`上移 ${item.label}`}><ArrowUp size={14}/></button><button onClick={() => move(id, 1)} disabled={index === draft.length - 1} aria-label={`下移 ${item.label}`}><ArrowDown size={14}/></button></div>;
        })}</section>
        <section className="nav-catalog content-surface"><div className="catalog-head"><div><span className="eyebrow">页面目录</span><h2>{draft.length} 个页面已选择</h2></div><small>服务升级时只更新链接目录</small></div>{bootstrap.catalog.groups.map((group: ServiceGroup) => <div className="catalog-group" key={group.id}><div className="catalog-group-title"><SourceMark source={group.source}/><strong>{group.label}</strong></div>{bootstrap.catalog.items.filter((item) => item.group === group.id).map((item) => <label key={item.id} className="catalog-item"><input type="checkbox" checked={draft.includes(item.id)} onChange={() => toggle(item.id)}/><span className="custom-check"/><NavigationIcon item={item}/><span className="catalog-copy"><strong>{item.label}</strong><small>{item.description}</small></span></label>)}</div>)}</section>
      </div>
    </main>
  );
}

export function App() {
  const pathname = usePathname();
  const isSample = pathname === "/schwab" || pathname === "/account-sample";
  const [bootstrap, setBootstrap] = useState<CenterBootstrap | null>(null);
  const [error, setError] = useState("");
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    if (isSample) return;
    let active = true;
    centerApi.bootstrap().then((payload) => {
      if (!active) return;
      setBootstrap(payload);
      window.localStorage.setItem(bootstrapCacheKey, JSON.stringify(payload));
    }).catch((cause: Error) => {
      if (!active) return;
      const cached = window.localStorage.getItem(bootstrapCacheKey);
      if (cached) {
        try {
          setBootstrap(JSON.parse(cached) as CenterBootstrap);
          setError(`实时 API 暂时不可用，正在使用最近一次目录缓存：${cause.message}`);
          return;
        } catch { window.localStorage.removeItem(bootstrapCacheKey); }
      }
      setError(cause.message);
    });
    return () => { active = false; };
  }, [isSample]);

  async function saveNavigation(next: NavigationConfig) {
    if (!bootstrap) return;
    setBootstrap({ ...bootstrap, navigation: next });
    try {
      const saved = await centerApi.saveNavigation(next);
      setBootstrap((current) => current ? { ...current, navigation: saved } : current);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "导航保存失败");
      throw cause;
    }
  }

  if (isSample) return <><PlatformNav/><div className="app-shell" data-route={pathname}><SampleSidebar/><div className="content-shell"><BrokerageSample/></div></div></>;

  if (error && !bootstrap) {
    return <><PlatformNav/><div className="boot-screen error"><div className="brand-symbol"><span/><span/><span/></div><h1>Quant Center 暂时无法连接</h1><p>{error}</p><button onClick={() => window.location.reload()}>重新连接</button></div></>;
  }
  if (!bootstrap) {
    return <><PlatformNav/><div className="boot-screen"><div className="brand-symbol loading"><span/><span/><span/></div><h1>Quant Center</h1><p>正在连接研究基础设施…</p></div></>;
  }
  return (
    <><PlatformNav/><div className="app-shell" data-route={pathname}>
      <Sidebar bootstrap={bootstrap} navigation={bootstrap.navigation} onModeChange={(mode) => saveNavigation({ ...bootstrap.navigation, mode }).catch(() => undefined)} mobileOpen={mobileOpen} closeMobile={() => setMobileOpen(false)} />
      {mobileOpen && <button className="mobile-backdrop" onClick={() => setMobileOpen(false)} aria-label="关闭导航遮罩" />}
      <div className="content-shell">
        <FieldBackdrop pathname={pathname} />
        <button className="mobile-menu" onClick={() => setMobileOpen(true)} aria-label="打开导航"><Menu size={19} /></button>
        {error && <div className="toast-error">{error}<button onClick={() => setError("")}><X size={15}/></button></div>}
        {pathname === "/" && <OverviewPage bootstrap={bootstrap} />}
        {pathname === "/strategies" && <StrategiesPage />}
        {pathname === "/experiments" && <ExperimentsPage />}
        {pathname === "/data" && <DataPage bootstrap={bootstrap} />}
        {pathname === "/services" && <ServicesPage bootstrap={bootstrap} />}
        {pathname === "/navigation" && <NavigationStudio bootstrap={bootstrap} navigation={bootstrap.navigation} saveNavigation={saveNavigation} />}
      </div>
    </div></>
  );
}
