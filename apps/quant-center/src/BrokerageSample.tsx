import { useEffect, useRef, useState } from "react";
import { ArrowLeft, CircleCheck, RefreshCw, ShieldCheck, Wallet } from "lucide-react";
import { sampleRequest, type SampleScenario, type WorkbenchData } from "./brokerageSampleApi";
import "./brokerageSample.css";

const money = (value: string | null | undefined) => value == null ? "未确认" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(Number(value));
const reasons: Record<string, string> = {
  UNKNOWN_RESERVATION_SEMANTICS: "可用现金是否已扣除挂单占用尚未确认",
  UNKNOWN_ORDER_RESERVATION_SEMANTICS: "挂单资金占用口径尚未确认",
  UNKNOWN_OPTION_OBLIGATION: "期权合约信息不完整，无法确认交割义务",
  "TERMINAL_ORDER_REMAINDER:SAMPLE-ORDER-002": "历史订单已结束，但剩余数量尚未核清",
  "QUOTE_STALE:QXEN": "QXEN 报价已过期，需要更新样本数据",
};
const statusNames: Record<string, string> = { ready: "条件满足", waiting_review: "等待人工复核", blocked: "不可用", canceled: "已取消", filled: "已成交", partially_filled: "部分成交" };

export function BrokerageSample() {
  const [scenario, setScenario] = useState<SampleScenario>("ready");
  const [data, setData] = useState<WorkbenchData | null>(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const requestVersion = useRef(0);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    const version = ++requestVersion.current;
    setData(null);
    setError("");
    setNotice("");
    setBusy("正在读取样本状态…");
    sampleRequest(scenario, "state", controller.signal).then(result => {
      if (version === requestVersion.current) setData(result);
    }).catch(() => {
      if (!controller.signal.aborted && version === requestVersion.current) setError("样本服务暂时不可用，请稍后重试。页面不会使用固定结果冒充同步成功。");
    }).finally(() => {
      if (version === requestVersion.current) setBusy("");
    });
    return () => controller.abort();
  }, [scenario, refresh]);

  async function operate(action: "sync" | "propose") {
    if (busy) return;
    const version = ++requestVersion.current;
    setBusy(action === "sync" ? "正在同步虚构账户…" : "正在重新生成清单…");
    setError("");
    setNotice("");
    try {
      const result = await sampleRequest(scenario, action);
      if (version === requestVersion.current) {
        setData(result);
        setNotice(action === "sync" ? "样本同步已执行并保存。" : "清单已重新计算生成；未扣减投入预算。");
      }
    } catch {
      if (version === requestVersion.current) { setData(null); setError("操作未完成，已隐藏旧数据。请重试读取状态，确认样本服务状态后再操作。"); }
    } finally { if (version === requestVersion.current) setBusy(""); }
  }
  const snapshot = data?.snapshot;
  const balance = snapshot?.account.balances[0];
  const proposal = data?.proposal;
  const funding = data?.funding;
  const stale = data?.state.current_proposal_state === "stale";
  const blockers = stale ? data?.state.proposal_freshness_reasons ?? [] : proposal?.blocking_reasons ?? [];
  const time = scenario === "stale" ? "2026-10-16 01:00 UTC" : "2026-10-16 00:00 UTC";
  return <main className="page brokerage-page">
    <header className="brokerage-heading">
      <div><span className="eyebrow">ACCOUNT & PLAN / SAMPLE</span><h1>嘉信实盘</h1><p>从指定策略走向嘉信执行。当前先用虚构账户核对持仓、资金与拟单，实盘通道未启用。</p></div>
      <span className="sample-stamp"><ShieldCheck size={17} />虚构样本 · 不可提交</span>
    </header>
    <div className="sample-controls">
      <fieldset><legend>演示场景</legend>{([
        ["ready", "正常样本"], ["blocked", "信息缺失"], ["stale", "报价过期"],
      ] as const).map(([value, label]) => <button key={value} type="button" aria-pressed={scenario === value} disabled={!!busy} onClick={() => setScenario(value)}>{label}</button>)}</fieldset>
      <div className="sample-actions"><button className="secondary-action" disabled={!!busy || !data} onClick={() => operate("sync")}><RefreshCw size={15} />同步样本账户</button><button className="primary-action" disabled={!!busy || !snapshot} onClick={() => operate("propose")}>重新生成清单</button></div>
    </div>
    <p className="sample-notice">固定样本时钟：{time}。按钮调用样本服务运行适配器和计算逻辑，保存样本状态；不连接券商或实时行情。</p>
    <div className="sample-operation" role="status" aria-live="polite">{busy || notice}</div>
    {error && <div className="sample-blockers" role="alert"><strong>{error}</strong><button className="secondary-action" onClick={() => setRefresh(n => n + 1)}>重试读取状态</button></div>}
    {data && <section className="sample-sync" aria-label="同步状态">
      <div className="sample-sync-title"><CircleCheck size={19} /><strong>{snapshot ? "样本快照已保存" : "尚未同步样本"}</strong><span>{data.state.state.complete ? "快照完整" : "等待同步"}</span></div>
      <dl><div><dt>最后成功同步（样本时钟）</dt><dd>{data.state.state.last_success_at ?? "尚无记录"}</dd></div><div><dt>同步内容</dt><dd>{snapshot ? `${snapshot.positions.length} 项持仓 · ${snapshot.orders.length} 笔订单 · ${snapshot.transactions.length} 笔交易 · ${snapshot.quotes.length} 份报价` : "点击“同步样本账户”开始"}</dd></div><div><dt>清单状态</dt><dd>{stale ? "已过期 · 当前不可用" : proposal?.status === "blocked" ? "信息缺失 · 已阻断" : proposal ? "已生成 · 含待复核项" : "尚未生成"}</dd></div></dl>
      <p>已执行同步 {data.operations.sync_count} 次 · 清单重算 {data.operations.propose_count} 次 · 最近实际操作 {data.operations.last_propose_at ?? data.operations.last_sync_at ?? "暂无"}</p>
    </section>}
    {snapshot && balance && <>
    <section className="sample-section" aria-labelledby="account-summary"><div className="sample-section-title"><div><span>01 / ACCOUNT</span><h2 id="account-summary">账户摘要</h2></div><p>{snapshot.account.alias} · 保证金账户 · USD</p></div>
      <dl className="sample-balances">{([
        ["账户余额", balance.balance], ["现金", balance.cash], ["可用现金", balance.available_cash], ["购买力", balance.buying_power], ["已结算现金", balance.settled_cash], ["可提取现金", balance.withdrawable_cash],
      ]).map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{money(value)}</dd></div>)}</dl>
      <p className="sample-note">现金、可用现金与购买力分别展示。拟单依据可用现金；当前样本挂单占用 {money(funding?.open_order_occupation)}{scenario === "blocked" ? "，占用口径待确认。" : "，现金字段已明确包含挂单预留。"}</p>
    </section>
    {funding && <section className="sample-section" aria-labelledby="funding-title">
      <div className="sample-section-title"><div><span>02 / CASH ALLOCATION</span><h2 id="funding-title">资金占用</h2></div><p>已存在的占用与拟单测算分别列出</p></div>
      <dl className="sample-balances funding-balances">
        <div><dt>可用现金</dt><dd>{money(funding.available_cash)}</dd></div>
        <div><dt>挂单占用</dt><dd>{money(funding.open_order_occupation)}</dd></div>
        <div><dt>核对占用后可分配现金</dt><dd>{money(funding.cash_after_open_orders)}</dd></div>
        <div><dt>拟单最大现金预留</dt><dd>{proposal ? money(funding.proposal_cash_use) : "尚无有效清单"}</dd></div>
        <div><dt>其中费用 / 缓冲</dt><dd>{proposal ? `${money(funding.proposal_fees)} / ${money(funding.cash_reserve)}` : "—"}</dd></div>
        <div><dt>拟单预留后现金</dt><dd>{proposal ? money(funding.remaining_cash_after_proposals) : "—"}</dd></div>
      </dl>
      <p className="sample-note">{funding.reservation_included === true ? "可用现金已包含挂单预留，不重复扣除。" : funding.reservation_included === false ? "可用现金尚未扣除挂单占用，先扣除再分配。" : "可用现金的挂单预留口径未知，暂停资金分配。"} 拟单预留仅为测算，不冻结券商资金。</p>
      {funding.order_ids.length > 0 && <p className="sample-note">占用关联订单：{funding.order_ids.join("、")}</p>}
      {funding.uncertain_order_ids.length > 0 && <p className="sample-note">待核对订单：{funding.uncertain_order_ids.join("、")}</p>}
    </section>}
    </>}
    <section className="sample-section" aria-labelledby="proposal-list"><div className="sample-section-title"><div><span>03 / PROPOSALS</span><h2 id="proposal-list">拟下单清单</h2></div><p>{proposal ? `计划版本 ${proposal.plan_version} · 整股限价 · 仅买入建议` : "先同步账户，再重新生成清单"}</p></div>
      {blockers.length > 0 && <div className="sample-blockers" role="alert"><strong>{stale ? "报价过期，已隐藏旧清单" : "以下条件尚未满足，暂停生成可用建议"}</strong><ul>{blockers.map(reason => <li key={reason}>{reasons[reason] ?? reason}</li>)}</ul></div>}
      {proposal && !stale && <div className="sample-table-scroll"><table className="sample-table"><caption className="sr-only">虚构账户拟买入建议</caption><thead><tr><th>标的 / 来源</th><th>状态</th><th>假设限价</th><th>拟买数量</th><th>买入金额</th><th>费用 / 预留</th><th>测算剩余预算</th></tr></thead><tbody>{proposal.items.map(item => <tr key={item.item_id}><td><strong>{item.symbol}</strong><small>{item.source.startsWith("equity:") ? "个股计划 · 第一层" : "ETF 周期计划"}</small></td><td><span className={`sample-status ${item.status}`}>{statusNames[item.status] ?? item.status}</span></td><td>{money(item.assumed_limit_price)}</td><td>{item.quantity} 股</td><td>{money(item.gross_amount)}</td><td>{money(item.estimated_fee)}<small>另留 {money(item.cash_reserve)}</small></td><td>{money(item.remaining_budget)}</td></tr>)}</tbody></table></div>}
      {proposal && !stale && <div className="sample-plan-details">{proposal.items.map(item => <details key={item.item_id}><summary>{item.source.startsWith("equity:") ? "个股第一层" : "ETF 计划"} · 预算与数据依据</summary><dl><div><dt>累计 / 本层预算</dt><dd>{money(item.strategy_budget)}</dd></div><div><dt>已归属实际投入</dt><dd>{money(item.spent_budget)}</dd></div><div><dt>本条最大现金预留</dt><dd>{money(item.maximum_cash_use)}</dd></div><div><dt>触发价格</dt><dd>{item.trigger_price ? money(item.trigger_price) : "按周期累计预算"}</dd></div><div><dt>报价时间</dt><dd>{item.quote_time}</dd></div></dl><p>账户快照 {proposal.snapshot_id.slice(0, 12)}… · 计划版本 {proposal.plan_version}</p><p>{data?.calculations.find(row => row.item_id === item.item_id)?.formula ?? "数量受预算、可用现金、费用预留及同标的持仓上限共同约束。"}</p><p>买入金额 = {item.quantity} 股 × {money(item.assumed_limit_price)} = {money(item.gross_amount)}；最大现金预留 = 买入金额 + 费用 {money(item.estimated_fee)} + 缓冲 {money(item.cash_reserve)}。</p></details>)}</div>}
      <p className="sample-note">“测算剩余预算”是假设买入金额和费用后的余额；预留现金另计。建议与人工复核均不记为实际成交，也不扣减预算。</p>
      {!proposal && !stale && !error && <p className="sample-empty">{snapshot ? "账户已同步。点击“重新生成清单”运行预算与拟单计算。" : "同步样本账户后，才能根据持仓和资金生成建议。"}</p>}
      <p className="sample-note">正常样本中两个计划共用 QXEN 持仓上限 10 股：已有 1.5 股，第一层建议 5 股，ETF 再建议 3 股，合计 9.5 股。</p>
    </section>
    {snapshot && <>
    <section className="sample-section" aria-labelledby="sample-positions"><div className="sample-section-title"><div><span>04 / POSITIONS</span><h2 id="sample-positions">持仓与历史订单</h2></div><p>保留碎股与期权，不生成期权交易建议</p></div>
      <div className="sample-table-scroll"><table className="sample-table"><caption>当前样本持仓</caption><thead><tr><th>标的</th><th>类型</th><th>数量</th><th>平均成本价</th><th>持仓总成本</th><th>市值</th><th>合约信息</th></tr></thead><tbody>{snapshot.positions.map(position => <tr key={position.position_id}><td><strong>{position.instrument.symbol}</strong></td><td>{position.instrument.asset_type === "option" ? "看涨期权 · 多头" : "股票"}</td><td>{position.quantity} {position.instrument.asset_type === "option" ? "份" : "股"}</td><td>{money(position.average_price?.amount)}</td><td>{money(position.cost_basis?.amount)}</td><td>{money(position.market_value?.amount ?? null)}</td><td>{position.instrument.option ? `乘数 ${position.instrument.option.contract_multiplier ?? "未知"} · ${position.instrument.option.deliverable ?? "交割信息未知"}` : "含已有碎股"}</td></tr>)}</tbody></table></div>
      <div className="sample-table-scroll"><table className="sample-table"><caption>历史订单（样本输入）</caption><thead><tr><th>标的</th><th>状态</th><th>申请数量</th><th>已成交</th><th>剩余数量</th></tr></thead><tbody>{snapshot.orders.map(order => <tr key={order.order_id}><td>{order.instrument.symbol}</td><td>{statusNames[order.status] ?? order.status}</td><td>{order.requested_quantity}</td><td>{order.filled_quantity}</td><td>{order.remaining_quantity}</td></tr>)}</tbody></table></div>
    </section>
    <section className="sample-section" aria-labelledby="transactions-title">
      <div className="sample-section-title"><div><span>05 / TRANSACTIONS</span><h2 id="transactions-title">交易流水</h2></div><p>资金流入与买卖事实分开核对</p></div>
      <div className="sample-table-scroll"><table className="sample-table"><caption className="sr-only">样本交易流水</caption><thead><tr><th>发生时间（UTC）</th><th>类型 / 标的</th><th>数量 / 价格</th><th>净额</th><th>关联订单 / 归属</th></tr></thead><tbody>{snapshot.transactions.map(tx => <tr key={tx.transaction_id}><td>{tx.transaction_time.replace("T", " ").replace("Z", "")}<small>{tx.transaction_id}</small></td><td>{tx.transaction_type === "cash_receipt" ? "资金转入" : tx.transaction_subtype === "BUY" ? "买入" : tx.transaction_type}<small>{tx.instrument?.symbol ?? "现金"}</small></td><td>{tx.quantity ?? "—"}<small>{money(tx.price?.amount)}</small></td><td>{money(tx.net_amount?.amount)}</td><td>{tx.order_id ?? "—"}<small>{tx.transaction_type === "cash_receipt" ? "现金变动，不计投资支出" : data?.ledger ? "查看下方预算归属记录" : "等待预算账本生成"}</small></td></tr>)}</tbody></table></div>
      {data?.ledger && <details className="sample-ledger"><summary>预算归属与投入记录</summary><p className="sample-note">只有已确认归属的成交计入投入，拟单不会新增投入记录。以下为当前账本中的实际证据。</p><div className="sample-table-scroll"><table className="sample-table"><thead><tr><th>证据</th><th>计划 / 层级</th><th>状态</th><th>投入金额</th></tr></thead><tbody>{data.ledger.entries.map(entry => <tr key={entry.evidence_id}><td>{entry.evidence_id}</td><td>{entry.plan_id ?? "待归属"}<small>{entry.layer_id ?? ""}</small></td><td>{entry.status === "attributed" ? "已归属" : entry.status === "corrected" ? "已更正" : "待核对"}</td><td>{money(entry.cost)}<small>费用 {money(entry.fee)}</small></td></tr>)}</tbody></table></div></details>}
    </section>
    </>}
    <footer className="sample-footer"><ShieldCheck size={17} /><p>全部账户、标的和交易均为虚构样本。此页面没有下单、改单、撤单或账户授权入口。</p></footer>
  </main>;
}

export function SampleSidebar() {
  return <aside className="sidebar sample-sidebar"><div className="brand-row"><Wallet size={35} /><div><strong>嘉信实盘</strong><small>SCHWAB WORKSPACE</small></div></div><nav className="sidebar-nav" aria-label="嘉信实盘页面导航"><a className="nav-link" href="/"><ArrowLeft size={17} /><span>返回总览</span></a><a className="nav-link active" href="/schwab" aria-current="page"><Wallet size={17} /><span>账户与拟单</span></a></nav><footer className="sidebar-foot"><ShieldCheck size={17}/><div><strong>虚构数据演示</strong><small>实盘通道未启用</small></div></footer></aside>;
}
