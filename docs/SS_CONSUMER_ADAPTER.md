# Stock Screener QuantStack Consumer Adapter

最后更新：2026-08-26

## Deployment Boundary

adapter 位于 `docker/stock-screener/quantstack_consumer_adapter.py`，由
`quant_workspace_compat.py` 作为只读 overlay 安装到 `PriceCacheService` 和 `YFinanceService`。锁定
SS upstream commit：`da4d01a83e420ac9c148ff4fe0260ee1e4228885`；adapter version：`1.6.0`。
未修改 upstream checkout，现有 SS frontend/backend API contract 不变。

adapter 只通过受认证的 QuantStack HTTP Batch API 读取 canonical 数据；模块不导入 SS database
models，也没有写 `stock_prices` / `stock_universe` 的路径。HTTP credential 仅在 header 中，timeout
和 retry 有上限，内部 service transport 明确不继承环境 HTTP proxy。

## Explicit Mapping

- `XSHG.600000 <-> 600000.SS`
- `XSHE.000001 <-> 000001.SZ`
- `BJSE.430047 <-> 430047.BJ`

unknown MIC/suffix、无 suffix 和非六位代码均 fail closed；不按数字前缀猜 venue，不 fallback 到
`.SS`。

canonical `ts_event` 转为 Asia/Shanghai 交易日期；OHLC 显式映射到 SS DataFrame。只有
`volume_unit=shares` 才能生成 `Volume`，amount 留在 adapter metadata。当前 unadjusted close 为兼容
SS 强制列生成 `Adj Close = Close`，并携带 `adjustment=none` 和
`adj_close_is_compatibility_alias=true`；它不回写 canonical，也不宣称复权。

## Cache and Failure Behavior

进程内 disposable cache 是有界 LRU；key 包含 dataset ID/version、snapshot token、instrument、date
range 和 frequency。snapshot token 已绑定 Manifest fingerprint 与 eligibility；cache 不是权威存储，
不写回 SS PostgreSQL/Redis。

分页允许每页 HTTP ETag 不同，但 dataset ID/version、Manifest fingerprint 或 snapshot token 任一变化
都会 fail closed。ineligible quality、unknown unit、mapping、timeout 或 bounded retry exhaustion 也
明确失败。shadow 下 candidate error 只生成脱敏 report，绝不覆盖 legacy response。

adapter 会核对 metadata 中完整 publication contract：dataset/version、gate/state、comparison、
canonical source/rule 与 evidence fingerprint 必须存在且相互一致。candidate access 要求
`candidate_eligible=true` 且 comparison complete；shadow access 只允许 `shadow_eligible=true`，可接受
明确 incomplete，但 blocked/quarantined/legacy_unknown 一律拒绝。access 由部署配置导出，不接受业务
请求临时放宽。任何缺字段、state 矛盾或 fingerprint 漂移都 fail closed，同时 legacy/shadow 默认行为
保持不变。

生产 shadow 先同步调用并返回 legacy，再把 candidate/comparison/report 放入一个 daemon worker；
worker 默认一个、queue 有界且满时 fail open，不为请求创建线程，也不依赖 Redis/Celery/数据库。
dead worker 拒绝入队；candidate、逐 symbol comparison、report 和 worker-loop 异常相互隔离。health
snapshot 提供 submitted/completed/dropped、queue/thread 状态和分类 failure counters，但不记录异常
正文或 credential。每个 task 还受 symbol/legacy-row 上限保护，超限 observation 不复制 DataFrame。
adapter 在 worker 中 lazy singleton 复用，shutdown 有明确时间上限。legacy、shadow-off 和 candidate
模式都不创建 shadow worker。

## Read Modes

- `legacy`：默认，完全调用原 SS 方法，不构造 adapter。
- `shadow`：必须同时显式开启 flag，并通过采样率、symbol allowlist 和最大 date window；用户响应仍
  来自 legacy，candidate 不阻塞请求线程。
- `quantstack_candidate`：仅 test/testing/staging/development 且二次开关允许；production fail closed。
- `quantstack_primary`：仅在显式 primary/cutover 配置成立时读取精确版本的 qfq release；不会回退
  legacy price cache。

配置和实际验证详见 `SS_SHADOW_READ.md`。

## Scan Freshness

primary 模式下，扫描准入和 K 线读取使用同一个精确 qfq dataset version。准入检查通过一次有界
Batch window 计算每只股票的最后交易日，不再查询 legacy `stock_prices` 或 legacy refresh state。
metadata、Batch API、calendar 或 snapshot identity 任一不可验证时仍 fail closed。

当前 qfq release 的正式 eligible universe 是沪深股票。严格 custom scan 请求北交所股票会明确
阻断；全市场 scan 会把北交所作为当前 release 未覆盖集合排除并返回 warning。沪深停牌或缺 bar
尾部仍使用上游既有的 99% freshness / 最多 100 只规则，不能借 primary overlay 放宽。

## PriceCache Read-path Coverage

Phase 2.6B 的离线 overlay 会发现并包装 locked upstream `PriceCacheService` 中实际存在的六类历史读取：

- 单只：`get_historical_data`、`get_cached_only`、`get_cached_only_fresh`；
- 批量：`get_many`、`get_many_cached_only`、`get_many_cached_only_fresh`。

`legacy` 和 `shadow` 都保留原方法的位置参数、关键字参数、返回对象与异常语义；shadow candidate 仍只在
后台 worker 执行。每个 observation/report 都记录具体 `read_path`。报告直接保留
symbol、window、frequency、read path、dataset version 与 snapshot token，
因此同一 symbol/window 经不同业务入口产生的证据不会被混为一条，也不需要额外计算查询哈希。

symbol 在采样前必须通过 `SS symbol -> QuantStack CN instrument` 显式映射；`AAPL` 等非 CN 或不支持
symbol 不进入 CN Batch API。shadow 的最大窗口可配置为 3650 天，用于覆盖十年数据湖，但这只建立
离线比较能力，不改变生产默认源。

## Direct YFinance Read-path Coverage

locked upstream 的 `technical.py` 和 scanner fallback 会直接调用
`YFinanceService.get_historical_data`。overlay 现在也包装这个 class-level 入口：只有显式可映射的 CN
symbol、daily interval 和 shadow gate 命中时，legacy 返回后才异步提交 `read_path=yfinance_direct`；
`AAPL`、`SPY` 和 intraday 请求保持原 YFinance 行为，不进入 CN Batch API。

PriceCache 在 miss/refresh 时也可能内部调用 YFinance provider。overlay 用 context-local provider guard
覆盖 PriceCache 原方法的动态调用范围；这些内部调用不会再次提交 `yfinance_direct`。反过来，direct
YFinance 的 `use_cache=True` 若由 PriceCache 返回有效 frame，外层能识别该次读取已经由 PriceCache
记录，不生成重复 observation；只有 cache miss 后真实 direct fallback 才留下 direct path 证据。

这一步补齐的是离线调用链 shadow 可观测性，不是生产数据源切换。真实隔离 shadow/soak、qfq
candidate、production primary/cutover 和删除 legacy/YFinance 回滚路径仍是后续独立阶段。
