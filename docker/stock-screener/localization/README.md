# Stock Screener 中文本地化层

这里维护 QuantStack 对 Stock Screener Web 的简体中文适配。官方 upstream
历史保持不变；少量必要的产品适配以编号补丁保存在相邻 `patches/` 目录。

`Dockerfile` 默认从 `VERSION_LOCK.json` 锁定的官方前端镜像派生，只额外注入
`quant-theme-init.js` 和 `quant-zh-cn.js`。首个脚本在浏览器绘制前设置共享
主题并抑制错误主题首帧，隐藏兜底最多持续 500ms；第二个脚本翻译静态页面、React 异步渲染内容、MUI
Portal 弹窗，以及 `title`、`placeholder`、`aria-label` 等辅助文本。

先把补丁应用到干净的 v1.4.0 checkout，并从 `frontend/` 构建下游基础镜像：

```bash
git apply /path/to/QuantStack/docker/stock-screener/patches/0001-ticker-cell-company-name-first.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0002-daily-snapshot-name-first-and-compact-health.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0003-chart-viewer-fit-available-height.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0004-kline-priority-and-timeframes.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0005-dual-market-production-closure.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0006-peer-modal-pagination.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0007-production-acceptance-corrections.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0008-complete-history-timeframes.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0009-production-acceptance-followups.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0010-browser-acceptance-corrections.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0011-market-switch-daily-tab.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0012-ss-home-and-incremental-history.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0013-cn-chart-primary-priority.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0014-cn-rs-on-demand.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0015-history-expansion-gesture.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0016-history-range-route.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0017-symbol-scoped-chart-instance.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0018-detail-chart-streaming-history.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0019-kline-tradingview-interaction-closure.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0020-kline-cold-first-read-prefetch.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0021-kline-eager-history-prefetch.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0022-kline-deep-first-request.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0023-stock-chart-parallel-dashboard.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0024-kline-empty-cold-range-guard.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0025-stock-chart-foreground-priority.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0026-chart-modal-eager-interaction.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0027-chart-navigation-initial-symbol.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0028-kline-progressive-first-paint.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0029-multitimeframe-shared-history.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0030-kline-history-foreground-priority.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0031-kline-viewport-paging.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0032-kline-background-preemption.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0033-kline-range-first-paint.patch
git apply /path/to/QuantStack/docker/stock-screener/patches/0034-kline-dual-market-loading-policy.patch
docker build \
  -f /path/to/QuantStack/docker/stock-screener/Dockerfile.frontend-downstream \
  --build-arg BUILDER_IMAGE=node:22-alpine@sha256:16e22a550f3863206a3f701448c45f7912c6896a62de43add43bb9c86130c3e2 \
  -t personal/stock-screener-frontend-downstream:1.4.0-name-first.35 \
  frontend
```

该构建默认写入同源 `/api`，与 production Nginx 代理契约一致；不要沿用
upstream Dockerfile 面向本机开发的 `http://localhost:8000` 默认值。

再叠加中文和 QuantStack 集成层：

```bash
docker build \
  --build-arg UPSTREAM_IMAGE=personal/stock-screener-frontend-downstream:1.4.0-name-first.35 \
  -t personal/stock-screener-frontend-zh:1.4.0-zh.59 \
  docker/stock-screener/localization
```

补丁 `0001` 将公司中文名设为主要标签、股票代码设为次要技术标识；补丁 `0002`
把同一层级扩展到顶部行情卡，并将市场健康摘要改为横向紧凑布局；补丁 `0003`
让在线和静态图表弹窗按平台导航、弹窗标题栏和底部提示栏之后的真实剩余高度
伸缩，避免浏览器缩放时裁掉日期轴和更新时间；补丁 `0004` 让当前股票 K 线优先，
并从十年日线按需生成 5日、日K、周K、月K、季K和年K，支持 `Cmd/Ctrl +/-` 缩放
加载，并在左滑越过已加载边界时逐级扩展到十年。缺少公司名时股票代码仍作为主要
标签。补丁 `0005` 让 CN/US 切换回到对应 Daily 主页，Scan 默认采用全局市场，
把未发布行业排名显示为正常空态，并统一 CN 详情与 Peers 的本币价格。专业缩写、
股票代码、公司名和数据值不翻译。补丁 `0008` 保留 5日/日K 的内嵌快速首屏，
周K按需读取本地五年历史，月K、季K、年K按需读取本地十年历史；这些周期不访问
Yahoo或BaoStock。
补丁 `0009` 修正 CN/US 自选过滤、市场菜单定位和快照切换缓存，并将触控板横移
固定为平移、浏览器捏合固定为缩放；日 K 到达左边界后按 `2y → 5y → 10y`
逐级读取已经落湖的本地历史。补丁 `0012` 进一步把左侧扩展改成只读取尚未加载的
较早日期区间，并在当前 K 线请求完成前暂停 RS、基本面、分组和相邻标的预取；顶部
SS 入口使用同页路由并保留当前市场。补丁 `0013` 关闭 CN 相邻股票预热，避免本地
NAS 读取挤占下一次显式点击；US 仍保留相邻预取。后端同时把主 K 线读取和后台
RS/批量读取放入独立容量队列。补丁 `0014` 让 CN 的 RS 线默认按需加载，只有用户
点击 RS 后才读取，避免上一只股票的后台 RS 阻塞下一只股票的主 K 线；US 仍保持
默认加载 RS。补丁 `0015` 把同一次触控板滚动事件合并为一次历史扩展，避免连续
触发相互重叠的 `2y/5y/10y` 读取。补丁 `0016` 把区间扩展切到独立的
`history-range` 接口，确保 FastAPI 实际解析 `start/end`，不会误回最近六个月。
补丁 `0017` 让个股切换按股票代码重新挂载图表实例；旧股票仍在途的历史扩展只能
结束在已卸载实例中，不会把旧 K 线合并到下一只股票。
补丁 `0018` 让个股图恢复 Lightweight Charts 原生滚轮、拖动、触控和惯性平移；用户
开始向左浏览后，在距已加载左端约 50 根 K 线时，通过既有 `history-range` 每次预取
一个固定 400 日且不重叠的本地历史页。同一图表最多一个请求在途，历史前插保持当前
视口，已加载区间往返不重复读取，也不访问 Yahoo 或 BaoStock。
补丁 `0019` 将 macOS 触控板 pinch 作为显式缩放手势处理，普通横向触控板滑动继续只做
平移；历史页前插时保存并平移 logical range，不再用被数据边界裁剪的 time range 恢复，
因此等待空白、历史返回和普通平移均保持相同可见跨度与 K 线柱宽。
补丁 `0020` 将首次历史需求合并为一个十年本地区间，并按当前可见柱数提前保留至少
1.25 个视口的历史余量，避免冷读时连续串行请求多个 400 日页面形成持久空白。
补丁 `0029` 将交互式日K、周K、月K、季K和年K统一投影到同一份本地日线历史：
首屏只读取两年，随后只发出一次十年前序区间读取；切换周期不再重复请求，前序数据
合并时保持当前逻辑视窗和K线宽度，到达数据真实起点后锁定左边界。
补丁 `0030` 把十年前序历史保留为个股弹窗首屏后的唯一前台读取；基本面、行业组、
完整导航清单和相邻标的预取只在这次历史请求结束后启动，避免冷 NAS 读取被低优先级
工作阻塞，同时保留两年日线的快速首帧。
补丁 `0031` 取消首屏后的十年前序自动读取：首屏仍只读两年日线，用户接近左边界约
50 根时才单飞读取一个固定 400 日且不重叠的前序页。日周月季年继续共用同一日线 store
和游标；历史前插保持 logical span 与柱宽。RS 和弹窗后台读取等待前台空闲 1.5 秒，若用户
先触发历史分页则取消尚在途的 RS，让 CN/US 的 K 线读取始终优先。
补丁 `0033` 让交互式个股图的首屏也直接使用现有 `history-range` 快路径，只读取一个
400 日窗口；不再先走旧的 `history?period=2y` 全量路径。后续左移继续按同样的 400 日
不重叠页面补载，五个周期仍共享同一份日线 store，缩放和平移语义不变。
补丁 `0034` 移除早期为 CN 设置的相邻股票预取禁用分支。CN 与 US 现在都只在当前图和
前台历史空闲 1.5 秒后启动同一套相邻价格、基本面和轻量详情预取；任务携带
`AbortSignal`，一旦用户开始左移补历史即被前台请求抢占。CN qfq、US total-return 和
两市场符号解析仍保留在数据适配层，CN 的 RS 默认按需策略也不因此改变。
后端在正式扫描发布后，从同一批本地行情一次性派生
RS、价格摘要、市场宽度和健康度并预热快照；US 行业分类源尚未接入时明确显示未发布，
不会伪造行业或分组排名。升级
Stock Screener 时，先在新 upstream commit 上重放补丁，再运行测试与实际页面
巡检，并分别递增 `name-first.N` 与 `zh.N` 修订号。

Quant Center 的所有 SS 入口携带当前 `zh.N` 壳版本，用于绕开浏览器里升级前遗留的
旧 HTML，但统一外壳不能依赖该查询参数。直接访问 SS 根 URL、刷新或恢复旧标签页都必须
获得同一中文外壳；根页面、`index.html` 和 `quant-zh-cn.js` 必须返回 `no-store`。
QuantStack 品牌和“量化中心”均须返回平台主页。验收必须同时覆盖首页点击和 SS 根 URL
直达，不得用临时查询参数代替真实入口。

每次升级或调整文案后必须执行源码覆盖审计。审计会扫描上游 React 源码中的
界面标题、说明、空状态、输入提示和辅助文本，并在发现可翻译遗漏时返回非零：

```bash
node scripts/audit_stock_screener_localization.mjs
```

派生镜像会要求浏览器每次重新验证 `index.html` 和 `quant-zh-cn.js`，避免升级后
旧标签页或启发式缓存继续显示未注入导航和中文适配的页面。带内容哈希的官方
`/assets/` 文件仍按 upstream 配置长期缓存。
