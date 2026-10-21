/* QuantStack downstream localization for Stock Screener v1.4.0. */
(() => {
  'use strict';

  const translations = new Map(Object.entries({
    'STOCK SCANNER': 'Stock Screener',
    'STOCK SCANNER DAILY': 'Stock Screener · 每日概览',
    'Daily': '每日概览',
    '5 Days': '5日',
    'Daily K': '日K',
    'Weekly K': '周K',
    'Monthly K': '月K',
    'Quarterly K': '季K',
    'Yearly K': '年K',
    'Scan': '选股扫描',
    'Breadth': '市场宽度',
    'Groups': '行业组',
    'Backtest': '回测验证',
    'Themes': '市场主题',
    'Assistant': '研究助手',
    'Operations': '运行监控',
    'Read-only': '只读',
    'TICKER': '股票代码',
    'Go to ticker': '跳转到股票代码',
    'Markets ready': '市场数据就绪',
    'View operations': '查看运行监控',
    'Select strategy profile': '选择策略配置',
    'Scheduled Tasks': '定时任务',
    'Sign out': '退出登录',
    'Switch to light mode': '切换到浅色模式',
    'Switch to dark mode': '切换到深色模式',
    'Open assistant': '打开研究助手',
    'Close assistant': '关闭研究助手',
    'Start new conversation': '开始新对话',
    'Server Access': '服务器访问',
    'Sign in to Stock Scanner': '登录 Stock Screener',
    'Server mode now requires an authenticated browser session before protected API routes are available.': '服务器模式要求先建立已认证的浏览器会话，才能访问受保护的 API。',
    'Server authentication is required but not configured.': '服务器要求身份验证，但尚未完成配置。',
    'Server password': '服务器密码',
    'Sign in': '登录',
    'Static market selector': '静态市场选择器',

    // Cross-route page copy and controls. Product names and established market
    // abbreviations stay intact while the surrounding operator UI is Chinese.
    'Activity unavailable': '运行状态不可用',
    'Checking activity': '正在检查运行状态',
    'Refresh warning': '刷新警告',
    'Bootstrap progress': '初始化进度',
    'Bootstrap queued.': '初始化任务已排队。',
    'Local Setup': '本地初始化',
    'Universe Refresh': '股票池刷新',
    'Price Refresh': '价格数据刷新',
    'Fundamentals Refresh': '基本面数据刷新',
    'Breadth Calculation': '市场宽度计算',
    'Pipeline queued...': '数据管道任务已排队…',
    'Scan queued...': '扫描任务已排队…',
    'Cache': '缓存',
    'Cache error': '缓存异常',
    'Cache refresh starting...': '正在启动缓存刷新…',
    'Loading cache status...': '正在加载缓存状态…',
    'Unknown cache status': '未知缓存状态',
    'Refresh Cache': '刷新缓存',
    'Force Full Refresh': '强制完整刷新',
    'Cancel stuck task': '取消卡住的任务',
    'Force Cancel': '强制取消',
    'Fundamentals refresh started': '基本面数据刷新已启动',
    'Partial': '部分完成',
    'Stuck': '已卡住',
    'Schedule': '计划任务',
    'Queued': '已排队',
    'Admin Key': '管理密钥',
    'Task search': '搜索任务',
    'Benchmark age:': '基准数据时效：',
    'Freshness:': '数据新鲜度：',
    'Universe:': '股票池：',

    'All Australia': '全部澳大利亚股票',
    'All Canada': '全部加拿大股票',
    'All China A-shares': '全部中国 A 股',
    'All Germany': '全部德国股票',
    'All Hong Kong': '全部港股',
    'All India': '全部印度股票',
    'All Japan': '全部日本股票',
    'All Malaysia': '全部马来西亚股票',
    'All Singapore': '全部新加坡股票',
    'All South Korea': '全部韩国股票',
    'All Taiwan': '全部台湾股票',
    'All United States': '全部美国股票',
    'Australia': '澳大利亚',
    'Canada': '加拿大',
    'China A-shares': '中国 A 股',
    'Germany': '德国',
    'Hong Kong': '中国香港',
    'India': '印度',
    'Japan': '日本',
    'Malaysia': '马来西亚',
    'Singapore': '新加坡',
    'South Korea': '韩国',
    'Taiwan': '中国台湾',
    'United States': '美国',
    'Beijing Stock Exchange': '北京证券交易所',
    'Bombay Stock Exchange': '孟买证券交易所',
    'Bursa Malaysia': '马来西亚证券交易所',
    'Frankfurt': '法兰克福证券交易所',
    'Hong Kong Exchange': '香港交易所',
    'National Stock Exchange': '印度国家证券交易所',
    'Shanghai Stock Exchange': '上海证券交易所',
    'Shenzhen Stock Exchange': '深圳证券交易所',
    'Singapore Exchange': '新加坡交易所',
    'Taiwan Stock Exchange': '台湾证券交易所',
    'Tokyo Stock Exchange': '东京证券交易所',
    'Toronto Stock Exchange': '多伦多证券交易所',
    'TSX Venture Exchange': '多伦多创业交易所',
    'Xetra': '德国 Xetra 交易所',
    'Test Mode': '测试模式',

    '1 Session': '1 个交易日',
    '5 Sessions': '5 个交易日',
    '1 Week': '1 周',
    '1 Month': '1 个月',
    '3 Months': '3 个月',
    '6 Months': '6 个月',
    'Monthly': '每月',
    'Quarterly': '每季度',
    '34-Day': '34 天',
    '5-day Ratio': '5 天比率',
    '10-day Ratio': '10 天比率',
    '5D Ratio': '5 天比率',
    '10D Ratio': '10 天比率',
    '10-day Net': '10 天净值',
    '1M Basket': '1 个月组合',
    '52W Range': '52 周区间',
    'Price History': '价格历史',
    'Price Trend (30d)': '价格趋势（30 天）',
    'RS Trend (30d)': 'RS 趋势（30 天）',
    'Breadth Date': '市场宽度日期',
    'Feature Date': '特征日期',

    '1D Chg': '1 日涨跌',
    '1D Chg %': '1 日涨跌幅',
    '1W Chg': '1 周涨跌',
    '1W Chg %': '1 周涨跌幅',
    '1M Chg': '1 月涨跌',
    '1M Chg %': '1 月涨跌幅',
    '3M Chg': '3 月涨跌',
    '3M Chg %': '3 月涨跌幅',
    '6M Chg': '6 月涨跌',
    '6M Chg %': '6 月涨跌幅',
    '52W Hi': '52 周最高价',
    '52W Lo': '52 周最低价',
    '% from 52W Hi': '距 52 周最高价',
    '% from 52W Lo': '距 52 周最低价',
    'Gap': '跳空幅度',
    'Gap %': '跳空幅度',
    'Avg Gap': '平均跳空幅度',
    'ADR %': '平均日波幅（ADR）',
    'Dollar Vol (local)': '成交额（本币）',
    'ADV (USD)': '日均成交额（美元）',
    'Mkt Cap (local)': '总市值（本币）',
    'Mkt Cap (USD)': '总市值（美元）',
    'GICS Sector': 'GICS 板块',
    'Beta': '贝塔系数',
    'Fwd P/E': '预期市盈率',
    'Inst Own': '机构持股',
    'Inst Ownership': '机构持股比例',
    'Inst Δ 90D': '机构持股 90 天变化',
    'Profit': '利润',
    'Revenue Growth': '营收增长率',
    'Rev Growth': '营收增长率',
    'EPS Growth': 'EPS 增长率',
    'Sales Growth': '销售额增长率',
    'Recent Earnings': '近期盈利',
    'Next Earnings': '下次财报',
    'Days Until': '剩余天数',

    'Composite': '综合评分',
    'Rating / Score': '评级／评分',
    'RS Rating': 'RS 评级',
    'EPS Rating': 'EPS 评级',
    'EPS Rtg': 'EPS 评级',
    'EPS Q/Q': 'EPS 同比增长',
    'EPS Y/Y': 'EPS 年度增长',
    'EPS TTM': 'EPS（过去 12 个月）',
    'Sales Q/Q': '销售额同比增长',
    'Sales Y/Y': '销售额年度增长',
    'Avg RS Rating': '平均 RS 评级',
    'RS 1M': 'RS 1 个月',
    'RS 3M': 'RS 3 个月',
    'RS 12M': 'RS 12 个月',
    'RS Blue Dot': 'RS 蓝点信号',
    'RS Hi': 'RS 创新高',
    'RS line (stock vs. benchmark) with blue-dot leadership signals': 'RS 线（股票相对基准）及蓝点领先信号',
    'MA Align': '均线排列',
    'Pwr Trend': '强势趋势',
    'Vol Surge': '成交量激增',
    'Vol Break': '放量突破',
    'New IPO': '近期上市',
    'Pattern': '形态',
    'Squeeze': '波动收缩',
    'Pivot': '枢轴点',
    'Pkt Pivot': '袖珍枢轴点',
    'Pvt Dist': '距枢轴点',
    'VCP Detected': '检测到 VCP',
    'VCP DETECTED': '检测到 VCP',
    'VCP Ready': 'VCP 已就绪',
    'VCP Score': 'VCP 评分',
    'No VCP': '未检测到 VCP',
    'NO VCP': '未检测到 VCP',
    'Setup Engine RS line at new high before price': '形态识别引擎：RS 线先于价格创新高',
    'Visible in the scan table, but not yet scannable because price history is still limited.': '已显示在扫描表中，但因价格历史仍有限，暂时无法参与扫描。',

    'Basing': '筑底',
    'Advancing': '上涨阶段',
    'Topping': '筑顶',
    'Declining': '下跌阶段',
    'Aligned': '排列一致',
    'Not Aligned': '排列不一致',
    'Perfect': '完全符合',
    'DOES NOT PASS': '未通过',
    'PASSES TEMPLATE': '符合模板',
    'PASSED CHECKS': '已通过检查',
    'FAILED CHECKS': '未通过检查',
    'PATTERN CANDIDATES': '候选形态',
    'INVALIDATION FLAGS': '失效信号',
    'KEY LEVELS': '关键价位',
    'SETUP SUMMARY': '形态摘要',
    'D: Setup Details': 'D：形态详情',
    'Price &gt; 50-day MA': '价格高于 50 日均线',
    '50-day &gt; 150-day MA': '50 日均线高于 150 日均线',
    '150-day &gt; 200-day MA': '150 日均线高于 200 日均线',
    '200-day MA trending up': '200 日均线向上',

    'Overview': '概览',
    'Constituents': '成分股',
    'Current Rank': '当前排名',
    'Rank Changes': '排名变化',
    'Rank History': '排名历史',
    'Top Rank Gainers': '排名上升最多',
    'Top Rank Losers': '排名下降最多',
    'By Group': '按行业组',
    'Grouped': '分组显示',
    'Flat': '平铺显示',
    'Stocks Up 4%+': '上涨 4% 以上股票',
    'Stocks Down 4%+': '下跌 4% 以上股票',
    'Explosive': '爆发',
    'Leader Overlap': '领涨股重合度',
    'Theme Alert Overlap': '主题提醒重合度',
    'Avoid New Entries': '避免新开仓',
    'Sizing': '仓位建议',
    'Window Risk': '时间窗口风险',
    'Beat Count (4)': '超预期次数（4）',
    'Miss Count (4)': '不及预期次数（4）',
    'Screeners': '筛选器',

    'Create new watchlist': '新建观察列表',
    'New watchlist': '新观察列表',
    'Watchlist name': '观察列表名称',
    'Manage watchlist': '管理观察列表',
    'Manage watchlists': '管理观察列表',
    'Download watchlist as CSV': '将观察列表下载为 CSV',
    'Remove from watchlist': '从观察列表移除',
    'Enter symbol (e.g., AAPL or TVC:VIX)': '输入股票代码（例如 AAPL 或 TVC:VIX）',
    'Add symbol (e.g., NVDA)': '添加股票代码（例如 NVDA）',
    'Click to jump to symbol': '点击跳转到该股票',
    'Next (Space)': '下一只（空格）',
    'Previous (Shift+Space)': '上一只（Shift+空格）',
    'Space: Next Stock': '空格：下一只股票',
    'Shift+Space: Previous': 'Shift+空格：上一只',
    'Esc: Close': 'Esc：关闭',
    'Move up': '上移',
    'Move down': '下移',

    'All Sentiments': '全部情绪',
    'All Sources': '全部来源',
    'All Themes': '全部主题',
    'Bearish': '看空',
    'Bullish': '看多',
    'Emerging': '新兴',
    'Fading': '热度减弱',
    'Hot': '热门',
    'Content Sources': '内容来源',
    'Source name': '来源名称',
    'Search title, source, or ticker...': '搜索标题、来源或股票代码…',
    'Open original': '打开原文',
    'Open sources': '查看来源',
    'View details': '查看详情',
    'Tickers': '相关股票',
    'Translation confidence': '翻译置信度',
    'Manage themes': '管理主题',
    'New theme': '新建主题',
    'New subgroup name': '新子组名称',
    'Theme Group': '主题组',
    'Mentions 7d': '7 天提及次数',
    'Mentions 7D': '7 天提及次数',
    'Return 1w': '1 周收益率',
    'Pending Merge Suggestions': '待处理的主题合并建议',
    'Run theme consolidation to find new duplicate themes': '运行主题归并，查找新的重复主题',
    'Confidence is high': '置信度较高',
    'Reject merge': '拒绝合并',
    'Change Note': '变更说明',
    'Preview changes without writing to storage': '仅预览变更，不写入存储',
    'Apply immediately and create a versioned audit record': '立即应用并创建版本化审计记录',
    'Stage changes for later promotion': '暂存变更，稍后再发布',
    'Policy Controls': '策略控制',
    'LLM Settings': 'LLM 设置',
    'CLOUD MODELS': '云端模型',
    'LOCAL MODELS (OLLAMA)': '本地模型（Ollama）',
    'Ollama API URL': 'Ollama API 地址',
    'Default: http://localhost:11434': '默认：http://localhost:11434',
    'Refresh status': '刷新状态',
    'Status:': '状态：',
    'No L2 themes assigned': '尚未分配二级主题',
    'No exported theme views are available in this static build.': '此静态版本没有可用的主题导出视图。',
    'Themes are unavailable in this static build.': '此静态版本不提供主题功能。',

    'Avg Return': '平均收益率',
    'Median': '中位数',
    'Avg 5S': '平均 5 日收益',
    'Median 5S': '5 日收益中位数',
    'Avg 5S Return': '平均 5 日收益率',
    'Avg MFE': '平均最大有利变动',
    'Avg MAE': '平均最大不利变动',
    'Skipped': '已跳过',
    'Failure Clusters': '失败类型聚类',

    'Failed to load breadth data.': '市场宽度数据加载失败。',
    'Failed to load group rankings.': '行业组排名加载失败。',
    'Failed to load themes data.': '市场主题数据加载失败。',
    'Failed to load the selected theme view.': '所选主题视图加载失败。',
    'Failed to load the static chart payload.': '静态图表数据加载失败。',
    'Failed to load the static scan dataset.': '静态扫描数据集加载失败。',
    'Unknown hydration error': '未知页面初始化错误',
    'Loading children...': '正在加载下级项目…',
    'Loading policy config...': '正在加载策略配置…',
    'Loading results...': '正在加载结果…',
    'Actor': '操作人',
    'all': '全部',
    'and': '以及',
    'Backfill 30d': '回补 30 天数据',
    'Band': '区间',
    'Base': '基准',
    'Blue:': '蓝色：',
    'Green:': '绿色：',
    'Orange:': '橙色：',
    'Red:': '红色：',
    'Close group detail': '关闭行业组详情',
    'Conf': '置信度',
    'Corr': '相关性',
    'Custom': '自定义',
    'Deactivate': '停用',
    'Description (optional)': '说明（可选）',
    'Fund': '基本面',
    'Fundamental': '基本面',
    'History': '历史记录',
    'Items': '项目数',
    'Labels': '标签',
    'Lifecycle': '生命周期',
    'local': '本地',
    'Low completeness (0-25):': '低完整度（0–25）：',
    'MARKET THEMES': '市场主题',
    'Ment.': '提及数',
    'Merges': '合并建议',
    'News': '新闻',
    'no': '否',
    'yes': '是',
    'Passes': '通过项',
    'Preset Name': '预设名称',
    'Refresh data': '刷新数据',
    'Retryable Failed Items': '可重试的失败项目',
    'Sales': '销售额',
    'Search...': '搜索…',
    'Select...': '请选择…',
    'Stage 1:': '阶段 1：',
    'Stage 2:': '阶段 2：',
    'Stage 3:': '阶段 3：',
    'Stage 4:': '阶段 4：',
    'S1 - Basing': 'S1－筑底阶段',
    'S2 - Advancing': 'S2－上涨阶段',
    'S3 - Topping': 'S3－筑顶阶段',
    'S4 - Declining': 'S4－下跌阶段',
    'Stance': '市场态势',
    'Static': '静态数据',
    'subset/related': '子集／相关',
    'Technical': '技术面',
    'View Chart': '查看图表',
    'Twitter': 'Twitter',
    'Reddit': 'Reddit',
    'Substack': 'Substack',

    'Daily Market Snapshot': '每日市场快照',
    'Market Snapshot': '市场快照',
    'Daily Snapshot': '每日快照',
    'Snapshot': '快照',
    'Key Markets': '主要市场',
    'Watchlists': '观察列表',
    'Watchlist': '观察列表',
    'My Watchlists': '我的观察列表',
    'Stockbee MM': 'Stockbee 动量模型（MM）',
    'Digest': '市场摘要',
    'Market Digest': '市场摘要',
    'Market Stance': '市场态势',
    'Market Health': '市场健康度',
    'Market Health & Exposure': '市场健康度与仓位暴露',
    'Exposure': '仓位暴露',
    '/ 100 exposure': '/ 100 仓位',
    'Exposure over time': '仓位暴露变化',
    'Distribution days': '派发日数量',
    'Trend': '趋势',
    'Follow-through day': '跟进日',
    '● follow-through day': '● 跟进日',
    'Why this score': '评分依据',
    'Ma Alignment': '均线排列',
    'Dist From 50dma': '距 50 日均线',
    'Dist From 200dma': '距 200 日均线',
    'Distribution Drag': '派发日扣分',
    'Downtrend/Caution': '下跌趋势／谨慎',
    'Uptrend/Confirmed': '上涨趋势／已确认',
    'Uptrend/Pressure': '上涨趋势／承压',
    'Rally Attempt': '反弹尝试',
    'neutral': '中性',
    'uptrend': '上涨趋势',
    'downtrend': '下跌趋势',
    'Leaders': '领涨股',
    'Top Scan Candidates': '优选扫描候选股',
    'Leaders in Leading Groups': '强势行业组中的领涨股',
    'Top 10 Groups': '排名前十的行业组',
    'No group rankings available.': '暂无行业组排名。',
    'No group rankings are available.': '暂无行业组排名。',
    'No default liquidity floor. Click a row for chart details.': '未设置默认流动性下限。点击行可查看图表详情。',
    'Recent Events': '近期事件',
    'scan pick': '扫描候选',
    'Market Breadth (4%+ Movers)': '市场宽度（涨跌幅超过 4%）',
    'CSI 300': '沪深 300',
    'Shanghai Composite': '上证指数',
    'Kweichow Moutai': '贵州茅台',
    'Ping An Bank': '平安银行',
    'CATL': '宁德时代',
    'Semiconductors': '半导体',
    'Banks': '银行',
    'partial': '部分完成',
    'Watchlist Highlights': '观察列表亮点',
    'Risk Notes': '风险提示',
    'Freshness': '数据新鲜度',
    'Validation Snapshot': '验证快照',
    'Recent Theme Alerts': '近期主题提醒',
    'No digest data is available.': '暂无市场摘要数据。',
    'No data available': '暂无数据',
    'No data': '暂无数据',
    'No results': '暂无结果',
    'Loading...': '加载中…',
    'Loading': '加载中',
    'Refresh': '刷新',
    'Retry': '重试',
    'Reset': '重置',
    'Clear': '清空',
    'Close': '关闭',
    'Cancel': '取消',
    'Save': '保存',
    'Delete': '删除',
    'Remove': '移除',
    'Edit': '编辑',
    'Add': '添加',
    'Create': '创建',
    'Update': '更新',
    'Apply': '应用',
    'Confirm': '确认',
    'Continue': '继续',
    'Done': '完成',
    'View': '查看',
    'Details': '详情',
    'Settings': '设置',
    'Actions': '操作',
    'Action': '操作',
    'Search': '搜索',
    'Filter': '筛选',
    'Filters': '筛选条件',
    'Sort': '排序',
    'Export': '导出',
    'Download': '下载',
    'Previous': '上一页',
    'Next': '下一页',
    'Previous Scans': '历史扫描',
    'New Scan': '新建扫描',
    'All': '全部',
    'None': '无',
    'Yes': '是',
    'No': '否',
    'Unknown': '未知',
    'Available': '可用',
    'Unavailable': '不可用',
    'Enabled': '已启用',
    'Disabled': '已禁用',
    'Active': '运行中',
    'Inactive': '未运行',
    'Healthy': '正常',
    'Degraded': '降级',
    'Warning': '警告',
    'Error': '错误',
    'Success': '成功',
    'Failed': '失败',
    'Pending': '等待中',
    'Running': '运行中',
    'Completed': '已完成',
    'Cancelled': '已取消',
    'queued': '排队中',
    'waiting': '等待中',
    'reserved': '已预留',
    'running': '运行中',
    'stale': '已过期',
    'stuck': '已卡住',
    'failed': '失败',
    'cancelled': '已取消',
    'online': '在线',
    'offline': '离线',
    'Open': '打开',
    'Closed': '已关闭',
    'Ready': '就绪',
    'Stale': '数据过期',
    'Current': '当前',
    'Latest': '最新',
    'External Fetch Lease': '外部数据抓取租约',
    'Market Workload Leases': '各市场工作负载租约',

    'Market': '市场',
    'Markets': '市场',
    'All markets': '全部市场',
    'Symbol': '股票代码',
    'Symbols': '股票代码',
    'Name': '名称',
    'Company': '公司',
    'Exchange': '交易所',
    'Currency': '币种',
    'Country': '国家或地区',
    'Sector': '板块',
    'Industry': '行业',
    'Industry Group': '行业组',
    'Group': '行业组',
    'Rank': '排名',
    'Rating': '评级',
    'Score': '评分',
    'Reason': '原因',
    'Status': '状态',
    'State': '状态',
    'Description': '说明',
    'Notes': '备注',
    'Source': '来源',
    'Date': '日期',
    'Session': '交易日',
    'Time': '时间',
    'Last': '最新',
    'Last Price': '最新价',
    'Price': '价格',
    'Change': '涨跌幅',
    'Volume': '成交量',
    'Avg Volume': '平均成交量',
    'Market Cap': '总市值',
    'Mkt Cap': '总市值',
    'MCap': '总市值',
    'Shares': '股数',
    'Stocks': '股票数',
    'stocks': '只股票',
    'Top Stock': '领涨股',
    'Universe': '股票池',
    'Results': '结果',
    'Results:': '结果：',
    'Result': '结果',
    'Candidates': '候选股',
    'Selected': '已选择',
    'Selection': '选择',
    'Profile': '策略配置',
    'Strategy Profile': '策略配置',
    'Period': '周期',
    'Range': '范围',
    'From': '起始',
    'To': '结束',
    'Today': '今天',
    'Strategies:': '策略：',
    'Method': '合成方法',
    'Avg': '平均值',
    'Pick a market to start': '请先选择市场',
    'Min Price': '最低价格',
    'Max Price': '最高价格',
    'Min RS': '最低 RS',
    'Min Vol': '最低成交量',
    'Min EPS %': '最低 EPS 增长率',
    'Min Sales %': '最低销售增长率',

    'Run Scan': '运行选股扫描',
    'Start Scan': '开始扫描',
    'Scan Results': '扫描结果',
    'Scan Picks': '扫描候选股',
    'Custom Scan': '自定义扫描',
    'Preset': '预设',
    'Presets': '预设',
    'Select Preset': '选择预设',
    'Save as new preset': '另存为新预设',
    'Update preset': '更新当前预设',
    'Rename': '重命名',
    'Saved Filters': '已保存的筛选条件',
    'Save Filter': '保存筛选条件',
    'Filter Presets': '筛选预设',
    'All Stocks': '全部股票',
    'Minervini': '米涅尔维尼（Minervini）',
    'Minervini Scan': '米涅尔维尼选股',
    'Technical Indicators': '技术指标',
    'Fundamentals': '基本面',
    'Growth': '成长性',
    'Momentum': '动量',
    'Velocity': '变化速度',
    'Trend': '趋势',
    'Performance': '表现',
    'Liquidity': '流动性',
    'Price Range': '价格范围',
    'Volume Range': '成交量范围',
    'Market Cap Range': '市值范围',
    'Minimum': '最小值',
    'Maximum': '最大值',
    'Min': '最小',
    'Max': '最大',
    'Ascending': '升序',
    'Descending': '降序',
    'Chart': '图表',
    'Charts': '图表',
    'Price Chart': '价格图表',
    'Peer Comparison': '同业比较',
    'Stock Details': '股票详情',
    'Stock Information': '股票信息',
    'Metrics': '指标',
    'Key Metrics': '关键指标',
    'Add to Watchlist': '加入观察列表',
    'Add to watchlist': '加入观察列表',
    'Remove from Watchlist': '从观察列表移除',
    'Export to CSV': '导出为 CSV',
    'Market Cap display:': '总市值显示方式：',
    'Rows per page:': '每页行数：',
    'Go to previous page': '转到上一页',
    'Go to next page': '转到下一页',
    'China A-shares (XSHG)': '中国 A 股（上交所）',
    'China A-shares (XSHE)': '中国 A 股（深交所）',

    'Sym': '代码',
    'RS Trend': 'RS 趋势',
    'IBD Industry': 'IBD 行业',
    'IBD Group': 'IBD 行业组',
    'Grp': '组别',
    'Grp Rank': '行业组排名',
    'Comp': '综合分',
    'Cust': '自定义',
    'VolB': '放量',
    'Pat': '形态',
    'Sqz': '收缩',
    'Stg': '阶段',
    'Stage': '阶段',
    'Pass': '通过',
    'Watch': '观察',
    'Other': '其他',
    'IPO Weighted': 'IPO 加权',
    'Composite uses only the screeners that have enough history to run.': '综合评分只使用具有足够历史数据、能够正常运行的筛选器。',

    'Market Breadth': '市场宽度',
    'Breadth Overview': '市场宽度概览',
    'Breadth History': '市场宽度历史',
    'Group Attribution': '行业组归因',
    'Up 4%+': '上涨 4% 以上',
    'Down 4%+': '下跌 4% 以上',
    'Advancers': '上涨家数',
    'Decliners': '下跌家数',
    'Unchanged': '平盘家数',
    'Split': '分布',
    'Group Rankings': '行业组排名',
    'Group Rank': '行业组排名',
    'Relative Rotation Graph': '相对轮动图（RRG）',
    'RRG': '相对轮动图（RRG）',
    'Leading': '领先',
    'Weakening': '转弱',
    'Lagging': '落后',
    'Improving': '改善',
    'Filter by quadrant': '按象限筛选',
    'Table View': '表格视图',
    'Chart View': '图表视图',
    'Table': '表格',
    'Data Availability': '数据可用性',
    'Missing': '缺失',
    'Cluster': '聚类',
    'Scanned': '扫描数',
    'Top': '排名最高',
    'Price 30d': '价格 30 天',
    'RS 30d': 'RS 30 天',
    'Avg RS': '平均 RS',
    'EPS Q': 'EPS 季度增长',
    'EPS Y': 'EPS 年度增长',
    'Sls Q': '销售额季度增长',
    'Sls Y': '销售额年度增长',
    'Up 13%+ (34d)': '34 天内上涨 13% 以上',
    'Down 13%+ (34d)': '34 天内下跌 13% 以上',
    'Up 25%+ (21d)': '21 天内上涨 25% 以上',
    'Down 25%+ (21d)': '21 天内下跌 25% 以上',
    'Up 25%+ (63d)': '63 天内上涨 25% 以上',
    'Down 25%+ (63d)': '63 天内下跌 25% 以上',
    'Up 50%+ (21d)': '21 天内上涨 50% 以上',
    'Down 50%+ (21d)': '21 天内下跌 50% 以上',

    'Validation': '回测验证',
    'Theme Alerts': '主题提醒',
    'Win Rate': '胜率',
    'Return': '收益率',
    'Average Return': '平均收益率',
    'Median Return': '收益率中位数',
    'Sample Size': '样本数',
    'Horizon': '持有周期',
    'Confidence': '置信度',
    'Positive': '正面',
    'Negative': '负面',
    'Neutral': '中性',

    'Runtime Activity': '运行状态',
    'System Operations': '系统运行监控',
    'Operational Dashboard': '运行监控面板',
    'Telemetry': '遥测数据',
    'Alerts': '告警',
    'Severity': '严重程度',
    'Metric': '指标',
    'Owner': '负责人',
    'Opened': '发生时间',
    'Queue': '队列',
    'Queues': '队列',
    'All queues': '全部队列',
    'All states': '全部状态',
    'Depth': '积压数',
    'Oldest': '最早任务',
    'Worker': '工作进程',
    'Workers': '工作进程',
    'Task': '任务',
    'Tasks': '任务',
    'Task search': '搜索任务',
    'Progress': '进度',
    'Age': '已运行时间',
    'Blocker': '阻塞原因',
    'Heartbeat': '心跳',
    'Active Tasks': '运行中的任务',
    'Reserved': '已预留',
    'Scheduled': '已计划',
    'Benchmark age': '基准数据时效',
    'Low completeness (0-25)': '低完整度（0–25）',

    'Theme': '主题',
    'Theme Review': '主题审核',
    'Validated': '已验证',
    'Sentiment': '情绪',
    'Excerpt': '摘要',
    'Title': '标题',
    'Direction': '方向',
    'Type': '类型',
    'Relation': '关系',
    'Relationship Graph Context': '关系图谱上下文',
    'Manage Sources': '管理来源',
    'Model Settings': '模型设置',
    'Policy Settings': '策略设置',

    'Ask about scans, themes, breadth, symbols, or compare internal signals with current web context.': '可询问选股扫描、市场主题、市场宽度、股票代码，或将内部信号与当前网络信息进行比较。',
    'Send': '发送',
    'New conversation': '新对话',
    'Conversation': '对话',
    'Conversations': '对话',
    'Thinking...': '思考中…',
    'Copy': '复制',
    'Copied': '已复制',

    'Weekly': '每周',
    'Primary market': '主要市场',
    'Interval (min)': '间隔（分钟）',
    'Last Fetched': '最近获取时间',
    'Last Run': '最近运行时间',
    'Click to refresh': '点击刷新',
    'Import Symbols': '导入股票代码',
    'Manage Watchlist': '管理观察列表',
    'Manage Watchlists': '管理观察列表',
    'Create a watchlist first to add assistant suggestions.': '请先创建观察列表，再添加研究助手建议的股票。',
    'Add assistant tickers to watchlist': '将研究助手建议的股票加入观察列表',

    'Theme Rankings': '主题排名',
    'Emerging Themes': '新兴主题',
    'Candidate Theme Queue Review': '候选主题队列审核',
    'Review Theme Merge Suggestions': '审核主题合并建议',
    'Manage Themes': '管理主题',
    'Theme Settings': '主题设置',
    'Theme Policy Controls': '主题策略控制',
    'Manage Content Sources': '管理内容来源',
    'Sources (7D)': '来源（7 天）',
    'Mentions (7D)': '提及次数（7 天）',
    'Source Theme': '来源主题',
    'Target Theme': '目标主题',
    'Source Type': '来源类型',
    'Version History (Revert)': '版本历史（可恢复）',
    'Lifecycle Thresholds': '生命周期阈值',
    'Matcher Thresholds': '匹配阈值',
    'Extraction Model': '抽取模型',
    'Pipeline Health': '数据管道健康度',
    'Pipelines': '数据管道',
    'Relationship': '关系',
    'Similarity': '相似度',
    'Reject': '拒绝',
    'Priority': '优先级',
    'Category': '分类',
    'Author': '作者',
    'URL': '网址',

    'Operations': '运行监控',
    'Market activity': '市场活动',
    'Job console': '任务控制台',
    'Lease status': '租约状态',
    'Active alerts': '当前告警',
    'Progress pending': '等待进度更新',
    'Working…': '处理中…',
    'Acknowledge': '确认告警',
    'Acknowledge failed': '确认告警失败',
    'Per-market telemetry, live queue inventory, and safe background-job controls.': '按市场展示遥测数据、实时队列清单及安全的后台任务控制。',
    'Shows queued, waiting, reserved, running, stale, and stuck jobs across all Celery queues.': '展示全部 Celery 队列中排队、等待、预留、运行、过期和卡住的任务。',
    'No queue data available.': '暂无队列数据。',
    'No worker inspect data available.': '暂无工作进程检查数据。',
    'No queued or running jobs match the current filters.': '当前筛选条件下没有排队或运行中的任务。',
    'No runtime activity available.': '暂无运行活动。',
    'Failed to refresh runtime activity. Showing last known status.': '刷新运行状态失败，当前显示最近一次已知状态。',
    'Failed to load runtime activity.': '运行状态加载失败。',
    'Failed to load Operations job inventory.': '运行任务清单加载失败。',
    'Failed to load telemetry summaries. Check backend connectivity.': '遥测摘要加载失败，请检查后端连接。',
    'Failed to load alerts.': '告警加载失败。',

    'Validation': '回测验证',
    'Source': '来源',
    'Lookback': '回看周期',
    'Scan Picks': '扫描候选股',
    'Theme Alerts': '主题提醒',
    'Deterministic scorecard for published scan picks and theme alerts using cached price history only.': '仅使用缓存的历史价格，对已发布的扫描候选股和主题提醒进行确定性评分。',
    'Recent History (Last 90 Days)': '近期历史（最近 90 天）',
    'Failure Clusters': '失败类型聚类',
    'Avg Conf (30D)': '平均置信度（30 天）',
    'Sample': '样本',
    'Event': '事件',
    'Entry': '入场',
    'Target': '目标',
    'Failed to load validation overview': '回测验证概览加载失败',

    'OVERVIEW': '概览',
    'SCORES': '评分',
    'PRICE & TECHNICAL': '价格与技术面',
    'RELATIVE STRENGTH': '相对强度',
    'GROWTH': '成长性',
    'VALUATION': '估值',
    'SETUP ENGINE': '形态识别引擎',
    'VCP PATTERN': 'VCP 形态',
    'PREFERRED SETUPS': '优选形态',
    'CAUTION FLAGS': '风险信号',
    'ACTION PLAN': '行动计划',
    'EVENT NOTES': '事件备注',
    'Earnings': '盈利',
    'Setup': '形态',
    'Readiness': '准备度',
    'Quality': '质量',
    'Persistence': '持续性',
    'Tight Near Highs': '高位紧缩',
    'Contracting Depth': '回撤深度收缩',
    'Contracting Volume': '成交量收缩',
    'Dist to Pivot': '距枢轴点',
    'Pivot Date': '枢轴日期',
    'Exit Risk': '退出风险',
    'Exposure over time': '仓位暴露变化',
    'Deteriorating': '恶化',
    'Strengthening': '增强',
    'Provenance': '数据来源记录',
    'Browse Articles': '浏览文章',
    'Failed to load chart data': '图表数据加载失败',
    'Failed to load group charts': '行业组图表加载失败',
    'Failed to load the daily snapshot.': '每日快照加载失败。',

    // Source-audited copy from all Stock Screener routes, including optional
    // pages which may be disabled in the current deployment.
    '- All results were filtered out': '— 所有结果均已被筛除',
    '⚠️ Price may be overextended': '⚠️ 股价可能已过度延伸',
    '200-day MA': '200 日均线',
    '3. Start server: ollama serve': '3. 启动服务：ollama serve',
    '50-day MA': '50 日均线',
    '52-Week Range': '52 周区间',
    'Above 50MA': '高于 50 日均线',
    'Active:': '已启用：',
    'Alignment Criteria': '排列条件',
    'Alignment Status': '排列状态',
    'All Industry Groups': '全部行业组',
    'All theme merge suggestions have been reviewed': '所有主题合并建议均已审核',
    'Approved merges will appear here': '已批准的合并将显示在这里',
    'Background hydration failed. Showing the exported first page only.': '后台数据加载失败，当前仅显示已导出的第一页。',
    'Base Depths (should be contracting)': '基底深度（应逐步收窄）',
    'Bases Found': '发现的基底数量',
    'Bootstrap order': '初始化顺序',
    'Candidate queue is empty.': '候选队列为空。',
    'Click "Start Scan" to begin scanning all stocks in your universe': '点击“开始扫描”，扫描股票池中的全部股票',
    'Click a group to expand its 4%+ movers. Stocks without an IBD industry group are bucketed under "No Group".': '点击行业组可展开涨跌幅超过 4% 的股票。没有 IBD 行业组的股票归入“未分组”。',
    'Company Information': '公司信息',
    'Confidence Score': '置信度评分',
    'Created': '创建时间',
    'Cross-Market (USD-normalised)': '跨市场（美元标准化）',
    'Current Extraction Model': '当前抽取模型',
    'Current Price': '当前价格',
    'Current Rankings': '当前排名',
    'Daily Digest': '每日摘要',
    'Daily Movers (4%+)': '每日大幅波动股票（4%+）',
    'Daily Scan': '每日扫描',
    'Decision Summary': '决策摘要',
    'Detected': '已识别',
    'Drag to reorder. Use TradingView format for special symbols (e.g., FX:USDSGD, BITSTAMP:BTCUSD, TVC:VIX)': '拖动可调整顺序。特殊代码请使用 TradingView 格式（例如 FX:USDSGD、BITSTAMP:BTCUSD、TVC:VIX）',
    'drag to zoom': '拖动缩放',
    'Enabled market queue': '已启用的市场队列',
    'Enabled markets': '已启用市场',
    'Enter a stock symbol to view Minervini analysis': '输入股票代码以查看米涅尔维尼分析',
    'Enter a stock symbol to view price chart': '输入股票代码以查看价格图表',
    'Enter admin key to load policy settings.': '输入管理员密钥以加载策略设置。',
    'EPS (TTM)': '每股收益（TTM）',
    'EPS Growth (QoQ)': '每股收益增长（环比）',
    'Event Risk & Action Plan': '事件风险与行动计划',
    'First-run market bootstrap': '首次运行市场初始化',
    'From 200-day MA': '距 200 日均线',
    'From 50-day MA': '距 50 日均线',
    'From High': '距阶段高点',
    'Historical Validation': '历史验证',
    'Industry Peers': '同行业股票',
    'Initial sync is running. The workspace will open after the primary market has current data and a published scan. Additional markets continue loading in the background.': '正在进行首次同步。主要市场获得最新数据并发布扫描结果后，工作台将自动打开；其他市场会继续在后台加载。',
    'Insufficient historical data — scores and pattern detection are unavailable for this stock.': '历史数据不足，暂时无法为该股票评分或识别形态。',
    'IPO Age': '上市时长',
    'It can use StockScreenClaude scan outputs, themes, breadth, watchlists, and wider web context in one response.': '它可以在一次回答中综合 Stock Screener 的扫描结果、主题、市场宽度、观察列表及更广泛的网络信息。',
    'L1 Grouped': '一级分组',
    'L1 Theme Groups': '一级主题组',
    'Laggards': '落后股票',
    'Latest Breadth Data': '最新市场宽度数据',
    'Limited data available. Try waiting a moment and searching again.': '可用数据有限，请稍候片刻再重新搜索。',
    'LLM Reasoning': 'LLM 推理说明',
    'Loading chart...': '正在加载图表…',
    'Loading stock data...': '正在加载股票数据…',
    'MA Alignment': '均线排列',
    'Market Classification': '市场分类',
    'Market Regime & Risk Context': '市场状态与风险环境',
    'Market regime context is unavailable for this symbol.': '该股票暂无市场状态背景数据。',
    'Market Themes': '市场主题',
    'Minervini Template Analysis': '米涅尔维尼模板分析',
    'Momentum Score History': '动量评分历史',
    'Moving Average Alignment': '均线排列',
    'Multi-Day Ratios': '多日比率',
    'No 4%+ movers were attributed for the lookback window.': '回看窗口内没有归属于行业组的 4%+ 波动股票。',
    'No active alerts.': '当前没有活动告警。',
    'No active caution flags.': '当前没有有效的风险信号。',
    'No additional event-risk notes.': '没有其他事件风险备注。',
    'No alerts': '暂无告警',
    'No articles found': '未找到文章',
    'No articles found matching your filters': '未找到符合筛选条件的文章',
    'No breadth data available. Consider running a backfill.': '暂无市场宽度数据，可考虑执行历史数据补录。',
    'No constituent stocks to chart.': '没有可绘图的成分股。',
    'No emerging themes detected': '未发现新兴主题',
    'No exposure data yet. It is computed daily after the breadth step (or run the backfill).': '暂无仓位暴露数据。系统会在每日市场宽度计算后生成，也可执行历史数据补录。',
    'No leader candidates are available.': '暂无领涨候选股。',
    'No losing clusters are available for this selection.': '当前选择没有亏损聚类数据。',
    'No material risk notes are available.': '暂无重大风险说明。',
    'No merge history yet': '暂无合并历史',
    'No news sources found for this theme.': '未找到该主题的新闻来源。',
    'No peer data available for this stock.': '该股票暂无同行比较数据。',
    'No peer data is available from the latest published feature run.': '最近一次已发布的特征计算没有同行比较数据。',
    'No pending merge suggestions': '没有待审核的合并建议',
    'No policy breaches': '没有违反策略的情况',
    'No policy history yet.': '暂无策略变更历史。',
    'No price data': '暂无价格数据',
    'No ranked themes are available.': '暂无主题排名。',
    'No recent theme alerts are available.': '暂无近期主题提醒。',
    'No results found': '未找到结果',
    'No RRG data available yet. Group-ranking history is required to plot rotation.': '暂无 RRG 数据。绘制轮动图需要行业组历史排名。',
    'No setup guidance is available.': '暂无形态操作指引。',
    'No stock workspace data is available.': '暂无该股票的工作台数据。',
    'No stocks in this subgroup': '该子组中暂无股票',
    'No stocks in this watchlist yet.': '该观察列表中暂无股票。',
    'No stocks in this watchlist yet. Add stocks from scan results, themes, or peer tables.': '该观察列表中暂无股票。可从扫描结果、主题或同行列表中添加。',
    'No subgroups yet. Add a subgroup to start organizing stocks.': '暂无子组。添加子组后即可开始整理股票。',
    'No subgroups yet. Click the settings icon to add subgroups and stocks.': '暂无子组。点击设置图标添加子组和股票。',
    'No symbols in watchlist. Add some above.': '观察列表中暂无股票代码，请在上方添加。',
    'No symbols in watchlist. Click the settings icon to add some.': '观察列表中暂无股票代码，点击设置图标即可添加。',
    'No task currently holds the external fetch lease.': '当前没有任务占用外部数据抓取租约。',
    'No themes created yet': '尚未创建主题',
    'No themes yet': '暂无主题',
    'No themes yet. Create your first theme to get started.': '暂无主题。创建第一个主题即可开始。',
    'No validation events are available for this selection.': '当前选择暂无验证事件。',
    'No watchlist highlights are available.': '暂无观察列表亮点。',
    'No watchlists created yet': '尚未创建观察列表',
    'No watchlists yet': '暂无观察列表',
    'No watchlists yet. Create your first watchlist to get started.': '暂无观察列表。创建第一个观察列表即可开始。',
    'Ollama Configuration': 'Ollama 配置',
    'open Stockbee MM in a new tab': '在新标签页打开 Stockbee MM',
    'Overall Score': '综合评分',
    'P/E Ratio': '市盈率（P/E）',
    'Pass Tmpl': '通过模板',
    'Passing score: 70+': '通过分数：70 分以上',
    'Paste newline, comma, tab, or CSV-formatted symbols. The importer will dedupe entries and report invalid symbols.': '粘贴以换行、逗号、制表符分隔或 CSV 格式的股票代码。导入工具会自动去重并报告无效代码。',
    'Pattern Characteristics': '形态特征',
    'Performance vs SPY': '相对 SPY 表现',
    'Pick the primary market for startup defaults. The workspace opens after the primary market finishes its first scan-backed snapshot.': '选择启动时默认使用的主要市场。主要市场完成首次扫描快照后，工作台将自动打开。',
    'Price (30d)': '价格（30 天）',
    'Price Separation': '价格偏离度',
    'Prior Rank': '上期排名',
    'Promote high-quality candidates to active or reject noise in bulk.': '将高质量候选主题转为活跃状态，或批量剔除噪声。',
    'Rank History (1 Year)': '排名历史（1 年）',
    'Rating Scale': '评级标准',
    'Recent Sessions': '近期交易日',
    'Refreshing...': '正在刷新…',
    'Related Themes': '相关主题',
    'Relative Strength Rating': '相对强度评级',
    'Reset zoom': '重置缩放',
    'Retry Loading Results': '重新加载结果',
    'RS (30d)': 'RS（30 天）',
    'Runbook': '运行手册',
    'Safe-by-default controls with preview, staged rollout, and versioned audit history.': '默认采用安全控制，支持变更预览、分阶段发布和带版本的审计历史。',
    'Scan cancelled. Showing partial results.': '扫描已取消，当前显示部分结果。',
    'Score Breakdown': '评分明细',
    'Screener Breakdown': '筛选评分明细',
    'Screener explanations are unavailable for this symbol.': '该股票暂无筛选评分说明。',
    'Sector:': '板块：',
    'Setup details not available for this stock.': '该股票暂无形态详情。',
    'Some data sources are degraded. Scores may be less reliable.': '部分数据源状态异常，评分可靠性可能降低。',
    'Sources:': '来源：',
    'Space / Shift+Space': '空格键 / Shift+空格键',
    'Stage Definitions': '阶段定义',
    'Start a conversation with the assistant': '开始与研究助手对话',
    'Static chart payloads are unavailable for this market.': '该市场暂无静态图表数据。',
    'Static chart payloads failed to load. Scan results remain available without chart modals.': '静态图表数据加载失败。扫描结果仍可使用，但无法打开图表弹窗。',
    'Strategy profile data is unavailable.': '暂无策略配置数据。',
    'Suggested Name': '建议名称',
    'Theme Discovery': '主题发现',
    'Theme:': '主题：',
    'This symbol is not currently linked to an active theme cluster.': '该股票当前未关联任何活跃主题集群。',
    'To use local models, install and run Ollama:': '如需使用本地模型，请安装并运行 Ollama：',
    'Top Trending': '热门趋势',
    'VCP Pattern': 'VCP 形态',
    'VCP Pattern Detection': 'VCP 形态识别',
    'Watchlist:': '观察列表：',
    'Weinstein Stage Analysis': '威廉斯坦阶段分析',
    'OK': '正常',
    'Stock Count': '股票数量',
    'Stocks up 4%+ (daily count)': '单日上涨 4% 以上的股票数量',
    'Stocks down 4%+ (daily count)': '单日下跌 4% 以上的股票数量',
    'Financials': '金融',
    'Commercial Banks': '商业银行',
    'Price refresh': '价格数据刷新',
    'idle': '空闲',
    'completed': '已完成',
    'Task:': '任务：',
    'Updated:': '更新时间：',
    'Total stocks scanned:': '扫描股票总数：',
    'Calculation time:': '计算耗时：',
    'Error loading rankings:': '排名加载失败：',
    'unavailable': '暂无数据',
    'safe': '安全',
  }));

  const patterns = [
    [/^Results:\s*([\d,]+)\s+stocks?$/i, '结果：$1 只股票'],
    [/^Completed:\s*([\d,]+)\s+passed$/i, '已完成：$1 只通过'],
    [/^([\d,]+)\s+fields? unavailable or computed\s+—\s+click for details$/i, '$1 个字段不可用或为计算值，点击查看详情'],
    [/^China A-shares\s+\(([^)]+)\)$/i, '中国 A 股（$1）'],
    [/^RS Improving\s+\((.+) over (\d+)d\)$/i, 'RS 改善（$2 日变化 $1）'],
    [/^RS Declining\s+\((.+) over (\d+)d\)$/i, 'RS 转弱（$2 日变化 $1）'],
    [/^RS Flat\s+\((.+) over (\d+)d\)$/i, 'RS 横盘（$2 日变化 $1）'],
    [/^Market Breadth \(4%\+ Movers\) with (.+) Overlay$/i, '市场宽度（涨跌幅超过 4%），叠加 $1'],
    [/^with (.+) Overlay$/i, '，叠加 $1'],
    [/^Dollar volume >= (.+)\. Click a row for chart details\.$/i, '成交额不低于 $1。点击行可查看图表详情。'],
    [/^Top 20 by report card: group rank <= (.+), RS >= (.+), dollar volume >= (.+)\.$/i, '按综合评分取前 20：行业组排名不高于 $1，RS 不低于 $2，成交额不低于 $3。'],
    [/^Top 20 by report card: group rank <= (.+), RS >= (.+)\.$/i, '按综合评分取前 20：行业组排名不高于 $1，RS 不低于 $2。'],
    [/^(.+) \| (\d+)d: (.+) \(Up\)$/i, '$1｜$2 日：$3（上涨）'],
    [/^(.+) \| (\d+)d: (.+) \(Down\)$/i, '$1｜$2 日：$3（下跌）'],
    [/^(.+) \| (\d+)d: (.+) \(Flat\)$/i, '$1｜$2 日：$3（横盘）'],
    [/^cap\s+(.+)$/i, '上限 $1'],
    [/^Jan (\d+)$/i, '1 月 $1 日'],
    [/^Feb (\d+)$/i, '2 月 $1 日'],
    [/^Mar (\d+)$/i, '3 月 $1 日'],
    [/^Apr (\d+)$/i, '4 月 $1 日'],
    [/^May (\d+)$/i, '5 月 $1 日'],
    [/^Jun (\d+)$/i, '6 月 $1 日'],
    [/^Jul (\d+)$/i, '7 月 $1 日'],
    [/^Aug (\d+)$/i, '8 月 $1 日'],
    [/^Sep (\d+)$/i, '9 月 $1 日'],
    [/^Oct (\d+)$/i, '10 月 $1 日'],
    [/^Nov (\d+)$/i, '11 月 $1 日'],
    [/^Dec (\d+)$/i, '12 月 $1 日'],
    [/^Jan (\d+), (\d{4})$/i, '$2 年 1 月 $1 日'],
    [/^Feb (\d+), (\d{4})$/i, '$2 年 2 月 $1 日'],
    [/^Mar (\d+), (\d{4})$/i, '$2 年 3 月 $1 日'],
    [/^Apr (\d+), (\d{4})$/i, '$2 年 4 月 $1 日'],
    [/^May (\d+), (\d{4})$/i, '$2 年 5 月 $1 日'],
    [/^Jun (\d+), (\d{4})$/i, '$2 年 6 月 $1 日'],
    [/^Jul (\d+), (\d{4})$/i, '$2 年 7 月 $1 日'],
    [/^Aug (\d+), (\d{4})$/i, '$2 年 8 月 $1 日'],
    [/^Sep (\d+), (\d{4})$/i, '$2 年 9 月 $1 日'],
    [/^Oct (\d+), (\d{4})$/i, '$2 年 10 月 $1 日'],
    [/^Nov (\d+), (\d{4})$/i, '$2 年 11 月 $1 日'],
    [/^Dec (\d+), (\d{4})$/i, '$2 年 12 月 $1 日'],
    [/^(.+\.S[ZS]) Price$/i, '$1 价格'],
    [/^Total stocks scanned:\s*([\d,]+)\s*\|\s*Calculation time:\s*(.+)$/i, '扫描股票总数：$1｜计算耗时：$2'],
    [/^Error loading rankings:\s*(.+)$/i, '排名加载失败：$1'],
    [/^Request failed with status code (\d+)$/i, '请求失败，状态码 $1'],
    [/^Workspace is partially degraded:\s*(.+)$/i, '工作台部分数据异常：$1'],
    [/^(\d+)d:\s*(.+)\s+\(Up\)$/i, '$1 日：$2（上涨）'],
    [/^(\d+)d:\s*(.+)\s+\(Down\)$/i, '$1 日：$2（下跌）'],
    [/^([\d,]+)–([\d,]+) of ([\d,]+)$/i, '$1–$2 / 共 $3'],
    [/^Delete preset \"(.+)\"\?$/i, '确定删除预设“$1”吗？'],
    [/^Refresh\s+(.+)\s+data$/i, '刷新 $1 数据'],
    [/^Error:\s*(.+)$/i, '错误：$1'],
    [/^Profile:\s*(.+)$/i, '策略配置：$1'],
    [/^Last updated:\s*(.+)$/i, '最后更新：$1'],
    [/^Updated:\s*(.+)$/i, '更新时间：$1'],
    [/^As of\s+(.+)$/i, '截至 $1'],
    [/^Showing\s+([\d,]+)\s+of\s+([\d,]+)$/i, '显示 $1 / $2'],
    [/^([\d,]+)\s+results?$/i, '$1 条结果'],
    [/^Page\s+(\d+)\s+of\s+(\d+)$/i, '第 $1 / $2 页'],
    [/^Failed to load\s+(.+)\.?$/i, '$1加载失败。'],
    [/^Unable to load\s+(.+)\.?$/i, '无法加载$1。'],
    [/^No\s+(.+)\s+available\.?$/i, '暂无$1。'],
    [/^No\s+(.+)\s+found\.?$/i, '未找到$1。'],
    [/^Select\s+(.+)$/i, '选择$1'],
    [/^Search\s+(.+)$/i, '搜索$1'],
    [/^All\s+(.+)$/i, '全部$1'],
    [/^Add\s+(.+)$/i, '添加$1'],
    [/^Create\s+(.+)$/i, '创建$1'],
    [/^Delete\s+(.+)$/i, '删除$1'],
    [/^Remove\s+(.+)$/i, '移除$1'],
    [/^View\s+(.+)$/i, '查看$1'],
    [/^Open\s+(.+)$/i, '打开$1'],
    [/^(\d+)\s+stocks?$/i, '$1 只股票'],
    [/^(\d+)\s+days?$/i, '$1 天'],
    [/^(\d+)\s+hours?$/i, '$1 小时'],
    [/^(\d+)\s+minutes?$/i, '$1 分钟'],
    [/^(\d+)M$/i, '$1 个月'],
    [/^(\d+)Y$/i, '$1 年'],
    [/^(\d+)D$/i, '$1 天'],
    [/^(\d+)mo$/i, '$1 个月'],
  ];

  const embeddedTerms = [
    [/\bAll markets\b/g, '全部市场'],
    [/\bAll queues\b/g, '全部队列'],
    [/\bAll states\b/g, '全部状态'],
    [/\bMarket Cap\b/g, '总市值'],
    [/\bIndustry Group\b/g, '行业组'],
    [/\bLast updated\b/g, '最后更新'],
    [/\bKey markets\b/gi, '主要市场'],
    [/\bpartial\b/gi, '部分完成'],
    [/\bSemiconductors\b/g, '半导体'],
    [/\bBanks\b/g, '银行'],
    [/\bIndustry Peers\b/g, '同行业股票'],
    [/\bPrice refresh\b/g, '价格数据刷新'],
    [/\bauto\b/gi, '自动'],
    [/\bJan (\d+), (\d{4})\b/g, '$2 年 1 月 $1 日'],
    [/\bFeb (\d+), (\d{4})\b/g, '$2 年 2 月 $1 日'],
    [/\bMar (\d+), (\d{4})\b/g, '$2 年 3 月 $1 日'],
    [/\bApr (\d+), (\d{4})\b/g, '$2 年 4 月 $1 日'],
    [/\bMay (\d+), (\d{4})\b/g, '$2 年 5 月 $1 日'],
    [/\bJun (\d+), (\d{4})\b/g, '$2 年 6 月 $1 日'],
    [/\bJul (\d+), (\d{4})\b/g, '$2 年 7 月 $1 日'],
    [/\bAug (\d+), (\d{4})\b/g, '$2 年 8 月 $1 日'],
    [/\bSep (\d+), (\d{4})\b/g, '$2 年 9 月 $1 日'],
    [/\bOct (\d+), (\d{4})\b/g, '$2 年 10 月 $1 日'],
    [/\bNov (\d+), (\d{4})\b/g, '$2 年 11 月 $1 日'],
    [/\bDec (\d+), (\d{4})\b/g, '$2 年 12 月 $1 日'],
    [/\bprice\b/g, '价格'],
  ];

  const translate = (value) => {
    if (typeof value !== 'string') return value;
    const trimmed = value.trim();
    if (!trimmed) return value;
    let translated = translations.get(trimmed);
    if (!translated) {
      for (const [pattern, replacement] of patterns) {
        if (pattern.test(trimmed)) {
          translated = trimmed.replace(pattern, replacement);
          break;
        }
      }
    }
    translated = embeddedTerms.reduce(
      (current, [pattern, replacement]) => current.replace(pattern, replacement),
      translated || trimmed,
    );
    if (translated === trimmed) return value;
    // JSX often splits a sentence into adjacent text nodes. When the translated
    // continuation starts with Chinese punctuation, keeping the source node's
    // leading ASCII space produces the visibly incorrect ` ） ，` sequence.
    const leading = /^[，。；：！？、]/.test(translated) ? '' : (value.match(/^\s*/)?.[0] || '');
    const trailing = value.match(/\s*$/)?.[0] || '';
    return `${leading}${translated}${trailing}`;
  };

  const ignoredParents = new Set(['SCRIPT', 'STYLE', 'CODE', 'PRE', 'TEXTAREA']);
  const translateTextNode = (node) => {
    if (!node?.parentElement || ignoredParents.has(node.parentElement.tagName)) return;
    if (node.parentElement.closest('[data-no-translate], [translate="no"]')) return;
    const next = translate(node.nodeValue);
    if (next !== node.nodeValue) node.nodeValue = next;
  };

  const translatedAttributes = ['title', 'placeholder', 'aria-label', 'alt'];
  const translateElement = (element) => {
    if (!(element instanceof Element)) return;
    for (const attribute of translatedAttributes) {
      if (!element.hasAttribute(attribute)) continue;
      const current = element.getAttribute(attribute);
      const next = translate(current);
      if (next !== current) element.setAttribute(attribute, next);
    }
    if (element instanceof HTMLInputElement && ['button', 'submit', 'reset'].includes(element.type)) {
      const next = translate(element.value);
      if (next !== element.value) element.value = next;
    }
  };

  const translateTree = (root) => {
    if (root.nodeType === Node.TEXT_NODE) {
      translateTextNode(root);
      return;
    }
    if (!(root instanceof Element || root instanceof Document || root instanceof DocumentFragment)) return;
    if (root instanceof Element) translateElement(root);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();
    while (node) {
      if (node.nodeType === Node.TEXT_NODE) translateTextNode(node);
      else translateElement(node);
      node = walker.nextNode();
    }
  };

  const start = () => {
    document.documentElement.lang = 'zh-CN';
    document.title = 'Quant Center';
    const style = document.createElement('style');
    style.textContent = `
      :root{font-family:"PingFang SC","Microsoft YaHei","Noto Sans CJK SC",Inter,sans-serif;--quant-platform-nav-offset:64px}
      .quant-platform-active #root{min-height:calc(100vh - var(--quant-platform-nav-offset))}
      .quant-platform-active #root>.MuiBox-root{min-height:calc(100vh - var(--quant-platform-nav-offset))!important}
      .quant-platform-nav{height:52px;position:sticky;top:6px;z-index:1500;display:flex;align-items:center;gap:12px;margin:6px 10px;padding:0 10px 0 18px;border:2px solid;border-radius:12px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;font-size:14px;font-weight:400;line-height:21px;transition:background-color .18s,border-color .18s,color .18s,box-shadow .18s}
      .quant-platform-brand{flex:0 0 auto;margin:0;padding:10px 0!important;font-family:inherit;font-size:14px!important;font-weight:700!important;line-height:21px!important;white-space:nowrap;color:inherit!important;background:transparent!important}
      .quant-platform-nav-links{min-width:0;flex:1;display:flex;align-items:center;gap:6px;overflow-x:auto;scrollbar-width:none}
      .quant-platform-nav-links::-webkit-scrollbar{display:none}
      .quant-platform-nav a{flex:0 0 auto;padding:10px 14px;color:inherit;text-decoration:none;border-radius:7px;font-family:inherit;font-size:14px;font-weight:400;line-height:21px;white-space:nowrap}
      .quant-platform-nav[data-surface="dark"]{color:#171a19;border-color:#168cff;background:#fff;box-shadow:0 0 0 1px rgba(22,140,255,.2),0 8px 28px rgba(22,140,255,.24)}
      .quant-platform-nav[data-surface="dark"] a{color:#3c4541}
      .quant-platform-nav[data-surface="dark"] a:hover{color:#050606;background:#eaf4ff}
      .quant-platform-nav[data-surface="dark"] a[aria-current="page"]{color:#fff;background:#101312;box-shadow:inset 0 0 0 1px rgba(22,140,255,.65)}
      .quant-platform-nav[data-surface="light"]{color:#f4f7f5;border-color:#35f27c;background:#090b0a;box-shadow:0 0 0 1px rgba(53,242,124,.2),0 8px 28px rgba(53,242,124,.22)}
      .quant-platform-nav[data-surface="light"] a{color:#c7cfcb}
      .quant-platform-nav[data-surface="light"] a:hover{color:#fff;background:#17251c}
      .quant-platform-nav[data-surface="light"] a[aria-current="page"]{color:#080a09;background:#fff;box-shadow:inset 0 0 0 1px rgba(53,242,124,.75)}
      .quant-platform-theme-toggle{width:34px;height:34px;flex:0 0 34px;display:grid;place-items:center;padding:0;color:inherit;border:1px solid currentColor;border-radius:8px;background:transparent;cursor:pointer;opacity:.82;transition:opacity .16s,transform .16s,background .16s}
      .quant-platform-theme-toggle:hover{opacity:1;transform:translateY(-1px)}
      .quant-platform-nav[data-surface="dark"] .quant-platform-theme-toggle:hover{background:#eaf4ff}
      .quant-platform-nav[data-surface="light"] .quant-platform-theme-toggle:hover{background:#17251c}
      .quant-platform-active .MuiModal-root{top:var(--quant-platform-nav-offset)!important;height:calc(100% - var(--quant-platform-nav-offset))!important}
      .quant-platform-active .MuiModal-root>.MuiBox-root{top:var(--quant-platform-nav-offset)!important}
      @media(max-width:700px){.quant-platform-nav{padding-inline:10px;font-size:13px}.quant-platform-brand{font-size:12px!important}.quant-platform-nav a{padding:10px;font-size:13px}}
    `;
    document.head.appendChild(style);

    if (window.self === window.top) {
      const host = window.location.hostname;
      const port = window.location.port ? `:${window.location.port}` : '';
      const suffix = host.replace(/^stock-screener\./, '');
      const originFor = (subdomain) => `${window.location.protocol}//${subdomain}.${suffix}${port}`;
      const nav = document.createElement('nav');
      nav.className = 'quant-platform-nav';
      nav.dataset.surface = document.documentElement.dataset.quantPlatformTheme || 'dark';
      nav.setAttribute('aria-label', 'QuantStack 量子栈统一导航');
      nav.innerHTML = `<a class="quant-platform-brand" href="${originFor('quant')}/" aria-label="返回 QuantStack 量子栈主页">QuantStack 量子栈</a>
        <div class="quant-platform-nav-links">
          <a href="${originFor('quant')}/">量化中心</a>
          <a href="${originFor('vibe-trading')}/agent">AI 投研（VT）</a>
          <a href="${window.location.origin}${window.location.search || '/'}" data-quant-ss-home aria-current="page">行情选股(SS)</a>
          <a href="${originFor('nautilus')}/">策略实施(NT)</a>
        </div>
        <button class="quant-platform-theme-toggle" type="button" data-no-translate></button>`;
      document.body.insertBefore(nav, document.body.firstChild);
      document.body.classList.add('quant-platform-active');
      nav.querySelector('[data-quant-ss-home]')?.addEventListener('click', (event) => {
        event.preventDefault();
        const selectedMarket = document.querySelector(
          '[role="combobox"][aria-label="Market selector"]',
        )?.textContent?.match(/\b(CN|US)\b/)?.[1];
        const market = new URLSearchParams(window.location.search).get('market')
          || selectedMarket
          || null;
        const target = market ? `/?market=${encodeURIComponent(market)}` : '/';
        window.history.pushState({}, '', target);
        window.dispatchEvent(new PopStateEvent('popstate'));
        window.dispatchEvent(new CustomEvent('quant:ss-home', { detail: { market } }));
      });
    }
    translateTree(document.body);

    const platformThemeCookie = 'quant-platform-theme';
    const validPlatformTheme = (value) => value === 'dark' || value === 'light';
    const readPlatformTheme = () => {
      const value = document.cookie.split(';').map((part) => part.trim())
        .find((part) => part.startsWith(`${platformThemeCookie}=`))
        ?.slice(platformThemeCookie.length + 1);
      return validPlatformTheme(value) ? value : null;
    };
    const writePlatformTheme = (theme) => {
      if (!validPlatformTheme(theme)) return;
      const host = window.location.hostname;
      const domain = host === 'localhost' || host === '127.0.0.1' || !host.includes('.')
        ? ''
        : `; Domain=.${host.replace(/^[^.]+\./, '')}`;
      const secure = window.location.protocol === 'https:' ? '; Secure' : '';
      document.cookie = `${platformThemeCookie}=${theme}; Path=/; Max-Age=31536000; SameSite=Lax${secure}${domain}`;
    };
    const lightToggleTitles = ['Switch to light mode', '切换到浅色模式'];
    const darkToggleTitles = ['Switch to dark mode', '切换到深色模式'];
    const getNativeThemeToggle = () => Array.from(document.querySelectorAll('#root button[title]'))
      .find((button) => [...lightToggleTitles, ...darkToggleTitles].includes(button.getAttribute('title')));
    const currentPlatformSurface = () => {
      const nativeTitle = getNativeThemeToggle()?.getAttribute('title');
      if (lightToggleTitles.includes(nativeTitle)) return 'dark';
      if (darkToggleTitles.includes(nativeTitle)) return 'light';
      const channels = getComputedStyle(document.body).backgroundColor.match(/[\d.]+/g)?.slice(0, 3).map(Number);
      if (!channels || channels.length < 3) return 'dark';
      const luminance = (channels[0] * .2126 + channels[1] * .7152 + channels[2] * .0722) / 255;
      return luminance < .5 ? 'dark' : 'light';
    };
    const revealPlatformTheme = () => {
      document.documentElement.classList.remove('quant-theme-pending');
    };
    const isPlatformThemePending = () => document.documentElement.classList.contains('quant-theme-pending');
    const renderPlatformThemeToggle = (surface) => {
      const button = document.querySelector('.quant-platform-theme-toggle');
      if (!button) return;
      const toLight = surface === 'dark';
      const label = toLight ? '切换到浅色模式' : '切换到深色模式';
      if (button.getAttribute('aria-label') === label) return;
      button.setAttribute('aria-label', label);
      button.setAttribute('title', label);
      button.innerHTML = toLight
        ? '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.42 1.42M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.42-1.42M17.66 6.34l1.41-1.41"/></svg>'
        : '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
    };
    const syncPlatformSurface = () => {
      const nav = document.querySelector('.quant-platform-nav');
      if (!nav) return;
      const surface = currentPlatformSurface();
      if (nav.dataset.surface !== surface) nav.dataset.surface = surface;
      renderPlatformThemeToggle(surface);
    };
    let applyingPlatformTheme = false;
    const applyRequestedPlatformTheme = () => {
      const desired = readPlatformTheme();
      const current = currentPlatformSurface();
      syncPlatformSurface();
      if (!desired) {
        writePlatformTheme(current);
        return;
      }
      if (desired === current) {
        return;
      }
      if (applyingPlatformTheme) return;
      const nativeToggle = getNativeThemeToggle();
      if (!nativeToggle) return;
      applyingPlatformTheme = true;
      nativeToggle.click();
      const settle = (attempt = 0) => {
        syncPlatformSurface();
        if (currentPlatformSurface() === desired || attempt >= 30) {
          applyingPlatformTheme = false;
          return;
        }
        requestAnimationFrame(() => settle(attempt + 1));
      };
      requestAnimationFrame(() => settle());
    };
    syncPlatformSurface();
    applyRequestedPlatformTheme();
    document.querySelector('.quant-platform-theme-toggle')?.addEventListener('click', () => {
      const desired = currentPlatformSurface() === 'dark' ? 'light' : 'dark';
      writePlatformTheme(desired);
      applyRequestedPlatformTheme();
    });
    let initialThemeStableFrames = 0;
    const settleInitialPlatformTheme = (attempt = 0) => {
      applyRequestedPlatformTheme();
      const desired = readPlatformTheme() || currentPlatformSurface();
      initialThemeStableFrames = currentPlatformSurface() === desired
        ? initialThemeStableFrames + 1
        : 0;
      if (initialThemeStableFrames >= 12 || attempt >= 180) {
        revealPlatformTheme();
        return;
      }
      requestAnimationFrame(() => settleInitialPlatformTheme(attempt + 1));
    };
    requestAnimationFrame(() => settleInitialPlatformTheme());
    document.addEventListener('click', (event) => {
      const button = event.target instanceof Element ? event.target.closest('#root button[title]') : null;
      if (!button) return;
      const title = button.getAttribute('title');
      if (!['Switch to light mode', 'Switch to dark mode', '切换到浅色模式', '切换到深色模式'].includes(title)) return;
      if (applyingPlatformTheme || isPlatformThemePending()) return;
      setTimeout(() => {
        const surface = currentPlatformSurface();
        writePlatformTheme(surface);
        syncPlatformSurface();
      }, 320);
    }, true);
    window.setInterval(applyRequestedPlatformTheme, 800);
    window.addEventListener('focus', applyRequestedPlatformTheme);
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) applyRequestedPlatformTheme();
    });

    let scheduled = false;
    const pending = new Set();
    const flush = () => {
      scheduled = false;
      for (const node of pending) translateTree(node);
      pending.clear();
      syncPlatformSurface();
    };
    const observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        if (mutation.type === 'attributes') pending.add(mutation.target);
        else if (mutation.type === 'characterData') pending.add(mutation.target);
        else mutation.addedNodes.forEach((node) => pending.add(node));
      }
      if (!scheduled && pending.size) {
        scheduled = true;
        requestAnimationFrame(flush);
      }
    });
    observer.observe(document.body, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: translatedAttributes,
    });
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
})();
