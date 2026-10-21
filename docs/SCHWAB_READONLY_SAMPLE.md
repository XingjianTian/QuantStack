# Schwab read-only sample

This document describes the first-stage account and investment-plan sample. It
uses entirely fictional fixture data and produces non-executable proposal
batches. It does not require a Schwab developer application, account login,
OAuth token, or network access.

## Scope and safety boundary

The investing layer depends on provider-neutral contracts from
`quant_stack.brokerage.contracts`. Only `SchwabAdapter` imports the Schwab
client boundary. Plans, budget accounting, and proposal generation do not
import `schwab-py` or provider response fields.

The sample CLI exposes four local operations:

* `sync-sample` reads the fictional account, orders, transactions, quotes, and
  coverage fixtures and stores a timestamped snapshot in a private SQLite
  runtime directory.
* `propose-sample` builds the fictional ledger and computes a proposal batch
  from that snapshot. It persists the ledger and current proposal in one
  transaction.
* `show-state` reads the persisted sample state after a process restart.
* `review` records a review against an exact proposal input fingerprint.

There is no live switch, credential argument, OAuth command, order payload,
submit, replace, or cancel operation. The CLI rejects unknown options such as
`--live`, and every stored proposal carries `non_executable=true` and
`fictional=true`.

## Run the fixture flow

Use the repository's prepared environment and keep the runtime state outside
the source tree:

```bash
cd /path/to/user/Projects/Quant/QuantStack-schwab-readonly
export PYTHONPATH="$PWD/src:$PWD"
quantstack_venv=/tmp/quantstack-schwab-venv
UV_PROJECT_ENVIRONMENT="$quantstack_venv" uv sync --frozen --extra brokerage
PYTHON="$quantstack_venv/bin/python"
STATE_DIR="$(mktemp -d /tmp/quantstack-schwab-sample.XXXXXX)"

"$PYTHON" -m quant_stack.brokerage_cli \
  --state-root "$STATE_DIR" \
  sync-sample --at 2026-10-16T00:00:00Z

"$PYTHON" -m quant_stack.brokerage_cli \
  --state-root "$STATE_DIR" \
  show-state
```

The sync command exits `0` when the fictional snapshot is complete. The
machine-readable result includes the snapshot ID, account alias, counts for
positions/orders/transactions/quotes, sync state, and `non_executable=true`.
The runtime directory is mode `0700`; its SQLite and lock files are mode
`0600`. `show-state` accepts an optional `--at` timestamp. It checks the
current snapshot's business time, sync completeness, and every quote used by
the sample plan. A proposal is shown as `current` only while those inputs are
fresh; an older `ready` batch is reported as `stale` with
`proposal_freshness_reasons`.

Generate the proposal from the same saved snapshot:

```bash
set +e
"$PYTHON" -m quant_stack.brokerage_cli \
  --state-root "$STATE_DIR" \
  propose-sample --at 2026-10-16T00:00:00Z
proposal_status=$?
set -e
test "$proposal_status" -eq 2
```

The fictional fixture intentionally lacks two pieces of funding metadata: the
cash field does not state whether open-order reservation is already included,
and an option position has no contract multiplier. The proposal therefore
fails closed with explicit reasons such as
`UNKNOWN_RESERVATION_SEMANTICS`, `UNKNOWN_ORDER_RESERVATION_SEMANTICS`, and
`UNKNOWN_OPTION_OBLIGATION`. A blocked proposal is a successful safety result:
it does not deduct budget, mutate positions, or call a broker write endpoint.

The CLI reserves a separate fictional `ready` fixture profile. Its independent
raw account, order, option, and coverage JSON must explicitly record the
funding reservation semantic, option contract multiplier, and confirmed
cancellation. The adapter validates that fixture through the same mapping
path; the CLI does not rewrite a normalized snapshot or clear unknown fields:

```bash
READY_STATE="$(mktemp -d /tmp/quantstack-schwab-ready.XXXXXX)"
"$PYTHON" -m quant_stack.brokerage_cli \
  --profile ready \
  --state-root "$READY_STATE" \
  propose-sample --at 2026-10-16T00:00:00Z
```

The independent `tests/fixtures/brokerage/ready/` profile is now present and
validated through the same raw mapping path. This command exits `0`, returns a
`ready` batch with whole-share quantities, and still marks every item
`non_executable=true`. An equity layer that requires review remains
`waiting_review`; the ETF item can be `ready` because it has no manual-review
flag. If the same saved snapshot is proposed at `2026-10-16T01:00:00Z`, its
`00:00Z` quote is stale: the whole batch is `blocked` and the CLI exits `2`.
The default `blocked` profile remains useful for checking that unknown account
semantics stop a proposal.

The commands above use the lock file and an external uv-managed environment;
the environment is outside the source tree so runtime files cannot become
source artifacts. To run the ready flow through uv after synchronization:

```bash
UV_PROJECT_ENVIRONMENT="$quantstack_venv" uv run python -m quant_stack.brokerage_cli \
  --profile ready --state-root "$READY_STATE" \
  propose-sample --at 2026-10-16T00:00:00Z
```

## Calculation rules represented by the sample

`ETFPlan` stores an explicit timezone, period, start date, execution day,
annual cap, whole-share rule, catch-up policy, fee, reserve, and optional
limit price. Due periods are derived from the local calendar, and annual caps
are applied per calendar year. Recalculation is pure; it does not create a
budget entry.

Only `ROLL` catch-up is supported in this sample. `SKIP` is rejected until a
persisted period-grant watermark can distinguish prior-period spend reliably.
It cannot silently reuse accumulated spend against a new monthly allowance.

`EquityLayer` stores a trigger price, layer budget, sequence, review flag, and
total position limit. Existing positions are opening facts and do not count as
new spend. Attributed fills and transaction-only buys are deduplicated by
stable evidence IDs. Unattributed or conflicting evidence blocks the proposal
and remains visible for later correction. Corrections and reversals are
explicit negative ledger entries tied to the original evidence.

The ledger carries confirmed facts across overlapping broker windows. Two
fictional monthly periods of `$100.00` with one attributed `$60.00` buy produce
`$200.00` accrued, `$60.00` spent, and `$140.00` remaining. Rebuilding the
second window does not create another budget entry. If the first window only
contained a transaction aggregate and a later window supplies its execution,
the execution replaces the provisional entry in place; a changed amount blocks
the ledger rather than charging both values. Fee-only transactions are retained
even when they have no instrument or quantity. A correction or reversal whose
target cannot be found is retained as unattributed and cannot reduce spent
budget.

Attributed buys must match the configured rule's security. A wrong or missing
buy symbol retains the evidence and attribution for audit, marks the fact
unattributed, and blocks proposals. Fees with a known security must also match;
cash-only fees remain valid. Rebuilding an overlapping window cannot turn an
incorrect attribution into confirmed spend.

Adjustments must reference another eligible original fact in the same account,
rule, and currency. Self references, adjustment chains, and cumulative refunds
larger than the original cost remain unresolved and block proposals. A second
adjustment against the same original fact is conservatively blocked. Unknown
transaction shapes are retained rather than treated as cash deposits.

The proposal generator uses a shared cash pool. It distinguishes available
cash from open-order occupation, requires an explicit reservation semantic,
uses quote age and quote currency checks, and calculates whole-share quantity
from the assumed limit price, fee, and reserve. Same-symbol equity layers use
the smallest configured position cap; known short-put delivery obligations
reduce that cap. Unknown option metadata blocks the result. Recalculation
replaces the current proposal pointer atomically and never marks a suggestion
as executed.
Missing or blank option deliverables remain unknown at both normalization and
proposal preflight. Standard stock delivery must be explicitly evidenced.

Each ledger also keeps a signed position-quantity watermark. The first
complete snapshot establishes the opening baseline; a later quantity change
must match normalized buy or sell evidence in that snapshot's cumulative fact
set. A change with no explainable transaction, an unknown option event, or an
unmappable corporate action blocks the ledger. Changes to only `as_of`, market
value, or cost basis do not change the quantity watermark and do not create
budget spend.

An order whose cancellation is still pending remains in the occupation and
blocks the account with an `UNCONFIRMED_ORDER_CANCELLATION` reason. Only a
confirmed cancellation with zero remaining quantity releases that occupation.
The preflight also rejects a ledger belonging to another account, an old or
future snapshot, missing or unknown option `put_call` or contract metadata, and
a plan attribution that names no configured rule. A review cannot revive an
invalidated batch or approve a batch after its review window has expired.
Sync failure invalidation survives a restart and a later successful sync of the
same snapshot. Plan-content changes invalidate the current proposal even before
recalculation. A disabled overall plan produces no positive buy quantities.

Manual transaction attribution is an explicit public API. A reviewer creates
an `Attribution` record with a reason, reviewer identity, timestamp, plan or
layer, and optional `supersedes` reference, then rebuilds the current window
against the previous ledger and persists the replacement:

```python
from quant_stack.investing import Attribution, build_ledger, persist_ledger

review = Attribution(
    evidence_id="execution:fictional-execution-001",
    plan_id="fictional-etf-plan",
    reason="matched to the ETF schedule",
    recorded_at=review_time,
    recorded_by="fictional-reviewer",
    supersedes="attribution:prior-review-001",
)
ledger = build_ledger(
    current_snapshot,
    plan,
    prior=previous_ledger,
    attributions={"fictional-transaction-001": review},
)
persist_ledger(store, ledger)
```

The revised ledger keeps old evidence once, records the new attribution, and
remains blocked until every included buy execution is attributed. The ledger
is versioned by current snapshot, plan, entries, and attribution history, so a
later proposal must use the matching version. If a prior current proposal
exists, `persist_ledger` removes its current pointer atomically when the ledger
version changes; `propose-sample` or the proposal API must generate a fresh
batch before anything can be shown as current.

Late transaction or fill facts are retained immutably even when their provider
window is older than the current snapshot. The sync state then records the
fact IDs in `pending_fact_ids` and marks the account incomplete with
`LATE_WINDOW_FACTS`; the current proposal remains blocked. The facts are not
cleared or silently charged by a ledger rebuild. A newer complete provider
window that includes all pending facts must be published before the ledger and
proposal can be committed again.

## Restart and atomicity

`PrivateSnapshotStore` is the private runtime store shared by brokerage sync
and investing persistence. The ledger and proposal writer checks that account,
snapshot, plan, and ledger versions agree, then writes the ledger, proposal,
and current pointer inside one SQLite transaction. A restarted process reloads
the same ledger and current proposal. The investing writer uses the transaction
view directly and does not re-enter lock-taking store methods. Commit-time
validation re-reads the current sync state, complete snapshot, plan fingerprint,
ledger version, and quote freshness against its injected commit clock in that
same transaction. Replacing a ledger for a newer snapshot requires an explicit
expected prior ledger version, so a concurrent writer cannot overwrite a newer
calculation. A simulated failure after staging the ledger leaves both the
ledger and current proposal
absent, proving rollback rather than a half-published state.

## Verification

Run the focused read-only suite:

```bash
PYTHONPATH="$PWD/src:$PWD" \
  "$PYTHON" -m pytest -q tests/brokerage tests/investing
"$PYTHON" -m ruff check \
  src/quant_stack/brokerage src/quant_stack/investing \
  src/quant_stack/brokerage_cli.py tests/brokerage tests/investing
```

The current WP03/04 evidence is 43 core investing tests passed, five CLI tests
passed including the ready-profile and stale-quote regressions, 51 investing
tests passed in total, and the owned-path Ruff check passed.

The suite covers timezone and annual-cap scheduling, catch-up policy, Decimal
validation, initial-position separation, fill/transaction deduplication,
unattributed and corrected evidence, open-order occupation, shared cash,
position caps, short-put/option uncertainty, quote and sync freshness,
proposal replacement and review fingerprints, SQLite restart recovery, and the
fixture-only CLI boundary.

For a source-level write-path check:

```bash
rg -n -i \
  '(place_order|submit_order|cancel_order|replace_order|order_write|oauth|token)' \
  src/quant_stack/investing src/quant_stack/brokerage_cli.py
```

The investing package should produce no broker-write or OAuth call sites. The
fixture CLI's only broker-facing operation is the read-only fixture adapter;
real Schwab authorization is a later integration step requiring explicit
application approval and user-provided login authorization.


## 第三轮后规则身份修正

ETF成交归属不得携带layer_id；个股层级的plan_id与非空layer_id必须指向同一实际层级。身份冲突保留成交和人工归属审计，标为UNATTRIBUTED并返回ENTRY_RULE_IDENTITY_MISMATCH；非法更正目标返回CORRECTION_TARGET_RULE_IDENTITY_MISMATCH。prior/overlap重建保持阻断，不释放预算。合法层级省略layer_id时按实际层级规范化。

本地最终验证：全仓1242 passed、2 skipped；本任务133 passed，Ruff/锁文件/独立CLI验证通过。第四轮限定正式复审PASS，以上身份修正及相关回归已闭合，M1样本里程碑完成；本轮仅归档结果并复用现有验证，PR保持Draft。真实接入和已声明的R08路径竞态加固仍属后续阶段。

## Quant Center 本地样本工作台

顶部选择与“策略实施”同级的“嘉信实盘”，或直接访问 `/schwab`（旧 `/account-sample` 入口保留）。页面调用独立的本地样本服务，
不需要主 Center API。此服务只运行固定虚构 fixture，不接受真实账户、凭证、文件路径或远端 URL。
页面展示账户摘要、持仓平均成本与总成本、市值、交易流水、预算归属记录、资金占用及拟单计算依据。

终端一：按前面的 `uv sync --frozen --extra brokerage` 准备环境后启动服务：

```bash
cd /path/to/user/Projects/Quant/QuantStack-schwab-readonly
PYTHONPATH="$PWD/src:$PWD" "$PYTHON" -m quant_stack.brokerage_sample_api
# 仅监听 127.0.0.1:8917
```

终端二：启动本地页面（不部署）：

```bash
cd /path/to/user/Projects/Quant/QuantStack-schwab-readonly/apps/quant-center
npm ci
npm run dev -- --host 127.0.0.1 --port 5187 --strictPort
# 浏览器打开 http://127.0.0.1:5187/schwab
```

首次页面无账户快照。点击“同步样本账户”实际运行 SchwabAdapter、schwab-py 的 fixture transport 和
快照保存逻辑；再点击“重新生成清单”运行已有预算与投资计算，并持久化清单。
页面不导入固定 JSON 作为运行结果，服务未启动或请求失败会显示错误并隐藏旧结果。
刷新网页读取上次保存状态。正常与信息缺失场景各自保存，报价过期场景使用正常样本并以01:00 UTC检查时效；
重新计算仍不能让固定的过期报价变新。

样本业务时间固定在2026-10-16，操作计数和最近操作时间使用实际时间。
状态保存在 `~/.local/state/quantstack-brokerage-workbench/{ready,blocked}`，权限0700；
快照、账本、清单及操作记录可在重启后恢复。清单重算更新建议，不扣实际投入；
同一输入重复操作不增加预算事实。挂单占用口径未知时暂停资金分配。
正常样本中已有订单均已取消，所以当前挂单占用为0；拟单预留302.80美元仅为测算，不冻结资金。

独立服务只开放本地状态查询、样本同步和样本拟单接口，拒绝非本机来源与未允许的网页Origin。
Vite将 `/brokerage-sample-api/v1/brokerage-sample` 代理到8917端口，保留其余Center API代理配置。
没有券商交易或OAuth入口，不需要申请嘉信API或登录授权。

原 `brokerageSample.generated.json` 保留为离线导出和前端测试样本，页面只引用其类型，不打包数据作为后备结果。
需要重新导出时可运行 `PYTHONPATH="$PWD/src:$PWD" "$PYTHON" scripts/export_brokerage_sample.py`。

本次本地工作台属于M1后新增范围，历史M1限定CPDW PASS不覆盖新增代码；
本轮使用后端回归、前端交互测试、构建、lint及网页验证。用户随后授权合并并将虚构工作台接入正式网站；
授权不包含真实账户或券商订单写入，尚无需API申请或登录授权。

正式网站使用独立的 `brokerage-sample` 容器，仅共享 Center nginx 的loopback网络；
不发布8917端口，不挂载真实数据湖、API密钥或券商令牌，状态写入独立私有卷。
同源 `/brokerage-sample-api/` 沿用网站现有登录会话保护，由Center nginx转发至loopback；
客户端只携带主站同源会话，服务端明确允许配置的 `QUANT_SAMPLE_SITE_ORIGIN`。
生产启动/回滚见 `docs/DEPLOYMENT_NAS.md`，后续目标为指定策略并实施至嘉信，当前尚未实现真实执行。
