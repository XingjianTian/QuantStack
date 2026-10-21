# Market Data Type Evolution

## 统一原则

QuantStack 使用统一 instrument identity、UTC event time 和 availability time 组织不同市场数据，
但不把所有事件压进一张宽表。每个类型拥有独立 schema、partition、retention 和查询接口，并
通过 `instrument_id`、`ts_event`、`ts_init`/`available_at` 关联。

所有未来类型都遵循以下共同字段：

- `instrument_id`：QuantStack canonical ID，不直接用 provider symbol 作为主键；
- `ts_event`：交易所事件时间，统一为 timezone-aware UTC；
- `ts_init`：QuantStack 首次接收/生成时间，用于延迟和 point-in-time 判断；
- `sequence`：provider 提供时保留，用于排序、去重和 gap detection；
- `provider`/`source_revision`：保留数据来源和修订事实；
- `flags`：交易状态、修正、拍卖或异常标记，不用隐式空值表达。

## 类型设计

### Bar

Bar 是时间窗口聚合结果。`frequency`、price type、aggregation source 和 adjustment policy 必须
由 manifest 明确。Daily/Minute Bar 使用相同逻辑模型，但分开分区和质量策略；Raw price 与
adjusted price 不互相覆盖。

### Tick

Tick 表示 provider 的最小市场事件 envelope，不等同于成交。它用于保留无法预先分类的原始
事件流，并包含 event type、sequence、payload revision。Normalized 层应尽量转换为更明确的
Trade、Quote 或 OrderBookDelta；Tick payload 不作为长期因子接口。

### Trade

Trade 是已成交事件，计划字段包括 price、size、trade_id、aggressor_side、conditions 和
venue。CN/HK/US 对 odd lot、auction、off-exchange 和 correction 的语义不同，normalizer 必须
保留 flags，不能把未知方向推断成买卖方向。

### Quote

Quote 表示 best bid/offer snapshot 或更新，计划字段包括 bid/ask price、bid/ask size、venue、
sequence 和 quote condition。必须区分 consolidated quote 与 venue quote；crossed/locked market
由质量层标记，不静默删除。

### OrderBook

OrderBook 分为：

- `OrderBookSnapshot`：某一 sequence 的完整 N 档快照；
- `OrderBookDelta`：add/update/delete/clear 增量事件；
- `BookLevel`：side、price、size、order_count 和 level。

重建时必须从一个已校验 snapshot 开始，连续应用 sequence delta；检测到 gap 后立即标记 book
失效，直到新 snapshot 到达。L1、L2 和 L3 数据不能混用同一 schema version。

## Storage 与 API 演进

```text
daily/minute bars -> date partitions, Parquet, range query
trades/quotes      -> date/hour partitions, Parquet, time + instrument pruning
ticks/book deltas  -> hour/minute partitions, append-only event files
book snapshots     -> snapshot partitions + sequence lineage
```

API 将按类型增加只读 endpoint 和分页/cursor contract。大规模事件查询应由 storage adapter 做
predicate pushdown，API 仍不暴露 DuckDB/Parquet。实时订阅、WebSocket、broker feed 和 execution
不属于本阶段；将来若增加，也必须与历史 Data Layer 和交易权限边界分离。

## Schema 版本路线

第一阶段只保留现有 Instrument、Bar、Fundamental 和 Corporate Action schemas。本文件定义未来
接口语义，不新增伪实现。下一阶段按 Trade → Quote → OrderBookSnapshot/Delta → generic Tick 的
顺序分别增加 schema、Pydantic model、Synthetic fixture、quality rules 和 manifest kind；每种
类型通过独立 contract review 后才能进入 ingestion service。
