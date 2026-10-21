# Canonical Market Data Batch API

最后更新：2026-08-04

## Routes and Auth

Phase 2.3A 保留现有单标的 API，并在同一个 API-key dependency 下增加：

- `POST /market/batch/instruments`
- `POST /market/batch/bars`
- `GET /market/datasets/{dataset_id}/metadata?dataset_version=...`

Batch routes 使用严格认证：服务端必须配置 `QUANT_API_KEY`，否则返回 503；客户端缺失或提供错误的
`X-Quant-API-Key` 返回 401。service credential 只从环境进入 header，不使用 URL token。响应不返回 Parquet、
DuckDB、NAS 路径或 legacy archive URI。超过 request body、instrument、page 或 date range 限制会明确
失败；响应支持 gzip。

## Eligibility and Version Selection

reader 每页先验证 `catalog/current-manifest-set.json` 与 DuckDB `datasets` 投影完全一致，然后只选择：

- `layer=normalized`；
- `quality_status=passed|warning`；
- `quant-data://normalized/` canonical namespace；
- 非 legacy、非 quarantined、非 failed dataset。

客户端不能传 `include_quarantined` 绕过。显式 version 必须同时给 dataset ID；不存在返回 404，
pointer/catalog 或 payload 不一致时 fail closed。`latest` 按 `created_at/version/dataset_id` 确定性选择
最近 eligible manifest。

类型合同同样 fail closed：bars 只选择 `DatasetKind.MARKET_BARS`，instruments 只选择
`DatasetKind.INSTRUMENTS`。Phase 2.3A metadata route 仅查询 `MARKET_BARS`；未来扩展其他类型时必须增加
显式 kind 参数和 allowlist，不使用无类型选择。

Phase 2.3B 还要求 Manifest 能解析到当前 schema 的 verified Publication Receipt。服务端只从受信配置
读取 `candidate|shadow` consumer access；请求参数不能绕过 eligibility。legacy、`legacy_unknown`、缺失、
hash 不一致或完整 replay 失败的 receipt 均拒绝。candidate 必须同时满足 quality passed、comparison
complete、catalog published 与 candidate eligible；shadow 可读取明确 `incomplete` 且 shadow eligible
的版本，但不能读取 blocked/quarantined。metadata 返回脱敏 publication gate/state、comparison、
canonical rule、evidence fingerprint 与 route evidence；不返回物理路径或 provider request。metadata ETag
绑定 evidence fingerprint，bars/instruments ETag 继续只绑定 canonical snapshot。

Phase 2.3B calendar/state contract 还返回脱敏的 `calendar_capability`、`market_state_capability`、
Manifest 中的 official `calendar_evidence` 与 finite `market_state_sample`，以及显式
`consumer_eligibility`。内部 evidence file refs 会在 API 层移除。calendar 字段声明官方来源版本、
exchange、受支持范围和 unknown fail-closed 边界；state 字段声明 verified/partial/blocked 状态与
effective date，不把当前 snapshot 伪装为历史 PIT。reference/optional route 即使 quality passed，
`candidate_eligible` 仍为 false。

## Query Contract

instrument query 支持 market、instrument IDs、symbols、venues、active、dataset/version、page size 和
cursor。bars query 支持 instrument IDs、start/end、frequency、dataset/version、page size 和 cursor。
结果按 instrument ID（bars 再按 `ts_event`）稳定排序。

`snapshot_token` 绑定 dataset ID/version、完整 Manifest fingerprint、quality eligibility 和 query
contract `1.1.0`。Dataset Semantics Registry 是 route/capability/consumer policy 的权威来源；Batch
只接受 `eligible_consumers` 包含 `batch_api` 且 manifest 与 semantics 匹配的数据集，未注册或不匹配
均 fail closed。metadata 安全暴露 semantics schema version 与 SHA-256 fingerprint；metadata ETag
同时绑定 fingerprint，因此 semantics 变化会使旧 `If-None-Match` 失效。bars/instruments ETag 仍只
绑定数据 snapshot 与页查询，semantics-only 变化不会引发数据页缓存抖动。cursor 绑定 snapshot token
和去除 page size/cursor 后的 filter hash，不能跨查询或跨 snapshot 重放。HTTP ETag 还绑定
route/operation、完整 page request、page size 和 cursor；因此同一页稳定，第一页 ETag 不会误命中
第二页或不同 page size。`If-None-Match` 在 Parquet scan 前返回 304。

每页使用同一个 read-only DuckDB connection 完成 catalog gate 与 payload query；不会按 symbol 建连接
或发 HTTP。响应体不含易变 metrics；`catalog_queries`、`payload_queries`、connections、scans 以及
prepare/payload/total timing 只通过 response headers 和服务日志暴露。所有客户端错误使用固定、
脱敏 detail，不回显 SQL 或物理路径。可配置限制：

- `QUANT_BATCH_MAX_REQUEST_BYTES`（默认 1 MiB）
- `QUANT_BATCH_MAX_INSTRUMENTS`（默认 6,000）
- `QUANT_BATCH_MAX_PAGE_SIZE`（默认 100,000）
- `QUANT_BATCH_MAX_DATE_RANGE_DAYS`（默认 3,660）

日志只记录 operation、rows、bytes、latency、dataset/version 和 ETag prefix，不记录完整 symbol list
或 credential。

## Deterministic Performance Gate

本机 deterministic canonical fixture：5,500 instruments × 20 trading days = 110,000 rows。

| Metric | Result |
| --- | ---: |
| HTTP pages | 2 |
| catalog queries / payload queries | 2 / 2 |
| DuckDB connections / Parquet scans | 2 / 2 |
| per-symbol HTTP requests | 0 |
| first page wall time | 1,185.521 ms |
| prepare / payload / total service time, both pages | 9.862 / 259.433 / 269.295 ms |
| response bytes, both pages | 51,544,048 |
| peak RSS | 670,192 KiB |
| ETag 304 latency | 7.573 ms |
| ETag 304 payload queries / Parquet scans | 0 / 0 |

排序、分页和 304 均通过。NAS 重新准备了 5,500×20 offline staging fixture，并受控验证缺 key=401、
错 key=401、正确 key metadata/bars=200、metadata kind=`market_bars`、20-row bars、stable snapshot、
exact-page 304 且 payload queries/scans=0，以及错误不泄露物理路径。没有调用真实 Provider，也没有把
约 50 MB deterministic response 或其内存峰值描述为生产 SLA；更小默认 page、并发限制和真实 NAS
soak 仍是 Phase 2.3B gate。
