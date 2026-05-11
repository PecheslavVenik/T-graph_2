# LDBC FinBench в проекте

В проект добавлен runnable workload, основанный на официальном LDBC FinBench dataset.

Локальный официальный архив:

```text
bench/sf1.tar
```

Runner/importer:

```text
scripts/finbench-data.sh
scripts/finbench-suite.sh
bench/workloads/finbench.toml
```

## Что делает importer

`scripts/finbench-data.sh` берет официальный `bench/sf1.tar`, распаковывает его в `target/finbench/sf1`, читает `sf1/raw/*/part-*.csv` и маппит FinBench entities в универсальную модель проекта:

| FinBench | graph-api model |
|---|---|
| `person` | `g_nodes`, `node_type = PERSON` |
| `company` | `g_nodes`, `node_type = COMPANY` |
| `account` | `g_nodes`, `node_type = ACCOUNT` |
| `loan` | `g_nodes`, `node_type = LOAN` |
| `medium` | `g_nodes`, `node_type = MEDIUM` |
| `personOwnAccount`, `companyOwnAccount` | `CUSTOMER_OWNERSHIP` edges |
| `transfer`, `loantransfer` | `ACCOUNT_FLOW` edges |
| `deposit`, `repay` | `LOAN_FLOW` edges |
| `signIn` | `SHARED_INFRASTRUCTURE` edges |
| `personApplyLoan`, `companyApplyLoan` | `LOAN_APPLICATION` edges |
| `personGuarantee`, `companyGuarantee` | guarantee edges |
| `personInvest`, `companyInvest` | `INVESTMENT` edges |

Importer также пишет seed file:

```text
target/bench-seeds/finbench.json
```

Это нужно, потому что реальные IDs в официальном dataset заранее неизвестны. Runner читает seed file и подставляет реальные `node_id`, `party_rk`, `account_no` в workload cases.

## Быстрая проверка

Smoke import:

```bash
BENCH_DB=data/finbench_smoke.duckdb \
BENCH_SCALE=smoke \
./scripts/finbench-data.sh
```

Smoke benchmark на одном backend:

```bash
BENCH_DB=data/finbench_smoke.duckdb \
BENCH_SCALE=smoke \
BENCH_REQUESTS=2 \
BENCH_WARMUP=1 \
BENCH_CONCURRENCY=1 \
BENCH_LOG_DIR=target/finbench-smoke \
./scripts/finbench-suite.sh --prepare-data --backend duckpgq
```

Smoke benchmark на всех backend'ах:

```bash
BENCH_DB=data/finbench_smoke.duckdb \
BENCH_SCALE=smoke \
BENCH_REQUESTS=1 \
BENCH_WARMUP=1 \
BENCH_CONCURRENCY=1 \
BENCH_LOG_DIR=target/finbench-smoke-all \
./scripts/finbench-suite.sh \
  --backend duckpgq \
  --backend neo4j \
  --backend memgraph \
  --backend postgres-age \
  --backend arangodb \
  --backend kuzu \
  --backend janusgraph
```

Проверенный smoke artifact:

```text
target/finbench-smoke-all/20260427-200644/summary.md
```

Результат этого smoke-прогона: все 7 backend'ов `status=ok`, все FinBench cases `errors=0`.

## Полный SF1 прогон

Подготовка полного SF1 dataset:

```bash
BENCH_DB=data/finbench_sf1.duckdb \
BENCH_SCALE=sf1 \
./scripts/finbench-data.sh
```

Полный прогон:

```bash
BENCH_DB=data/finbench_sf1.duckdb \
BENCH_SCALE=sf1 \
BENCH_REQUESTS=300 \
BENCH_WARMUP=30 \
BENCH_CONCURRENCY=8 \
BENCH_LOG_DIR=target/finbench-sf1 \
./scripts/finbench-suite.sh \
  --backend duckpgq \
  --backend neo4j \
  --backend memgraph \
  --backend postgres-age \
  --backend arangodb \
  --backend kuzu \
  --backend janusgraph
```

## Важные оговорки

Это не сертифицированный официальный LDBC audit run. Это practical FinBench-adapted workload: официальный FinBench SF1 dataset загружается в модель проекта и прогоняется через единый HTTP API, чтобы выбрать backend для конкретной системы.

Для research-обоснования правильная формулировка:

```text
The evaluation uses an LDBC FinBench dataset mapped to the application's universal graph model and runs a FinBench-inspired transaction workload through the production API layer.
```

## Scoring

FinBench ranking в проекте больше не выбирается по субъективным весам. Primary metric - `scientific_ops_per_second`: equal-operation throughput по заранее объявленным transaction cases:

```text
finbench_node_summary
finbench_person_expand
finbench_account_flow_hub
finbench_shortest_guarantee_path
```

`health` и `dictionary` не входят в primary metric. Backend валиден только если все included cases прошли с `errors = 0` и положительным throughput.

`decision_score` оставлен как вторичная инженерная оценка для production API, но он не выбирает scientific leader. Подробное обоснование лежит в `docs/benchmark-scoring.md`.

DuckPGQ shortest-path намеренно выполняется через bounded BFS поверх expand lookup, а не через DuckPGQ shortest path operator. На FinBench projection DuckPGQ CSR path operator падал внутри DuckDB/DuckPGQ, поэтому стабильный production benchmark не должен зависеть от этой нестабильной функции.
