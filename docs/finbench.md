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

Пояснение сути эксперимента, primary metric и устройства SF-базы: `docs/benchmark-experiment-and-sf-dataset.md`.

## Что делает importer

`scripts/finbench-data.sh` берет FinBench archive (`bench/sf0.1.tar`, `bench/sf1.tar` или путь из `FINBENCH_ARCHIVE`), распаковывает его в `target/finbench/<scale>`, читает `raw/*/part-*.csv` и маппит FinBench entities в универсальную модель проекта:

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

Importer также пишет seed file через `scripts/finbench-seeds.py`:

```text
target/bench-seeds/finbench.json
```

Это нужно, потому что реальные IDs в dataset заранее неизвестны. Runner читает seed file и подставляет реальные `node_id`, `party_rk`, `account_no` в workload cases. В seed file лежит не один зашитый ID, а `case_variables`: наборы параметров для каждого case, выбранные из реальных `source_system='finbench'` nodes/edges.

Seed policy:

- `finbench_node_summary` - real person nodes с высокой incident degree;
- `finbench_person_expand` - real persons, у которых есть `PERSON_GUARANTEE_PERSON`;
- `finbench_account_flow_hub` - real accounts с высоким outgoing `ACCOUNT_FLOW`;
- `finbench_shortest_guarantee_path` - real person guarantee paths глубины `2..4`;
- synthetic control edges не добавляются. Если подходящих seed-ов нет, подготовка падает с ошибкой.

Содержательные поля из node-CSV сохраняются в `g_nodes.attrs_json`, чтобы не плодить отдельные колонки под каждый тип FinBench-сущности. Например, для `PERSON` сохраняются `gender`, `birthday`, `country`, `city`; для `ACCOUNT` - `type`, `email`, `freqLoginType`, `lastLoginTime`, `accountLevel`, `inDegree`, `OutDegree`, `isExplicitDeleted`, `Owner`; для `LOAN` - `loanAmount`, `balance`, `usage`, `interestRate`. Поле `city` дополнительно пишется в отдельную колонку для `PERSON` и `COMPANY`, потому что оно используется общим поиском и DTO-атрибутами.

## Быстрая проверка

Windows: подготовить базу, поднять Docker и дождаться готового API:

```powershell
.\scripts\finbench-docker.ps1
```

Скрипт печатает готовый backend URL для фронта. По умолчанию это:

```text
http://localhost:18080
```

Если локальная база уже была создана старым importer'ом, пересоберите ее:

```powershell
.\scripts\finbench-docker.ps1 -ForceData
```

Windows/PowerShell через WSL:

```powershell
.\scripts\finbench-data.ps1 -DbPath data\finbench_sf0_1.duckdb
```

Скрипт ожидает архив `bench/sf0.1.tar` или `bench/sf1.tar` и установленный `duckdb` CLI внутри WSL. Для `sf0.1.tar` он автоматически использует `target/finbench/sf0.1`, чтобы путь к распакованному dataset совпадал с именем архива.

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
BENCH_ITERATIONS=3 \
BENCH_LOG_DIR=target/finbench-sf1 \
./scripts/finbench-suite.sh \
  --require-all-backends \
  --backend duckpgq \
  --backend neo4j \
  --backend memgraph \
  --backend postgres-age \
  --backend arangodb \
  --backend kuzu \
  --backend janusgraph
```

## Канонический запуск без хардкода

Для research-результата используйте один command path: `scripts/finbench-data.sh` готовит dataset и seed file, `scripts/finbench-suite.sh` запускает один benchmark campaign. Не редактируйте Python/Bash-скрипты под конкретный backend и не вписывайте seed ID вручную.

### 1. Подготовить dataset и seed file

Для текущего локального `sf0.1` archive:

```bash
BENCH_DB=data/finbench_sf0_1.duckdb \
BENCH_SCALE=sf0.1 \
FINBENCH_ARCHIVE=bench/sf0.1.tar \
FINBENCH_DATASET_DIR=target/finbench/sf0.1 \
FINBENCH_SEED_SAMPLE_LIMIT=32 \
./scripts/finbench-data.sh
```

Что здесь настраивается:

- `BENCH_DB` - куда положить canonical DuckDB dataset;
- `BENCH_SCALE` - label масштаба в артефактах;
- `FINBENCH_ARCHIVE` - какой FinBench archive использовать;
- `FINBENCH_DATASET_DIR` - куда распакован/будет распакован archive;
- `FINBENCH_SEED_SAMPLE_LIMIT` - сколько real seed variants выбрать на case.

После этого должен появиться:

```text
target/bench-seeds/finbench.json
```

Этот файл генерируется из реальных `source_system='finbench'` данных. Его не надо править руками для результата.

### 2. Запустить один полный campaign-run

```bash
BENCH_DB=data/finbench_sf0_1.duckdb \
BENCH_SCALE=sf0.1 \
GRAPH_KUZU_PATH=target/kuzu-finbench-sf0.1-campaign \
BENCH_REQUESTS=300 \
BENCH_WARMUP=30 \
BENCH_CONCURRENCY=8 \
BENCH_ITERATIONS=3 \
BENCH_LOG_DIR=target/finbench-sf0.1-campaign \
./scripts/finbench-suite.sh --require-all-backends
```

Почему без `--backend`: список backend-ов уже объявлен в `bench/workloads/finbench.toml` как `default_backends`. Так меньше риска случайно забыть кандидата или собрать итог из разных запусков. Для smoke/debug можно запускать `--backend duckpgq`, но это не финальный ranking.

`--require-all-backends` нужен для research-run: команда завершится ошибкой, если хотя бы один backend не прошел scientific validity gate. Это лучше, чем молча получить неполную таблицу.

### 3. Где смотреть результат

Итог лежит в:

```text
target/finbench-sf0.1-campaign/<run_id>/
```

Минимальный набор файлов для отчета:

- `summary.md` - человекочитаемый итог;
- `run.json` - полный машинный результат;
- `manifest.json` - commit, dirty status, host/tool versions, dataset/backend/workload fingerprints;
- `backend-summary.csv` - итоговая таблица по backend-ам;
- `cases.csv` - per-case aggregates;
- `raw/<backend>/<case>.csv` - каждый measured request.

Финальный вывод можно делать только из этих файлов одного `<run_id>`.

### 4. Что можно менять без хардкода

Разрешенные knobs:

- env-переменные `BENCH_DB`, `BENCH_SCALE`, `FINBENCH_ARCHIVE`, `FINBENCH_DATASET_DIR`;
- runner knobs `BENCH_REQUESTS`, `BENCH_WARMUP`, `BENCH_CONCURRENCY`, `BENCH_ITERATIONS`, `BENCH_LOG_DIR`;
- backend list через `bench/workloads/finbench.toml`, если это новая версия workload-а;
- backend configs в `bench/backends/*.toml`;
- Java adapter implementation для новой СУБД.

Запрещено для финального research-run:

- править `target/bench-seeds/finbench.json` руками;
- менять workload/cases между backend-ами;
- запускать разные backend-и отдельными командами и потом склеивать таблицу;
- использовать `--allow-existing`, потому что состояние уже запущенного приложения/проекции не гарантирует clean start;
- добавлять synthetic edges, чтобы конкретный case "точно проходил";
- менять API limits или request body под конкретную СУБД.

### 5. Как понять, что прогон годится

В `summary.md` и `backend-summary.csv` должно быть:

- все backend-и имеют `status=ok`;
- все backend-и имеют `scientific_valid=True`;
- included scientific cases имеют `errors=0`;
- `manifest.json` показывает один и тот же workload/dataset для всего campaign;
- `cv_%` и `ci95_ops/s` позволяют объяснить, насколько устойчива разница.

## Важные оговорки

Это не сертифицированный официальный LDBC audit run. Это practical FinBench-adapted workload: FinBench dataset загружается в модель проекта и прогоняется через единый HTTP API, чтобы выбрать backend для конкретной системы.

Финальный research-ranking должен быть взят из одного полного campaign-run, а не из ручного объединения нескольких прогонов. Минимальный набор артефактов для отчета:

- `summary.md` - человекочитаемый итог;
- `run.json` - полный машинный результат;
- `manifest.json` - provenance: git commit, dirty status, host/tool versions, workload/backend config fingerprints;
- `backend-summary.csv` - итоговые метрики по backend-ам;
- `cases.csv` - агрегаты по case-ам;
- `raw/<backend>/<case>.csv` - каждый measured request с `iteration` и `variant_index`.

Для research-обоснования правильная формулировка:

```text
The evaluation uses an LDBC FinBench dataset mapped to the application's universal graph model and runs a FinBench-adapted transaction workload through the production API layer.
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
