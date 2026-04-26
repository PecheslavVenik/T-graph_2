# Benchmarking

Цель стенда: сравнивать СУБД и graph backend-и воспроизводимо, без хардкода в скриптах и без переписывания workload-а под каждую технологию.

## Архитектура

Benchmark layer состоит из трех частей:

| слой | где лежит | что делает |
| --- | --- | --- |
| Workload | `bench/workloads/*.toml` | Описывает датасет, seed-ы, HTTP cases, веса и SLO. |
| Backend | `bench/backends/*.toml` | Описывает как поднять конкретную СУБД/backend и какие env дать приложению. |
| Runner | `scripts/bench-runner.py` | Готовит данные, стартует backend, прогревает, меряет, считает score, пишет отчеты. |

Главный принцип: новый backend добавляется новым TOML-файлом и реализацией `GraphQueryBackend` в приложении. Workload при этом не меняется.

`adapter_status` в backend TOML:

| статус | смысл |
| --- | --- |
| `implemented` | backend можно реально запускать и сравнивать |
| `adapter_required` | СУБД включена в research shortlist, но Java adapter/projection еще не реализован |

Runner не запускает `adapter_required` backend-и и помечает их как `skipped`, чтобы в отчете не появлялись фальшивые цифры.

## Быстрый запуск

Подготовить production-like синтетический AML-граф:

```bash
BENCH_SCALE=serious make bench-data
```

Прогнать все backend-и из workload-а:

```bash
BENCH_PREPARE_DATA=true \
BENCH_SCALE=serious \
BENCH_REQUESTS=300 \
BENCH_CONCURRENCY=8 \
make bench-suite
```

Прогнать только один backend:

```bash
./scripts/bench-suite.sh --backend duckpgq
./scripts/bench-suite.sh --backend neo4j
```

Посмотреть все зарегистрированные backend-и и статус adapter-а:

```bash
./scripts/bench-suite.sh --list-backends
```

Прогнать часть cases:

```bash
./scripts/bench-suite.sh \
  --backend duckpgq \
  --cases "dictionary node_summary expand_person expand_account_flow_hub"
```

Результаты пишутся в `target/bench/<run_id>/`:

- `summary.md` - основной отчет и текущий лидер.
- `run.json` - машинно-читаемый полный результат.
- `cases.csv` - сводная таблица по cases.
- `raw/<backend>/<case>.csv` - сырые latency/status samples.
- `app-<backend>.log` - startup/sync log приложения.

## Датасеты

Текущий генератор `scripts/bench-data.sh` строит AML-oriented property graph:

| preset | nodes | edges | назначение |
| --- | ---: | ---: | --- |
| `small` | ~17.5k | ~93k | smoke/sanity на ноутбуке |
| `serious` | ~175k | ~1.6M | основной research comparison |
| `stress` | ~875k | ~7.8M | capacity planning |

Для custom scale:

```bash
BENCH_SCALE=custom \
BENCH_PERSONS=100000 \
BENCH_ACCOUNTS=200000 \
BENCH_COMPANIES=10000 \
BENCH_DEVICES=50000 \
BENCH_ACCOUNT_FLOW_EDGES=2000000 \
BENCH_PERSON_KNOWS_EDGES=500000 \
BENCH_SHARED_INFRA_EDGES=500000 \
BENCH_CORPORATE_EDGES=100000 \
make bench-data
```

## Benchmark Families

Это не сертифицированный официальный прогон LDBC/Graph500. Для исследовательского проекта здесь используется практичный workload, вдохновленный известными benchmark-семействами и адаптированный к AML-задаче.

| family | зачем нужен | как представлен в workload-е |
| --- | --- | --- |
| LDBC SNB Interactive | интерактивные graph reads, one-hop/two-hop neighborhood queries | `node_summary`, `expand_person` |
| Graph500 / GAP Benchmark Suite | traversal-heavy BFS/shortest-path нагрузка | `expand_person`, `shortest_path_depth4` |
| LinkBench-style high degree reads | поведение на hub-узлах и skewed degree distribution | `expand_account_flow_hub` |
| Domain AML workload | платежные связи, ownership, shared infrastructure, corporate control | весь `aml.toml` датасет и cases |

Если нужна строго научная/публикуемая цифра, добавляйте отдельный workload с официальным датасетом и driver-ом. Этот стенд оставляет тот же runner и backend configs, меняется только `bench/workloads/<name>.toml` и генератор/импортер данных.

## Score

Каждый case имеет:

- `weight` - вклад в итоговый score.
- `ideal_p95_ms`, `good_p95_ms`, `acceptable_p95_ms` - границы p95.
- `benchmark_family` - почему этот case есть в сравнении.

Startup/projection sync считается отдельным SLO через `[startup_slo]`. Это важно: например, Neo4j может быстро отвечать после импорта, но дорогой startup sync тоже является частью стоимости технологии.

Hard guardrails:

- 5xx/timeout в стабильном прогоне должны быть 0.
- `p99` выше `2.5x p95` считается признаком нестабильности и штрафуется.
- Startup OOM/failure = backend не прошел workload на этом масштабе.
- Сравнивать надо одинаковый workload, одинаковый датасет, одинаковые лимиты API.

## Как добавить новую СУБД

1. Реализовать backend в приложении:

```java
@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "MEMGRAPH")
class MemgraphGraphQueryBackend implements GraphQueryBackend {
    // findExpandEdges(...)
    // findShortestPath(...)
}
```

2. Добавить значение в `GraphSource`, health indicator и runtime/projection loader, если СУБД требует отдельной проекции из canonical DuckDB.

3. Скопировать `bench/backends/_template.toml` в новый файл:

```bash
cp bench/backends/_template.toml bench/backends/memgraph.toml
```

4. Заполнить `id`, `name`, `services.compose`, `env.GRAPH_QUERY_BACKEND` и секреты через env placeholders.

5. Прогнать тот же workload:

```bash
./scripts/bench-suite.sh --backend memgraph --cases "node_summary expand_person shortest_path_depth4"
```

Нельзя менять workload, seed-ы или лимиты API специально под конкретную СУБД. Меняются только adapter и backend config.

## Зарегистрированные СУБД

Сейчас в bench-системе зарегистрированы:

| backend | status | зачем включен |
| --- | --- | --- |
| `duckpgq` | implemented | embedded baseline: DuckDB canonical storage + PGQ traversal |
| `neo4j` | implemented | зрелая property graph DB, Cypher baseline |
| `memgraph` | implemented | Cypher-compatible low-latency/in-memory-first кандидат |
| `kuzu` | implemented | embedded columnar property graph кандидат |
| `postgres-age` | implemented | PostgreSQL operational baseline плюс Apache AGE/openCypher |
| `arangodb` | implemented | multi-model document/graph кандидат для graph + evidence payloads |
| `janusgraph` | implemented | distributed Gremlin/TinkerPop кандидат для scale-out проверки |

Внешние сервисы для кандидатов добавлены в `docker-compose.yml` под profile `bench-candidates`. `Kuzu` отдельного compose-сервиса не имеет, потому что это embedded/in-process кандидат.

## Что сравнивать дальше

Текущий код уже поддерживает `DuckDB + DuckPGQ` и `Neo4j`. Для полноценного исследования разумный следующий shortlist:

| кандидат | почему стоит проверить |
| --- | --- |
| PostgreSQL + Apache AGE или отдельный SQL/recursive backend | понятная эксплуатация, SQL-экосистема, хорошая baseline-точка |
| Memgraph | Cypher-compatible, low-latency graph queries, удобен для streaming/projection |
| Kuzu | embedded graph DB, интересен как альтернатива DuckDB-style локальному движку |
| ArangoDB | multi-model document/graph, полезен если граф и документы должны жить вместе |
| JanusGraph | distributed Gremlin/TinkerPop вариант, полезен как scale-out contrast |
| Neo4j Enterprise/GDS | если нужны mature graph algorithms и operational tooling |

Distributed graph DB имеет смысл добавлять только если single-node `serious/stress` уже не закрывает объем или SLA.

## Как потом оставить выбранную СУБД

Benchmark layer не должен попадать в runtime path прода. После выбора:

1. Зафиксировать `GRAPH_QUERY_BACKEND=<WINNER>` в production deployment.
2. Оставить только нужный `GraphQueryBackend` и его config/health beans.
3. Удалить или отключить проигравшие projection loaders и docker services.
4. Оставить `bench/` и `scripts/bench-runner.py` в репозитории как regression/performance suite.
5. Для релизов гонять минимальный workload на `small`, перед архитектурными изменениями - `serious`, перед capacity planning - `stress`.

## Legacy quick script

`scripts/bench.sh` оставлен как простой curl-based smoke benchmark против уже запущенного приложения. Для сравнения СУБД используйте `scripts/bench-suite.sh`, потому что он пишет воспроизводимые артефакты и не содержит hardcoded backend logic.
