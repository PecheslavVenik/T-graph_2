# Benchmark audit, 2026-04-26

Документ фиксирует, что именно было прогнано, как это было прогнано, сколько занял каждый шаг, как формировались очки и какой вывод из этого можно делать по выбору СУБД для текущей AML-style graph API задачи.

## Короткий вывод

Контрольный all-backend прогон завершился успешно: все 7 backend'ов имеют статус `ok`, все benchmark cases прошли с `errors = 0`.

Рейтинг по итоговому score этого прогона:

| Место | Backend | Score | Главный вывод |
|---:|---|---:|---|
| 1 | DuckDB + DuckPGQ | 100.00 | Лучший результат в текущем embedded-сценарии. Самый простой production-кандидат, если хватает embedded модели и локального DuckDB. |
| 2 | Memgraph | 99.91 | Лучший внешний graph backend в этом прогоне. Очень сильный shortest-path и Cypher-совместимая модель. |
| 3 | ArangoDB | 90.82 | Хороший multi-model кандидат: стабилен, быстрый на expand, средний на shortest-path. |
| 4 | Kuzu | 88.35 | Очень сильный embedded-кандидат для expand/read, но текущий shortest-path adapter проседает. |
| 5 | PostgreSQL + AGE / SQL projection | 85.73 | Операционно привлекателен, но traversal/path в текущем adapter медленнее лидеров. |
| 6 | JanusGraph | 77.90 | Работает, но тяжелый operational footprint и не лучший interactive latency. Имеет смысл только если нужен scale-out graph stack. |
| 7 | Neo4j | 64.77 | Зрелая СУБД, но в текущей реализации сильно проиграла на shortest-path. Нужно отдельно оптимизировать Cypher/GDS/projection. |

Мое субъективное мнение после этого прогона: для текущего проекта самый здравый shortlist - DuckPGQ как embedded baseline, Memgraph как внешний graph production-кандидат, ArangoDB/Kuzu как альтернативы в зависимости от требований к multi-model или embedded graph. Neo4j и JanusGraph нельзя выкидывать "по имени", но текущие adapter/query strategy для этой конкретной задачи пока не дают им выиграть.

## Важная честность про масштаб

Этот документ основан на свежем контрольном all-backend прогоне `small` scale:

- `nodes = 17 508`
- `edges = 93 513`
- `identifiers = 22 519`
- `pgq_edges = 124 021`

Это уже не пустой smoke-test, но это не финальный "serious/stress" исследовательский прогон. Цель этого прогона была другая: убедиться, что все backend'ы действительно рабочие, что adapters сопоставимы, что scoring считается одинаково и что benchmark system корректно собирает артефакты.

Для финального исследовательского текста я бы не ограничивался этим прогоном. Нужен отдельный ночной/длинный прогон `BENCH_SCALE=serious`, минимум 300 measured requests, concurrency 8, warmup 30, плюс повтор 3-5 раз для устойчивости. Команда для такого прогона приведена ниже.

## Что именно бенчилось

Benchmark system построена вокруг одного API слоя приложения. Это важно: сравнивались не изолированные синтетические запросы напрямую в СУБД, а поведение backend'ов под одинаковыми domain API операциями.

Файлы системы:

| Часть | Файл | Роль |
|---|---|---|
| Workload | `bench/workloads/aml.toml` | Описывает cases, веса, SLO, seed values, endpoint paths. |
| Backend configs | `bench/backends/*.toml` | Описывают backend id, тип, env vars, docker service dependencies, reset/startup behavior. |
| Runner | `scripts/bench-runner.py` | Генерирует dataset, стартует app для каждого backend, гоняет cases, считает score, пишет artifacts. |
| Entrypoint | `scripts/bench-suite.sh` | Тонкая оболочка над runner. |
| Dataset generator | `scripts/bench-data.sh` | Формирует DuckDB dataset, который дальше синхронизируется в backend projection. |

Backend'ы в этом прогоне:

| Backend id | Технология | Категория |
|---|---|---|
| `duckpgq` | DuckDB + DuckPGQ | Embedded SQL/PGQ baseline. |
| `neo4j` | Neo4j | Native property graph database. |
| `memgraph` | Memgraph | Native in-memory/operational graph database, Cypher-compatible. |
| `postgres-age` | PostgreSQL + AGE / SQL projection | Relational database with graph extension/projection baseline. |
| `arangodb` | ArangoDB | Multi-model document/key-value/graph database. |
| `kuzu` | Kuzu | Embedded graph database. |
| `janusgraph` | JanusGraph | Distributed graph stack with Gremlin. |

## Откуда взяты идеи benchmark cases

Это не официальный сертифицированный прогон LDBC/GAP/Graph500/LinkBench. Это domain workload, вдохновленный известными benchmark families и адаптированный под реальные API операции проекта.

| Case | Endpoint | Benchmark family | Почему подходит |
|---|---|---|---|
| `health` | `GET /actuator/health` | control | Контроль живости приложения. В итоговый score не входит. |
| `dictionary` | `GET /api/v1/graph/dictionary` | domain metadata lookup | Проверяет metadata/read path и базовый overhead приложения. |
| `node_summary` | `GET /api/v1/graph/node-summary` | LDBC SNB Interactive inspired | One-hop read вокруг выбранной вершины. |
| `expand_person` | `POST /api/v1/graph/expand` | LDBC SNB Interactive / Graph500 BFS inspired | Интерактивное раскрытие окружения person node. |
| `expand_account_flow_hub` | `POST /api/v1/graph/expand` | LinkBench / high-degree traversal inspired | Раскрытие account-flow hub, ближе к AML investigative workload. |
| `shortest_path_depth4` | `POST /api/v1/graph/shortest-path` | GAP Benchmark Suite / Graph500 traversal inspired | Depth-limited path search, самый важный traversal stress case. |

Почему так правильно для этого проекта: пользовательская задача здесь не "считать PageRank на гигантском графе", а интерактивно исследовать AML-граф через API. Поэтому latency p95 на expand/summary/shortest-path важнее, чем batch throughput на алгоритмах общего назначения.

## Точная команда контрольного прогона

Прогон был выполнен так:

```bash
GRAPH_KUZU_PATH=data/graph_bench_kuzu_audit.kuzu \
BENCH_DB=data/graph_bench_small_run.duckdb \
BENCH_SCALE=small \
BENCH_REQUESTS=5 \
BENCH_CONCURRENCY=1 \
BENCH_WARMUP=2 \
BENCH_CURL_MAX_TIME=30 \
BENCH_LOG_DIR=target/bench-audit \
./scripts/bench-suite.sh \
  --backend duckpgq \
  --backend neo4j \
  --backend memgraph \
  --backend postgres-age \
  --backend arangodb \
  --backend kuzu \
  --backend janusgraph \
  --cases health,dictionary,node_summary,expand_person,expand_account_flow_hub,shortest_path_depth4
```

Параметры:

| Параметр | Значение | Что означает |
|---|---:|---|
| `BENCH_SCALE` | `small` | Масштаб dataset для контрольной проверки всех backend'ов. |
| `BENCH_REQUESTS` | `5` | Количество measured requests на case. |
| `BENCH_WARMUP` | `2` | Количество warmup requests перед измеряемой частью. |
| `BENCH_CONCURRENCY` | `1` | Последовательный прогон для чистого adapter correctness/control. |
| `BENCH_CURL_MAX_TIME` | `30` | Timeout одного HTTP request. |
| `BENCH_LOG_DIR` | `target/bench-audit` | Директория артефактов. |

Артефакты прогона:

| Artifact | Путь |
|---|---|
| Summary | `target/bench-audit/20260426-162016/summary.md` |
| Full JSON | `target/bench-audit/20260426-162016/run.json` |
| Cases CSV | `target/bench-audit/20260426-162016/cases.csv` |
| Per-backend app logs | `target/bench-audit/20260426-162016/app-*.log` |

## Общая хронология

Все timestamp'ы ниже взяты из `run.json`. В файле время хранится в UTC. Владивостокское время = UTC + 10 часов.

| Шаг | UTC start | UTC end | Владивосток | Duration | Статус |
|---|---|---|---|---:|---|
| Полный benchmark run | 06:20:16 | 06:28:17 | 16:20:16-16:28:17 | 480.991 s | ok |
| DuckPGQ backend | 06:20:16 | 06:20:33 | 16:20:16-16:20:33 | 17.208 s | ok |
| Neo4j backend | 06:20:33 | 06:23:07 | 16:20:33-16:23:07 | 154.734 s | ok |
| Memgraph backend | 06:23:07 | 06:23:46 | 16:23:07-16:23:46 | 38.415 s | ok |
| PostgreSQL AGE backend | 06:23:46 | 06:24:32 | 16:23:46-16:24:32 | 45.681 s | ok |
| ArangoDB backend | 06:24:32 | 06:25:31 | 16:24:32-16:25:31 | 59.189 s | ok |
| Kuzu backend | 06:25:31 | 06:25:54 | 16:25:31-16:25:54 | 23.417 s | ok |
| JanusGraph backend | 06:25:54 | 06:28:17 | 16:25:54-16:28:17 | 142.345 s | ok |

Backend duration включает подготовку backend/app для выбранной СУБД, ожидание readiness, warmup/measured requests по всем cases и остановку процесса приложения. `startup_seconds` ниже - это отдельная метрика: время от старта приложения до здорового `/actuator/health`; она не полностью равна backend duration, потому что вокруг есть docker/reset/projection/case overhead.

## Startup time

| Backend | Startup, s | Startup score | Комментарий |
|---|---:|---:|---|
| DuckPGQ | 14.177 | 100.00 | Быстрый embedded старт. |
| Neo4j | 104.600 | 72.57 | Дольше good-threshold, но в пределах acceptable. |
| Memgraph | 32.249 | 99.06 | Почти ideal, небольшой штраф за превышение 30 s. |
| PostgreSQL AGE | 24.305 | 100.00 | Быстрый старт. |
| ArangoDB | 40.250 | 95.73 | Нормально, но не embedded. |
| Kuzu | 16.314 | 100.00 | Быстрый embedded старт. |
| JanusGraph | 74.533 | 81.44 | Работает, но тяжелее остальных. |

Startup SLO:

| Уровень | Значение |
|---|---:|
| Ideal | <= 30 s |
| Good | <= 90 s |
| Acceptable | <= 300 s |
| Weight | 0.10 |

## Case SLO и веса

Итоговый score - weighted average по startup и case scores. `health` нужен для контроля, но в итоговый score не входит.

| Case | Weight | Ideal p95 | Good p95 | Acceptable p95 |
|---|---:|---:|---:|---:|
| `health` | 0.00 | 10 ms | 25 ms | 100 ms |
| `dictionary` | 0.05 | 20 ms | 40 ms | 100 ms |
| `node_summary` | 0.15 | 40 ms | 75 ms | 150 ms |
| `expand_person` | 0.25 | 100 ms | 200 ms | 500 ms |
| `expand_account_flow_hub` | 0.25 | 180 ms | 350 ms | 800 ms |
| `shortest_path_depth4` | 0.30 | 250 ms | 450 ms | 1000 ms |

Почему такие веса:

- `shortest_path_depth4` имеет вес 0.30, потому что path search обычно самый тяжелый и самый показательный traversal для graph backend.
- `expand_person` и `expand_account_flow_hub` имеют по 0.25, потому что это основные интерактивные операции расследования.
- `node_summary` имеет 0.15, потому что это важная read операция, но менее тяжелая.
- `dictionary` имеет 0.05, потому что metadata lookup нужен, но не должен решать выбор СУБД.
- `health` имеет 0.00, потому что это контроль доступности, а не graph performance.

## Как формировались очки

Каждый case получает score от 0 до 100 на основе `p95_ms`, количества ошибок и хвоста `p99_ms`.

Формула:

```text
if p95 is missing or errors > 0:
    score = 0
elif p95 <= ideal:
    score = 1.00
elif p95 <= good:
    score = 0.75 + 0.25 * ((good - p95) / (good - ideal))
elif p95 <= acceptable:
    score = 0.40 + 0.35 * ((acceptable - p95) / (acceptable - good))
else:
    score = max(0.0, 0.40 * acceptable / p95)

if p99 > p95 * 2.5:
    score = score * 0.85

case_score = score * 100
```

Итоговый backend score:

```text
backend_score =
  weighted_average(
    startup_score * 0.10,
    dictionary_score * 0.05,
    node_summary_score * 0.15,
    expand_person_score * 0.25,
    expand_account_flow_hub_score * 0.25,
    shortest_path_depth4_score * 0.30
  )
```

Суммарный вес = 1.10, потому что startup идет как отдельная эксплуатационная метрика поверх workload cases. Это осознанно: для production backend важно не только быстро отвечать, но и предсказуемо подниматься.

## Итоговые результаты по p95

`p95_ms` - основная latency метрика. Все cases в таблице ниже прошли с `errors = 0`.

| Backend | health | dictionary | node_summary | expand_person | account_flow_hub | shortest_path |
|---|---:|---:|---:|---:|---:|---:|
| DuckPGQ | 4.762 | 6.982 | 29.246 | 96.312 | 101.733 | 182.634 |
| Neo4j | 5.006 | 6.349 | 131.402 | 81.793 | 150.431 | 7101.851 |
| Memgraph | 3.482 | 6.328 | 28.053 | 63.623 | 153.774 | 56.953 |
| PostgreSQL AGE | 78.595 | 5.055 | 27.375 | 123.301 | 191.751 | 780.309 |
| ArangoDB | 6.715 | 12.951 | 43.966 | 68.710 | 149.324 | 541.394 |
| Kuzu | 3.344 | 11.076 | 32.513 | 53.511 | 165.766 | 728.624 |
| JanusGraph | 25.648 | 6.479 | 81.263 | 163.643 | 205.173 | 757.424 |

## Case scores

| Backend | dictionary | node_summary | expand_person | account_flow_hub | shortest_path | Startup | Total |
|---|---:|---:|---:|---:|---:|---:|---:|
| DuckPGQ | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 |
| Neo4j | 100.00 | 48.68 | 100.00 | 100.00 | 5.63 | 72.57 | 64.77 |
| Memgraph | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 99.06 | 99.91 |
| PostgreSQL AGE | 100.00 | 100.00 | 94.17 | 98.27 | 53.98 | 100.00 | 85.73 |
| ArangoDB | 100.00 | 97.17 | 100.00 | 100.00 | 69.18 | 95.73 | 90.82 |
| Kuzu | 100.00 | 100.00 | 100.00 | 100.00 | 57.27 | 100.00 | 88.35 |
| JanusGraph | 100.00 | 72.08 | 84.09 | 96.30 | 55.44 | 81.44 | 77.90 |

## Измеренное время каждого case

`Case total` - длительность шага case внутри runner, включая warmup и measured phase. `Measured wall` - только measured request batch из CSV.

| Backend | Case | UTC start | UTC end | Case total, s | Measured wall, s | p95, ms | Score |
|---|---|---|---|---:|---:|---:|---:|
| DuckPGQ | health | 06:20:30 | 06:20:30 | 0.033 | 0.014 | 4.762 | n/a |
| DuckPGQ | dictionary | 06:20:30 | 06:20:30 | 0.078 | 0.028 | 6.982 | 100.00 |
| DuckPGQ | node_summary | 06:20:30 | 06:20:30 | 0.241 | 0.130 | 29.246 | 100.00 |
| DuckPGQ | expand_person | 06:20:30 | 06:20:31 | 0.727 | 0.464 | 96.312 | 100.00 |
| DuckPGQ | expand_account_flow_hub | 06:20:31 | 06:20:32 | 0.699 | 0.494 | 101.733 | 100.00 |
| DuckPGQ | shortest_path_depth4 | 06:20:32 | 06:20:33 | 0.999 | 0.592 | 182.634 | 100.00 |
| Neo4j | health | 06:22:18 | 06:22:18 | 0.042 | 0.021 | 5.006 | n/a |
| Neo4j | dictionary | 06:22:18 | 06:22:18 | 0.076 | 0.025 | 6.349 | 100.00 |
| Neo4j | node_summary | 06:22:18 | 06:22:18 | 0.364 | 0.270 | 131.402 | 48.68 |
| Neo4j | expand_person | 06:22:18 | 06:22:19 | 1.133 | 0.368 | 81.793 | 100.00 |
| Neo4j | expand_account_flow_hub | 06:22:19 | 06:22:21 | 1.087 | 0.723 | 150.431 | 100.00 |
| Neo4j | shortest_path_depth4 | 06:22:21 | 06:23:07 | 46.433 | 32.924 | 7101.851 | 5.63 |
| Memgraph | health | 06:23:43 | 06:23:43 | 0.037 | 0.015 | 3.482 | n/a |
| Memgraph | dictionary | 06:23:43 | 06:23:43 | 0.073 | 0.025 | 6.328 | 100.00 |
| Memgraph | node_summary | 06:23:43 | 06:23:44 | 0.223 | 0.121 | 28.053 | 100.00 |
| Memgraph | expand_person | 06:23:44 | 06:23:44 | 0.563 | 0.305 | 63.623 | 100.00 |
| Memgraph | expand_account_flow_hub | 06:23:44 | 06:23:45 | 0.968 | 0.696 | 153.774 | 100.00 |
| Memgraph | shortest_path_depth4 | 06:23:45 | 06:23:46 | 0.368 | 0.231 | 56.953 | 100.00 |
| PostgreSQL AGE | health | 06:24:23 | 06:24:23 | 0.462 | 0.333 | 78.595 | n/a |
| PostgreSQL AGE | dictionary | 06:24:23 | 06:24:23 | 0.069 | 0.021 | 5.055 | 100.00 |
| PostgreSQL AGE | node_summary | 06:24:23 | 06:24:23 | 0.189 | 0.120 | 27.375 | 100.00 |
| PostgreSQL AGE | expand_person | 06:24:23 | 06:24:24 | 1.084 | 0.587 | 123.301 | 94.17 |
| PostgreSQL AGE | expand_account_flow_hub | 06:24:24 | 06:24:26 | 1.350 | 0.891 | 191.751 | 98.27 |
| PostgreSQL AGE | shortest_path_depth4 | 06:24:26 | 06:24:31 | 5.633 | 3.458 | 780.309 | 53.98 |
| ArangoDB | health | 06:25:24 | 06:25:24 | 0.039 | 0.022 | 6.715 | n/a |
| ArangoDB | dictionary | 06:25:24 | 06:25:25 | 0.096 | 0.036 | 12.951 | 100.00 |
| ArangoDB | node_summary | 06:25:25 | 06:25:25 | 0.268 | 0.169 | 43.966 | 97.17 |
| ArangoDB | expand_person | 06:25:25 | 06:25:25 | 0.594 | 0.280 | 68.710 | 100.00 |
| ArangoDB | expand_account_flow_hub | 06:25:25 | 06:25:26 | 1.041 | 0.654 | 149.324 | 100.00 |
| ArangoDB | shortest_path_depth4 | 06:25:26 | 06:25:30 | 3.855 | 2.592 | 541.394 | 69.18 |
| Kuzu | health | 06:25:47 | 06:25:47 | 0.041 | 0.014 | 3.344 | n/a |
| Kuzu | dictionary | 06:25:47 | 06:25:47 | 0.113 | 0.036 | 11.076 | 100.00 |
| Kuzu | node_summary | 06:25:47 | 06:25:47 | 0.227 | 0.138 | 32.513 | 100.00 |
| Kuzu | expand_person | 06:25:47 | 06:25:48 | 0.502 | 0.263 | 53.511 | 100.00 |
| Kuzu | expand_account_flow_hub | 06:25:48 | 06:25:49 | 0.997 | 0.683 | 165.766 | 100.00 |
| Kuzu | shortest_path_depth4 | 06:25:49 | 06:25:54 | 4.929 | 3.476 | 728.624 | 57.27 |
| JanusGraph | health | 06:28:07 | 06:28:07 | 0.207 | 0.108 | 25.648 | n/a |
| JanusGraph | dictionary | 06:28:07 | 06:28:07 | 0.112 | 0.026 | 6.479 | 100.00 |
| JanusGraph | node_summary | 06:28:07 | 06:28:07 | 0.262 | 0.183 | 81.263 | 72.08 |
| JanusGraph | expand_person | 06:28:07 | 06:28:11 | 3.136 | 0.604 | 163.643 | 84.09 |
| JanusGraph | expand_account_flow_hub | 06:28:11 | 06:28:12 | 1.229 | 0.696 | 205.173 | 96.30 |
| JanusGraph | shortest_path_depth4 | 06:28:12 | 06:28:16 | 4.524 | 1.774 | 757.424 | 55.44 |

## Разбор по backend'ам

### DuckDB + DuckPGQ

Результат: `score = 100.00`.

Сильные стороны:

- Все ключевые cases уложились в ideal SLO.
- Лучший production fit, если допустим embedded режим.
- Минимальная операционная сложность: нет отдельного сетевого сервиса, проще deploy и backup model.

Риски:

- Это embedded/аналитический профиль, а не отдельная graph database server архитектура.
- Нужно отдельно проверить поведение при конкурентной записи/обновлении projection, если такая нагрузка будет production-критичной.

Субъективный вывод: главный baseline и, возможно, основной backend для production, если проекту не нужен отдельный graph server.

### Memgraph

Результат: `score = 99.91`.

Сильные стороны:

- Лучший внешний graph backend в этом прогоне.
- `shortest_path_depth4 p95 = 56.953 ms`, это лучший результат среди всех backend'ов.
- Очень хорошие expand cases.
- Cypher-like модель ближе к Neo4j, но здесь показала себя быстрее на workload проекта.

Риски:

- Нужно отдельно проверить memory profile на `serious/stress`.
- Для production нужно посмотреть durability/backup/replication требования.

Субъективный вывод: если нужен внешний graph DB сервис, Memgraph сейчас выглядит самым сильным кандидатом.

### ArangoDB

Результат: `score = 90.82`.

Сильные стороны:

- Хороший баланс: все cases прошли, expand быстрый.
- Multi-model может быть полезен, если часть данных удобно держать как documents plus graph.

Риски:

- Shortest-path средний: `p95 = 541.394 ms`.
- Не выигрывает у Memgraph как graph-only backend и не выигрывает у DuckPGQ как embedded baseline.

Субъективный вывод: хороший компромиссный кандидат, если multi-model важнее максимальной graph latency.

### Kuzu

Результат: `score = 88.35`.

Сильные стороны:

- Embedded graph architecture.
- Очень сильные expand/read cases: `expand_person p95 = 53.511 ms`, `account_flow_hub p95 = 165.766 ms`.
- Быстрый startup.

Риски:

- `shortest_path_depth4 p95 = 728.624 ms`, из-за этого общий score просел.
- Нужно профилировать именно adapter/query для shortest path. По expand видно, что сам backend не выглядит слабым.

Субъективный вывод: перспективный embedded graph кандидат, но перед выбором надо дожать path search.

### PostgreSQL + AGE / SQL projection

Результат: `score = 85.73`.

Сильные стороны:

- Очень хороший startup.
- Сильный metadata/read path.
- Операционно Postgres часто проще принять в production, потому что команда уже умеет его обслуживать.

Риски:

- Path traversal слабее лидеров: `shortest_path_depth4 p95 = 780.309 ms`.
- Текущий adapter умеет работать через SQL projection fallback. Для строгого research вывода именно про AGE нужно отдельно подтвердить extension-enabled execution path и зафиксировать это в артефактах.

Субъективный вывод: надежный operational fallback, но не лучший graph-performance выбор для интерактивного traversal.

### JanusGraph

Результат: `score = 77.90`.

Сильные стороны:

- Backend теперь реально стартует, синхронизирует projection и проходит all cases.
- Gremlin модель дает гибкость.

Риски:

- Тяжелый startup и operational footprint.
- Для single-node interactive API latency не выглядит выигрышно.
- JanusGraph раскрывается там, где нужен distributed graph stack; текущий workload это не доказывает.

Субъективный вывод: оставлять в исследовании можно как scale-out альтернативу, но выбирать для текущего production я бы не стал без отдельного требования на распределенный graph.

### Neo4j

Результат: `score = 64.77`.

Сильные стороны:

- Зрелая экосистема, Cypher, tooling.
- Expand cases нормальные: `expand_person p95 = 81.793 ms`, `account_flow_hub p95 = 150.431 ms`.

Риски:

- `shortest_path_depth4 p95 = 7101.851 ms`, именно это обрушило итоговый score.
- `node_summary p95 = 131.402 ms`, тоже ниже ожидаемого для mature graph DB.
- Возможно, проблема не в Neo4j как СУБД, а в текущей query/projection strategy. Нужно отдельно проверить Cypher shortest path, indexes, relationship type filtering, Graph Data Science projection.

Субъективный вывод: нельзя делать вывод "Neo4j плохой". Корректный вывод: "в текущем adapter/query варианте Neo4j проиграл workload проекта".

## Почему я считаю прогон корректным

Проверки корректности:

- Все 7 backend'ов завершились со статусом `ok`.
- Во всех measured cases `errors = 0`.
- Все backend'ы гонялись через один и тот же HTTP API приложения.
- Seeds одинаковые для всех backend'ов:
  - `party_rk = BENCH_PARTY_0000001`
  - `target_party_rk = BENCH_PARTY_0000004`
  - `account_no = BENCH_ACC_0000001`
  - `node_id = N_BENCH_PERSON_0000001`
  - `path_relation_family = PERSON_KNOWS_PERSON`
- Dataset один и тот же: `data/graph_bench_small_run.duckdb`.
- Результаты сохранены в machine-readable `run.json` и `cases.csv`.
- Per-backend app logs сохранены рядом с артефактами.
- После прогона приложение не осталось висеть на `localhost:8080`.

Что было специально поправлено в benchmark system перед этим прогоном:

- Kuzu получил bulk CSV import через `COPY`, schema reset и исправление order by aliases.
- ArangoDB adapter исправлен под batch insert response как list/map.
- PostgreSQL AGE adapter получил non-fatal extension setup и SQL recursive CTE shortest-path.
- JanusGraph получил исправленный Docker startup, schema setup, vertex id cache для edge sync и explicit Gremlin BFS shortest-path.
- Runner получил `started_at`, `ended_at`, `total_wall_seconds` для run/backend/case, service startup grace и reset volumes для backend'ов вроде JanusGraph.

## Что является "идеалом", к которому стремиться

Для текущей задачи идеальный backend должен:

- Иметь `errors = 0`.
- Держать `p95 <= ideal` на всех weighted cases.
- Не иметь жирного tail: `p99 <= p95 * 2.5`.
- Подниматься быстрее 30 секунд в controlled environment.
- Не требовать ручного reset/hardcoded steps между прогонами.
- Иметь понятную production story: backup, monitoring, migration, reproducible projection build, configurable adapter selection.

Практический целевой профиль:

| Operation | Target p95 |
|---|---:|
| Dictionary metadata | <= 20 ms |
| Node summary | <= 40 ms |
| Person expand | <= 100 ms |
| Account-flow hub expand | <= 180 ms |
| Shortest path depth <= 4 | <= 250 ms |
| Startup | <= 30 s |

В этом контрольном прогоне этим требованиям полностью соответствовали DuckPGQ и Memgraph. DuckPGQ чуть выиграл по итоговому score, Memgraph выиграл shortest-path latency.

## Команда для финального serious-прогона

Для исследовательского отчета я бы запускал так:

```bash
GRAPH_KUZU_PATH=data/graph_bench_kuzu_serious.kuzu \
BENCH_DB=data/graph_bench_serious.duckdb \
BENCH_SCALE=serious \
BENCH_REQUESTS=300 \
BENCH_CONCURRENCY=8 \
BENCH_WARMUP=30 \
BENCH_CURL_MAX_TIME=30 \
BENCH_LOG_DIR=target/bench-serious \
./scripts/bench-suite.sh \
  --backend duckpgq \
  --backend neo4j \
  --backend memgraph \
  --backend postgres-age \
  --backend arangodb \
  --backend kuzu \
  --backend janusgraph \
  --cases health,dictionary,node_summary,expand_person,expand_account_flow_hub,shortest_path_depth4
```

Для научной аккуратности:

- Повторить прогон 3-5 раз.
- Сравнивать median score и p95/p99 distribution, а не один lucky/unlucky run.
- Зафиксировать hardware, Docker limits, JVM heap, версии СУБД и версии adapters.
- Добавить `serious` и `stress` таблицы отдельно от `small`.
- Для Neo4j отдельно прогнать optimized variant.
- Для PostgreSQL AGE отдельно зафиксировать extension-enabled variant без fallback, если тезис именно про AGE.

## Финальное субъективное мнение

Если выбирать прямо сейчас по текущему проверенному состоянию:

1. DuckPGQ - основной pragmatic production candidate для embedded architecture.
2. Memgraph - основной external graph DB candidate.
3. ArangoDB - запасной multi-model candidate.
4. Kuzu - перспективный embedded graph candidate после оптимизации shortest-path.
5. PostgreSQL AGE - operational fallback, но не performance winner.
6. JanusGraph - research/scale-out option, не лучший выбор для текущего interactive API.
7. Neo4j - требует optimization pass; текущий вариант не проходит как лучший backend.

Главный инженерный вывод: benchmark system уже можно использовать как правильный сменяемый слой выбора СУБД. Но финальное исследовательское утверждение "лучшая СУБД для задачи" надо делать после serious multi-run, а не только после этого control audit.
