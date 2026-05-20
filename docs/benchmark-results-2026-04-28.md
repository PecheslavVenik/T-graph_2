# FinBench SF0.1 Benchmark Results, 2026-04-28

> Статус: diagnostic / superseded.
>
> Эти числа полезны как отладочный снимок после доведения backend-ов до рабочего состояния, но их нельзя использовать как финальный research-ranking без повторного единого campaign-run. Причина: итоговая таблица была сведена из нескольких валидных прогонов после последовательных фиксов backend-ов. Для защиты использовать новый pipeline из `scripts/bench-runner.py`: один запуск, один `run.json`, один `manifest.json`, seed-ы из датасета, повторения, confidence intervals и raw samples.

Документ фиксирует исторический прогон LDBC FinBench-inspired workload на локальной машине в рамках `graph_api_v2`.

## Что запускалось

- Датасет: `data/finbench_sf0_1.duckdb`
- Scale: `sf0.1`
- Workload: `bench/workloads/finbench.toml`
- Запросов на кейс: `300`
- Warmup на кейс: `30`
- Concurrency: `8`
- HTTP timeout runner: `30s`
- Scientific cases:
  - `finbench_node_summary`
  - `finbench_person_expand`
  - `finbench_account_flow_hub`
  - `finbench_shortest_guarantee_path`

## Методика scoring

Основной research-рейтинг считается не субъективными весами, а throughput:

```text
scientific_ops_per_second =
  sum(successful operations in scientific cases) /
  sum(measured wall seconds for scientific cases)
```

`combined_scientific_score` - это нормализация относительно лучшего backend в этом наборе:

```text
combined_scientific_score = 100 * backend_ops_per_second / best_ops_per_second
```

`decision_score` оставлен как вторичная продуктовая эвристика: p95/SLO + веса кейсов + startup. Его нельзя использовать как "научный" рейтинг, только как удобную бизнес-оценку под текущий UX.

## Что пришлось чинить

- Для внешних СУБД включен clean-start через benchmark Docker volumes:
  - Neo4j: `graph_api_neo4j_data`
  - Memgraph: `graph_api_memgraph_data`
  - Postgres AGE: `graph_api_postgres_age_data`
  - ArangoDB: `graph_api_arangodb_data`, `graph_api_arangodb_apps`
  - JanusGraph: `graph_api_janusgraph_data`
- В `scripts/bench-runner.py` добавлен generic `services.wait` hook.
- Для Neo4j добавлен readiness check через `cypher-shell RETURN 1`; timeout readiness-команды теперь ретраится, а не валит backend сразу.
- Для JanusGraph:
  - добавлены Gremlin client limits/timeouts;
  - добавлен query backpressure: `GRAPH_JANUSGRAPH_MAX_CONCURRENT_QUERIES=2`;
  - projection batch уменьшен с `250` до `100`;
  - heap контейнера выставлен в более стабильный режим `-Xmx1g`.

## Артефакты прогонов

| run | local time | UTC | wall_s | notes |
| --- | --- | --- | ---: | --- |
| `target/finbench-sf0.1-janusgraph-fixed/20260428-204312` | 20:43:12-21:02:50 | 10:43:12-11:02:50 | 1178.696 | JanusGraph после batch/backpressure фикса |
| `target/finbench-sf0.1-nonjanus-fixed/20260428-210545` | 21:05:45-21:17:25 | 11:05:45-11:17:25 | 700.758 | DuckPGQ, Memgraph, Postgres AGE, ArangoDB, Kuzu; Neo4j failed из-за уже исправленного wait-timeout |
| `target/finbench-sf0.1-neo4j-fixed/20260428-231547` | 23:15:47-23:20:35 | 13:15:47-13:20:35 | 288.122 | Neo4j после исправления readiness retry |

## Итоговый рейтинг

| rank | backend | scientific_ops/s | combined_scientific_score | decision_score | startup_s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | arangodb | 50.528 | 100.00 | 83.66 | 86.483 |
| 2 | duckpgq | 47.803 | 94.61 | 83.65 | 20.212 |
| 3 | kuzu | 44.456 | 87.98 | 84.69 | 42.440 |
| 4 | neo4j | 34.838 | 68.95 | 71.57 | 164.776 |
| 5 | memgraph | 28.493 | 56.39 | 72.69 | 169.014 |
| 6 | postgres-age | 26.907 | 53.25 | 75.67 | 76.591 |
| 7 | janusgraph | 4.394 | 8.70 | 12.02 | 713.979 |

## Per-case results

| backend | case | ok | errors | rps | p95_ms | p99_ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arangodb | account_flow_hub | 300 | 0 | 19.85 | 514.912 | 577.237 |
| arangodb | node_summary | 300 | 0 | 46.50 | 266.207 | 351.717 |
| arangodb | person_expand | 300 | 0 | 390.53 | 29.921 | 36.305 |
| arangodb | shortest_guarantee_path | 300 | 0 | 212.05 | 54.647 | 62.590 |
| duckpgq | account_flow_hub | 300 | 0 | 21.09 | 500.630 | 544.511 |
| duckpgq | node_summary | 300 | 0 | 42.45 | 285.590 | 358.291 |
| duckpgq | person_expand | 300 | 0 | 265.39 | 50.874 | 64.192 |
| duckpgq | shortest_guarantee_path | 300 | 0 | 111.97 | 135.099 | 172.571 |
| kuzu | account_flow_hub | 300 | 0 | 20.97 | 448.677 | 497.111 |
| kuzu | node_summary | 300 | 0 | 40.68 | 305.108 | 357.391 |
| kuzu | person_expand | 300 | 0 | 216.50 | 53.747 | 61.027 |
| kuzu | shortest_guarantee_path | 300 | 0 | 76.46 | 123.917 | 141.201 |
| neo4j | account_flow_hub | 300 | 0 | 16.74 | 700.349 | 812.337 |
| neo4j | node_summary | 300 | 0 | 30.84 | 451.319 | 682.737 |
| neo4j | person_expand | 300 | 0 | 118.13 | 147.823 | 206.715 |
| neo4j | shortest_guarantee_path | 300 | 0 | 70.46 | 271.262 | 1157.822 |
| memgraph | account_flow_hub | 300 | 0 | 12.92 | 884.057 | 1027.353 |
| memgraph | node_summary | 300 | 0 | 23.25 | 507.328 | 578.254 |
| memgraph | person_expand | 300 | 0 | 70.42 | 167.140 | 213.015 |
| memgraph | shortest_guarantee_path | 300 | 0 | 173.84 | 68.751 | 80.263 |
| postgres-age | account_flow_hub | 300 | 0 | 14.70 | 713.834 | 799.524 |
| postgres-age | node_summary | 300 | 0 | 30.85 | 364.560 | 460.977 |
| postgres-age | person_expand | 300 | 0 | 44.47 | 219.377 | 264.605 |
| postgres-age | shortest_guarantee_path | 300 | 0 | 38.86 | 258.887 | 301.964 |
| janusgraph | account_flow_hub | 300 | 0 | 1.87 | 7845.818 | 9170.598 |
| janusgraph | node_summary | 300 | 0 | 11.32 | 1293.604 | 1750.548 |
| janusgraph | person_expand | 300 | 0 | 19.29 | 1249.694 | 2291.873 |
| janusgraph | shortest_guarantee_path | 300 | 0 | 4.26 | 6939.241 | 9278.724 |

## Вывод

Для текущей задачи и текущего локального SF0.1 workload лучший research-кандидат по объективному throughput - `arangodb`: `50.528 ops/s`.

`duckpgq` почти не отстает: `47.803 ops/s`, при этом имеет лучший startup и самую простую embedded-операционку. Это хороший production fallback, если важнее простота поставки и меньше внешней инфраструктуры.

`kuzu` выглядит сильным embedded-графовым вариантом: хороший `decision_score`, быстрый startup и конкурентный `account_flow_hub`. Его стоит держать в short-list вместе с ArangoDB и DuckPGQ.

`neo4j` рабочий, но на этом workload уступает top-3; startup/projection дороже, а shortest-path p99 заметно шумнее.

`memgraph` рабочий и быстрый на shortest-path, но проиграл на node/account/person кейсах и имеет дорогой startup.

`postgres-age` рабочий и операционно понятный, но как graph-query backend в этом workload медленнее native/embedded graph вариантов.

`janusgraph` теперь технически рабочий, но это худший кандидат для single-node interactive workload: startup `713.979s`, p95 на тяжелых кейсах до `7-8s`. Его имеет смысл оставлять только как scale-out/Gremlin reference, а не как основной backend для текущей задачи.

## Итоговая рекомендация

Short-list для дальнейшего исследования:

1. `arangodb` - текущий лидер по scientific throughput.
2. `duckpgq` - лучший embedded/simple-production кандидат, почти равен лидеру.
3. `kuzu` - сильный embedded graph кандидат с хорошим балансом.

Не рекомендовать как основной backend сейчас:

- `janusgraph` для single-node interactive режима;
- `postgres-age`, если главная цель - быстрые graph traversal запросы;
- `neo4j`/`memgraph` можно оставить как контрольные graph-native сравнения, но в текущем прогоне они не лидируют.
