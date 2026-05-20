# Суть benchmark-эксперимента и SF-базы

Этот документ объясняет, что именно измеряет benchmark в проекте, почему такой эксперимент имеет смысл, что такое FinBench SF-база и какие выводы из результата можно делать.

## Коротко

Benchmark в проекте отвечает на вопрос:

```text
Какой graph backend лучше обслуживает интерактивные graph investigation операции
внутри текущей архитектуры graph-api при одинаковом dataset, API и workload?
```

Это не официальный audited LDBC FinBench result. Это FinBench-adapted application-level benchmark: официальный FinBench dataset загружается в универсальную модель проекта и прогоняется через production API проекта.

Корректная формулировка для отчета:

```text
The evaluation uses an LDBC FinBench dataset mapped to the application's universal graph model
and runs a FinBench-adapted transaction workload through the production API layer.
```

## Что является объектом эксперимента

Эксперимент сравнивает не абстрактную "скорость СУБД вообще", а конкретную роль СУБД в системе:

```text
selected DBMS as GraphQueryBackend / projection backend for graph-api
```

Полный measured path:

```text
HTTP request
  -> Spring controller/service
  -> seed/id resolution
  -> GraphQueryBackend adapter
  -> selected DB/projection
  -> graph result mapping
  -> DTO/JSON response
```

Поэтому результат прикладной: он показывает, как быстро пользовательский API получает ответ от backend-а. Это не native-only сравнение Cypher/AQL/SQL engines без application layer.

## Экспериментальная переменная

Меняется только выбранный backend:

```text
GRAPH_QUERY_BACKEND = DUCKPGQ | NEO4J | MEMGRAPH | ARANGODB | KUZU | POSTGRES_AGE | JANUSGRAPH
```

Остальное должно оставаться одинаковым:

- FinBench dataset;
- universal graph model;
- API endpoints;
- workload TOML;
- request bodies;
- API limits (`maxNodes`, `maxEdges`, `maxDepth`, `maxNeighborsPerSeed`);
- seed generation policy;
- number of requests;
- warmup;
- concurrency;
- iterations;
- host/runtime context;
- один benchmark campaign-run.

Идея эксперимента: если при фиксированных условиях меняется только backend, разница в throughput/latency относится к backend integration path, а не к разным данным или разным запросам.

## Что такое SF-база

`SF` означает `Scale Factor`: масштаб сгенерированного FinBench dataset. В LDBC FinBench доступны разные масштабы, например `SF0.01`, `SF0.1`, `SF0.3`, `SF1`, `SF3`, `SF10`. Чем выше SF, тем больше исходный financial graph.

В этом проекте локальная SF-база обычно выглядит так:

```text
bench/sf0.1.tar                  # официальный/выгруженный FinBench archive
target/finbench/sf0.1/raw/*      # распакованные FinBench CSV
data/finbench_sf0_1.duckdb       # импортированная canonical DuckDB база проекта
target/bench-seeds/finbench.json # request seed variants, выбранные из данных
```

Важно: `data/finbench_sf0_1.duckdb` - это не "сырой FinBench как есть". Это canonical DuckDB database проекта, куда FinBench CSV импортированы в универсальную graph-api модель:

```text
g_nodes
g_edges
g_identifiers
g_pgq_edges
```

DuckDB здесь выполняет две роли:

- canonical store / staging layer для проекта;
- источник данных для projection sync во внешние graph backend-ы.

Поэтому размер `.duckdb` файла не равен буквально scale factor. На размер влияют формат хранения DuckDB, индексы, служебные таблицы, JSON-атрибуты и PGQ projection.

## Что лежит в FinBench SF0.1

FinBench моделирует финансовый граф: людей, компании, счета, кредиты, каналы/устройства и связи между ними.

Сущности:

| FinBench raw entity | В проекте |
| --- | --- |
| `person` | `PERSON` node |
| `company` | `COMPANY` node |
| `account` | `ACCOUNT` node |
| `loan` | `LOAN` node |
| `medium` | `MEDIUM` node |

Связи:

| FinBench raw relation | В проекте |
| --- | --- |
| `personOwnAccount`, `companyOwnAccount` | `CUSTOMER_OWNERSHIP` / `OWNS` |
| `transfer`, `loantransfer` | `ACCOUNT_FLOW` / `TRANSFERS_TO` |
| `deposit`, `repay` | `LOAN_FLOW` |
| `signIn` | `SHARED_INFRASTRUCTURE` |
| `personApplyLoan`, `companyApplyLoan` | `LOAN_APPLICATION` |
| `personGuarantee`, `companyGuarantee` | guarantee relation families |
| `personInvest`, `companyInvest` | `INVESTMENT` |

Фактическая локальная импортированная `data/finbench_sf0_1.duckdb`:

| Метрика | Значение |
| --- | ---: |
| FinBench nodes | 64,485 |
| FinBench edges | 409,536 |
| FinBench identifiers | 68,490 |
| PGQ projection edges | 461,091 |
| DuckDB file size | ~483 MB |
| source archive size | ~191 MB |

Распределение nodes:

| node_type | count |
| --- | ---: |
| `ACCOUNT` | 26,347 |
| `COMPANY` | 4,000 |
| `LOAN` | 16,138 |
| `MEDIUM` | 10,000 |
| `PERSON` | 8,000 |

Распределение imported edges:

| relation_family / edge_type | count |
| --- | ---: |
| `ACCOUNT_FLOW / TRANSFERS_TO` | 187,389 |
| `CUSTOMER_OWNERSHIP / OWNS` | 26,347 |
| `LOAN_FLOW / DEPOSITS_TO` | 51,686 |
| `LOAN_FLOW / REPAYS` | 50,495 |
| `LOAN_APPLICATION / APPLIES_FOR` | 16,138 |
| `SHARED_INFRASTRUCTURE / SIGNED_IN_WITH` | 44,540 |
| `PERSON_GUARANTEE_PERSON / GUARANTEES` | 4,694 |
| `COMPANY_GUARANTEE_COMPANY / GUARANTEES` | 2,315 |
| `INVESTMENT / INVESTS_IN` | 25,932 |

Оговорка: текущий importer не является полным official FinBench loader. Например, raw `withdraw` присутствует в SF0.1 archive, но в текущей mapping-логике проекта не импортируется как отдельная relation family. Это значит, что локальная SF-база является FinBench-derived graph-api projection, а не полной native FinBench transaction dataset representation.

## Как готовится база

Подготовка выполняется `scripts/finbench-data.sh`:

1. Проверяет наличие DuckDB CLI.
2. Находит/распаковывает FinBench archive.
3. Накатывает SQL migrations проекта в `.duckdb`.
4. Читает raw CSV из `target/finbench/<scale>/raw/*`.
5. Маппит FinBench nodes/edges в `g_nodes`, `g_edges`, `g_identifiers`.
6. Строит служебные projection таблицы.
7. Запускает `scripts/finbench-seeds.py`.

`scripts/finbench-seeds.py` выбирает реальные request parameters из уже импортированного графа:

- person nodes с высокой incident degree для `node_summary`;
- persons с guarantee edges для `person_expand`;
- accounts с большим outgoing `ACCOUNT_FLOW` для account hub scenario;
- реальные guarantee paths глубины `2..4` для shortest-path scenario.

Seed file не должен редактироваться руками для research-run.

## Что прогоняет workload

Workload объявлен в `bench/workloads/finbench.toml`.

Control cases:

| case | Зачем нужен | Входит в primary metric |
| --- | --- | --- |
| `health` | Проверить живость приложения | Нет |
| `dictionary` | Проверить metadata/read path | Нет |

Scientific included cases:

| case | Что проверяет |
| --- | --- |
| `finbench_node_summary` | Быстрый обзор окружения real person node |
| `finbench_person_expand` | Интерактивное раскрытие `PERSON_GUARANTEE_PERSON` |
| `finbench_account_flow_hub` | High-degree account-flow traversal |
| `finbench_shortest_guarantee_path` | Bounded path search до глубины 4 |

Эти операции выбраны потому, что они похожи на реальные AML/financial investigation сценарии: посмотреть окружение субъекта, раскрыть денежные связи, пройти по гарантийным/ownership цепочкам, найти объяснимый path.

## Главная метрика

Primary metric:

```text
scientific_ops_per_second =
  sum(successful operations for included scientific cases)
  /
  sum(measured wall seconds for included scientific cases)
```

Пример:

```text
node_summary:              900 ok за 30 s
person_expand:             900 ok за 10 s
account_flow_hub:          900 ok за 60 s
shortest_guarantee_path:   900 ok за 20 s

total_ops  = 3600
total_time = 120 s

scientific_ops_per_second = 30 ops/s
```

Backend попадает в scientific ranking только если проходит validity gate:

- backend status = `ok`;
- все included scientific cases выполнены;
- по included cases `errors = 0`;
- есть успешные операции;
- measured wall time положительный;
- throughput положительный.

`scientific_score` - только нормализация внутри одного campaign-run:

```text
scientific_score =
  100 * backend_scientific_ops_per_second / best_valid_ops_per_second_in_this_run
```

`decision_score` - вторичная продуктовая эвристика. Он использует p95/SLO/startup/case weights и помогает обсуждать UX/operations trade-offs, но не выбирает scientific leader.

## Почему это можно обосновать

Методология является controlled engineering experiment:

- заранее объявленный workload;
- один dataset;
- одинаковые request limits;
- seed-ы из данных, а не handwritten per backend IDs;
- одинаковые concurrency/warmup/iterations;
- control endpoints исключены из primary metric;
- backend с errors не ранжируется как valid;
- сохраняются raw samples;
- сохраняется provenance: git, hardware, workload/backend config fingerprints, dataset fingerprint.

Это не делает результат официальным LDBC FinBench result, но делает его защищаемым application-level benchmark-ом для выбора backend-а в текущей системе.

## Что этот benchmark доказывает

Можно утверждать:

```text
На данном FinBench-derived dataset, в данной архитектуре graph-api,
при данном workload и runner settings backend X обслуживает заявленные
graph investigation API operations быстрее/стабильнее backend Y.
```

Нельзя утверждать:

- "это официальный LDBC FinBench результат";
- "эта СУБД быстрее всех вообще";
- "эта СУБД лучше как primary storage без DuckDB layer";
- "проверена durability";
- "проверены ACID/crash recovery/write throughput";
- "проверен полный official Transaction Workload";
- "результаты можно сравнивать между разными машинами/датасетами без оговорок".

## Внешние источники

- LDBC FinBench: https://ldbcouncil.org/benchmarks/finbench/
- LDBC FinBench specification: https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf
- LDBC FinBench datasets: https://ldbcouncil.org/data-sets-surf-repository/finbench.html
- TPC benchmark overview: https://www.tpc.org/information/benchmarks5.asp

