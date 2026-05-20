# Benchmark methodology for DBMS selection

Цель этой системы - не "нарисовать очки", а получить воспроизводимый evidence trail для выбора СУБД под graph-api workload.

## Что считается фактом

Фактами считаются только артефакты одного benchmark campaign:

- `manifest.json` - условия эксперимента: git commit, dirty status, host/tool versions, workload config, backend configs, dataset/seed file fingerprints;
- `run.json` - полный результат runner-а;
- `backend-summary.csv` - итоговые backend metrics;
- `cases.csv` - per-case aggregates;
- `raw/<backend>/<case>.csv` - каждый measured HTTP request с `iteration`, `request_index`, `variant_index`, latency, HTTP status и ошибкой.

Если итоговая таблица собрана руками из разных директорий `target/finbench-*`, это diagnostic analysis, а не финальный research result.

## Что именно измеряется

Текущий FinBench workload измеряет полный путь:

```text
HTTP API -> Spring service -> graph adapter -> selected DB/projection -> response DTO
```

Важно: это **application-level projection benchmark**, а не полностью независимый DBMS benchmark. DuckDB сейчас используется как canonical store/staging layer проекта: в него импортируется FinBench dataset, из него строятся projection-ы во внешние graph backend-ы, через него резолвятся identifiers и дочитываются DTO-атрибуты. Сами graph traversal операции (`expand`, `shortest-path`) выполняются выбранным `GraphQueryBackend` (`Neo4j`, `Memgraph`, `ArangoDB`, `Kuzu`, `Postgres AGE`, `JanusGraph`, `DuckPGQ`), но runtime path все еще содержит canonical DuckDB-слой приложения.

Поэтому этот benchmark отвечает на вопрос:

```text
Какая СУБД лучше работает как graph-query/projection backend внутри текущей архитектуры graph-api?
```

Он не отвечает напрямую на более широкий вопрос:

```text
Какая СУБД быстрее как полностью самостоятельное хранилище FinBench без DuckDB canonical layer?
```

Для второго вопроса нужен отдельный native/official benchmark track: загрузить FinBench data независимо в каждую СУБД, реализовать query templates для каждой СУБД, прогнать официальный LDBC FinBench driver или максимально близкий native harness, и исключить DuckDB из runtime path.

Это не официальный audited LDBC FinBench driver run, поэтому в тексте нельзя писать "мы получили официальный результат LDBC FinBench". Корректная формулировка:

```text
The evaluation uses an LDBC FinBench dataset mapped to the application's universal graph model and runs a FinBench-adapted transaction workload through the production API layer.
```

## Почему результаты не из головы

Runner заранее получает workload из `bench/workloads/finbench.toml`. Там объявлены:

- backend list;
- transaction cases;
- requests/concurrency/warmup/iterations;
- primary metric;
- secondary decision score profiles.

Dataset готовится `scripts/finbench-data.sh`. Request seed-ы готовятся `scripts/finbench-seeds.py` из реальных данных:

- no handwritten backend-specific IDs;
- no synthetic shortest-path control edges;
- no per-backend seed changes;
- если seed-ы нельзя получить из dataset, подготовка падает.

## Обоснование benchmark-процесса

Benchmark строится как controlled experiment: меняется только backend, а workload, dataset, API limits, request mix, seed policy, concurrency и scoring остаются одинаковыми. Это нужно, чтобы итог отвечал на вопрос "какая СУБД лучше подходит этому приложению при одинаковой задаче", а не "какой backend получил более удобный сценарий".

| Шаг | Что делаем | Почему именно так | Что это доказывает |
| --- | --- | --- | --- |
| 1. Берем FinBench dataset | Загружаем FinBench CSV в canonical DuckDB/model через `scripts/finbench-data.sh`. | FinBench прямо целится в financial graph scenarios: anti-fraud, risk control, neighborhood reads и transaction workload. Это ближе к задаче проекта, чем случайный synthetic graph. | Данные и связи не придуманы под DuckDB/Neo4j/ArangoDB, а взяты из внешнего benchmark-семейства. |
| 2. Маппим в universal graph model | Все СУБД получают одну и ту же модель `g_nodes/g_edges/g_identifiers`, затем backend-specific projection. | Приложение уже живет в universal API/model. Сравнение native schema каждой СУБД было бы отдельным исследованием и смешало бы качество моделирования с качеством backend-а. | Сравнивается backend под одну и ту же application contract, но не полностью независимое native-хранилище каждой СУБД. |
| 3. Выбираем seed-ы из данных | `scripts/finbench-seeds.py` выбирает реальные node/account/path parameter sets из `source_system='finbench'`. | Один handwritten seed может случайно попасть в легкий/тяжелый случай. Несколько real seed variants уменьшают зависимость от одного удачного ID. Synthetic control edges запрещены. | Запросы воспроизводимы и не подгоняются под backend. |
| 4. Прогоняем через HTTP API | Runner вызывает `/node-summary`, `/expand`, `/shortest-path`, а не напрямую Cypher/AQL/SQL. | Цель проекта - выбрать СУБД для production API. Пользователь платит latency всего пути: service, adapter, projection, DB, DTO. Native-only DB benchmark не покажет стоимость интеграции. | Результат применим к текущей системе, а не только к лабораторному движку. |
| 5. Разделяем control и scientific cases | `health`/`dictionary` остаются для sanity, но не входят в primary ranking. | Control endpoints могут быть быстрыми независимо от graph backend-а. Включать их в рейтинг было бы способом исказить результат. | Primary metric отражает graph workload, а не служебные ручки. |
| 6. Делаем warmup | Перед измерением отправляется `warmup` requests. | JVM, connection pools, caches и query planning дают холодный шум. Warmup отделяет steady-state latency от первого запуска. | Измеряется стабильная работа после прогрева. |
| 7. Делаем несколько iterations | Каждый case повторяется `runner.iterations` раз, raw samples сохраняются с номером iteration. | Один прогон может быть шумным из-за ОС, Docker, GC, background IO. Iterations дают variance, CI и CV. | Можно отличить уверенную разницу от случайного шума. |
| 8. Используем одинаковый concurrency | `BENCH_CONCURRENCY` одинаков для всех backend-ов. | Graph API интерактивный, но не strictly single-user. Concurrency показывает поведение под параллельными пользователями и connection pool pressure. | Сравнение ближе к реальному API-режиму. |
| 9. Primary metric - throughput | `scientific_ops_per_second = successful graph operations / measured graph wall time`. | Throughput в понятных единицах меньше подвержен субъективности, чем weighted score. Это похоже на подход известных benchmark-ов: есть primary metric и отдельно validity/disclosure. | Рейтинг "быстрее/медленнее" строится не на экспертных весах. |
| 10. Validity gate | Backend валиден только если все scientific cases прошли с `errors=0`. | Быстрый backend, который падает на shortest-path или timeout-ится, не решает задачу системы. Частичный успех нельзя ранжировать как победу. | Победитель умеет выполнять весь заявленный workload. |
| 11. `decision_score` отдельно | Weighted p95/SLO/startup score не выбирает scientific leader. | Вес latency/startup - продуктовая, а не научная договоренность. Ее можно обсуждать, но нельзя выдавать за объективный benchmark score. | В отчете видно, где факты, а где инженерная интерпретация. |
| 12. Один campaign-run | Финальный вывод берется из одного `run.json`, а не из нескольких директорий. | Ручное объединение прогонов смешивает разные условия: код, настройки, daemon state, cache, volume state. | Итоговая таблица воспроизводима и проверяема. |

## Почему не иначе

Почему не официальный LDBC driver: официальный driver дал бы более строгий benchmark конкретной СУБД, но он не измерил бы production API проекта, adapter overhead, projection sync и DTO path. Поэтому официальный LDBC FinBench используется как внешняя опора для данных и transaction shape, а результат честно называется FinBench-adapted API-level benchmark.

Почему не native queries напрямую: native Cypher/AQL/SQL сравнил бы query engines в изоляции. Для проекта важнее, какая СУБД лучше работает за `GraphQueryBackend` и одинаковым REST contract. Native-only тест можно добавить отдельным workload-ом, но он не заменяет API-level выбор.

Почему DuckDB есть у всех backend-ов: текущая production architecture уже имеет canonical relational/embedded слой, а graph DB используется как ускоряющая/специализированная projection для traversal. Это позволяет честно сравнить сменяемые graph backend-ы без переписывания всей предметной модели под каждую СУБД. Но это ограничение надо явно раскрывать: такой результат нельзя называть независимым сравнением СУБД как primary storage.

Почему equal-operation mix: у проекта пока нет подтвержденной production telemetry с реальными долями операций. Любые веса transaction mix были бы субъективными. Equal-operation mix проще защитить: каждый заявленный тип graph operation получает одинаковое число measured operations. Когда появится telemetry, нужно добавить отдельный workload version с зафиксированными ratios.

Почему не один seed: graph performance сильно зависит от degree/skew/path depth. Один seed может случайно быть слишком легким или слишком тяжелым. Поэтому seed file содержит набор `case_variables`, а runner идет по ним deterministic round-robin.

Почему не один запуск: latency на JVM/Docker/локальной машине шумит. Поэтому runner пишет per-iteration statistics, confidence interval и coefficient of variation. Если CI пересекаются, надо писать "результаты статистически близки", а не притворяться, что десятые доли ops/s что-то доказывают.

Почему startup отдельно: startup/projection sync важен для эксплуатации, но смешивать его с steady-state query throughput нельзя. Поэтому startup входит только во вторичный `decision_score`, а primary scientific throughput считает measured graph operations.

## Primary metric

Основная метрика:

```text
scientific_ops_per_second =
  sum(successful operations for included transaction cases)
  /
  sum(measured wall seconds for included transaction cases)
```

Validity gate:

- backend status is `ok`;
- все included cases выполнены;
- по included cases `errors = 0`;
- successful operations and measured wall time are positive.

`scientific_score` - это только нормализация внутри одного campaign:

```text
100 * backend_scientific_ops_per_second / best_valid_scientific_ops_per_second
```

`decision_score` - вторичная экспертная оценка для production trade-offs. Ее нельзя использовать как научный primary ranking.

## Repetitions and uncertainty

`BENCH_ITERATIONS` / `runner.iterations` задает число повторений каждого measured case. Summary показывает:

- aggregate `scientific_ops/s`;
- per-iteration mean throughput;
- 95% confidence interval;
- coefficient of variation.

Если confidence intervals перекрываются или `cv_%` высокий, вывод должен быть осторожным: СУБД статистически близки на этом workload, либо нужен более длинный прогон.

## External methodological anchors

Методология опирается на известные benchmark-principles:

- LDBC FinBench - financial graph transaction workload and throughput-oriented reporting;
- LDBC SNB Interactive - graph transaction workload with latency disclosure;
- TPC benchmarks - primary metric plus validity/disclosure rules;
- Graph500 / LDBC Graphalytics - graph workload reporting through a clear primary metric.

Ссылки:

- https://ldbcouncil.org/benchmarks/finbench/
- https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf
- https://www.tpc.org/information/benchmarks5.asp
- https://ldbcouncil.org/benchmarks/graphalytics/
