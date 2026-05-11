# Benchmark scoring methodology

В проекте теперь есть две разные оценки, и их нельзя смешивать:

| Оценка | Для чего | Как считается | Можно ли считать научной primary metric |
| --- | --- | --- | --- |
| `scientific_ops_per_second` | Основной research ranking СУБД | Throughput на заранее объявленных transaction cases после validity gate | Да, это primary metric |
| `scientific_score` | Удобная нормализация primary metric внутри одного прогона | `100 * backend_ops_per_second / best_valid_ops_per_second` | Только presentation score |
| `decision_score` | Продуктовый выбор под AML API | Weighted p95/SLO utility function | Нет, это экспертная secondary metric |

Главное правило: если в тексте нужно обосновать "какая СУБД быстрее", использовать `scientific_ops_per_second`. Если нужно обосновать "какая СУБД лучше подходит продукту с учетом latency SLO, startup и UX", можно обсуждать `decision_score`, но только как вторичную инженерную интерпретацию.

## Внешние опоры

Такой подход ближе к классическим benchmark rules, чем один субъективный weighted score:

| Опора | Что берем |
| --- | --- |
| LDBC FinBench | Финансовый graph transaction workload и primary throughput metric для заданного scale factor. |
| LDBC SNB Interactive | Transaction throughput + disclosure по latency percentiles. |
| TPC-C | Одна primary throughput metric при validity/response-time constraints. |
| TPC-H | Composite throughput/power metric с full disclosure. |
| Graph500 | Primary metric TEPS для traversal workload. |

Ссылки:

- LDBC FinBench: https://ldbcouncil.org/benchmarks/finbench/
- FinBench VLDB 2025 paper: https://ldbcouncil.org/resources/publications/finbench-vldb-2025/
- LDBC SNB Interactive: https://ldbcouncil.org/benchmarks/snb/interactive/
- TPC benchmarks: https://www.tpc.org/information/benchmarks5.asp
- LDBC Graphalytics: https://ldbcouncil.org/benchmarks/graphalytics/

Это все еще не официальный audited LDBC run: мы прогоняем FinBench-adapted workload через production API проекта, а не через официальный driver. Но методология стала честнее: primary ranking теперь строится по throughput и validity, а не по подбираемым весам.

## Scientific primary metric

`scientific_ops_per_second` считается так:

```text
scientific_ops_per_second =
  sum(successful operations for included transaction cases)
  /
  sum(measured wall seconds for included transaction cases)
```

В FinBench workload включены только transaction-like graph cases:

```text
finbench_node_summary
finbench_person_expand
finbench_account_flow_hub
finbench_shortest_guarantee_path
```

`health` и `dictionary` исключены из primary metric. Они остаются в отчете как operational/control checks, но не могут сделать DuckDB, Neo4j или любую другую СУБД победителем.

Почему это меньше похоже на подгонку:

- нет `ideal/good/acceptable` thresholds;
- нет весов cases;
- нет startup веса;
- нет штрафа за p99;
- нет коэффициентов, которые можно подкрутить под конкретную СУБД;
- metric выражена в понятных единицах: successful operations per second.

Так как runner отправляет одинаковое число measured requests на каждый included case, формула эквивалентна equal-operation transaction mix: каждый тип операции представлен одинаковым количеством операций. Если позже появится официальный или доменный transaction mix, его надо добавить отдельным workload-ом и явно указать ratios.

## Validity gate

Backend получает валидный scientific result только если:

- backend status = `ok`;
- каждый case из `scientific_score.include_cases` был выполнен;
- по каждому included case `errors = 0`;
- по каждому included case есть успешные операции;
- measured wall time и throughput положительные.

Если backend быстрее на части операций, но падает на одной обязательной операции, он не получает scientific ranking. Это важнее, чем красивый частичный throughput.

## Scientific score

`scientific_score` - это не отдельная научная метрика, а нормализация для таблицы:

```text
scientific_score =
  100 * backend_scientific_ops_per_second / best_valid_scientific_ops_per_second_in_this_run
```

Победитель получает `100`, остальные показывают процент от его throughput. Сравнивать `scientific_score` между разными датасетами, машинами или настройками нельзя. Для cross-run сравнения использовать `scientific_ops_per_second` вместе с dataset scale, hardware и runner settings.

## Objective facts

Эти значения измеряются runner-ом и должны попадать в research report:

- `scientific_ops_per_second`;
- `scientific_valid` и причины invalid;
- `errors`;
- `rps`;
- `avg_ms`, `p50_ms`, `p95_ms`, `p99_ms`;
- `app_p95_ms`, если endpoint возвращает `meta.executionMs`;
- `startup_seconds`;
- `total_wall_seconds`;
- `raw/<backend>/<case>.csv`.

Если возникает спор, смотреть сначала raw metrics и validity, потом `scientific_ops_per_second`, и только после этого `decision_score`.

## Decision score

`decision_score` оставлен намеренно, но понижен до secondary metric. Он отвечает не на вопрос "кто научно быстрее", а на вопрос "что приятнее и безопаснее для конкретного production API".

Каждый weighted case получает latency score от 0 до 100:

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
```

Итог:

```text
decision_score = weighted_average(startup_score, case_scores)
```

Субъективность `decision_score` находится здесь:

- набор cases;
- веса cases;
- SLO-границы `ideal/good/acceptable`;
- вес startup;
- штраф за толстый хвост;
- выбранный score profile.

Это не плохо, но это нельзя выдавать за научную primary metric.

## Anti-bias rules

Чтобы scoring не выглядел подогнанным под DuckDB или любую другую СУБД:

- primary ranking всегда по `scientific_ops_per_second`;
- included cases объявлены в `bench/workloads/*.toml` до запуска;
- `scientific_ops_per_second` не использует case weights и SLO thresholds;
- `decision_score` показывается отдельно и не выбирает scientific leader;
- все raw CSV сохраняются;
- если меняются included cases, это новый workload или новая версия workload-а;
- если меняется hardware, dataset scale, requests, warmup или concurrency, результаты нельзя напрямую сравнивать со старым прогоном.

## Формулировка для отчета

```text
The primary ranking metric is equal-operation transaction throughput, reported as scientific_ops_per_second. A backend is eligible only if it passes the validity gate: all predeclared transaction cases complete successfully with zero errors and positive measured throughput. The normalized scientific_score is reported only for readability within the same run. The separate decision_score is an expert-defined production utility score and is not used as the scientific primary metric.
```

Такой wording честнее: он признает, что наш API-прогон не является официальным LDBC audit, но убирает главный риск подгонки - subjective weights больше не выбирают победителя.
