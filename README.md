# Graph API v2 (Операционный анализ)

Stateless REST API для расследовательской графовой аналитики на DuckDB с переключаемым graph query backend.
Основной проверенный backend для demo/MVP - `DuckDB + DuckPGQ`: canonical storage в DuckDB, обход графа через DuckPGQ projection. Остальные adapter-ы в кодовой базе нужны для R&D/benchmark-сравнения и считаются experimental/unverified, если для конкретного backend-а не прогнан отдельный сценарий.
Модель данных поддерживает generic AML graph: `PERSON`, `ACCOUNT`, `COMPANY`, `DEVICE`, `ADDRESS` и другие node types поверх общей схемы `g_nodes/g_edges/g_identifiers`.

## Что умеет API
- `POST /api/v1/graph/expand` - умное 1-hop расширение для расследовательского графа с анти-hub ранжированием
- `POST /api/v1/graph/shortest-path` - кратчайший путь (minimum hops) внутри выбранного relation family
- `POST /api/v1/graph/query` - старт расследования с безопасного read-only SQL-запроса
- `POST /api/v1/graph/import/preview` и `/import/commit` - импорт CSV с нодами/ребрами в canonical graph
- `GET /api/v1/graph/nodes/search` - поиск опорной ноды по имени, идентификатору или атрибутам для ручного ресерча
- `GET /api/v1/graph/dictionary` - справочник типов связей/статусов для легенды фронта
- `POST /api/v1/graph/export?format=JSON|CSV|NDJSON` - экспорт графа, который фронт уже собрал
- Стабильные `nodeId`/`edgeId` для merge на фронте
- Backend не генерирует интерактивный HTML export: он отдает данные графа и stable IDs, а frontend отвечает за layout, hover, pinning, hide/merge UI и HTML export
- Метрики и health endpoints (`/actuator/*`)

## Доменный фокус MVP
- Backend больше не привязан к `PERSON`-only модели: relation families и node types можно расширять без изменения базовой схемы
- Если `relationFamily` не передан, используется конфигурируемое значение `graph.default-relation-family` (по умолчанию `PERSON_KNOWS_PERSON` для обратной совместимости)
- В seed-данных по-прежнему есть расследовательские семьи `PERSON_KNOWS_PERSON`, `PERSON_RELATIVE_PERSON`, `PERSON_SAME_CITY_PERSON`
- Контракт уже поддерживает и generic AML families: `ACCOUNT_FLOW`, `CUSTOMER_OWNERSHIP`, `SHARED_INFRASTRUCTURE`, `CORPORATE_CONTROL`
- Backend сам ограничивает первый экран графа: candidate budget, top-K по seed, global node/edge budget, hub suppression

## Архитектура (кратко)
- `GraphController` - HTTP слой
- `InvestigationService` - оркестрация расследовательских сценариев, ранжирование и budget-лимиты через backend-интерфейс `GraphQueryBackend`
- `GraphNodeRepository`, `GraphEdgeRepository`, `GraphDictionaryRepository`, `GraphSqlRepository` - резолв идентификаторов, чтение узлов/ребер, справочники и SQL-backed graph slices
- `DuckPgqGraphQueryRepository` - текущая DuckPGQ-реализация `GraphQueryBackend`
- `Neo4jGraphQueryBackend`, `MemgraphGraphQueryBackend`, `KuzuGraphQueryBackend`, `PostgresAgeGraphQueryBackend`, `ArangoGraphQueryBackend`, `JanusGraphQueryBackend` - experimental adapters для R&D benchmark
- `db/migration` - миграции Flyway (схема + seed)

## Быстрый старт (локально, JVM)
Требования:
- Java 21+ (проверялось на Java 25)
- Maven wrapper (`./mvnw` уже в репозитории)

Запуск:
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

`local` профиль создает DuckDB файл `target/graph_local.db`, включает Flyway и накатывает demo seed из `src/main/resources/db/migration`. Это воспроизводимый сценарий для чистого clone: никакой `data/*.duckdb` файл заранее не нужен. По умолчанию активен `DuckPGQ`.

Если нужен внешний FinBench dataset, используйте отдельный профиль:
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=finbench
```

Профиль `finbench` ожидает уже подготовленный `./data/finbench_sf0_1.duckdb` и выключает Flyway, потому что база должна быть предзагружена. Подготовка FinBench описана в `docs/finbench.md`; это не обязательный path для demo/smoke.

## Быстрый старт (Docker Compose)
```bash
docker compose up --build -d
```

Docker по умолчанию использует FinBench DuckDB-файл из локальной папки `./data`:
`./data/finbench_sf0_1.duckdb` монтируется в контейнер как `/data/graph_api/finbench_sf0_1.duckdb`.
Flyway в Docker-сценарии выключен, потому что FinBench-база должна быть уже подготовлена.

Имя файла можно переопределить через переменную `GRAPH_API_DUCKDB_FILE`, например:
```bash
GRAPH_API_DUCKDB_FILE=finbench_smoke.duckdb docker compose up --build -d
```

Если порт `8080` занят, поднимите API на другом host-порту:
```bash
GRAPH_API_PORT=18080 docker compose up --build -d
```

Если файл отсутствует, контейнер завершится с ошибкой вместо создания пустой DuckDB. Подготовка FinBench описана в `docs/finbench.md`.
На Windows можно подготовить базу через PowerShell/WSL-обертку:
```powershell
.\scripts\finbench-data.ps1 -DbPath data\finbench_sf0_1.duckdb
```

Если после изменения seed-данных frontend видит старую базу от предыдущего Docker volume-сценария, удалите старый volume:
```bash
docker compose down -v
```

Поднять Neo4j для альтернативного backend-а:
```bash
docker compose --profile neo4j up -d neo4j
```

Остановить:
```bash
docker compose down
```

## Документация API и наблюдаемость
- Swagger UI: `http://localhost:8080/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8080/api-docs`
- Health: `http://localhost:8080/actuator/health`
- Prometheus: `http://localhost:8080/actuator/prometheus`
- Статический контракт: `src/main/resources/openapi/graph-api-v1.yaml`

## Smoke-проверка
```bash
make smoke
```
или
```bash
./scripts/smoke.sh
```

Smoke рассчитан на demo seed из Flyway (`PARTY_1001`, `N_PARTY_1001`, `ACCOUNT_FLOW`, `CORPORATE_CONTROL`) и проверяет health, dictionary, expand, shortest-path и export CSV против уже запущенного приложения. Для нестандартного порта передайте base URL первым аргументом:
```bash
./scripts/smoke.sh http://localhost:18080
```

## Матрица соответствия ТЗ
Краткая матрица backend/frontend ответственности вынесена в `docs/customer-requirements-matrix.md`.

## Основание demo taxonomy
Demo node/edge types не являются production-онтологией заказчика. Они используются для воспроизводимого smoke/demo seed и обоснованы публичной финансовой graph-моделью LDBC FinBench. Mapping и источники вынесены в `docs/domain-taxonomy-basis.md`.

## Benchmarking СУБД
Основной research runner сравнивает backend-и по одному workload-у и пишет воспроизводимый отчет:
```bash
BENCH_PREPARE_DATA=true BENCH_SCALE=serious BENCH_REQUESTS=300 BENCH_CONCURRENCY=8 make bench-suite
```

Backend-и подключаются декларативно через `bench/backends/*.toml`, workload-и через `bench/workloads/*.toml`. Новый backend добавляется реализацией `GraphQueryBackend` и отдельным TOML-файлом, без правки benchmark runner-а.

Список зарегистрированных СУБД и статус adapter-а:
```bash
./scripts/bench-suite.sh --list-backends
```

Быстрый curl-based smoke benchmark против уже запущенного приложения:
```bash
make bench
```

Настройки quick benchmark:
```bash
BENCH_REQUESTS=300 BENCH_CONCURRENCY=8 BENCH_WARMUP=20 ./scripts/bench.sh http://localhost:8080
```

Подробно: `docs/benchmarking.md`.

## Примеры запросов
Базовый URL:
```bash
BASE="http://localhost:8080/api/v1"
```

Готовая HTTP-коллекция для IntelliJ/VS Code REST Client:
- `docs/requests.http`

Expand:
```bash
curl -s -X POST "$BASE/graph/expand" \
  -H "Content-Type: application/json" \
  -d '{
    "seeds":[{"type":"PARTY_RK","value":"PARTY_1001"}],
    "relationFamily":"PERSON_KNOWS_PERSON",
    "direction":"OUTBOUND",
    "maxNeighborsPerSeed":5,
    "maxNodes":100,
    "maxEdges":150,
    "includeAttributes":true
  }'
```

Expand by account seed:
```bash
curl -s -X POST "$BASE/graph/expand" \
  -H "Content-Type: application/json" \
  -d '{
    "seeds":[{"type":"ACCOUNT_NO","value":"40817810000000002001"}],
    "relationFamily":"ACCOUNT_FLOW",
    "direction":"OUTBOUND",
    "maxNeighborsPerSeed":5,
    "maxNodes":100,
    "maxEdges":150,
    "includeAttributes":true
  }'
```

Shortest path:
```bash
curl -s -X POST "$BASE/graph/shortest-path" \
  -H "Content-Type: application/json" \
  -d '{
    "source":{"type":"PARTY_RK","value":"PARTY_1001"},
    "target":{"type":"PARTY_RK","value":"PARTY_1003"},
    "relationFamily":"PERSON_KNOWS_PERSON",
    "direction":"OUTBOUND",
    "maxDepth":4
  }'
```

Shortest path to company by tax id:
```bash
curl -s -X POST "$BASE/graph/shortest-path" \
  -H "Content-Type: application/json" \
  -d '{
    "source":{"type":"PARTY_RK","value":"PARTY_1001"},
    "target":{"type":"TAX_ID","value":"7701234567"},
    "relationFamily":"CORPORATE_CONTROL",
    "direction":"OUTBOUND",
    "maxDepth":2
  }'
```

Dictionary:
```bash
curl -s "$BASE/graph/dictionary"
```

Node summary before expand:
```bash
curl -s "$BASE/graph/node-summary?nodeId=N_PARTY_1001"
```

Node summary for a filtered expand preview:
```bash
curl -s "$BASE/graph/node-summary?nodeId=N_PARTY_1001&relationFamily=CUSTOMER_OWNERSHIP&direction=OUTBOUND"
```

Search nodes for a manual anchor/seed:
```bash
curl -s "$BASE/graph/nodes/search?query=Alice&nodeType=PERSON&limit=10&includeAttributes=true"
```

Start investigation from SQL seed query:
```bash
curl -s -X POST "$BASE/graph/query" \
  -H "Content-Type: application/json" \
  -d '{
    "sql":"select node_id from g_nodes where is_blacklist = true",
    "resultMode":"SEEDS",
    "relationFamily":"ALL_RELATIONS",
    "direction":"BOTH",
    "maxNeighborsPerSeed":25,
    "maxNodes":200,
    "maxEdges":300,
    "includeAttributes":true
  }'
```

Return graph slice from SQL edge query:
```bash
curl -s -X POST "$BASE/graph/query" \
  -H "Content-Type: application/json" \
  -d '{
    "sql":"select edge_id from g_edges where tx_sum > 100000",
    "resultMode":"GRAPH",
    "maxNodes":200,
    "maxEdges":300,
    "includeAttributes":true
  }'
```

Import CSV preview:
```bash
curl -s -X POST "$BASE/graph/import/preview" \
  -F "file=@graph-import.csv"
```

Import CSV commit:
```bash
curl -s -X POST "$BASE/graph/import/commit" \
  -F "file=@graph-import.csv"
```

Минимальный CSV может содержать и ноды, и связи в одном файле:
```csv
record_type,node_id,node_type,display_name,party_rk,account_no,from_node_id,to_node_id,edge_id,edge_type,relation_family,directed
NODE,N_IMPORT_1,PERSON,Imported Customer,PARTY_IMPORT_1,,,,,,,
NODE,N_IMPORT_2,ACCOUNT,Imported Account,,40817810000000999999,,,,,,
EDGE,,,,,,N_IMPORT_1,N_IMPORT_2,E_IMPORT_1,OWNS,CUSTOMER_OWNERSHIP,true
```

Поддерживаемые node-колонки: `node_id`/`id`, `node_type`/`entity_type`, `display_name`/`name`, `party_rk`, `person_id`, `phone_no`/`phone`, `full_name`, `is_blacklist`, `is_vip`, `employer`, `city`, `source_system`, `pagerank_score`, `hub_score`, `attrs_json`, а также `identifier_*`.

Поддерживаемые edge-колонки: `edge_id`, `from_node_id`/`source`/`from`, `to_node_id`/`target`/`to`, `edge_type`/`type`/`relation`, `relation_family`, `directed`, `tx_count`, `tx_sum`, `strength_score`, `evidence_count`, `source_system`, `first_seen_at`, `last_seen_at`, `attrs_json`.

CSV import валидирует непустые numeric/date поля. Например, `pagerank_score=not-a-number`, `tx_count=not-a-long`, `first_seen_at=not-an-instant` вернут errors с `rowNumber`, `field`, `value` и не будут молча превращены в `0`/`null`. Пустые optional-поля остаются допустимыми.

Export NDJSON:
```bash
curl -s -X POST "$BASE/graph/export?format=NDJSON" \
  -H "Content-Type: application/json" \
  -d '{
    "nodes":[{"nodeId":"N1","displayName":"Node 1"}],
    "edges":[]
  }'
```

Export CSV:
```bash
curl -s -X POST "$BASE/graph/export?format=CSV" \
  -H "Content-Type: application/json" \
  -d '{
    "nodes":[{"nodeId":"N1","displayName":"Node 1"}],
    "edges":[]
  }'
```

## Режимы DuckPGQ
Основные env-флаги:
- `GRAPH_QUERY_BACKEND=DUCKPGQ` для основного demo path; `NEO4J`, `MEMGRAPH`, `POSTGRES_AGE`, `ARANGODB`, `JANUSGRAPH`, `KUZU` доступны как experimental adapter values
- `GRAPH_DUCKPGQ_ENABLED=true|false`
- `GRAPH_DUCKPGQ_AUTO_LOAD=true|false`
- `GRAPH_DUCKPGQ_SYNC_GRAPH_STATE_ON_STARTUP=true|false`

Поведение:
- `enabled=true, auto-load=true` - backend поднимает projection tables и property graphs на старте
- `sync-graph-state-on-startup=false` - extension загружается, но projection tables и property graphs не пересобираются автоматически
- если активен `DUCKPGQ` и `duckpgq` недоступен, приложение падает при старте

## Статус graphDB backend-ов

| backend | текущий статус |
| --- | --- |
| DuckDB + DuckPGQ | основной demo/MVP backend, покрыт integration smoke/test path |
| Neo4j | experimental adapter: есть код и unit-level coverage, production-ready поддержка не заявляется |
| Memgraph | experimental/unverified adapter для benchmark-кандидата |
| Kuzu | experimental/unverified adapter для benchmark-кандидата |
| PostgreSQL + Apache AGE | experimental/unverified adapter для benchmark-кандидата |
| ArangoDB | experimental/unverified adapter для benchmark-кандидата |
| JanusGraph | experimental/unverified adapter для benchmark-кандидата |

`bench/backends/*.toml` регистрируют кандидатов для исследования и не означают production-ready поддержку всех СУБД. Перед демонстрацией или защитой конкретного backend-а нужно отдельно прогнать его compose/service setup, projection sync, smoke и workload.

## Режимы Neo4j
Основные env-флаги:
- `GRAPH_QUERY_BACKEND=NEO4J`
- `GRAPH_NEO4J_URI=bolt://localhost:7687`
- `GRAPH_NEO4J_USERNAME=neo4j`
- `GRAPH_NEO4J_PASSWORD=graph-api-password`
- `GRAPH_NEO4J_DATABASE=neo4j`
- `GRAPH_NEO4J_SYNC_GRAPH_STATE_ON_STARTUP=true|false`
- `GRAPH_NEO4J_CLEAR_PROJECTION_ON_STARTUP=true|false`

Поведение:
- Neo4j используется как graph query backend, а canonical `g_nodes/g_edges/g_identifiers` по-прежнему живут в DuckDB
- при `sync-graph-state-on-startup=true` backend на старте пересобирает projection graph в Neo4j из текущих данных DuckDB
- по умолчанию sync делает upsert projection-узлов/ребер без массового удаления; `clear-projection-on-startup=true` удаляет только projection-узлы с owner marker `graph_api_v2`
- API-контракт не меняется, но `meta.source` становится `NEO4J`

## Production profile
Для прода запускайте с профилем `prod` и задавайте секреты/разрешенные origin-ы явно:

```bash
SPRING_PROFILES_ACTIVE=prod \
GRAPH_CORS_ALLOWED_ORIGINS=https://app.example.com \
GRAPH_NEO4J_PASSWORD=... \
java -jar app.jar
```

В `prod` профиле Swagger/OpenAPI выключены по умолчанию, health details скрыты, unsigned DuckDB extensions запрещены по умолчанию, а Neo4j startup sync выключен до явного `GRAPH_NEO4J_SYNC_GRAPH_STATE_ON_STARTUP=true`.

## Для фронта и ML-команды
- Основной merge-friendly формат: `nodes[]`, `edges[]`, `meta`
- Для ручной опорной ноды фронт может дергать `GET /graph/nodes/search?query=...`, показывать найденные `nodes[]`, а выбранный результат передавать в `expand` как seed `{ "type": "NODE_ID", "value": nodeId }`
- Для сценария `Start from query` фронт может дергать `POST /graph/query`: `SEEDS` ожидает SQL с `node_id` и затем расширяет найденные seed-ноды, `GRAPH` ожидает `node_id`, `edge_id` или `source`/`target` и возвращает готовый срез графа
- Для сценария `Start from file` фронт загружает CSV в `POST /graph/import/preview`, показывает counts/errors, затем по подтверждению пользователя отправляет тот же файл в `POST /graph/import/commit`
- Перед `expand` можно дергать `GET /graph/node-summary?nodeId=...` и показывать пользователю сводку по клику на узел
- `node-summary` возвращает общие counts по соседям, разбивку по `relationFamilies`, `edgeTypes`, `neighborNodeTypes` и признак, урежет ли узел дефолтный budget expand-а
- `nodes[]` теперь могут нести `nodeType` и generic `identifiers`
- `edges[]` теперь могут нести `relationFamily`, `sourceSystem`, `firstSeenAt`, `lastSeenAt`
- `meta.source` приходит от активного backend-а: `DUCKPGQ` или `NEO4J`
- `meta.relationFamily`, `meta.rankingStrategy`, `meta.candidateEdgeCount`, `meta.warnings` объясняют, как backend сузил результат
- `expand` больше не принимает `existingGraph`: фронт сам досклеивает граф по стабильным `nodeId` и `edgeId`
- JSON/CSV/NDJSON export - backend responsibility; интерактивный HTML export - frontend responsibility, потому что он зависит от layout, pinning, hover, hide nodes и merge UI
- Контрактные заглушки интеграции: `src/main/java/com/pm/graph_api_v2/integration`

## Known gaps / out of scope для дипломного demo
- Security/auth, multi-tenant authorization и audit trail не реализованы; это production gap, а не часть demo backend scope.
- Интерактивный HTML export, layout gravity, pinning, hover, hide nodes и визуальный merge UI реализуются на frontend.
- Experimental graphDB adapters требуют отдельной проверки перед заявлением production-ready поддержки.

## Команды для разработки
```bash
make test
make run
make up
make logs
make down
```
