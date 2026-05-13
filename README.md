# Graph API v2 (Операционный анализ)

Stateless REST API для расследовательской графовой аналитики на DuckDB с переключаемым graph query backend.
Сейчас поддерживаются `DuckPGQ` и `Neo4j`.
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
- `GraphRepository` - резолв идентификаторов и загрузка узлов/ребер
- `DuckPgqGraphQueryRepository` - текущая DuckPGQ-реализация `GraphQueryBackend`
- `Neo4jGraphQueryBackend` - Neo4j-реализация `GraphQueryBackend`
- `db/migration` - миграции Flyway (схема + seed)

## Быстрый старт (локально, JVM)
Требования:
- Java 21+ (проверялось на Java 25)
- Maven wrapper (`./mvnw` уже в репозитории)

Запуск:
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

`local` профиль подключает большую FinBench DuckDB базу `./data/finbench_sf0_1.duckdb` и выключает Flyway, потому что эта база уже предзагружена. По умолчанию активен `DuckPGQ`. Его можно переключить на `Neo4j` через `GRAPH_QUERY_BACKEND=NEO4J`.

## Быстрый старт (Docker Compose)
```bash
docker compose up --build -d
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
- `GRAPH_QUERY_BACKEND=DUCKPGQ|NEO4J`
- `GRAPH_DUCKPGQ_ENABLED=true|false`
- `GRAPH_DUCKPGQ_AUTO_LOAD=true|false`
- `GRAPH_DUCKPGQ_SYNC_GRAPH_STATE_ON_STARTUP=true|false`

Поведение:
- `enabled=true, auto-load=true` - backend поднимает projection tables и property graphs на старте
- `sync-graph-state-on-startup=false` - extension загружается, но projection tables и property graphs не пересобираются автоматически
- если активен `DUCKPGQ` и `duckpgq` недоступен, приложение падает при старте

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
- Контрактные заглушки интеграции: `src/main/java/com/pm/graph_api_v2/integration`

## Команды для разработки
```bash
make test
make run
make up
make logs
make down
```
