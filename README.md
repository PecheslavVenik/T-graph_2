# Graph API v2 (Операционный анализ)

Stateless REST API для расследовательской графовой аналитики на DuckDB и DuckPGQ.

## Что умеет API
- `POST /api/v1/graph/expand` - умное 1-hop расширение для расследовательского графа с анти-hub ранжированием
- `POST /api/v1/graph/shortest-path` - кратчайший путь (minimum hops) внутри выбранного relation family
- `GET /api/v1/graph/dictionary` - справочник типов связей/статусов для легенды фронта
- `POST /api/v1/graph/export?format=JSON|CSV|NDJSON` - экспорт графа, который фронт уже собрал
- Стабильные `nodeId`/`edgeId` для merge на фронте
- Метрики и health endpoints (`/actuator/*`)

## Доменный фокус MVP
- Базовый расследовательский сценарий по умолчанию: `PERSON_KNOWS_PERSON`
- Следующие relation family уже поддержаны контрактом и seed-данными: `PERSON_RELATIVE_PERSON`, `PERSON_SAME_CITY_PERSON`
- Backend сам ограничивает первый экран графа: candidate budget, top-K по seed, global node/edge budget, hub suppression

## Архитектура (кратко)
- `GraphController` - HTTP слой
- `InvestigationService` - оркестрация расследовательских сценариев, ранжирование и budget-лимиты
- `GraphRepository` - резолв идентификаторов и загрузка узлов/ребер
- `DuckPgqGraphQueryRepository` - DuckPGQ-only graph-запросы
- `db/migration` - миграции Flyway (схема + seed)

## Быстрый старт (локально, JVM)
Требования:
- Java 21+ (проверялось на Java 25)
- Maven wrapper (`./mvnw` уже в репозитории)

Запуск:
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

DuckPGQ обязателен. При старте приложение сначала делает `LOAD duckpgq`, а если extension еще не установлена, пытается выполнить `INSTALL duckpgq` и повторный `LOAD`.

## Быстрый старт (Docker Compose)
```bash
docker compose up --build -d
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

Dictionary:
```bash
curl -s "$BASE/graph/dictionary"
```

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
- `GRAPH_DUCKPGQ_ENABLED=true|false`
- `GRAPH_DUCKPGQ_AUTO_LOAD=true|false`

Поведение:
- `enabled=true, auto-load=true` - backend поднимает projection tables и property graphs на старте
- если `duckpgq` недоступен, приложение падает при старте: это DuckPGQ-only backend без fallback

## Для фронта и ML-команды
- Основной merge-friendly формат: `nodes[]`, `edges[]`, `meta`
- `meta.source` всегда `DUCKPGQ`
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
