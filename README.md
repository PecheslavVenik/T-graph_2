# Graph API v2 (Операционный анализ)

Stateless REST API для графовой аналитики на DuckDB с приоритетом DuckPGQ и SQL fallback.

## Что умеет API
- `POST /api/v1/graph/expand` - расширение графа на 1 hop от одного или нескольких seed
- `POST /api/v1/graph/shortest-path` - кратчайший путь (minimum hops)
- `GET /api/v1/graph/dictionary` - справочник типов связей/статусов для легенды фронта
- `POST /api/v1/graph/export?format=JSON|CSV` - экспорт графа, который фронт уже собрал
- Стабильные `nodeId`/`edgeId` для merge на фронте
- Метрики и health endpoints (`/actuator/*`)

## Архитектура (кратко)
- `controller` - HTTP слой
- `service` - оркестрация, лимиты, fallback policy, merge логика
- `repository`
  - `GraphRepository` - резолв идентификаторов + получение узлов/ребер
  - `DuckPgqGraphQueryRepository` - DuckPGQ graph-запросы
  - `SqlGraphQueryRepository` - SQL fallback graph-запросы
- `db/migration` - миграции Flyway (схема + seed)

## Быстрый старт (локально, JVM)
Требования:
- Java 21+ (проверялось на Java 25)
- Maven wrapper (`./mvnw` уже в репозитории)

Запуск:
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Локальный профиль по умолчанию работает через SQL fallback (`graph.duckpgq.force-fallback=true`), чтобы не блокироваться на extension в dev-окружении.

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
    "direction":"OUTBOUND",
    "maxNeighborsPerSeed":10,
    "maxNodes":100,
    "maxEdges":100,
    "includeAttributes":true
  }'
```

Shortest path:
```bash
curl -s -X POST "$BASE/graph/shortest-path" \
  -H "Content-Type: application/json" \
  -d '{
    "source":{"type":"PARTY_RK","value":"PARTY_1001"},
    "target":{"type":"PARTY_RK","value":"PARTY_1004"},
    "direction":"OUTBOUND",
    "maxDepth":4
  }'
```

Dictionary:
```bash
curl -s "$BASE/graph/dictionary"
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
- `GRAPH_DUCKPGQ_FORCE_FALLBACK=true|false`
- `GRAPH_DUCKPGQ_AUTO_LOAD=true|false`
- `GRAPH_DUCKPGQ_FAIL_ON_UNAVAILABLE=true|false`

Поведение:
- по умолчанию: попытка DuckPGQ, при проблемах fallback в SQL
- strict mode (`FAIL_ON_UNAVAILABLE=true`): вместо fallback возвращается `503`

## Для фронта и ML-команды
- Основной merge-friendly формат: `nodes[]`, `edges[]`, `meta`
- `meta.source` показывает реальный движок: `DUCKPGQ` или `SQL_FALLBACK`
- `expand` поддерживает `existingGraph` для слияния текущего графа фронта с новым серверным результатом
- Контрактные заглушки интеграции: `src/main/java/com/pm/graph_api_v2/integration`

## Команды для разработки
```bash
make test
make run
make up
make logs
make down
```
