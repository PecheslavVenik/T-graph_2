# Матрица соответствия ТЗ заказчика

| Требование заказчика | Ответственность backend/frontend | Backend endpoint/модуль | Статус |
| --- | --- | --- | --- |
| 1-hop исследование по `id` / `phone_no` / `party_rk` | Backend резолвит seed identifiers и возвращает bounded graph slice; frontend выбирает seed и отображает результат | `POST /api/v1/graph/expand`, `SeedRef`, `GraphRequestNormalizer`, `GraphExpansionService`, `GraphNodeRepository` | Поддержано для `NODE_ID`/aliases, `PHONE_NO`, `PARTY_RK` и других identifier types из `g_identifiers` |
| Полный вывод всей graph database для frontend canvas | Backend возвращает все `g_nodes` и все `g_edges` без seed-ов, фильтров и лимитов; frontend мержит/рендерит по stable IDs | `GET /api/v1/graph/full`, `GraphExpansionService`, `GraphNodeRepository`, `GraphEdgeRepository` | Поддержано |
| Фильтры по связям | Backend применяет `relationFamily`, `edgeTypes`, `direction`; frontend дает controls и показывает active filters | `GraphExpandRequest`, `GraphPathService`, `GraphQueryBackend.findExpandEdges` | Поддержано |
| Shortest path | Backend ищет bounded minimum-hop path; frontend задает source/target/maxDepth и визуализирует путь | `POST /api/v1/graph/shortest-path`, `GraphPathService`, `GraphBackendPathSearch` | Поддержано |
| Stable `nodeId`/`edgeId` для frontend merge | Backend возвращает стабильные идентификаторы и генерирует deterministic edge id при CSV import без `edge_id`; frontend делает merge/dedupe по этим id | `GraphNodeDto`, `GraphEdgeDto`, `StableIdUtil`, `GraphImportService` | Поддержано |
| Dictionary/legend data | Backend отдает списки типов/статусов/style hints; frontend строит legend и визуальные стили | `GET /api/v1/graph/dictionary`, `GraphDictionaryService`, `GraphDictionaryRepository` | Поддержано |
| Node details для hover/list | Backend отдает node summary, facets и expand preview; frontend показывает hover/list panels | `GET /api/v1/graph/node-summary`, `GET /api/v1/graph/nodes/search`, `GraphNodeReadService` | Поддержано |
| Export JSON/CSV/NDJSON | Backend сериализует уже собранный frontend graph payload; frontend передает выбранный граф | `POST /api/v1/graph/export?format=JSON|CSV|NDJSON`, `GraphExportService` | Поддержано |
| Интерактивный HTML export | Frontend responsibility: HTML зависит от layout, pinning, hover, hide nodes и merge UI. Backend только предоставляет graph data и stable IDs | Нет backend endpoint; использовать `expand`, `shortest-path`, `query`, `dictionary`, `node-summary`, `export` для данных | Out of backend scope, задокументировано |
| Layout gravity / pinning / hide nodes | Frontend responsibility: это состояние и интерактивное поведение визуализации | Backend endpoints возвращают данные без layout state | Out of backend scope, задокументировано |

## Production gaps

- Security/auth, authorization by customer/user and audit trail are out of scope for the diploma demo backend.
- DuckDB + DuckPGQ is the primary verified demo backend. Other graphDB adapters are R&D/benchmark candidates until their own smoke/workload evidence is produced.
