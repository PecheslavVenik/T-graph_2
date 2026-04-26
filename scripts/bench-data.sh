#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${BENCH_DB:-data/graph_bench.duckdb}"
SCALE="${BENCH_SCALE:-serious}"
THREADS="${BENCH_DUCKDB_THREADS:-4}"
FORCE_MIGRATIONS="${BENCH_FORCE_MIGRATIONS:-false}"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI is required. Install DuckDB or set PATH so 'duckdb' is available." >&2
  exit 1
fi

case "$SCALE" in
  small)
    PERSONS="${BENCH_PERSONS:-5000}"
    ACCOUNTS="${BENCH_ACCOUNTS:-10000}"
    COMPANIES="${BENCH_COMPANIES:-500}"
    DEVICES="${BENCH_DEVICES:-2000}"
    ACCOUNT_FLOW_EDGES="${BENCH_ACCOUNT_FLOW_EDGES:-50000}"
    PERSON_KNOWS_EDGES="${BENCH_PERSON_KNOWS_EDGES:-20000}"
    SHARED_INFRA_EDGES="${BENCH_SHARED_INFRA_EDGES:-10000}"
    CORPORATE_EDGES="${BENCH_CORPORATE_EDGES:-2000}"
    HUB_ACCOUNT_EDGES="${BENCH_HUB_ACCOUNT_EDGES:-1000}"
    HUB_PERSON_EDGES="${BENCH_HUB_PERSON_EDGES:-500}"
    ;;
  serious)
    PERSONS="${BENCH_PERSONS:-50000}"
    ACCOUNTS="${BENCH_ACCOUNTS:-100000}"
    COMPANIES="${BENCH_COMPANIES:-5000}"
    DEVICES="${BENCH_DEVICES:-20000}"
    ACCOUNT_FLOW_EDGES="${BENCH_ACCOUNT_FLOW_EDGES:-1000000}"
    PERSON_KNOWS_EDGES="${BENCH_PERSON_KNOWS_EDGES:-250000}"
    SHARED_INFRA_EDGES="${BENCH_SHARED_INFRA_EDGES:-200000}"
    CORPORATE_EDGES="${BENCH_CORPORATE_EDGES:-50000}"
    HUB_ACCOUNT_EDGES="${BENCH_HUB_ACCOUNT_EDGES:-10000}"
    HUB_PERSON_EDGES="${BENCH_HUB_PERSON_EDGES:-5000}"
    ;;
  stress)
    PERSONS="${BENCH_PERSONS:-250000}"
    ACCOUNTS="${BENCH_ACCOUNTS:-500000}"
    COMPANIES="${BENCH_COMPANIES:-25000}"
    DEVICES="${BENCH_DEVICES:-100000}"
    ACCOUNT_FLOW_EDGES="${BENCH_ACCOUNT_FLOW_EDGES:-5000000}"
    PERSON_KNOWS_EDGES="${BENCH_PERSON_KNOWS_EDGES:-1000000}"
    SHARED_INFRA_EDGES="${BENCH_SHARED_INFRA_EDGES:-1000000}"
    CORPORATE_EDGES="${BENCH_CORPORATE_EDGES:-250000}"
    HUB_ACCOUNT_EDGES="${BENCH_HUB_ACCOUNT_EDGES:-50000}"
    HUB_PERSON_EDGES="${BENCH_HUB_PERSON_EDGES:-25000}"
    ;;
  custom)
    PERSONS="${BENCH_PERSONS:?BENCH_PERSONS is required for custom scale}"
    ACCOUNTS="${BENCH_ACCOUNTS:?BENCH_ACCOUNTS is required for custom scale}"
    COMPANIES="${BENCH_COMPANIES:?BENCH_COMPANIES is required for custom scale}"
    DEVICES="${BENCH_DEVICES:?BENCH_DEVICES is required for custom scale}"
    ACCOUNT_FLOW_EDGES="${BENCH_ACCOUNT_FLOW_EDGES:?BENCH_ACCOUNT_FLOW_EDGES is required for custom scale}"
    PERSON_KNOWS_EDGES="${BENCH_PERSON_KNOWS_EDGES:?BENCH_PERSON_KNOWS_EDGES is required for custom scale}"
    SHARED_INFRA_EDGES="${BENCH_SHARED_INFRA_EDGES:?BENCH_SHARED_INFRA_EDGES is required for custom scale}"
    CORPORATE_EDGES="${BENCH_CORPORATE_EDGES:?BENCH_CORPORATE_EDGES is required for custom scale}"
    HUB_ACCOUNT_EDGES="${BENCH_HUB_ACCOUNT_EDGES:-0}"
    HUB_PERSON_EDGES="${BENCH_HUB_PERSON_EDGES:-0}"
    ;;
  *)
    echo "Unknown BENCH_SCALE '$SCALE'. Use small, serious, stress, or custom." >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "$DB_PATH")"

migrations() {
  find src/main/resources/db/migration -name 'V*.sql' \
    | awk -F'/V|__' '{ print $2 " " $0 }' \
    | sort -n \
    | cut -d' ' -f2-
}

schema_ready() {
  if [ ! -f "$DB_PATH" ]; then
    return 1
  fi

  local ready
  ready="$(
    duckdb "$DB_PATH" -noheader -csv -c "
      SELECT COUNT(*)
      FROM information_schema.tables
      WHERE table_name IN ('g_nodes', 'g_edges', 'g_identifiers');
    " 2>/dev/null | tr -d '[:space:]' || true
  )"
  [ "$ready" = "3" ]
}

echo "Preparing benchmark database: $DB_PATH"
echo "Scale=$SCALE persons=$PERSONS accounts=$ACCOUNTS companies=$COMPANIES devices=$DEVICES"
echo "Edges account_flow=$ACCOUNT_FLOW_EDGES person_knows=$PERSON_KNOWS_EDGES shared_infra=$SHARED_INFRA_EDGES corporate=$CORPORATE_EDGES hub_account=$HUB_ACCOUNT_EDGES hub_person=$HUB_PERSON_EDGES"

if [ "$FORCE_MIGRATIONS" = "true" ] || ! schema_ready; then
  for migration in $(migrations); do
    duckdb "$DB_PATH" < "$migration" >/dev/null
  done
else
  echo "Schema already exists; skipping migrations. Set BENCH_FORCE_MIGRATIONS=true to reapply them."
fi

duckdb "$DB_PATH" <<SQL
PRAGMA threads=${THREADS};

DELETE FROM g_pgq_edges WHERE edge_id LIKE 'E_BENCH_%';
DELETE FROM g_identifiers WHERE node_id LIKE 'N_BENCH_%';
DELETE FROM g_edges WHERE edge_id LIKE 'E_BENCH_%';
DELETE FROM g_nodes WHERE node_id LIKE 'N_BENCH_%';

BEGIN TRANSACTION;

INSERT INTO g_nodes (
    node_id, node_type, display_name, party_rk, person_id, phone_no, full_name,
    is_blacklist, is_vip, employer, city, source_system, pagerank_score, hub_score, attrs_json
)
SELECT
    'N_BENCH_PERSON_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'PERSON',
    'Bench Person ' || CAST(i AS VARCHAR),
    'BENCH_PARTY_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'BENCH_PERSON_ID_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    '+7900' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'Bench Person ' || CAST(i AS VARCHAR),
    i % 997 = 0,
    i % 251 = 0,
    'Employer ' || CAST((i % 200) AS VARCHAR),
    'City ' || CAST((i % 80) AS VARCHAR),
    'bench',
    CASE WHEN i <= 100 THEN 0.95 ELSE 0.05 + ((i % 100) / 1000.0) END,
    CASE WHEN i <= 25 THEN 0.95 ELSE ((i % 100) / 100.0) END,
    '{"segment":"bench_person"}'
FROM range(1, ${PERSONS} + 1) AS t(i);

INSERT INTO g_nodes (
    node_id, node_type, display_name, source_system, pagerank_score, hub_score, attrs_json
)
SELECT
    'N_BENCH_ACC_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'ACCOUNT',
    'Bench Account ' || CAST(i AS VARCHAR),
    'bench',
    CASE WHEN i <= 100 THEN 0.90 ELSE 0.03 + ((i % 100) / 1200.0) END,
    CASE WHEN i <= 50 THEN 0.98 ELSE ((i % 100) / 100.0) END,
    '{"segment":"bench_account"}'
FROM range(1, ${ACCOUNTS} + 1) AS t(i);

INSERT INTO g_nodes (
    node_id, node_type, display_name, source_system, pagerank_score, hub_score, attrs_json
)
SELECT
    'N_BENCH_COMP_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'COMPANY',
    'Bench Company ' || CAST(i AS VARCHAR),
    'bench',
    0.02 + ((i % 50) / 1000.0),
    CASE WHEN i <= 20 THEN 0.90 ELSE ((i % 100) / 120.0) END,
    '{"segment":"bench_company"}'
FROM range(1, ${COMPANIES} + 1) AS t(i);

INSERT INTO g_nodes (
    node_id, node_type, display_name, source_system, pagerank_score, hub_score, attrs_json
)
SELECT
    'N_BENCH_DEV_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'DEVICE',
    'Bench Device ' || CAST(i AS VARCHAR),
    'bench',
    0.01 + ((i % 50) / 2000.0),
    CASE WHEN i <= 100 THEN 0.85 ELSE ((i % 100) / 150.0) END,
    '{"segment":"bench_device"}'
FROM range(1, ${DEVICES} + 1) AS t(i);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'PARTY_RK', party_rk
FROM g_nodes
WHERE source_system = 'bench' AND node_type = 'PERSON';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'PERSON_ID', person_id
FROM g_nodes
WHERE source_system = 'bench' AND node_type = 'PERSON';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT
    node_id,
    'ACCOUNT_NO',
    'BENCH_ACC_' || regexp_extract(node_id, '([0-9]+)$', 1)
FROM g_nodes
WHERE source_system = 'bench' AND node_type = 'ACCOUNT';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT
    node_id,
    'TAX_ID',
    'BENCH_TAX_' || regexp_extract(node_id, '([0-9]+)$', 1)
FROM g_nodes
WHERE source_system = 'bench' AND node_type = 'COMPANY';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT
    node_id,
    'DEVICE_ID',
    'BENCH_DEVICE_' || regexp_extract(node_id, '([0-9]+)$', 1)
FROM g_nodes
WHERE source_system = 'bench' AND node_type = 'DEVICE';

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_OWN_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'N_BENCH_PERSON_' || lpad(CAST(((i - 1) % ${PERSONS}) + 1 AS VARCHAR), 7, '0'),
    'N_BENCH_ACC_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'OWNS',
    TRUE,
    0,
    0,
    'CUSTOMER_OWNERSHIP',
    0.50 + ((i % 100) / 200.0),
    1 + (i % 5),
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"ownership"}'
FROM range(1, ${ACCOUNTS} + 1) AS t(i);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_FLOW_' || lpad(CAST(i AS VARCHAR), 9, '0'),
    'N_BENCH_ACC_' || lpad(CAST(((i - 1) % ${ACCOUNTS}) + 1 AS VARCHAR), 7, '0'),
    'N_BENCH_ACC_' || lpad(CAST(((i * 37) % ${ACCOUNTS}) + 1 AS VARCHAR), 7, '0'),
    'TRANSFERS_TO',
    TRUE,
    1 + (i % 25),
    100.0 + ((i % 10000) * 13.37),
    'ACCOUNT_FLOW',
    0.30 + ((i % 100) / 120.0),
    1 + (i % 10),
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"account_flow"}'
FROM range(1, ${ACCOUNT_FLOW_EDGES} + 1) AS t(i)
WHERE ((i - 1) % ${ACCOUNTS}) <> ((i * 37) % ${ACCOUNTS});

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_KNOWS_' || lpad(CAST(i AS VARCHAR), 9, '0'),
    'N_BENCH_PERSON_' || lpad(CAST(((i - 1) % ${PERSONS}) + 1 AS VARCHAR), 7, '0'),
    'N_BENCH_PERSON_' || lpad(CAST(((i * 17) % ${PERSONS}) + 1 AS VARCHAR), 7, '0'),
    'KNOWS',
    FALSE,
    0,
    0,
    'PERSON_KNOWS_PERSON',
    0.25 + ((i % 100) / 110.0),
    1 + (i % 8),
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"person_knows"}'
FROM range(1, ${PERSON_KNOWS_EDGES} + 1) AS t(i)
WHERE ((i - 1) % ${PERSONS}) <> ((i * 17) % ${PERSONS});

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_SHARED_' || lpad(CAST(i AS VARCHAR), 9, '0'),
    'N_BENCH_PERSON_' || lpad(CAST(((i - 1) % ${PERSONS}) + 1 AS VARCHAR), 7, '0'),
    'N_BENCH_DEV_' || lpad(CAST(((i * 19) % ${DEVICES}) + 1 AS VARCHAR), 7, '0'),
    'USES_DEVICE',
    FALSE,
    0,
    0,
    'SHARED_INFRASTRUCTURE',
    0.20 + ((i % 100) / 130.0),
    1 + (i % 6),
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"shared_infra"}'
FROM range(1, ${SHARED_INFRA_EDGES} + 1) AS t(i);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_CORP_' || lpad(CAST(i AS VARCHAR), 9, '0'),
    'N_BENCH_COMP_' || lpad(CAST(((i - 1) % ${COMPANIES}) + 1 AS VARCHAR), 7, '0'),
    'N_BENCH_COMP_' || lpad(CAST(((i * 13) % ${COMPANIES}) + 1 AS VARCHAR), 7, '0'),
    'CONTROLS',
    TRUE,
    0,
    0,
    'CORPORATE_CONTROL',
    0.35 + ((i % 100) / 140.0),
    1 + (i % 4),
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"corporate"}'
FROM range(1, ${CORPORATE_EDGES} + 1) AS t(i)
WHERE ((i - 1) % ${COMPANIES}) <> ((i * 13) % ${COMPANIES});

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_HUB_ACC_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'N_BENCH_ACC_0000001',
    'N_BENCH_ACC_' || lpad(CAST(((i * 11) % ${ACCOUNTS}) + 1 AS VARCHAR), 7, '0'),
    'TRANSFERS_TO',
    TRUE,
    25,
    50000.0 + i,
    'ACCOUNT_FLOW',
    0.95,
    10,
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"hub_account"}'
FROM range(1, ${HUB_ACCOUNT_EDGES} + 1) AS t(i)
WHERE ((i * 11) % ${ACCOUNTS}) + 1 <> 1;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_BENCH_HUB_PERSON_' || lpad(CAST(i AS VARCHAR), 7, '0'),
    'N_BENCH_PERSON_0000001',
    'N_BENCH_PERSON_' || lpad(CAST(((i * 7) % ${PERSONS}) + 1 AS VARCHAR), 7, '0'),
    'KNOWS',
    FALSE,
    0,
    0,
    'PERSON_KNOWS_PERSON',
    0.90,
    8,
    'bench',
    TIMESTAMP '2026-01-01 00:00:00',
    TIMESTAMP '2026-04-01 00:00:00',
    '{"generator":"bench","edge_group":"hub_person"}'
FROM range(1, ${HUB_PERSON_EDGES} + 1) AS t(i)
WHERE ((i * 7) % ${PERSONS}) + 1 <> 1;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
VALUES
    ('E_BENCH_PATH_1_2', 'N_BENCH_PERSON_0000001', 'N_BENCH_PERSON_0000002', 'KNOWS', FALSE, 0, 0, 'PERSON_KNOWS_PERSON', 0.99, 10, 'bench', TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-04-01 00:00:00', '{"generator":"bench","edge_group":"path"}'),
    ('E_BENCH_PATH_2_3', 'N_BENCH_PERSON_0000002', 'N_BENCH_PERSON_0000003', 'KNOWS', FALSE, 0, 0, 'PERSON_KNOWS_PERSON', 0.99, 10, 'bench', TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-04-01 00:00:00', '{"generator":"bench","edge_group":"path"}'),
    ('E_BENCH_PATH_3_4', 'N_BENCH_PERSON_0000003', 'N_BENCH_PERSON_0000004', 'KNOWS', FALSE, 0, 0, 'PERSON_KNOWS_PERSON', 0.99, 10, 'bench', TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-04-01 00:00:00', '{"generator":"bench","edge_group":"path"}');

COMMIT;

ANALYZE;

SELECT 'bench_nodes' AS metric, COUNT(*) AS value FROM g_nodes WHERE source_system = 'bench'
UNION ALL
SELECT 'bench_edges', COUNT(*) FROM g_edges WHERE source_system = 'bench'
UNION ALL
SELECT 'bench_identifiers', COUNT(*) FROM g_identifiers WHERE node_id LIKE 'N_BENCH_%';
SQL

echo
echo "Benchmark seed values:"
echo "  BENCH_PARTY_RK=BENCH_PARTY_0000001"
echo "  BENCH_TARGET_PARTY_RK=BENCH_PARTY_0000004"
echo "  BENCH_ACCOUNT_NO=BENCH_ACC_0000001"
echo "  BENCH_PATH_RELATION_FAMILY=PERSON_KNOWS_PERSON"
