#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${BENCH_DB:-data/finbench_sf1.duckdb}"
SCALE="${BENCH_SCALE:-sf1}"
THREADS="${BENCH_DUCKDB_THREADS:-4}"
FORCE_MIGRATIONS="${BENCH_FORCE_MIGRATIONS:-false}"
ARCHIVE="${FINBENCH_ARCHIVE:-bench/sf1.tar}"
EXTRACT_ROOT="${FINBENCH_EXTRACT_ROOT:-target/finbench}"
DATASET_DIR="${FINBENCH_DATASET_DIR:-$EXTRACT_ROOT/sf1}"
SEED_FILE="${FINBENCH_SEED_FILE:-target/bench-seeds/finbench.json}"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI is required. Install DuckDB or set PATH so 'duckdb' is available." >&2
  exit 1
fi

if [ ! -f "$ARCHIVE" ] && [ ! -d "$DATASET_DIR/raw" ]; then
  echo "FinBench archive not found: $ARCHIVE" >&2
  echo "Put the official LDBC FinBench archive at bench/sf1.tar or set FINBENCH_ARCHIVE." >&2
  exit 1
fi

case "$SCALE" in
  smoke|small)
    PERSON_LIMIT="${FINBENCH_PERSON_LIMIT:-5000}"
    ACCOUNT_LIMIT="${FINBENCH_ACCOUNT_LIMIT:-10000}"
    COMPANY_LIMIT="${FINBENCH_COMPANY_LIMIT:-1000}"
    LOAN_LIMIT="${FINBENCH_LOAN_LIMIT:-3000}"
    MEDIUM_LIMIT="${FINBENCH_MEDIUM_LIMIT:-3000}"
    EDGE_LIMIT="${FINBENCH_EDGE_LIMIT:-25000}"
    ;;
  sf1|serious)
    PERSON_LIMIT="${FINBENCH_PERSON_LIMIT:-0}"
    ACCOUNT_LIMIT="${FINBENCH_ACCOUNT_LIMIT:-0}"
    COMPANY_LIMIT="${FINBENCH_COMPANY_LIMIT:-0}"
    LOAN_LIMIT="${FINBENCH_LOAN_LIMIT:-0}"
    MEDIUM_LIMIT="${FINBENCH_MEDIUM_LIMIT:-0}"
    EDGE_LIMIT="${FINBENCH_EDGE_LIMIT:-0}"
    ;;
  custom)
    PERSON_LIMIT="${FINBENCH_PERSON_LIMIT:-0}"
    ACCOUNT_LIMIT="${FINBENCH_ACCOUNT_LIMIT:-0}"
    COMPANY_LIMIT="${FINBENCH_COMPANY_LIMIT:-0}"
    LOAN_LIMIT="${FINBENCH_LOAN_LIMIT:-0}"
    MEDIUM_LIMIT="${FINBENCH_MEDIUM_LIMIT:-0}"
    EDGE_LIMIT="${FINBENCH_EDGE_LIMIT:-0}"
    ;;
  *)
    echo "Unknown BENCH_SCALE '$SCALE'. Use smoke, small, sf1, serious, or custom." >&2
    exit 1
    ;;
esac

limit_clause() {
  local value="$1"
  if [ "$value" = "0" ]; then
    printf ''
  else
    printf 'LIMIT %s' "$value"
  fi
}

PERSON_LIMIT_SQL="$(limit_clause "$PERSON_LIMIT")"
ACCOUNT_LIMIT_SQL="$(limit_clause "$ACCOUNT_LIMIT")"
COMPANY_LIMIT_SQL="$(limit_clause "$COMPANY_LIMIT")"
LOAN_LIMIT_SQL="$(limit_clause "$LOAN_LIMIT")"
MEDIUM_LIMIT_SQL="$(limit_clause "$MEDIUM_LIMIT")"
EDGE_LIMIT_SQL="$(limit_clause "$EDGE_LIMIT")"

mkdir -p "$(dirname "$DB_PATH")" "$EXTRACT_ROOT" "$(dirname "$SEED_FILE")"

RAW_DIR="$DATASET_DIR/raw"
raw_ready() {
  for dir in person account company loan medium personOwnAccount companyOwnAccount transfer loantransfer deposit repay signIn personApplyLoan companyApplyLoan personGuarantee companyGuarantee personInvest companyInvest; do
    [ -f "$RAW_DIR/$dir/_SUCCESS" ] || return 1
  done
}

if ! raw_ready; then
  echo "Extracting FinBench archive: $ARCHIVE -> $EXTRACT_ROOT"
  tar --no-same-owner --no-same-permissions -xf "$ARCHIVE" -C "$EXTRACT_ROOT"
fi

if ! raw_ready; then
  echo "FinBench raw directory is incomplete: $RAW_DIR" >&2
  exit 1
fi

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

echo "Preparing FinBench database: $DB_PATH"
echo "Scale=$SCALE archive=$ARCHIVE raw=$RAW_DIR"
echo "Limits persons=$PERSON_LIMIT accounts=$ACCOUNT_LIMIT companies=$COMPANY_LIMIT loans=$LOAN_LIMIT media=$MEDIUM_LIMIT edges_per_relation=$EDGE_LIMIT"

if [ "$FORCE_MIGRATIONS" = "true" ] || ! schema_ready; then
  for migration in $(migrations); do
    duckdb "$DB_PATH" < "$migration" >/dev/null
  done
else
  echo "Schema already exists; skipping migrations. Set BENCH_FORCE_MIGRATIONS=true to reapply them."
fi

duckdb "$DB_PATH" <<SQL
PRAGMA threads=${THREADS};

DELETE FROM g_pgq_edges WHERE edge_id LIKE 'E_FIN_%';
DELETE FROM g_identifiers WHERE node_id LIKE 'FB_%' OR id_value LIKE 'FINBENCH_%';
DELETE FROM g_edges WHERE edge_id LIKE 'E_FIN_%';
DELETE FROM g_nodes WHERE node_id LIKE 'FB_%' OR source_system LIKE 'finbench%';

CREATE OR REPLACE TEMP VIEW fin_person AS
SELECT * FROM read_csv('${RAW_DIR}/person/part-*.csv', delim='|', header=true, all_varchar=true) ${PERSON_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_account AS
SELECT * FROM read_csv('${RAW_DIR}/account/part-*.csv', delim='|', header=true, all_varchar=true) ${ACCOUNT_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_company AS
SELECT * FROM read_csv('${RAW_DIR}/company/part-*.csv', delim='|', header=true, all_varchar=true) ${COMPANY_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_loan AS
SELECT * FROM read_csv('${RAW_DIR}/loan/part-*.csv', delim='|', header=true, all_varchar=true) ${LOAN_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_medium AS
SELECT * FROM read_csv('${RAW_DIR}/medium/part-*.csv', delim='|', header=true, all_varchar=true) ${MEDIUM_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_person_own_account AS
SELECT * FROM read_csv('${RAW_DIR}/personOwnAccount/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_company_own_account AS
SELECT * FROM read_csv('${RAW_DIR}/companyOwnAccount/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_transfer AS
SELECT * FROM read_csv('${RAW_DIR}/transfer/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_loan_transfer AS
SELECT * FROM read_csv('${RAW_DIR}/loantransfer/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_deposit AS
SELECT * FROM read_csv('${RAW_DIR}/deposit/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_repay AS
SELECT * FROM read_csv('${RAW_DIR}/repay/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_signin AS
SELECT * FROM read_csv('${RAW_DIR}/signIn/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_person_apply_loan AS
SELECT * FROM read_csv('${RAW_DIR}/personApplyLoan/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_company_apply_loan AS
SELECT * FROM read_csv('${RAW_DIR}/companyApplyLoan/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_person_guarantee AS
SELECT * FROM read_csv('${RAW_DIR}/personGuarantee/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_company_guarantee AS
SELECT * FROM read_csv('${RAW_DIR}/companyGuarantee/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_person_invest AS
SELECT * FROM read_csv('${RAW_DIR}/personInvest/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

CREATE OR REPLACE TEMP VIEW fin_company_invest AS
SELECT * FROM read_csv('${RAW_DIR}/companyInvest/part-*.csv', delim='|', header=true, all_varchar=true) ${EDGE_LIMIT_SQL};

BEGIN TRANSACTION;

INSERT INTO g_nodes (
    node_id, node_type, display_name, party_rk, person_id, full_name, is_blacklist,
    city, source_system, attrs_json, created_at, updated_at
)
SELECT
    'FB_PERSON_' || id,
    'PERSON',
    COALESCE(NULLIF(name, ''), 'FinBench Person ' || id),
    'FB_PARTY_' || id,
    'FB_PERSON_ID_' || id,
    COALESCE(NULLIF(name, ''), 'FinBench Person ' || id),
    COALESCE(TRY_CAST(isBlocked AS BOOLEAN), FALSE),
    NULLIF(city, ''),
    'finbench',
    CAST(json_object(
        'benchmark', 'LDBC FinBench',
        'entity', 'Person',
        'gender', NULLIF(gender, ''),
        'birthday', NULLIF(birthday, ''),
        'country', NULLIF(country, ''),
        'city', NULLIF(city, '')
    ) AS VARCHAR),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    CURRENT_TIMESTAMP
FROM fin_person;

INSERT INTO g_nodes (
    node_id, node_type, display_name, is_blacklist, city, source_system, attrs_json, created_at, updated_at
)
SELECT
    'FB_COMPANY_' || id,
    'COMPANY',
    COALESCE(NULLIF(name, ''), 'FinBench Company ' || id),
    COALESCE(TRY_CAST(isBlocked AS BOOLEAN), FALSE),
    NULLIF(city, ''),
    'finbench',
    CAST(json_object(
        'benchmark', 'LDBC FinBench',
        'entity', 'Company',
        'country', NULLIF(country, ''),
        'city', NULLIF(city, ''),
        'business', NULLIF(business, ''),
        'description', NULLIF(description, ''),
        'url', NULLIF(url, '')
    ) AS VARCHAR),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    CURRENT_TIMESTAMP
FROM fin_company;

INSERT INTO g_nodes (
    node_id, node_type, display_name, phone_no, is_blacklist, source_system, attrs_json, created_at, updated_at
)
SELECT
    'FB_ACCOUNT_' || id,
    'ACCOUNT',
    COALESCE(NULLIF(nickname, ''), 'FinBench Account ' || id),
    NULLIF(phonenum, ''),
    COALESCE(TRY_CAST(isBlocked AS BOOLEAN), FALSE),
    'finbench',
    CAST(json_object(
        'benchmark', 'LDBC FinBench',
        'entity', 'Account',
        'type', NULLIF(type, ''),
        'email', NULLIF(email, ''),
        'freqLoginType', NULLIF(freqLoginType, ''),
        'lastLoginTime', NULLIF(lastLoginTime, ''),
        'accountLevel', TRY_CAST(accountLevel AS BIGINT),
        'inDegree', TRY_CAST(inDegree AS BIGINT),
        'OutDegree', TRY_CAST(OutDegree AS BIGINT),
        'isExplicitDeleted', TRY_CAST(isExplicitDeleted AS BOOLEAN),
        'Owner', NULLIF(Owner, '')
    ) AS VARCHAR),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    CURRENT_TIMESTAMP
FROM fin_account;

INSERT INTO g_nodes (
    node_id, node_type, display_name, source_system, attrs_json, created_at, updated_at
)
SELECT
    'FB_LOAN_' || id,
    'LOAN',
    'FinBench Loan ' || id,
    'finbench',
    CAST(json_object(
        'benchmark', 'LDBC FinBench',
        'entity', 'Loan',
        'loanAmount', TRY_CAST(loanAmount AS DOUBLE),
        'balance', TRY_CAST(balance AS DOUBLE),
        'usage', NULLIF(usage, ''),
        'interestRate', TRY_CAST(interestRate AS DOUBLE)
    ) AS VARCHAR),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    CURRENT_TIMESTAMP
FROM fin_loan;

INSERT INTO g_nodes (
    node_id, node_type, display_name, is_blacklist, source_system, attrs_json, created_at, updated_at
)
SELECT
    'FB_MEDIUM_' || id,
    'MEDIUM',
    COALESCE(NULLIF(type, ''), 'FinBench Medium') || ' ' || id,
    COALESCE(TRY_CAST(isBlocked AS BOOLEAN), FALSE),
    'finbench',
    CAST(json_object(
        'benchmark', 'LDBC FinBench',
        'entity', 'Medium',
        'type', NULLIF(type, ''),
        'lastLogin', NULLIF(lastLogin, ''),
        'riskLevel', NULLIF(riskLevel, '')
    ) AS VARCHAR),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    CURRENT_TIMESTAMP
FROM fin_medium;

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'PARTY_RK', party_rk FROM g_nodes WHERE source_system = 'finbench' AND node_type = 'PERSON';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'PERSON_ID', person_id FROM g_nodes WHERE source_system = 'finbench' AND node_type = 'PERSON';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'ACCOUNT_NO', 'FB_ACCOUNT_' || regexp_extract(node_id, 'FB_ACCOUNT_(.+)$', 1)
FROM g_nodes WHERE source_system = 'finbench' AND node_type = 'ACCOUNT';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'LOAN_ID', 'FB_LOAN_' || regexp_extract(node_id, 'FB_LOAN_(.+)$', 1)
FROM g_nodes WHERE source_system = 'finbench' AND node_type = 'LOAN';

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT node_id, 'MEDIUM_ID', 'FB_MEDIUM_' || regexp_extract(node_id, 'FB_MEDIUM_(.+)$', 1)
FROM g_nodes WHERE source_system = 'finbench' AND node_type = 'MEDIUM';

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_PERSON_OWN_ACCOUNT_' || personId || '_' || accountId,
    'FB_PERSON_' || personId,
    'FB_ACCOUNT_' || accountId,
    'OWNS',
    TRUE,
    0,
    0,
    'CUSTOMER_OWNERSHIP',
    0.90,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"personOwnAccount"}'
FROM fin_person_own_account
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_PERSON_' || personId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || accountId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_COMPANY_OWN_ACCOUNT_' || companyId || '_' || accountId,
    'FB_COMPANY_' || companyId,
    'FB_ACCOUNT_' || accountId,
    'OWNS',
    TRUE,
    0,
    0,
    'CUSTOMER_OWNERSHIP',
    0.85,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"companyOwnAccount"}'
FROM fin_company_own_account
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || companyId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || accountId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_TRANSFER_' || fromId || '_' || toId || '_' || multiplicityId || '_' || createTime,
    'FB_ACCOUNT_' || fromId,
    'FB_ACCOUNT_' || toId,
    'TRANSFERS_TO',
    TRUE,
    1,
    COALESCE(TRY_CAST(amount AS DOUBLE), 0),
    'ACCOUNT_FLOW',
    0.80,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"transfer"}'
FROM fin_transfer
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || fromId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || toId)
  AND fromId <> toId;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_LOAN_TRANSFER_' || fromId || '_' || toId || '_' || multiplicityId || '_' || createTime,
    'FB_ACCOUNT_' || fromId,
    'FB_ACCOUNT_' || toId,
    'TRANSFERS_TO',
    TRUE,
    1,
    COALESCE(TRY_CAST(amount AS DOUBLE), 0),
    'ACCOUNT_FLOW',
    0.75,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"loantransfer"}'
FROM fin_loan_transfer
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || fromId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || toId)
  AND fromId <> toId;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_DEPOSIT_' || loanId || '_' || accountId || '_' || createTime,
    'FB_LOAN_' || loanId,
    'FB_ACCOUNT_' || accountId,
    'DEPOSITS_TO',
    TRUE,
    1,
    COALESCE(TRY_CAST(amount AS DOUBLE), 0),
    'LOAN_FLOW',
    0.70,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"deposit"}'
FROM fin_deposit
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_LOAN_' || loanId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || accountId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_REPAY_' || accountId || '_' || loanId || '_' || createTime,
    'FB_ACCOUNT_' || accountId,
    'FB_LOAN_' || loanId,
    'REPAYS',
    TRUE,
    1,
    COALESCE(TRY_CAST(amount AS DOUBLE), 0),
    'LOAN_FLOW',
    0.70,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"repay"}'
FROM fin_repay
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || accountId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_LOAN_' || loanId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_SIGNIN_' || accountId || '_' || mediumId || '_' || multiplicityId || '_' || createTime,
    'FB_ACCOUNT_' || accountId,
    'FB_MEDIUM_' || mediumId,
    'SIGNED_IN_WITH',
    FALSE,
    0,
    0,
    'SHARED_INFRASTRUCTURE',
    0.65,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(deleteTime AS DOUBLE), 1893196800000) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"signIn"}'
FROM fin_signin
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_ACCOUNT_' || accountId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_MEDIUM_' || mediumId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_PERSON_APPLY_LOAN_' || personId || '_' || loanId,
    'FB_PERSON_' || personId,
    'FB_LOAN_' || loanId,
    'APPLIES_FOR',
    TRUE,
    0,
    COALESCE(TRY_CAST(loanAmount AS DOUBLE), 0),
    'LOAN_APPLICATION',
    0.70,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"personApplyLoan"}'
FROM fin_person_apply_loan
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_PERSON_' || personId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_LOAN_' || loanId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_COMPANY_APPLY_LOAN_' || companyId || '_' || loanId,
    'FB_COMPANY_' || companyId,
    'FB_LOAN_' || loanId,
    'APPLIES_FOR',
    TRUE,
    0,
    COALESCE(TRY_CAST(loanAmount AS DOUBLE), 0),
    'LOAN_APPLICATION',
    0.70,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"companyApplyLoan"}'
FROM fin_company_apply_loan
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || companyId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_LOAN_' || loanId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_PERSON_GUARANTEE_' || fromId || '_' || toId || '_' || createTime,
    'FB_PERSON_' || fromId,
    'FB_PERSON_' || toId,
    'GUARANTEES',
    FALSE,
    0,
    0,
    'PERSON_GUARANTEE_PERSON',
    0.80,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"personGuarantee"}'
FROM fin_person_guarantee
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_PERSON_' || fromId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_PERSON_' || toId)
  AND fromId <> toId;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_COMPANY_GUARANTEE_' || fromId || '_' || toId || '_' || createTime,
    'FB_COMPANY_' || fromId,
    'FB_COMPANY_' || toId,
    'GUARANTEES',
    FALSE,
    0,
    0,
    'COMPANY_GUARANTEE_COMPANY',
    0.80,
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"companyGuarantee"}'
FROM fin_company_guarantee
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || fromId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || toId)
  AND fromId <> toId;

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_PERSON_INVEST_' || investorId || '_' || companyId || '_' || createTime,
    'FB_PERSON_' || investorId,
    'FB_COMPANY_' || companyId,
    'INVESTS_IN',
    TRUE,
    0,
    0,
    'INVESTMENT',
    COALESCE(TRY_CAST(ratio AS DOUBLE), 0),
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"personInvest"}'
FROM fin_person_invest
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_PERSON_' || investorId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || companyId);

INSERT INTO g_edges (
    edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
    relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
)
SELECT
    'E_FIN_COMPANY_INVEST_' || investorId || '_' || companyId || '_' || createTime,
    'FB_COMPANY_' || investorId,
    'FB_COMPANY_' || companyId,
    'INVESTS_IN',
    TRUE,
    0,
    0,
    'INVESTMENT',
    COALESCE(TRY_CAST(ratio AS DOUBLE), 0),
    1,
    'finbench',
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    to_timestamp(COALESCE(TRY_CAST(createTime AS DOUBLE), 0) / 1000.0),
    '{"benchmark":"LDBC FinBench","relation":"companyInvest"}'
FROM fin_company_invest
WHERE EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || investorId)
  AND EXISTS (SELECT 1 FROM g_nodes n WHERE n.node_id = 'FB_COMPANY_' || companyId)
  AND investorId <> companyId;

COMMIT;

ANALYZE;

SELECT 'finbench_nodes' AS metric, COUNT(*) AS value FROM g_nodes WHERE source_system LIKE 'finbench%'
UNION ALL
SELECT 'finbench_edges', COUNT(*) FROM g_edges WHERE source_system LIKE 'finbench%'
UNION ALL
SELECT 'finbench_identifiers', COUNT(*) FROM g_identifiers WHERE node_id LIKE 'FB_%';
SQL

echo
python3 scripts/finbench-seeds.py "$DB_PATH" "$SEED_FILE" "${FINBENCH_SEED_SAMPLE_LIMIT:-32}"
