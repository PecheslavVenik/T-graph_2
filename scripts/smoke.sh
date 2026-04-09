#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
API_BASE="${BASE_URL}/api/v1"

tmp_body="$(mktemp)"
trap 'rm -f "$tmp_body"' EXIT

request() {
  local name="$1"
  local method="$2"
  local url="$3"
  local data="${4:-}"
  local expected="${5:-200}"

  local status
  if [[ -n "$data" ]]; then
    status="$(curl -s -o "$tmp_body" -w "%{http_code}" -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -d "$data")"
  else
    status="$(curl -s -o "$tmp_body" -w "%{http_code}" -X "$method" "$url")"
  fi

  if [[ "$status" != "$expected" ]]; then
    echo "[FAIL] $name (expected $expected, got $status)"
    cat "$tmp_body"
    exit 1
  fi

  echo "[OK] $name ($status)"
}

request "health" "GET" "${BASE_URL}/actuator/health" "" "200"
request "dictionary" "GET" "${API_BASE}/graph/dictionary" "" "200"
request "node-summary" "GET" "${API_BASE}/graph/node-summary?nodeId=N_PARTY_1001" "" "200"
request "node-summary-filtered" "GET" "${API_BASE}/graph/node-summary?nodeId=N_PARTY_1001&relationFamily=CUSTOMER_OWNERSHIP&direction=OUTBOUND" "" "200"

request "expand" "POST" "${API_BASE}/graph/expand" '{
  "seeds":[{"type":"PARTY_RK","value":"PARTY_1001"}],
  "direction":"OUTBOUND",
  "maxNeighborsPerSeed":10,
  "maxNodes":100,
  "maxEdges":100,
  "includeAttributes":true
}' "200"

request "shortest-path" "POST" "${API_BASE}/graph/shortest-path" '{
  "source":{"type":"PARTY_RK","value":"PARTY_1001"},
  "target":{"type":"PARTY_RK","value":"PARTY_1004"},
  "direction":"OUTBOUND",
  "maxDepth":4
}' "200"

request "expand-account-flow" "POST" "${API_BASE}/graph/expand" '{
  "seeds":[{"type":"ACCOUNT_NO","value":"40817810000000002001"}],
  "relationFamily":"ACCOUNT_FLOW",
  "direction":"OUTBOUND",
  "maxNeighborsPerSeed":10,
  "maxNodes":100,
  "maxEdges":100,
  "includeAttributes":true
}' "200"

request "shortest-path-tax-id" "POST" "${API_BASE}/graph/shortest-path" '{
  "source":{"type":"PARTY_RK","value":"PARTY_1001"},
  "target":{"type":"TAX_ID","value":"7701234567"},
  "relationFamily":"CORPORATE_CONTROL",
  "direction":"OUTBOUND",
  "maxDepth":2
}' "200"

request "export-csv" "POST" "${API_BASE}/graph/export?format=CSV" '{
  "nodes":[{"nodeId":"N1","displayName":"Node 1"}],
  "edges":[]
}' "200"

echo "Smoke checks passed."
