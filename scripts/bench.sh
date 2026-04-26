#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${BASE_URL:-http://localhost:8080}}"
REQUESTS="${BENCH_REQUESTS:-100}"
CONCURRENCY="${BENCH_CONCURRENCY:-1}"
WARMUP="${BENCH_WARMUP:-10}"
CURL_MAX_TIME="${BENCH_CURL_MAX_TIME:-30}"
CASES="${BENCH_CASES:-health dictionary node-summary expand shortest-path expand-account-flow}"
PARTY_RK="${BENCH_PARTY_RK:-PARTY_1001}"
TARGET_PARTY_RK="${BENCH_TARGET_PARTY_RK:-PARTY_1004}"
ACCOUNT_NO="${BENCH_ACCOUNT_NO:-40817810000000002001}"
PATH_RELATION_FAMILY="${BENCH_PATH_RELATION_FAMILY:-PERSON_KNOWS_PERSON}"
NODE_ID="${BENCH_NODE_ID:-N_PARTY_1001}"

API_BASE="${BASE_URL}/api/v1"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

now_seconds() {
  if command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'printf "%.6f", time'
  else
    date +%s
  fi
}

percentile() {
  local file="$1"
  local column="$2"
  local p="$3"

  awk -F, -v column="$column" '$2 ~ /^2/ && $column != "" {print $column}' "$file" \
    | sort -n \
    | awk -v p="$p" '
        { values[NR] = $1 }
        END {
          if (NR == 0) {
            printf "n/a\n";
            exit;
          }
          idx = int((p / 100) * NR);
          if (idx < (p / 100) * NR) {
            idx++;
          }
          if (idx < 1) {
            idx = 1;
          }
          if (idx > NR) {
            idx = NR;
          }
          printf "%.3f\n", values[idx];
        }'
}

aggregate() {
  local file="$1"
  local column="$2"

  awk -F, -v column="$column" '
    $2 ~ /^2/ && $column != "" {
      value = $column + 0;
      if (n == 0 || value < min) {
        min = value;
      }
      if (n == 0 || value > max) {
        max = value;
      }
      sum += value;
      n++;
    }
    END {
      if (n == 0) {
        printf "n/a,n/a,n/a\n";
      } else {
        printf "%.3f,%.3f,%.3f\n", sum / n, min, max;
      }
    }' "$file"
}

run_one() {
  local method="$1"
  local url="$2"
  local data="$3"
  local output="$4"
  local body="$TMP_DIR/body_${RANDOM}_${RANDOM}.json"
  local curl_output status time_total elapsed_ms app_ms

  if [[ -n "$data" ]]; then
    if ! curl_output="$(curl -sS --max-time "$CURL_MAX_TIME" -o "$body" -w "%{http_code} %{time_total}" \
      -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -d "$data" 2>/dev/null)"; then
      printf "0,000,\n" >> "$output"
      return
    fi
  else
    if ! curl_output="$(curl -sS --max-time "$CURL_MAX_TIME" -o "$body" -w "%{http_code} %{time_total}" \
      -X "$method" "$url" 2>/dev/null)"; then
      printf "0,000,\n" >> "$output"
      return
    fi
  fi

  status="${curl_output%% *}"
  time_total="${curl_output##* }"
  elapsed_ms="$(awk -v seconds="$time_total" 'BEGIN { printf "%.3f", seconds * 1000 }')"
  app_ms="$(sed -n 's/.*"executionMs"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$body" | head -n 1)"

  printf "%s,%s,%s\n" "$elapsed_ms" "$status" "$app_ms" >> "$output"
}

run_case() {
  local name="$1"
  local method="$2"
  local url="$3"
  local data="${4:-}"
  local results="$TMP_DIR/${name}.csv"
  local start end duration_s ok errors avg min max p50 p95 p99 app_p95 rps

  : > "$results"

  for ((i = 1; i <= WARMUP; i++)); do
    run_one "$method" "$url" "$data" "$TMP_DIR/warmup_${name}.csv"
  done

  start="$(now_seconds)"
  for ((i = 1; i <= REQUESTS; i++)); do
    run_one "$method" "$url" "$data" "$results" &
    if (( i % CONCURRENCY == 0 )); then
      wait
    fi
  done
  wait
  end="$(now_seconds)"

  ok="$(awk -F, '$2 ~ /^2/ { n++ } END { print n + 0 }' "$results")"
  errors="$(awk -F, '$2 !~ /^2/ { n++ } END { print n + 0 }' "$results")"
  IFS=, read -r avg min max < <(aggregate "$results" 1)
  p50="$(percentile "$results" 1 50)"
  p95="$(percentile "$results" 1 95)"
  p99="$(percentile "$results" 1 99)"
  app_p95="$(percentile "$results" 3 95)"
  duration_s="$(awk -v start="$start" -v end="$end" 'BEGIN { duration = end - start; if (duration <= 0) duration = 0.001; printf "%.3f", duration }')"
  rps="$(awk -v ok="$ok" -v duration="$duration_s" 'BEGIN { printf "%.2f", ok / duration }')"

  printf "%-22s %8s %8s %8s %10s %10s %10s %10s %10s %10s %10s %10s\n" \
    "$name" "$ok" "$errors" "$rps" "$avg" "$p50" "$p95" "$p99" "$min" "$max" "$app_p95" "$duration_s"
}

expand_payload='{
  "seeds":[{"type":"PARTY_RK","value":"'"$PARTY_RK"'"}],
  "direction":"OUTBOUND",
  "maxNeighborsPerSeed":10,
  "maxNodes":100,
  "maxEdges":100,
  "includeAttributes":true
}'

shortest_path_payload='{
  "source":{"type":"PARTY_RK","value":"'"$PARTY_RK"'"},
  "target":{"type":"PARTY_RK","value":"'"$TARGET_PARTY_RK"'"},
  "relationFamily":"'"$PATH_RELATION_FAMILY"'",
  "direction":"OUTBOUND",
  "maxDepth":4
}'

account_flow_payload='{
  "seeds":[{"type":"ACCOUNT_NO","value":"'"$ACCOUNT_NO"'"}],
  "relationFamily":"ACCOUNT_FLOW",
  "direction":"OUTBOUND",
  "maxNeighborsPerSeed":10,
  "maxNodes":100,
  "maxEdges":100,
  "includeAttributes":true
}'

printf "Benchmarking %s (requests=%s, concurrency=%s, warmup=%s, curl_max_time=%ss)\n\n" "$BASE_URL" "$REQUESTS" "$CONCURRENCY" "$WARMUP" "$CURL_MAX_TIME"
printf "%-22s %8s %8s %8s %10s %10s %10s %10s %10s %10s %10s %10s\n" \
  "case" "ok" "errors" "rps" "avg_ms" "p50_ms" "p95_ms" "p99_ms" "min_ms" "max_ms" "app_p95" "wall_s"

for case_name in $CASES; do
  case "$case_name" in
    health)
      run_case "health" "GET" "${BASE_URL}/actuator/health"
      ;;
    dictionary)
      run_case "dictionary" "GET" "${API_BASE}/graph/dictionary"
      ;;
    node-summary)
      run_case "node-summary" "GET" "${API_BASE}/graph/node-summary?nodeId=${NODE_ID}"
      ;;
    expand)
      run_case "expand" "POST" "${API_BASE}/graph/expand" "$expand_payload"
      ;;
    shortest-path)
      run_case "shortest-path" "POST" "${API_BASE}/graph/shortest-path" "$shortest_path_payload"
      ;;
    expand-account-flow)
      run_case "expand-account-flow" "POST" "${API_BASE}/graph/expand" "$account_flow_payload"
      ;;
    *)
      echo "Unknown benchmark case '$case_name'" >&2
      exit 1
      ;;
  esac
done

printf "\napp_p95 is response meta.executionMs p95 when the endpoint returns graph meta; n/a means the response has no executionMs field.\n"
