#!/usr/bin/env bash
set -euo pipefail

BENCH_WORKLOAD="${BENCH_WORKLOAD:-bench/workloads/finbench.toml}" \
  python3 scripts/bench-runner.py "$@"
