#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess
import sys
from typing import Any


DEFAULT_SAMPLE_LIMIT = 32


class SeedError(RuntimeError):
    pass


def duckdb_rows(db_path: pathlib.Path, sql: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["duckdb", "-readonly", str(db_path), "-json", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    parsed = json.loads(result.stdout or "[]")
    if not isinstance(parsed, list):
        raise SeedError("DuckDB JSON output is not a row list")
    return [row for row in parsed if isinstance(row, dict)]


def require_rows(name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        raise SeedError(f"cannot derive FinBench seed set: {name} returned no rows")
    return rows


def strip_rows(rows: list[dict[str, Any]], fields: list[str]) -> list[dict[str, str]]:
    stripped: list[dict[str, str]] = []
    for row in rows:
        item = {
            field: str(row[field])
            for field in fields
            if row.get(field) is not None
        }
        if len(item) == len(fields):
            stripped.append(item)
    return stripped


def first_value(rows: list[dict[str, Any]], field: str) -> str:
    value = rows[0].get(field)
    if value is None:
        raise SeedError(f"first seed row has no {field}")
    return str(value)


def build_seed_file(db_path: pathlib.Path, sample_limit: int) -> dict[str, Any]:
    node_summary = require_rows("finbench_node_summary", duckdb_rows(db_path, f"""
        WITH degree AS (
            SELECT from_node_id AS node_id, COUNT(*) AS edge_count
            FROM g_edges
            WHERE source_system = 'finbench'
            GROUP BY from_node_id
            UNION ALL
            SELECT to_node_id AS node_id, COUNT(*) AS edge_count
            FROM g_edges
            WHERE source_system = 'finbench'
            GROUP BY to_node_id
        )
        SELECT n.node_id, COALESCE(SUM(d.edge_count), 0) AS incident_edge_count
        FROM g_nodes n
        LEFT JOIN degree d ON d.node_id = n.node_id
        WHERE n.source_system = 'finbench'
          AND n.node_type = 'PERSON'
        GROUP BY n.node_id
        ORDER BY incident_edge_count DESC, n.node_id
        LIMIT {sample_limit};
    """))

    person_expand = require_rows("finbench_person_expand", duckdb_rows(db_path, f"""
        WITH guarantee_degree AS (
            SELECT from_node_id AS node_id, COUNT(*) AS outgoing_guarantees
            FROM g_edges
            WHERE source_system = 'finbench'
              AND relation_family = 'PERSON_GUARANTEE_PERSON'
            GROUP BY from_node_id
        )
        SELECT n.party_rk, n.node_id, d.outgoing_guarantees
        FROM guarantee_degree d
        JOIN g_nodes n ON n.node_id = d.node_id
        WHERE n.source_system = 'finbench'
          AND n.node_type = 'PERSON'
          AND n.party_rk IS NOT NULL
          AND n.party_rk <> ''
        ORDER BY d.outgoing_guarantees DESC, n.node_id
        LIMIT {sample_limit};
    """))

    account_flow = require_rows("finbench_account_flow_hub", duckdb_rows(db_path, f"""
        WITH transfer_degree AS (
            SELECT from_node_id AS node_id, COUNT(*) AS outgoing_transfers
            FROM g_edges
            WHERE source_system = 'finbench'
              AND relation_family = 'ACCOUNT_FLOW'
            GROUP BY from_node_id
        )
        SELECT i.id_value AS account_no, d.node_id, d.outgoing_transfers
        FROM transfer_degree d
        JOIN g_identifiers i ON i.node_id = d.node_id AND i.id_type = 'ACCOUNT_NO'
        WHERE i.id_value NOT LIKE 'FINBENCH_%'
        ORDER BY d.outgoing_transfers DESC, d.node_id
        LIMIT {sample_limit};
    """))

    shortest_pairs = require_rows("finbench_shortest_guarantee_path", duckdb_rows(db_path, f"""
        WITH RECURSIVE search(source_node_id, node_id, target_node_id, depth, path) AS (
            SELECT
                e.from_node_id,
                e.to_node_id,
                e.to_node_id,
                1,
                '|' || e.from_node_id || '|' || e.to_node_id || '|'
            FROM g_edges e
            WHERE e.source_system = 'finbench'
              AND e.relation_family = 'PERSON_GUARANTEE_PERSON'
              AND e.from_node_id <> e.to_node_id
            UNION ALL
            SELECT
                s.source_node_id,
                e.to_node_id,
                e.to_node_id,
                s.depth + 1,
                s.path || e.to_node_id || '|'
            FROM search s
            JOIN g_edges e ON e.from_node_id = s.node_id
            WHERE e.source_system = 'finbench'
              AND e.relation_family = 'PERSON_GUARANTEE_PERSON'
              AND s.depth < 4
              AND e.to_node_id <> s.source_node_id
              AND instr(s.path, '|' || e.to_node_id || '|') = 0
        )
        SELECT
            src.party_rk AS party_rk,
            dst.party_rk AS target_party_rk,
            s.source_node_id AS source_node_id,
            s.target_node_id AS target_node_id,
            s.depth,
            s.path
        FROM search s
        JOIN g_nodes src ON src.node_id = s.source_node_id
        JOIN g_nodes dst ON dst.node_id = s.target_node_id
        WHERE s.depth BETWEEN 2 AND 4
          AND src.source_system = 'finbench'
          AND dst.source_system = 'finbench'
          AND src.party_rk IS NOT NULL
          AND dst.party_rk IS NOT NULL
          AND src.party_rk <> ''
          AND dst.party_rk <> ''
        ORDER BY s.depth DESC, s.source_node_id, s.target_node_id
        LIMIT {sample_limit};
    """))

    node_counts = duckdb_rows(db_path, """
        SELECT node_type, COUNT(*) AS count
        FROM g_nodes
        WHERE source_system = 'finbench'
        GROUP BY node_type
        ORDER BY node_type;
    """)
    edge_counts = duckdb_rows(db_path, """
        SELECT relation_family, COUNT(*) AS count
        FROM g_edges
        WHERE source_system = 'finbench'
        GROUP BY relation_family
        ORDER BY relation_family;
    """)
    synthetic_edges = duckdb_rows(db_path, """
        SELECT COUNT(*) AS count
        FROM g_edges
        WHERE source_system <> 'finbench'
          AND edge_id LIKE 'E_FIN_%';
    """)

    top_shortest = shortest_pairs[0]
    return {
        "party_rk": first_value(shortest_pairs, "party_rk"),
        "target_party_rk": first_value(shortest_pairs, "target_party_rk"),
        "account_no": first_value(account_flow, "account_no"),
        "node_id": first_value(node_summary, "node_id"),
        "path_relation_family": "PERSON_GUARANTEE_PERSON",
        "case_variables": {
            "finbench_node_summary": strip_rows(node_summary, ["node_id"]),
            "finbench_person_expand": strip_rows(person_expand, ["party_rk"]),
            "finbench_account_flow_hub": strip_rows(account_flow, ["account_no"]),
            "finbench_shortest_guarantee_path": [
                {
                    "party_rk": str(row["party_rk"]),
                    "target_party_rk": str(row["target_party_rk"]),
                    "path_relation_family": "PERSON_GUARANTEE_PERSON",
                }
                for row in shortest_pairs
            ],
        },
        "seed_generation": {
            "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "source": "scripts/finbench-seeds.py",
            "policy": "derive all request seeds from existing source_system='finbench' nodes and edges; do not insert synthetic control paths",
            "sample_limit_per_case": sample_limit,
            "shortest_path_depth_range": "2..4",
            "selected_shortest_path": top_shortest.get("path"),
            "node_counts": node_counts,
            "edge_counts": edge_counts,
            "non_finbench_e_fin_edges": synthetic_edges[0].get("count") if synthetic_edges else None,
        },
    }


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: finbench-seeds.py <duckdb-path> <seed-json-path> [sample-limit]", file=sys.stderr)
        return 2
    db_path = pathlib.Path(sys.argv[1])
    output_path = pathlib.Path(sys.argv[2])
    sample_limit = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_SAMPLE_LIMIT
    if not db_path.exists():
        raise SeedError(f"DuckDB database does not exist: {db_path}")
    seed_file = build_seed_file(db_path, sample_limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(seed_file, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"FinBench seed values written to {output_path}")
    print(json.dumps(seed_file, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (subprocess.CalledProcessError, SeedError, ValueError) as exc:
        print(f"finbench seed error: {exc}", file=sys.stderr)
        raise SystemExit(1)
