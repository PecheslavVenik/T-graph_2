#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import math
import os
import pathlib
import statistics
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from string import Template
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]


class BenchmarkError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def load_toml(path: pathlib.Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item for item in value.replace(",", " ").split() if item]


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


def resolve_path(path: str | pathlib.Path) -> pathlib.Path:
    candidate = pathlib.Path(path)
    if candidate.is_absolute():
        return candidate
    return ROOT / candidate


def substitute(value: Any, variables: dict[str, str]) -> Any:
    if isinstance(value, str):
        return Template(value).safe_substitute(variables)
    if isinstance(value, list):
        return [substitute(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, variables) for key, item in value.items()}
    return value


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(1, math.ceil((p / 100.0) * len(ordered))) - 1
    return ordered[min(index, len(ordered) - 1)]


def rounded(value: float | None, digits: int = 3) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def http_call(method: str,
              url: str,
              body: str | None,
              timeout_seconds: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = body.encode("utf-8")
        headers["Content-Type"] = "application/json"

    started = time.perf_counter()
    status = 0
    response_body = ""
    error = ""

    try:
        request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = response.status
            response_body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_body = exc.read().decode("utf-8", errors="replace")
        error = str(exc)
    except Exception as exc:  # noqa: BLE001 - benchmark output should preserve failures.
        error = str(exc)

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    app_ms = None
    if response_body:
        try:
            parsed = json.loads(response_body)
            meta = parsed.get("meta") if isinstance(parsed, dict) else None
            if isinstance(meta, dict) and isinstance(meta.get("executionMs"), (int, float)):
                app_ms = float(meta["executionMs"])
        except json.JSONDecodeError:
            pass

    return {
        "elapsed_ms": elapsed_ms,
        "status": status,
        "app_ms": app_ms,
        "error": error,
    }


def is_healthy(base_url: str, timeout_seconds: float = 1.0) -> bool:
    try:
        result = http_call("GET", f"{base_url.rstrip('/')}/actuator/health", None, timeout_seconds)
        return 200 <= int(result["status"]) < 300
    except Exception:  # noqa: BLE001 - health probe must be defensive.
        return False


def run_requests(method: str,
                 url: str,
                 body: str | None,
                 total: int,
                 concurrency: int,
                 timeout_seconds: float) -> tuple[list[dict[str, Any]], float]:
    if total <= 0:
        return [], 0.0

    started = time.perf_counter()
    workers = max(1, min(concurrency, total))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(http_call, method, url, body, timeout_seconds)
            for _ in range(total)
        ]
        samples = [future.result() for future in concurrent.futures.as_completed(futures)]
    wall_seconds = max(time.perf_counter() - started, 0.001)
    return samples, wall_seconds


def summarize_samples(samples: list[dict[str, Any]], wall_seconds: float) -> dict[str, Any]:
    ok_samples = [sample for sample in samples if 200 <= int(sample["status"]) < 300]
    latencies = [float(sample["elapsed_ms"]) for sample in ok_samples]
    app_latencies = [
        float(sample["app_ms"])
        for sample in ok_samples
        if sample.get("app_ms") is not None
    ]

    return {
        "ok": len(ok_samples),
        "errors": len(samples) - len(ok_samples),
        "rps": rounded(len(ok_samples) / max(wall_seconds, 0.001), 2),
        "avg_ms": rounded(statistics.fmean(latencies), 3) if latencies else None,
        "p50_ms": rounded(percentile(latencies, 50), 3),
        "p95_ms": rounded(percentile(latencies, 95), 3),
        "p99_ms": rounded(percentile(latencies, 99), 3),
        "min_ms": rounded(min(latencies), 3) if latencies else None,
        "max_ms": rounded(max(latencies), 3) if latencies else None,
        "app_p95_ms": rounded(percentile(app_latencies, 95), 3),
        "wall_seconds": rounded(wall_seconds, 3),
    }


def score_latency(value: float | None,
                  ideal: float,
                  good: float,
                  acceptable: float,
                  errors: int = 0,
                  p99: float | None = None) -> float:
    if value is None or errors > 0:
        return 0.0
    if value <= ideal:
        score = 1.0
    elif value <= good:
        score = 0.75 + 0.25 * ((good - value) / max(good - ideal, 1.0))
    elif value <= acceptable:
        score = 0.40 + 0.35 * ((acceptable - value) / max(acceptable - good, 1.0))
    else:
        score = max(0.0, 0.40 * (acceptable / max(value, 1.0)))

    if p99 is not None and value > 0 and p99 > value * 2.5:
        score *= 0.85
    return max(0.0, min(1.0, score))


def case_url(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{base_url.rstrip('/')}{path}"


def write_raw_csv(path: pathlib.Path, samples: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["elapsed_ms", "status", "app_ms", "error"])
        writer.writeheader()
        for sample in samples:
            writer.writerow({
                "elapsed_ms": rounded(float(sample["elapsed_ms"]), 3),
                "status": sample["status"],
                "app_ms": sample.get("app_ms"),
                "error": sample.get("error", ""),
            })


def run_case(case_name: str,
             case_spec: dict[str, Any],
             variables: dict[str, str],
             base_url: str,
             requests: int,
             concurrency: int,
             warmup: int,
             timeout_seconds: float,
             raw_dir: pathlib.Path) -> dict[str, Any]:
    rendered = substitute(case_spec, variables)
    method = str(rendered.get("method", "GET")).upper()
    url = case_url(base_url, str(rendered["path"]))
    body = rendered.get("body")
    if isinstance(body, str):
        body = body.strip()
    else:
        body = None

    if warmup > 0:
        run_requests(method, url, body, warmup, concurrency, timeout_seconds)

    samples, wall_seconds = run_requests(method, url, body, requests, concurrency, timeout_seconds)
    raw_path = raw_dir / f"{case_name}.csv"
    write_raw_csv(raw_path, samples)

    summary = summarize_samples(samples, wall_seconds)
    summary.update({
        "case": case_name,
        "title": rendered.get("title", case_name),
        "benchmark_family": rendered.get("benchmark_family", ""),
        "raw_csv": str(raw_path.relative_to(raw_dir.parent)),
    })

    weight = float(rendered.get("weight", 0.0))
    if weight > 0:
        summary["score"] = rounded(score_latency(
            summary["p95_ms"],
            float(rendered["ideal_p95_ms"]),
            float(rendered["good_p95_ms"]),
            float(rendered["acceptable_p95_ms"]),
            int(summary["errors"]),
            summary["p99_ms"],
        ) * 100.0, 2)
    else:
        summary["score"] = None
    return summary


def tail_file(path: pathlib.Path, lines: int = 80) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])


def stop_process(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def compose_volume_names(volume_keys: list[str]) -> list[str]:
    if not volume_keys:
        return []
    try:
        result = subprocess.run(
            ["docker", "compose", "config", "--format", "json"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        config = json.loads(result.stdout)
        volumes = config.get("volumes", {}) if isinstance(config, dict) else {}
        project = str(config.get("name") or ROOT.name) if isinstance(config, dict) else ROOT.name
        resolved = []
        for key in volume_keys:
            spec = volumes.get(key)
            if isinstance(spec, dict) and spec.get("name"):
                resolved.append(str(spec["name"]))
            else:
                resolved.append(f"{project}_{key}")
        return resolved
    except Exception:  # noqa: BLE001 - fallback keeps runner compatible with older Docker Compose.
        return volume_keys


def reset_compose_services(services: list[str], volumes: list[str]) -> None:
    if services:
        subprocess.run(
            ["docker", "compose", "stop", *services],
            cwd=ROOT,
            check=False,
        )
        subprocess.run(
            ["docker", "compose", "rm", "-f", "-s", "-v", *services],
            cwd=ROOT,
            check=True,
        )
    for volume in compose_volume_names(volumes):
        subprocess.run(
            ["docker", "volume", "rm", "-f", volume],
            cwd=ROOT,
            check=False,
        )


def start_compose_services(services: list[str], grace_seconds: float = 0.0) -> None:
    if not services:
        return
    subprocess.run(
        ["docker", "compose", "up", "-d", *services],
        cwd=ROOT,
        check=True,
    )
    if grace_seconds > 0:
        time.sleep(grace_seconds)


def wait_for_service_readiness(services_cfg: dict[str, Any], variables: dict[str, str]) -> None:
    wait_cfg = services_cfg.get("wait")
    if not isinstance(wait_cfg, dict) or not wait_cfg.get("command"):
        return

    command = str(substitute(wait_cfg["command"], variables))
    timeout_seconds = float(wait_cfg.get("timeout_seconds", 120))
    interval_seconds = float(wait_cfg.get("interval_seconds", 2))
    command_timeout_seconds = float(wait_cfg.get("command_timeout_seconds", 10))
    deadline = time.perf_counter() + timeout_seconds
    last_output = ""

    while time.perf_counter() < deadline:
        try:
            result = subprocess.run(
                ["bash", "-lc", command],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            last_output = stdout + stderr + f"\ncommand timed out after {command_timeout_seconds:g}s"
            time.sleep(interval_seconds)
            continue
        if result.returncode == 0:
            return
        last_output = (result.stdout or "") + (result.stderr or "")
        time.sleep(interval_seconds)

    raise BenchmarkError(
        "service readiness command did not succeed in "
        f"{timeout_seconds:g}s: {command}\n{last_output[-2000:]}"
    )


def start_app(backend: dict[str, Any],
              variables: dict[str, str],
              base_url: str,
              log_path: pathlib.Path,
              allow_existing: bool) -> tuple[subprocess.Popen[Any] | None, float]:
    if is_healthy(base_url):
        if allow_existing:
            return None, 0.0
        raise BenchmarkError(f"{base_url} is already healthy; stop it or pass --allow-existing")

    services_cfg = backend.get("services", {})
    services = list(services_cfg.get("compose", []))
    if bool(services_cfg.get("reset_before_start", False)):
        reset_compose_services(
            services,
            list(services_cfg.get("reset_volumes", [])),
        )
    start_compose_services(
        services,
        float(services_cfg.get("startup_grace_seconds", 0.0)),
    )
    wait_for_service_readiness(services_cfg, variables)

    env = os.environ.copy()
    env.update({key: str(value) for key, value in substitute(backend.get("env", {}), variables).items()})
    command = str(substitute(backend.get("command", {}).get("start"), variables))
    startup_timeout = int(backend.get("startup_timeout_seconds", 300))

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_stream = log_path.open("w", encoding="utf-8")
    started = time.perf_counter()
    process = subprocess.Popen(
        ["bash", "-lc", command],
        cwd=ROOT,
        env=env,
        stdout=log_stream,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_stream.close()

    try:
        deadline = time.perf_counter() + startup_timeout
        while time.perf_counter() < deadline:
            if is_healthy(base_url):
                return process, time.perf_counter() - started
            if process.poll() is not None:
                raise BenchmarkError(
                    f"app exited while starting backend {backend.get('id')}; log:\n{tail_file(log_path)}"
                )
            time.sleep(2)
        raise BenchmarkError(
            f"app did not become healthy for backend {backend.get('id')} in {startup_timeout}s; "
            f"log:\n{tail_file(log_path)}"
        )
    except Exception:
        stop_process(process)
        raise


def load_backend(item: str, backend_dir: pathlib.Path) -> dict[str, Any]:
    explicit_path = pathlib.Path(item)
    if explicit_path.exists() or explicit_path.suffix == ".toml":
        path = resolve_path(explicit_path)
    else:
        path = backend_dir / f"{item.lower()}.toml"

    if not path.exists():
        raise BenchmarkError(f"backend config not found: {item}")
    backend = load_toml(path)
    backend["_path"] = str(path)
    return backend


def load_backend_configs(backend_dir: pathlib.Path) -> list[dict[str, Any]]:
    backends = []
    for path in sorted(backend_dir.glob("*.toml")):
        if path.name.startswith("_"):
            continue
        backend = load_toml(path)
        backend["_path"] = str(path)
        backends.append(backend)
    return backends


def adapter_status(backend: dict[str, Any]) -> str:
    return str(backend.get("adapter_status", "implemented"))


def print_backend_list(backends: list[dict[str, Any]]) -> None:
    print(f"{'id':<22} {'adapter':<18} {'enabled':<8} {'type':<28} name")
    for backend in backends:
        print(
            f"{backend.get('id', ''):<22} "
            f"{adapter_status(backend):<18} "
            f"{str(backend.get('enabled', True)).lower():<8} "
            f"{backend.get('type', ''):<28} "
            f"{backend.get('name', '')}"
        )


def prepare_dataset(workload: dict[str, Any],
                    variables: dict[str, str],
                    scale: str,
                    db_path: str) -> None:
    generator = workload.get("dataset", {}).get("generator")
    if not generator:
        raise BenchmarkError("workload has no dataset.generator")

    env = os.environ.copy()
    env["BENCH_DB"] = db_path
    env["BENCH_SCALE"] = scale
    command = str(substitute(generator, variables))
    subprocess.run(["bash", "-lc", command], cwd=ROOT, env=env, check=True)


def apply_seed_file(workload: dict[str, Any], variables: dict[str, str]) -> None:
    seed_file = workload.get("dataset", {}).get("seed_file")
    if not seed_file:
        return

    path = resolve_path(str(substitute(seed_file, variables)))
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as stream:
        seeds = json.load(stream)
    if not isinstance(seeds, dict):
        raise BenchmarkError(f"seed_file must contain a JSON object: {path}")

    for key, value in seeds.items():
        if value is not None:
            variables[str(key)] = str(value)


def apply_seed_env_overrides(variables: dict[str, str]) -> None:
    seed_env = {
        "party_rk": "BENCH_PARTY_RK",
        "target_party_rk": "BENCH_TARGET_PARTY_RK",
        "account_no": "BENCH_ACCOUNT_NO",
        "node_id": "BENCH_NODE_ID",
        "path_relation_family": "BENCH_PATH_RELATION_FAMILY",
    }
    for key, env_name in seed_env.items():
        if env_name in os.environ:
            variables[key] = os.environ[env_name]


def compute_decision_score(result: dict[str, Any],
                           workload: dict[str, Any],
                           cases_by_name: dict[str, dict[str, Any]],
                           case_weights: dict[str, Any] | None = None,
                           startup_weight: Any | None = None) -> tuple[float, float | None]:
    weighted = 0.0
    total_weight = 0.0
    startup_score_percent = None

    startup_slo = workload.get("startup_slo", {})
    effective_startup_weight = (
        float(startup_weight)
        if startup_weight is not None
        else float(startup_slo.get("weight", 0.0))
    )
    if effective_startup_weight > 0:
        startup_score = score_latency(
            float(result.get("startup_seconds", 0.0)),
            float(startup_slo.get("ideal_seconds", 30)),
            float(startup_slo.get("good_seconds", 90)),
            float(startup_slo.get("acceptable_seconds", 300)),
        )
        startup_score_percent = rounded(startup_score * 100.0, 2)
        weighted += startup_score * effective_startup_weight
        total_weight += effective_startup_weight

    for case_result in result.get("cases", []):
        case_spec = cases_by_name[case_result["case"]]
        if case_weights is None:
            weight = float(case_spec.get("weight", 0.0))
        else:
            weight = float(case_weights.get(case_result["case"], 0.0))
        if weight <= 0:
            continue
        case_score = (case_result.get("score") or 0.0) / 100.0
        weighted += case_score * weight
        total_weight += weight

    return rounded((weighted / total_weight) * 100.0 if total_weight else 0.0, 2), startup_score_percent


def add_backend_score(result: dict[str, Any], workload: dict[str, Any], cases_by_name: dict[str, dict[str, Any]]) -> None:
    score_profiles = workload.get("score_profiles", {})
    if result.get("status") != "ok":
        result["decision_score"] = 0.0
        result["score"] = 0.0
        if isinstance(score_profiles, dict) and score_profiles:
            result["decision_score_profiles"] = {
                str(profile_name): 0.0
                for profile_name in score_profiles
            }
            result["score_profiles"] = result["decision_score_profiles"]
        return

    score, startup_score = compute_decision_score(result, workload, cases_by_name)
    result["decision_score"] = score
    result["score"] = score
    if startup_score is not None:
        result["startup_score"] = startup_score

    if isinstance(score_profiles, dict) and score_profiles:
        result["decision_score_profiles"] = {}
        for profile_name, profile in score_profiles.items():
            if not isinstance(profile, dict):
                continue
            profile_score, _ = compute_decision_score(
                result,
                workload,
                cases_by_name,
                case_weights=profile.get("case_weights", {}),
                startup_weight=profile.get("startup_weight"),
            )
            result["decision_score_profiles"][str(profile_name)] = profile_score
        result["score_profiles"] = result["decision_score_profiles"]


def scientific_case_names(workload: dict[str, Any],
                          selected_case_names: list[str],
                          cases_by_name: dict[str, dict[str, Any]]) -> list[str]:
    scientific_cfg = workload.get("scientific_score", {})
    if isinstance(scientific_cfg, dict):
        configured = scientific_cfg.get("include_cases") or scientific_cfg.get("included_cases")
        if isinstance(configured, list) and configured:
            return [str(case_name) for case_name in configured]

    return [
        case_name
        for case_name in selected_case_names
        if float(cases_by_name.get(case_name, {}).get("weight", 0.0)) > 0
    ]


def evaluate_scientific_metric(result: dict[str, Any],
                               included_cases: list[str]) -> dict[str, Any]:
    metric = {
        "valid": False,
        "validity_reasons": [],
        "included_cases": included_cases,
        "measured_operations": 0,
        "measured_wall_seconds": 0.0,
        "ops_per_second": 0.0,
        "score": 0.0,
    }
    reasons = metric["validity_reasons"]

    if result.get("status") != "ok":
        reasons.append(f"backend status is {result.get('status')}")

    cases = {
        str(case.get("case")): case
        for case in result.get("cases", [])
        if isinstance(case, dict) and case.get("case") is not None
    }

    for case_name in included_cases:
        case = cases.get(case_name)
        if case is None:
            reasons.append(f"case {case_name} was not executed")
            continue

        errors = int(case.get("errors") or 0)
        ok = int(case.get("ok") or 0)
        wall_seconds = case.get("wall_seconds")
        rps = case.get("rps")

        if errors > 0:
            reasons.append(f"case {case_name} has {errors} errors")
        if ok <= 0:
            reasons.append(f"case {case_name} has no successful operations")
        if wall_seconds is None or float(wall_seconds) <= 0:
            reasons.append(f"case {case_name} has no measured wall time")
        if rps is None or float(rps) <= 0:
            reasons.append(f"case {case_name} has no positive throughput")

        if errors == 0 and ok > 0 and wall_seconds is not None and float(wall_seconds) > 0:
            metric["measured_operations"] += ok
            metric["measured_wall_seconds"] += float(wall_seconds)

    if not included_cases:
        reasons.append("scientific_score.include_cases is empty")

    if not reasons and metric["measured_operations"] > 0 and metric["measured_wall_seconds"] > 0:
        metric["valid"] = True
        metric["ops_per_second"] = rounded(
            float(metric["measured_operations"]) / float(metric["measured_wall_seconds"]),
            3,
        )

    metric["measured_wall_seconds"] = rounded(float(metric["measured_wall_seconds"]), 3)
    return metric


def add_scientific_scores(run: dict[str, Any],
                          workload: dict[str, Any],
                          selected_case_names: list[str],
                          cases_by_name: dict[str, dict[str, Any]]) -> None:
    included_cases = scientific_case_names(workload, selected_case_names, cases_by_name)
    scientific_cfg = workload.get("scientific_score", {})
    run["scientific_score"] = {
        "primary_metric": "equal_operation_throughput_ops_per_second",
        "description": (
            scientific_cfg.get("description", "")
            if isinstance(scientific_cfg, dict)
            else ""
        ),
        "include_cases": included_cases,
        "validity": "status=ok, all included cases executed, errors=0, positive successful throughput",
        "normalization": "scientific_score = 100 * backend_ops_per_second / best_valid_ops_per_second_in_this_run",
    }

    best_ops = 0.0
    for backend in run["backends"]:
        scientific = evaluate_scientific_metric(backend, included_cases)
        backend["scientific"] = scientific
        backend["scientific_valid"] = scientific["valid"]
        backend["scientific_ops_per_second"] = scientific["ops_per_second"]
        if scientific["valid"]:
            best_ops = max(best_ops, float(scientific["ops_per_second"] or 0.0))

    for backend in run["backends"]:
        scientific = backend.get("scientific", {})
        ops_per_second = float(scientific.get("ops_per_second") or 0.0)
        score = rounded((ops_per_second / best_ops) * 100.0, 2) if best_ops > 0 else 0.0
        scientific["score"] = score
        backend["scientific_score"] = score


def markdown_value(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def write_summary(output_dir: pathlib.Path, run: dict[str, Any]) -> pathlib.Path:
    summary_path = output_dir / "summary.md"
    successful = [backend for backend in run["backends"] if backend.get("status") == "ok"]
    scientific_valid = [
        backend
        for backend in successful
        if backend.get("scientific", {}).get("valid")
    ]
    scientific_leader = max(
        scientific_valid,
        key=lambda item: item.get("scientific_ops_per_second", 0.0),
        default=None,
    )
    decision_leader = max(successful, key=lambda item: item.get("decision_score", 0.0), default=None)
    score_profiles = run.get("score_profiles", {})
    profile_names = list(score_profiles.keys()) if isinstance(score_profiles, dict) else []

    lines: list[str] = []
    lines.append(f"# Benchmark Summary: {run['workload']['name']}")
    lines.append("")
    lines.append(f"- run_id: `{run['run_id']}`")
    lines.append(f"- dataset: `{run['dataset']['db_path']}` / scale `{run['dataset']['scale']}`")
    lines.append(f"- requests: `{run['runner']['requests']}`, concurrency: `{run['runner']['concurrency']}`, warmup: `{run['runner']['warmup']}`")
    if scientific_leader:
        lines.append(
            "- scientific leader: `{backend}` with `{ops}` ops/s (`scientific_score={score}`)".format(
                backend=scientific_leader["backend"]["id"],
                ops=markdown_value(scientific_leader.get("scientific_ops_per_second")),
                score=markdown_value(scientific_leader.get("scientific_score")),
            )
        )
    if decision_leader:
        lines.append(
            f"- decision-score leader: `{decision_leader['backend']['id']}` with `{decision_leader.get('decision_score')}`"
        )
    lines.append("")

    scientific_cfg = run.get("scientific_score", {})
    lines.append("## Scientific Primary Metric")
    lines.append("")
    lines.append("- metric: `equal_operation_throughput_ops_per_second`")
    lines.append("- formula: `sum(successful included operations) / sum(measured case wall seconds)`")
    lines.append("- validity: `status=ok`, every included case executed, `errors=0`, positive throughput")
    lines.append("- included_cases: `" + " ".join(scientific_cfg.get("include_cases", [])) + "`")
    if scientific_cfg.get("description"):
        lines.append(f"- description: {scientific_cfg['description']}")
    lines.append("")

    lines.append("## Backend Scores")
    lines.append("")
    lines.append("| backend | adapter | status | scientific_valid | scientific_ops/s | scientific_score | decision_score | startup_s | startup_score | log |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
    for backend in run["backends"]:
        lines.append(
            "| {id} | {adapter} | {status} | {scientific_valid} | {scientific_ops} | {scientific_score} | {decision_score} | {startup} | {startup_score} | {log} |".format(
                id=backend["backend"]["id"],
                adapter=backend["backend"].get("adapter_status", "implemented"),
                status=backend.get("status"),
                scientific_valid=markdown_value(backend.get("scientific_valid")),
                scientific_ops=markdown_value(backend.get("scientific_ops_per_second")),
                scientific_score=markdown_value(backend.get("scientific_score")),
                decision_score=markdown_value(backend.get("decision_score")),
                startup=markdown_value(backend.get("startup_seconds")),
                startup_score=markdown_value(backend.get("startup_score")),
                log=backend.get("log", "n/a"),
            )
        )
    lines.append("")

    invalid_scientific = [
        backend
        for backend in run["backends"]
        if not backend.get("scientific", {}).get("valid")
    ]
    if invalid_scientific:
        lines.append("## Scientific Invalid Backends")
        lines.append("")
        for backend in invalid_scientific:
            reasons = backend.get("scientific", {}).get("validity_reasons", [])
            reason_text = "; ".join(str(reason) for reason in reasons) if reasons else "unknown reason"
            lines.append(f"- `{backend['backend']['id']}`: {reason_text}")
        lines.append("")

    if profile_names:
        lines.append("## Decision Score Sensitivity")
        lines.append("")
        lines.append("Same raw case scores re-weighted with alternative research profiles. This is not the scientific primary ranking.")
        lines.append("")
        lines.append("| profile | purpose | startup_weight | case_weights |")
        lines.append("| --- | --- | ---: | --- |")
        for profile_name in profile_names:
            profile = score_profiles.get(profile_name, {})
            case_weights = profile.get("case_weights", {}) if isinstance(profile, dict) else {}
            case_weights_text = ", ".join(
                f"{case_name}:{float(weight):g}"
                for case_name, weight in case_weights.items()
            )
            lines.append(
                "| {name} | {purpose} | {startup_weight} | {case_weights} |".format(
                    name=profile_name,
                    purpose=profile.get("description", "") if isinstance(profile, dict) else "",
                    startup_weight=markdown_value(profile.get("startup_weight") if isinstance(profile, dict) else None),
                    case_weights=case_weights_text,
                )
            )
        lines.append("")
        lines.append("| backend | base_decision_score | " + " | ".join(profile_names) + " |")
        lines.append("| --- | ---: | " + " | ".join(["---:" for _ in profile_names]) + " |")
        for backend in run["backends"]:
            profile_scores = backend.get("decision_score_profiles", {})
            lines.append(
                "| {backend} | {base} | {profiles} |".format(
                    backend=backend["backend"]["id"],
                    base=markdown_value(backend.get("decision_score")),
                    profiles=" | ".join(
                        markdown_value(profile_scores.get(profile_name))
                        for profile_name in profile_names
                    ),
                )
            )
    lines.append("")

    lines.append("## Case Results")
    lines.append("")
    lines.append("| backend | case | family | ok | errors | rps | avg_ms | p95_ms | p99_ms | app_p95_ms | decision_score |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for backend in run["backends"]:
        for case in backend.get("cases", []):
            lines.append(
                "| {backend} | {case} | {family} | {ok} | {errors} | {rps} | {avg} | {p95} | {p99} | {app_p95} | {score} |".format(
                    backend=backend["backend"]["id"],
                    case=case["case"],
                    family=case.get("benchmark_family", ""),
                    ok=case["ok"],
                    errors=case["errors"],
                    rps=markdown_value(case["rps"]),
                    avg=markdown_value(case["avg_ms"]),
                    p95=markdown_value(case["p95_ms"]),
                    p99=markdown_value(case["p99_ms"]),
                    app_p95=markdown_value(case["app_p95_ms"]),
                    score=markdown_value(case.get("score")),
                )
            )
    lines.append("")

    failed = [backend for backend in run["backends"] if backend.get("status") != "ok"]
    if failed:
        lines.append("## Failed Backends")
        lines.append("")
        for backend in failed:
            lines.append(f"- `{backend['backend']['id']}`: {backend.get('error')}")
        lines.append("")

    lines.append("## Interpretation Rules")
    lines.append("")
    lines.append("- Scientific ranking uses throughput of the predeclared included transaction cases after validity gates; it does not use SLO weights.")
    lines.append("- `scientific_score` is only normalization against the best valid backend in this run; the primary value is `scientific_ops/s`.")
    lines.append("- `decision_score` is an expert-defined utility function for product trade-offs, weighted by TOML case weights and p95 SLOs.")
    lines.append("- Raw p50/p95/p99/rps/errors are the primary benchmark facts; weighted decision score is not an official LDBC metric.")
    lines.append("- Decision score sensitivity profiles re-weight the same raw case scores to show how much the product ranking depends on subjective priorities.")
    lines.append("- Startup/projection sync is scored separately from HTTP latency.")
    lines.append("- A backend with errors is penalized even if successful requests are fast.")
    lines.append("- The workload is LDBC/GAP/Graph500-inspired for repeatability, not an official certified benchmark run.")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary_path


def write_results(output_dir: pathlib.Path, run: dict[str, Any]) -> None:
    (output_dir / "run.json").write_text(
        json.dumps(run, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    with (output_dir / "cases.csv").open("w", newline="", encoding="utf-8") as stream:
        fieldnames = [
            "backend", "case", "ok", "errors", "rps", "avg_ms", "p50_ms", "p95_ms",
            "p99_ms", "min_ms", "max_ms", "app_p95_ms", "wall_seconds", "score", "decision_score",
        ]
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for backend in run["backends"]:
            for case in backend.get("cases", []):
                row = {key: case.get(key) for key in fieldnames}
                row["backend"] = backend["backend"]["id"]
                row["decision_score"] = case.get("score")
                writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run reproducible graph DB benchmark workloads.")
    parser.add_argument("--workload", default=os.environ.get("BENCH_WORKLOAD", "bench/workloads/aml.toml"))
    parser.add_argument("--backend", action="append", help="Backend id or TOML path. May be used multiple times.")
    parser.add_argument("--backend-dir", default="bench/backends")
    parser.add_argument("--cases", default=os.environ.get("BENCH_CASES"))
    parser.add_argument("--prepare-data", action="store_true", default=env_bool("BENCH_PREPARE_DATA"))
    parser.add_argument("--requests", type=int, default=int(os.environ["BENCH_REQUESTS"]) if os.environ.get("BENCH_REQUESTS") else None)
    parser.add_argument("--concurrency", type=int, default=int(os.environ["BENCH_CONCURRENCY"]) if os.environ.get("BENCH_CONCURRENCY") else None)
    parser.add_argument("--warmup", type=int, default=int(os.environ["BENCH_WARMUP"]) if os.environ.get("BENCH_WARMUP") else None)
    parser.add_argument("--timeout-seconds", type=float, default=float(os.environ["BENCH_CURL_MAX_TIME"]) if os.environ.get("BENCH_CURL_MAX_TIME") else None)
    parser.add_argument("--app-port", type=int, default=int(os.environ["BENCH_APP_PORT"]) if os.environ.get("BENCH_APP_PORT") else None)
    parser.add_argument("--db-path", default=os.environ.get("BENCH_DB"))
    parser.add_argument("--scale", default=os.environ.get("BENCH_SCALE"))
    parser.add_argument("--output-dir", default=os.environ.get("BENCH_LOG_DIR", "target/bench"))
    parser.add_argument("--allow-existing", action="store_true", default=env_bool("BENCH_ALLOW_EXISTING"))
    parser.add_argument("--fail-on-slo", action="store_true", default=env_bool("BENCH_FAIL_ON_SLO"))
    parser.add_argument("--list-backends", action="store_true", help="List backend configs and exit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workload_path = resolve_path(args.workload)
    workload = load_toml(workload_path)
    dataset = workload.get("dataset", {})
    runner_cfg = workload.get("runner", {})
    backend_dir = resolve_path(args.backend_dir)

    if args.list_backends:
        print_backend_list(load_backend_configs(backend_dir))
        return 0

    db_path = args.db_path or str(dataset.get("db_path", "data/graph_bench.duckdb"))
    scale = args.scale or str(dataset.get("scale", "serious"))
    app_port = args.app_port or int(runner_cfg.get("app_port", 8080))
    base_url = f"http://localhost:{app_port}"

    variables = {
        "db_path": db_path,
        "app_port": str(app_port),
        "neo4j_password": os.environ.get("GRAPH_NEO4J_PASSWORD", "graph-api-password"),
        "postgres_password": os.environ.get("GRAPH_POSTGRES_AGE_PASSWORD", os.environ.get("GRAPH_POSTGRES_PASSWORD", "graph-api-password")),
        "arango_root_password": os.environ.get("GRAPH_ARANGO_ROOT_PASSWORD", "graph-api-password"),
        "kuzu_path": os.environ.get("GRAPH_KUZU_PATH", "data/graph_bench.kuzu"),
    }
    for key, value in workload.get("seeds", {}).items():
        variables[key] = str(value)
    apply_seed_env_overrides(variables)

    requests = args.requests or int(runner_cfg.get("requests", 100))
    concurrency = args.concurrency or int(runner_cfg.get("concurrency", 1))
    warmup = args.warmup if args.warmup is not None else int(runner_cfg.get("warmup", 10))
    timeout_seconds = args.timeout_seconds or float(runner_cfg.get("timeout_seconds", 30))

    case_names = parse_list(args.cases) or list(workload.get("default_cases", []))
    cases_by_name = workload.get("case", {})
    missing_cases = [case for case in case_names if case not in cases_by_name]
    if missing_cases:
        raise BenchmarkError(f"unknown benchmark cases: {', '.join(missing_cases)}")

    backend_names = args.backend or parse_list(os.environ.get("BENCH_BACKENDS")) or list(workload.get("default_backends", []))
    backends = [load_backend(item, backend_dir) for item in backend_names]
    backends = [backend for backend in backends if backend.get("enabled", True)]
    if not backends:
        raise BenchmarkError("no enabled backends selected")

    if args.prepare_data:
        print(f"Preparing dataset {db_path} with scale={scale}", flush=True)
        prepare_dataset(workload, variables, scale, db_path)

    if not resolve_path(db_path).exists():
        raise BenchmarkError(f"benchmark DB does not exist: {db_path}. Run with --prepare-data first.")

    apply_seed_file(workload, variables)
    apply_seed_env_overrides(variables)

    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = resolve_path(args.output_dir) / run_id
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    run_started = time.perf_counter()

    run: dict[str, Any] = {
        "run_id": run_id,
        "started_at": utc_now(),
        "workload": {
            "name": workload.get("name"),
            "version": workload.get("version"),
            "path": str(workload_path),
        },
        "dataset": {
            "db_path": db_path,
            "scale": scale,
        },
        "runner": {
            "requests": requests,
            "concurrency": concurrency,
            "warmup": warmup,
            "timeout_seconds": timeout_seconds,
            "app_port": app_port,
        },
        "score_profiles": workload.get("score_profiles", {}),
        "variables": variables,
        "backends": [],
    }

    for backend in backends:
        backend_id = str(backend["id"])
        print(f"\n=== {backend_id} ===", flush=True)
        backend_started = time.perf_counter()
        log_path = output_dir / f"app-{backend_id}.log"
        backend_result: dict[str, Any] = {
            "backend": {
                "id": backend_id,
                "name": backend.get("name", backend_id),
                "type": backend.get("type", ""),
                "adapter_status": adapter_status(backend),
                "config": backend.get("_path"),
            },
            "status": "ok",
            "log": str(log_path.relative_to(output_dir)),
            "started_at": utc_now(),
            "cases": [],
        }
        process: subprocess.Popen[Any] | None = None

        try:
            if adapter_status(backend) != "implemented":
                backend_result["status"] = "skipped"
                backend_result["error"] = (
                    f"adapter_status={adapter_status(backend)}; implement GraphQueryBackend before running this candidate"
                )
                print(f"skipped: {backend_result['error']}", flush=True)
                run["backends"].append(backend_result)
                add_backend_score(backend_result, workload, cases_by_name)
                continue

            process, startup_seconds = start_app(backend, variables, base_url, log_path, args.allow_existing)
            backend_result["startup_seconds"] = rounded(startup_seconds, 3)
            print(f"startup_s={startup_seconds:.3f}", flush=True)

            backend_raw_dir = raw_dir / backend_id
            backend_raw_dir.mkdir(parents=True, exist_ok=True)
            for case_name in case_names:
                print(f"case={case_name}", flush=True)
                case_started = time.perf_counter()
                case_started_at = utc_now()
                case_result = run_case(
                    case_name,
                    cases_by_name[case_name],
                    variables,
                    base_url,
                    requests,
                    concurrency,
                    warmup,
                    timeout_seconds,
                    backend_raw_dir,
                )
                case_result["started_at"] = case_started_at
                case_result["ended_at"] = utc_now()
                case_result["total_wall_seconds"] = rounded(time.perf_counter() - case_started)
                backend_result["cases"].append(case_result)
                print(
                    "  ok={ok} errors={errors} p95={p95}ms p99={p99}ms rps={rps}".format(
                        ok=case_result["ok"],
                        errors=case_result["errors"],
                        p95=markdown_value(case_result["p95_ms"]),
                        p99=markdown_value(case_result["p99_ms"]),
                        rps=markdown_value(case_result["rps"]),
                    ),
                    flush=True,
                )
        except Exception as exc:  # noqa: BLE001 - one backend failure should not hide other candidates.
            backend_result["status"] = "failed"
            backend_result["error"] = str(exc)
            backend_result["startup_seconds"] = backend_result.get("startup_seconds")
            print(f"failed: {exc}", file=sys.stderr, flush=True)
        finally:
            stop_process(process)
            backend_result["ended_at"] = utc_now()
            backend_result["total_wall_seconds"] = rounded(time.perf_counter() - backend_started)

        add_backend_score(backend_result, workload, cases_by_name)
        run["backends"].append(backend_result)

    run["ended_at"] = utc_now()
    run["total_wall_seconds"] = rounded(time.perf_counter() - run_started)
    add_scientific_scores(run, workload, case_names, cases_by_name)
    write_results(output_dir, run)
    summary_path = write_summary(output_dir, run)

    print(f"\nResults: {summary_path}", flush=True)
    for backend in run["backends"]:
        print(
            "{backend}: status={status} scientific_ops/s={ops} scientific_score={scientific_score} decision_score={decision_score}".format(
                backend=backend["backend"]["id"],
                status=backend["status"],
                ops=markdown_value(backend.get("scientific_ops_per_second")),
                scientific_score=markdown_value(backend.get("scientific_score")),
                decision_score=markdown_value(backend.get("decision_score")),
            ),
            flush=True,
        )

    if not any(backend.get("status") == "ok" for backend in run["backends"]):
        return 1

    if args.fail_on_slo:
        failed_slo = any(
            (case.get("score") == 0.0 and float(cases_by_name[case["case"]].get("weight", 0)) > 0)
            for backend in run["backends"]
            for case in backend.get("cases", [])
        )
        if failed_slo:
            return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchmarkError as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        raise SystemExit(1)
