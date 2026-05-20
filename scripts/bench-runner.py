#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import platform
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
              timeout_seconds: float,
              request_index: int | None = None,
              variant_index: int | None = None) -> dict[str, Any]:
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
        "request_index": request_index,
        "variant_index": variant_index,
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


def run_requests(request_variants: list[dict[str, Any]],
                 total: int,
                 concurrency: int,
                 timeout_seconds: float,
                 variant_offset: int = 0) -> tuple[list[dict[str, Any]], float]:
    if total <= 0:
        return [], 0.0
    if not request_variants:
        raise BenchmarkError("benchmark case has no request variants")

    started = time.perf_counter()
    workers = max(1, min(concurrency, total))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                http_call,
                str(request_variants[(variant_offset + index) % len(request_variants)]["method"]),
                str(request_variants[(variant_offset + index) % len(request_variants)]["url"]),
                request_variants[(variant_offset + index) % len(request_variants)].get("body"),
                timeout_seconds,
                index,
                int(request_variants[(variant_offset + index) % len(request_variants)]["variant_index"]),
            )
            for index in range(total)
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
        writer = csv.DictWriter(
            stream,
            fieldnames=["iteration", "request_index", "variant_index", "elapsed_ms", "status", "app_ms", "error"],
        )
        writer.writeheader()
        for sample in samples:
            writer.writerow({
                "iteration": sample.get("iteration"),
                "request_index": sample.get("request_index"),
                "variant_index": sample.get("variant_index"),
                "elapsed_ms": rounded(float(sample["elapsed_ms"]), 3),
                "status": sample["status"],
                "app_ms": sample.get("app_ms"),
                "error": sample.get("error", ""),
            })


def prepare_request_variants(case_spec: dict[str, Any],
                             base_variables: dict[str, str],
                             case_variable_sets: list[dict[str, str]],
                             base_url: str) -> list[dict[str, Any]]:
    variants_source = case_variable_sets or [{}]
    variants: list[dict[str, Any]] = []
    for index, case_variables in enumerate(variants_source):
        variables = dict(base_variables)
        variables.update({str(key): str(value) for key, value in case_variables.items()})
        rendered = substitute(case_spec, variables)
        method = str(rendered.get("method", "GET")).upper()
        body = rendered.get("body")
        variants.append({
            "variant_index": index,
            "method": method,
            "url": case_url(base_url, str(rendered["path"])),
            "body": body.strip() if isinstance(body, str) else None,
            "variables": case_variables,
            "title": rendered.get("title"),
            "benchmark_family": rendered.get("benchmark_family", ""),
            "rendered": rendered,
        })
    return variants


def t_critical_95(sample_count: int) -> float:
    # Two-tailed t critical values for 95% confidence, indexed by degrees of freedom.
    by_degrees_of_freedom = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
        11: 2.201,
        12: 2.179,
        13: 2.160,
        14: 2.145,
        15: 2.131,
        16: 2.120,
        17: 2.110,
        18: 2.101,
        19: 2.093,
        20: 2.086,
        21: 2.080,
        22: 2.074,
        23: 2.069,
        24: 2.064,
        25: 2.060,
        26: 2.056,
        27: 2.052,
        28: 2.048,
        29: 2.045,
        30: 2.042,
    }
    if sample_count <= 1:
        return 0.0
    return by_degrees_of_freedom.get(sample_count - 1, 1.960)


def stats_with_ci(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "mean": None,
            "stddev": None,
            "cv_percent": None,
            "ci95_low": None,
            "ci95_high": None,
            "ci95_half_width": None,
        }
    mean = statistics.fmean(values)
    if len(values) == 1:
        return {
            "mean": rounded(mean),
            "stddev": 0.0,
            "cv_percent": 0.0,
            "ci95_low": rounded(mean),
            "ci95_high": rounded(mean),
            "ci95_half_width": 0.0,
        }
    stddev = statistics.stdev(values)
    half_width = t_critical_95(len(values)) * stddev / math.sqrt(len(values))
    return {
        "mean": rounded(mean),
        "stddev": rounded(stddev),
        "cv_percent": rounded((stddev / mean) * 100.0 if mean else 0.0, 2),
        "ci95_low": rounded(max(0.0, mean - half_width)),
        "ci95_high": rounded(mean + half_width),
        "ci95_half_width": rounded(half_width),
    }


def add_iteration_statistics(summary: dict[str, Any], iteration_results: list[dict[str, Any]]) -> None:
    rps_values = [
        float(item["rps"])
        for item in iteration_results
        if item.get("rps") is not None and int(item.get("errors") or 0) == 0
    ]
    p95_values = [
        float(item["p95_ms"])
        for item in iteration_results
        if item.get("p95_ms") is not None and int(item.get("errors") or 0) == 0
    ]
    summary["rps_iteration_stats"] = stats_with_ci(rps_values)
    summary["p95_ms_iteration_stats"] = stats_with_ci(p95_values)
    summary["rps_mean"] = summary["rps_iteration_stats"]["mean"]
    summary["rps_cv_percent"] = summary["rps_iteration_stats"]["cv_percent"]
    summary["rps_ci95_low"] = summary["rps_iteration_stats"]["ci95_low"]
    summary["rps_ci95_high"] = summary["rps_iteration_stats"]["ci95_high"]


def run_case(case_name: str,
             case_spec: dict[str, Any],
             variables: dict[str, str],
             case_variable_sets: list[dict[str, str]],
             base_url: str,
             requests: int,
             concurrency: int,
             warmup: int,
             iterations: int,
             timeout_seconds: float,
             raw_dir: pathlib.Path) -> dict[str, Any]:
    request_variants = prepare_request_variants(case_spec, variables, case_variable_sets, base_url)
    rendered = request_variants[0]["rendered"]

    if warmup > 0:
        run_requests(request_variants, warmup, concurrency, timeout_seconds)

    samples: list[dict[str, Any]] = []
    iteration_results: list[dict[str, Any]] = []
    total_wall_seconds = 0.0
    for iteration in range(1, max(1, iterations) + 1):
        iteration_samples, wall_seconds = run_requests(
            request_variants,
            requests,
            concurrency,
            timeout_seconds,
            variant_offset=(iteration - 1) * requests,
        )
        for sample in iteration_samples:
            sample["iteration"] = iteration
        iteration_summary = summarize_samples(iteration_samples, wall_seconds)
        iteration_summary["iteration"] = iteration
        iteration_results.append(iteration_summary)
        samples.extend(iteration_samples)
        total_wall_seconds += wall_seconds

    raw_path = raw_dir / f"{case_name}.csv"
    write_raw_csv(raw_path, samples)

    summary = summarize_samples(samples, max(total_wall_seconds, 0.001))
    summary.update({
        "case": case_name,
        "title": rendered.get("title", case_name),
        "benchmark_family": rendered.get("benchmark_family", ""),
        "raw_csv": str(raw_path.relative_to(raw_dir.parent)),
        "iterations": iteration_results,
        "iteration_count": len(iteration_results),
        "variant_count": len(request_variants),
        "variant_order": "deterministic_round_robin_with_iteration_offset",
        "uses_seed_variants": bool(case_variable_sets),
    })
    add_iteration_statistics(summary, iteration_results)

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


def load_seed_file(workload: dict[str, Any], variables: dict[str, str]) -> dict[str, Any]:
    seed_file = workload.get("dataset", {}).get("seed_file")
    if not seed_file:
        return {}

    path = resolve_path(str(substitute(seed_file, variables)))
    if not path.exists():
        return {"path": str(path), "loaded": False, "reason": "seed file does not exist"}

    with path.open("r", encoding="utf-8") as stream:
        seeds = json.load(stream)
    if not isinstance(seeds, dict):
        raise BenchmarkError(f"seed_file must contain a JSON object: {path}")

    for key, value in seeds.items():
        if value is not None and not isinstance(value, (dict, list)):
            variables[str(key)] = str(value)
    return {"path": str(path), "loaded": True, "content": seeds}


def case_seed_variants(seed_data: dict[str, Any], case_name: str) -> list[dict[str, str]]:
    content = seed_data.get("content")
    if not isinstance(content, dict):
        return []
    case_variables = content.get("case_variables")
    if not isinstance(case_variables, dict):
        return []
    entries = case_variables.get(case_name)
    if entries is None:
        entries = case_variables.get("*")
    if not isinstance(entries, list):
        return []

    variants: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        variant = {
            str(key): str(value)
            for key, value in entry.items()
            if value is not None and not isinstance(value, (dict, list))
        }
        if variant:
            variants.append(variant)
    return variants


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


def command_output(command: list[str], timeout_seconds: float = 5.0) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except Exception:  # noqa: BLE001 - provenance must not make the benchmark fail.
        return None
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    if result.returncode != 0:
        return None
    return output.splitlines()[0] if "\n" in output else output


def command_output_full(command: list[str], timeout_seconds: float = 5.0) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except Exception:  # noqa: BLE001 - provenance must not make the benchmark fail.
        return ""
    return ((result.stdout or "") + (result.stderr or "")).strip()


def file_fingerprint(path: pathlib.Path, hash_limit_bytes: int = 64 * 1024 * 1024) -> dict[str, Any]:
    resolved = resolve_path(path)
    if not resolved.exists():
        return {"path": str(resolved), "exists": False}
    stat = resolved.stat()
    result: dict[str, Any] = {
        "path": str(resolved),
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime_utc": dt.datetime.fromtimestamp(stat.st_mtime, dt.UTC).isoformat(timespec="seconds"),
    }
    if stat.st_size <= hash_limit_bytes or env_bool("BENCH_HASH_LARGE_FILES"):
        digest = hashlib.sha256()
        with resolved.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        result["sha256"] = digest.hexdigest()
    else:
        result["sha256"] = None
        result["sha256_reason"] = f"file is larger than {hash_limit_bytes} bytes; set BENCH_HASH_LARGE_FILES=true to hash it"
    return result


def collect_provenance(workload_path: pathlib.Path,
                       workload: dict[str, Any],
                       backend_configs: list[dict[str, Any]],
                       db_path: str,
                       seed_data: dict[str, Any],
                       case_names: list[str],
                       args: argparse.Namespace) -> dict[str, Any]:
    git_status = command_output_full(["git", "status", "--short"])
    memory_bytes = command_output(["sysctl", "-n", "hw.memsize"])
    cpu_model = command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
    backend_files = {
        str(backend.get("id")): file_fingerprint(pathlib.Path(str(backend.get("_path"))), hash_limit_bytes=1024 * 1024)
        for backend in backend_configs
        if backend.get("_path")
    }
    seed_path = seed_data.get("path")
    benchmark_cfg = workload.get("benchmark", {}) if isinstance(workload.get("benchmark"), dict) else {}
    references_cfg = workload.get("references")
    return {
        "kind": "graph-api benchmark campaign",
        "benchmark": benchmark_cfg,
        "official_benchmark_run": bool(benchmark_cfg.get("official_run", False)),
        "official_benchmark_note": (
            benchmark_cfg.get("official_run_note")
            or "FinBench-adapted API-level workload; not an audited LDBC FinBench driver run."
        ),
        "command_argv": sys.argv,
        "cwd": str(ROOT),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_model": cpu_model,
            "memory_bytes": int(memory_bytes) if memory_bytes and memory_bytes.isdigit() else memory_bytes,
            "python": sys.version.split()[0],
        },
        "git": {
            "commit": command_output(["git", "rev-parse", "HEAD"]),
            "branch": command_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
            "dirty": bool(git_status),
            "status_short": git_status,
        },
        "tools": {
            "java": command_output(["java", "-version"]),
            "maven_wrapper": file_fingerprint(ROOT / "mvnw", hash_limit_bytes=1024 * 1024),
            "docker": command_output(["docker", "--version"]),
            "docker_compose": command_output(["docker", "compose", "version"]),
            "duckdb": command_output(["duckdb", "--version"]),
        },
        "files": {
            "workload": file_fingerprint(workload_path, hash_limit_bytes=1024 * 1024),
            "dataset_db": file_fingerprint(pathlib.Path(db_path)),
            "seed_file": file_fingerprint(pathlib.Path(seed_path), hash_limit_bytes=1024 * 1024) if seed_path else None,
            "backend_configs": backend_files,
        },
        "selection": {
            "cases": case_names,
            "backends": [str(backend.get("id")) for backend in backend_configs],
            "allow_existing": bool(args.allow_existing),
        },
        "references": references_cfg if isinstance(references_cfg, list) else [
            {"name": "LDBC FinBench", "url": "https://ldbcouncil.org/benchmarks/finbench/"},
            {"name": "LDBC FinBench specification", "url": "https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf"},
        ],
    }


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
        "iteration_ops_per_second": [],
        "ops_per_second_mean": None,
        "ops_per_second_stddev": None,
        "ops_per_second_cv_percent": None,
        "ops_per_second_ci95_low": None,
        "ops_per_second_ci95_high": None,
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

    iteration_indexes: set[int] | None = None
    iterations_by_case: dict[str, dict[int, dict[str, Any]]] = {}
    for case_name in included_cases:
        case = cases.get(case_name)
        if case is None:
            continue
        by_iteration = {
            int(item.get("iteration")): item
            for item in case.get("iterations", [])
            if isinstance(item, dict) and item.get("iteration") is not None
        }
        iterations_by_case[case_name] = by_iteration
        indexes = set(by_iteration)
        iteration_indexes = indexes if iteration_indexes is None else iteration_indexes & indexes

    iteration_ops: list[float] = []
    for iteration in sorted(iteration_indexes or set()):
        iteration_reasons: list[str] = []
        measured_operations = 0
        measured_wall_seconds = 0.0
        for case_name in included_cases:
            case_iteration = iterations_by_case.get(case_name, {}).get(iteration)
            if case_iteration is None:
                iteration_reasons.append(f"case {case_name} has no iteration {iteration}")
                continue
            errors = int(case_iteration.get("errors") or 0)
            ok = int(case_iteration.get("ok") or 0)
            wall_seconds = float(case_iteration.get("wall_seconds") or 0.0)
            if errors > 0 or ok <= 0 or wall_seconds <= 0:
                iteration_reasons.append(f"case {case_name} iteration {iteration} is invalid")
                continue
            measured_operations += ok
            measured_wall_seconds += wall_seconds
        if not iteration_reasons and measured_operations > 0 and measured_wall_seconds > 0:
            iteration_ops.append(measured_operations / measured_wall_seconds)

    if iteration_ops:
        stats = stats_with_ci(iteration_ops)
        metric["iteration_ops_per_second"] = [rounded(value) for value in iteration_ops]
        metric["ops_per_second_mean"] = stats["mean"]
        metric["ops_per_second_stddev"] = stats["stddev"]
        metric["ops_per_second_cv_percent"] = stats["cv_percent"]
        metric["ops_per_second_ci95_low"] = stats["ci95_low"]
        metric["ops_per_second_ci95_high"] = stats["ci95_high"]

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
    lines.append(
        f"- requests: `{run['runner']['requests']}`, iterations: `{run['runner']['iterations']}`, "
        f"concurrency: `{run['runner']['concurrency']}`, warmup: `{run['runner']['warmup']}`"
    )
    provenance = run.get("provenance", {})
    git = provenance.get("git", {}) if isinstance(provenance, dict) else {}
    host = provenance.get("host", {}) if isinstance(provenance, dict) else {}
    if git:
        lines.append(
            f"- git: `{git.get('commit', 'n/a')}` branch `{git.get('branch', 'n/a')}`, dirty=`{git.get('dirty', 'n/a')}`"
        )
    if host:
        lines.append(f"- host: `{host.get('cpu_model') or host.get('processor') or host.get('machine')}` / `{host.get('platform')}`")
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
    lines.append("- repeated-run statistics: per-iteration transaction mix mean, 95% confidence interval and coefficient of variation")
    lines.append("- validity: `status=ok`, every included case executed, `errors=0`, positive throughput")
    lines.append("- included_cases: `" + " ".join(scientific_cfg.get("include_cases", [])) + "`")
    if scientific_cfg.get("description"):
        lines.append(f"- description: {scientific_cfg['description']}")
    lines.append("")

    lines.append("## Backend Scores")
    lines.append("")
    lines.append("| backend | adapter | status | scientific_valid | scientific_ops/s | iter_mean_ops/s | ci95_ops/s | cv_% | scientific_score | decision_score | startup_s | log |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |")
    for backend in run["backends"]:
        scientific = backend.get("scientific", {})
        ci_low = scientific.get("ops_per_second_ci95_low")
        ci_high = scientific.get("ops_per_second_ci95_high")
        ci_text = "n/a" if ci_low is None or ci_high is None else f"{markdown_value(ci_low)}..{markdown_value(ci_high)}"
        lines.append(
            "| {id} | {adapter} | {status} | {scientific_valid} | {scientific_ops} | {scientific_mean} | {ci95} | {cv} | {scientific_score} | {decision_score} | {startup} | {log} |".format(
                id=backend["backend"]["id"],
                adapter=backend["backend"].get("adapter_status", "implemented"),
                status=backend.get("status"),
                scientific_valid=markdown_value(backend.get("scientific_valid")),
                scientific_ops=markdown_value(backend.get("scientific_ops_per_second")),
                scientific_mean=markdown_value(scientific.get("ops_per_second_mean")),
                ci95=ci_text,
                cv=markdown_value(scientific.get("ops_per_second_cv_percent")),
                scientific_score=markdown_value(backend.get("scientific_score")),
                decision_score=markdown_value(backend.get("decision_score")),
                startup=markdown_value(backend.get("startup_seconds")),
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
    lines.append("| backend | case | family | variants | iterations | ok | errors | rps | rps_cv_% | avg_ms | p95_ms | p99_ms | app_p95_ms | decision_score |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for backend in run["backends"]:
        for case in backend.get("cases", []):
            lines.append(
                "| {backend} | {case} | {family} | {variants} | {iterations} | {ok} | {errors} | {rps} | {rps_cv} | {avg} | {p95} | {p99} | {app_p95} | {score} |".format(
                    backend=backend["backend"]["id"],
                    case=case["case"],
                    family=case.get("benchmark_family", ""),
                    variants=markdown_value(case.get("variant_count")),
                    iterations=markdown_value(case.get("iteration_count")),
                    ok=case["ok"],
                    errors=case["errors"],
                    rps=markdown_value(case["rps"]),
                    rps_cv=markdown_value(case.get("rps_cv_percent")),
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

    if provenance:
        lines.append("## Benchmark Provenance")
        lines.append("")
        files = provenance.get("files", {}) if isinstance(provenance, dict) else {}
        dataset_file = files.get("dataset_db", {}) if isinstance(files, dict) else {}
        seed_file = files.get("seed_file", {}) if isinstance(files, dict) else {}
        lines.append(f"- official_benchmark_run: `{provenance.get('official_benchmark_run')}`")
        lines.append(f"- note: {provenance.get('official_benchmark_note')}")
        if dataset_file:
            lines.append(
                "- dataset_db_size_bytes: `{size}`, dataset_db_sha256: `{sha}`".format(
                    size=markdown_value(dataset_file.get("size_bytes")),
                    sha=markdown_value(dataset_file.get("sha256")),
                )
            )
        if seed_file:
            lines.append(
                "- seed_file: `{path}`, sha256: `{sha}`".format(
                    path=seed_file.get("path"),
                    sha=markdown_value(seed_file.get("sha256")),
                )
            )
        lines.append("- raw_samples: `raw/<backend>/<case>.csv` contains every measured request with iteration and seed variant index")
        lines.append("")

    lines.append("## Interpretation Rules")
    lines.append("")
    lines.append("- Scientific ranking uses throughput of the predeclared included transaction cases after validity gates; it does not use SLO weights.")
    lines.append("- `scientific_score` is only normalization against the best valid backend in this run; the primary value is `scientific_ops/s`.")
    lines.append("- `decision_score` is an expert-defined utility function for product trade-offs, weighted by TOML case weights and p95 SLOs.")
    lines.append("- Raw p50/p95/p99/rps/errors are the primary benchmark facts; weighted decision score is not an official LDBC metric.")
    lines.append("- Multiple seed variants are taken from the prepared dataset when `case_variables` exist in the seed file; they are not handwritten per backend.")
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
    (output_dir / "manifest.json").write_text(
        json.dumps(run.get("provenance", {}), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    with (output_dir / "cases.csv").open("w", newline="", encoding="utf-8") as stream:
        fieldnames = [
            "backend", "case", "ok", "errors", "rps", "avg_ms", "p50_ms", "p95_ms",
            "p99_ms", "min_ms", "max_ms", "app_p95_ms", "wall_seconds", "iteration_count",
            "variant_count", "rps_mean", "rps_cv_percent", "rps_ci95_low", "rps_ci95_high",
            "score", "decision_score",
        ]
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for backend in run["backends"]:
            for case in backend.get("cases", []):
                row = {key: case.get(key) for key in fieldnames}
                row["backend"] = backend["backend"]["id"]
                row["decision_score"] = case.get("score")
                writer.writerow(row)

    with (output_dir / "backend-summary.csv").open("w", newline="", encoding="utf-8") as stream:
        fieldnames = [
            "backend", "status", "scientific_valid", "scientific_ops_per_second",
            "scientific_ops_per_second_mean", "scientific_ops_per_second_stddev",
            "scientific_ops_per_second_cv_percent", "scientific_ops_per_second_ci95_low",
            "scientific_ops_per_second_ci95_high", "scientific_score", "decision_score",
            "startup_seconds", "total_wall_seconds", "error",
        ]
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for backend in run["backends"]:
            scientific = backend.get("scientific", {})
            writer.writerow({
                "backend": backend["backend"]["id"],
                "status": backend.get("status"),
                "scientific_valid": backend.get("scientific_valid"),
                "scientific_ops_per_second": backend.get("scientific_ops_per_second"),
                "scientific_ops_per_second_mean": scientific.get("ops_per_second_mean"),
                "scientific_ops_per_second_stddev": scientific.get("ops_per_second_stddev"),
                "scientific_ops_per_second_cv_percent": scientific.get("ops_per_second_cv_percent"),
                "scientific_ops_per_second_ci95_low": scientific.get("ops_per_second_ci95_low"),
                "scientific_ops_per_second_ci95_high": scientific.get("ops_per_second_ci95_high"),
                "scientific_score": backend.get("scientific_score"),
                "decision_score": backend.get("decision_score"),
                "startup_seconds": backend.get("startup_seconds"),
                "total_wall_seconds": backend.get("total_wall_seconds"),
                "error": backend.get("error"),
            })


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
    parser.add_argument("--iterations", type=int, default=int(os.environ["BENCH_ITERATIONS"]) if os.environ.get("BENCH_ITERATIONS") else None)
    parser.add_argument("--timeout-seconds", type=float, default=float(os.environ["BENCH_CURL_MAX_TIME"]) if os.environ.get("BENCH_CURL_MAX_TIME") else None)
    parser.add_argument("--app-port", type=int, default=int(os.environ["BENCH_APP_PORT"]) if os.environ.get("BENCH_APP_PORT") else None)
    parser.add_argument("--db-path", default=os.environ.get("BENCH_DB"))
    parser.add_argument("--scale", default=os.environ.get("BENCH_SCALE"))
    parser.add_argument("--output-dir", default=os.environ.get("BENCH_LOG_DIR", "target/bench"))
    parser.add_argument("--allow-existing", action="store_true", default=env_bool("BENCH_ALLOW_EXISTING"))
    parser.add_argument("--fail-on-slo", action="store_true", default=env_bool("BENCH_FAIL_ON_SLO"))
    parser.add_argument("--require-all-backends", action="store_true", default=env_bool("BENCH_REQUIRE_ALL_BACKENDS"))
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
    iterations = args.iterations or int(runner_cfg.get("iterations", 1))
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

    seed_data = load_seed_file(workload, variables)
    if bool(dataset.get("seed_file_required", False)) and not seed_data.get("loaded"):
        raise BenchmarkError(f"required seed file was not loaded: {seed_data.get('path')}")
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
            "iterations": iterations,
            "concurrency": concurrency,
            "warmup": warmup,
            "timeout_seconds": timeout_seconds,
            "app_port": app_port,
        },
        "score_profiles": workload.get("score_profiles", {}),
        "variables": variables,
        "seed_data": seed_data,
        "provenance": collect_provenance(workload_path, workload, backends, db_path, seed_data, case_names, args),
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
                    case_seed_variants(seed_data, case_name),
                    base_url,
                    requests,
                    concurrency,
                    warmup,
                    iterations,
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
            "{backend}: status={status} scientific_ops/s={ops} iter_mean={mean} ci95={low}..{high} scientific_score={scientific_score} decision_score={decision_score}".format(
                backend=backend["backend"]["id"],
                status=backend["status"],
                ops=markdown_value(backend.get("scientific_ops_per_second")),
                mean=markdown_value(backend.get("scientific", {}).get("ops_per_second_mean")),
                low=markdown_value(backend.get("scientific", {}).get("ops_per_second_ci95_low")),
                high=markdown_value(backend.get("scientific", {}).get("ops_per_second_ci95_high")),
                scientific_score=markdown_value(backend.get("scientific_score")),
                decision_score=markdown_value(backend.get("decision_score")),
            ),
            flush=True,
        )

    if not any(backend.get("status") == "ok" for backend in run["backends"]):
        return 1
    if args.require_all_backends and any(
        backend.get("status") != "ok" or not backend.get("scientific", {}).get("valid")
        for backend in run["backends"]
    ):
        return 3

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
