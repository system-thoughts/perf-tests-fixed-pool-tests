#!/usr/bin/env python3
"""Exercise independent fixed-pool workers with real k6 processes.

This acceptance fixture is not a benchmark orchestrator or result aggregator.
It keeps all requests, logs and failures in a new --work-dir. The controlled
proxy does not establish Dragonfly readiness or performance capacity.
"""
import argparse
import copy
from contextlib import contextmanager, ExitStack
import hashlib
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

from integration import invoke, managed_process, read, replace_once, running_proxy, runtime_k6, write


@contextmanager
def prepare(root, address=("127.0.0.1", 0), public_host="127.0.0.1"):
    root.mkdir(parents=True, exist_ok=False)
    for name in ("data-command", "prepare-command"):
        (root / name).mkdir()
    data = invoke(["proxy-bench-data-prepare", "--size", "65536", "--count", "2",
                   "--root", str(root / "data"), "--base-url", "http://fixture.invalid"], root / "data-command")
    rules = root / "proxy-rules.json"
    write(rules, {"schema_version": 1, "filtered_query_params": [], "rewrite_url": False, "use_tls": False})
    with running_proxy(root / "data", address) as server:
        proxy = f"http://{public_host}:{server.server_port}"
        config = root / "target-config.yaml"
        write(config, {"schema_version": 1, "target": {"ref": "multi-worker-fixture", "proxy": proxy,
                       "expected_server_ip": "192.0.2.1", "instance_ref": "controlled-v1"},
                       "proxy_rules_ref": {"path": rules.name, "sha256": hashlib.sha256(rules.read_bytes()).hexdigest()}})
        invoke(["proxy-bench-seed-cache-prepare", "--manifest", data["manifest"]["path"],
                "--target-config", str(config), "--out", str(root / "target")], root / "prepare-command")
        yield server, {"PATH": os.environ["PATH"], "HOME": str(root), "XDG_CONFIG_HOME": str(root / "clean-config"), "MANIFEST": data["manifest"]["path"],
                       "TARGET_CONFIG": str(config), "TARGET_PREPARATION": str(root / "target/target-preparation.json"),
                       "HTTP_PROXY": proxy, "RATE": "17", "DURATION": "5s", "TIMEOUT": "2s",
                       "PRE_ALLOCATED_VUS": "4", "MAX_VUS": "8"}


def instrument(scripts, destination, missing_summary=False):
    """Copy only the runtime scripts; request labels exist in this fixture only."""
    destination.mkdir()
    for name in ("benchmark.js", "run-benchmark.sh", "lib/plan.js", "lib/summary.js"):
        target = destination / name
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(scripts / name, target)
    source = (destination / "benchmark.js").read_text()
    old = "headers: { ...spec.headers, Range: request.range },"
    source = replace_once(source, old, "headers: { ...spec.headers, Range: request.range, "
                            "'X-Acceptance-Worker': String(spec.worker_id), "
                            "'X-Acceptance-Iteration': String(exec.scenario.iterationInInstance) },")
    if missing_summary:
        source = replace_once(source, "return summaryOutputs(data, spec);", "return {};")
    (destination / "benchmark.js").write_text(source)
    write(destination / "instrumentation.json", {"request_labels_only": not missing_summary,
          "suppress_summary": missing_summary,
          "benchmark_sha256": hashlib.sha256(source.encode()).hexdigest()})


def check_worker(directory, records, plan_path, worker_id, expect="completed", require_loops=False):
    spec = read(directory / "worker-spec.json")
    expected_rates = [9, 8] if read(plan_path)["worker_count"] == 2 else [7, 6, 4]
    assert spec["worker_id"] == worker_id and spec["rate"] == expected_rates[worker_id], spec
    plan = read(plan_path)
    shard = read(plan_path.parent / plan["shards"][worker_id]["path"])
    objects = read(plan_path.parent / plan["objects_file"]["path"])
    seen = []
    for record in records:
        headers = {k.lower(): v for k, v in record["headers"].items()}
        assert int(headers["x-acceptance-worker"]) == worker_id
        k = int(record["iteration"])
        g = shard[k % len(shard)]
        start = g % plan["regions_per_object"] * plan["range_bytes"]
        assert record["url"] == objects[g // plan["regions_per_object"]]
        assert record["range"] == f"bytes={start}-{start + plan['range_bytes'] - 1}"
        assert record["method"] == "GET" and record["version"] == "HTTP/1.1"
        assert headers["accept-encoding"] == "identity"
        seen.append(k)
    summary = read(directory / "worker-summary.json")
    assert summary["execution_state"] == expect, summary
    own_result = summary["workers"][0]
    metrics = own_result["metrics"]
    assert summary["aggregate"] is None and summary["coverage"] is None
    if expect == "completed":
        assert summary["result_complete"] and summary["errors"] == [], summary
        count = summary["workers"][0]["plan_consumed"]["count"]
        assert sorted(seen) == list(range(int(count))), (worker_id, seen, count)
        assert metrics["REQUESTS_TOTAL"] == count
        native = read(directory / "k6-summary.json")["native_summary"]
        for key, metric, field in (("ACTUAL_QPS", "http_reqs", "rate"), ("P99_MS", "http_req_duration", "p(99)"),
                                   ("P50_MS", "http_req_duration", "med")):
            assert metrics[key] == native["metrics"][metric]["values"][field]
        assert metrics["TOOL_DURATION_S"] == native["state"]["testRunDurationMs"] / 1000
        assert metrics["SUCCESS_PCT"] == 100
        if require_loops:
            assert count >= 3 * len(shard), (worker_id, count, len(shard))
            assert own_result["coverage"]["unique_regions"] == len(shard)
            assert own_result["coverage"]["ratio"] == 1
    elif expect == "interrupted":
        assert own_result["coverage"] is None
    else:
        assert not summary["result_complete"] and own_result["coverage"] is None
    return {"worker_id": worker_id, "state": expect, "metrics": metrics,
            "wire_requests": len(records), "coverage": own_result["coverage"]}


def check_timing(records, workers, name, interrupted_at=None):
    windows = []
    for worker in range(workers):
        times = [r["received_at"] for r in records if r["headers"].get("X-Acceptance-Worker") == str(worker)]
        windows.append({"first": min(times), "last": max(times)} if times else None)
    if name in ("loops", "unequal-shards"):
        assert all(windows), (name, windows)
        assert max(w["first"] for w in windows) < min(w["last"] for w in windows), (name, windows)
    if name == "staggered":
        assert all(windows) and windows[0]["last"] < windows[1]["first"], windows
    if name == "interrupt-peer":
        assert interrupted_at is not None
        assert windows[0] and windows[0]["last"] > interrupted_at, (windows, interrupted_at)
    return windows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    for name in ("k6", "proxy-bench-worker-support", "proxy-bench-data-prepare",
                 "proxy-bench-plan-prepare", "proxy-bench-seed-cache-prepare"):
        assert shutil.which(name), f"missing {name} on PATH"
    root = args.work_dir.resolve()
    scripts = Path(__file__).resolve().parents[1]
    completed = []
    with prepare(root) as (server, base):
        k6_version = runtime_k6(root, scripts)
        for name, workers in (("loops", 2), ("unequal-shards", 3), ("invalid-peer", 2),
                              ("interrupt-peer", 2), ("missing-summary", 2), ("dropped", 2), ("staggered", 2)):
            case = root / name
            case.mkdir()
            (case / "plan-command").mkdir()
            result = invoke(["proxy-bench-plan-prepare", "--manifest", base["MANIFEST"], "--range", "16384",
                             "--workers", str(workers), "--plan-dir", str(root / "plans")], case / "plan-command")
            plan_path = Path(result["plan"]["path"])
            plan = read(plan_path)
            shards = [set(read(plan_path.parent / s["path"])) for s in plan["shards"]]
            assert set.union(*shards) == set(range(8)) and sum(map(len, shards)) == 8
            server.mode = "slow"
            offset = len(server.records)
            interrupted_at = None
            processes, directories = [], []
            with ExitStack() as resources:
                for worker in range(workers):
                    directory = case / str(worker)
                    directory.mkdir()
                    instrument(scripts, directory / "scripts", name == "missing-summary" and worker == 1)
                    env = {**base, "PLAN": str(plan_path), "WORKER_ID": str(worker), "RUN_ID": name,
                           "RESULT_DIR": str(directory / "results")}
                    if name == "invalid-peer" and worker == 1:
                        env["WORKER_ID"] = str(workers)
                    if name == "dropped":
                        env.update(PRE_ALLOCATED_VUS="1", MAX_VUS="1")
                    if name == "staggered" and worker == 1:
                        assert processes[0].wait(timeout=40) == 0
                    log = resources.enter_context((directory / "worker.log").open("w"))
                    directories.append(directory)
                    write(directory / "environment.json", env)
                    processes.append(resources.enter_context(managed_process(
                        ["/bin/sh", str(directory / "scripts/run-benchmark.sh")],
                        env=env, stdout=log, stderr=subprocess.STDOUT)))
                if name == "interrupt-peer":
                    deadline = time.monotonic() + 30
                    while not any(r["headers"].get("X-Acceptance-Worker") == "1" for r in server.records[offset:]):
                        assert time.monotonic() < deadline and processes[1].poll() is None
                        time.sleep(0.05)
                    time.sleep(0.5)
                    processes[1].send_signal(signal.SIGTERM)
                    interrupted_at = time.time()
                    write(case / "interruption.json", {"signal_sent_at": interrupted_at})
                codes = [p.wait(timeout=45) for p in processes]
            expected = [0] * workers
            if name in ("invalid-peer", "interrupt-peer", "missing-summary"):
                expected[1] = {"invalid-peer": 2, "interrupt-peer": 130, "missing-summary": 4}[name]
            assert codes == expected, (name, codes)
            records = copy.deepcopy(server.records[offset:])
            write(case / "requests.json", records)
            windows = check_timing(records, workers, name, interrupted_at)
            write(case / "request-windows.json", windows)
            outputs = []
            for worker, directory in enumerate(directories):
                own = [r for r in records if r["headers"].get("X-Acceptance-Worker") == str(worker)]
                if name == "invalid-peer" and worker == 1:
                    assert own == [] and not (directory / "results/worker-spec.json").exists()
                    continue
                state = "interrupted" if name == "interrupt-peer" and worker == 1 else "unknown" if name == "missing-summary" and worker == 1 else "completed"
                outputs.append(check_worker(directory / "results", own, plan_path, worker, state,
                                            name in ("loops", "unequal-shards")))
                inspect_log = directory / "inspect-command"
                inspect_log.mkdir()
                options = invoke(["k6", "inspect",
                                  "--env", f"RESULT_DIR={directory / 'results'}",
                                  str(directory / "scripts/benchmark.js")], inspect_log)
                executor = options["scenarios"]["fixed_pool"]
                expected_rate = ([9, 8] if workers == 2 else [7, 6, 4])[worker]
                assert executor["executor"] == "constant-arrival-rate" and executor["timeUnit"] == "1s", executor
                assert executor["rate"] == expected_rate, executor
            runs = [read(d / "results/run-spec.json") for d in directories if (d / "results/run-spec.json").exists()]
            assert all("k6" not in r["tool_versions"] for r in runs)
            for directory in directories:
                summary = directory / "results/k6-summary.json"
                if summary.exists():
                    assert "k6_version" not in read(summary)
            rates = [a["rate"] for a in runs[0]["workers"]]
            assert rates == ([9, 8] if workers == 2 else [7, 6, 4]), rates
            assert all([a["rate"] for a in r["workers"]] == rates and r["total_rate"] == 17 for r in runs)
            if name in ("loops", "unequal-shards", "dropped", "staggered", "interrupt-peer"):
                assert all(r == runs[0] for r in runs), name
            if name == "dropped":
                assert all(o["metrics"]["DROPPED_ITERATIONS"] > 0 for o in outputs)
            completed.append({"case": name, "exit_codes": codes, "rates": rates, "workers": outputs})
            write(root / "completed-cases.json", completed)
            print(f"PASS {name}: {codes}, rates={rates}", flush=True)
    write(root / "result.json", {"k6_version": k6_version, "cases": completed, "production_runtime_modified": False})


if __name__ == "__main__":
    main()
