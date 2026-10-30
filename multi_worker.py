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
from delivery import record_candidate

from integration import invoke, managed_process, read, replace_once, running_proxy, runtime_k6, worker_records, write


@contextmanager
def prepare(root, delivery, address=("127.0.0.1", 0), public_host="127.0.0.1", object_count=2):
    root.mkdir(parents=True, exist_ok=False)
    record_candidate(delivery, root)
    for name in ("data-command", "prepare-command"):
        (root / name).mkdir()
    data = invoke(["proxy-bench-prepare", "data", "--size", "65536", "--count", str(object_count),
                   "--root", str(root / "data"), "--base-url", "http://fixture.invalid"], root / "data-command")
    with running_proxy(root / "data", address) as server:
        proxy = f"http://{public_host}:{server.server_port}"
        config = root / "target-config.yaml"
        write(config, {"schema_version": 1, "target": {"ref": "multi-worker-fixture", "proxy": proxy,
                       "expected_server_ip": "192.0.2.1"}})
        invoke(["proxy-bench-prepare", "seed-cache", "--manifest", data["manifest"]["path"],
                "--target-config", str(config), "--out", str(root / "target")], root / "prepare-command")
        yield server, {"PATH": os.environ["PATH"], "HOME": str(root), "XDG_CONFIG_HOME": str(root / "clean-config"), "MANIFEST": data["manifest"]["path"],
                       "TARGET_CONFIG": str(config), "TARGET_PREPARATION": str(root / "target/target-preparation.json"),
                       "HTTP_PROXY": proxy, "RATE": "17", "DURATION": "5s", "TIMEOUT": "2s",
                       "PRE_ALLOCATED_VUS": "4", "MAX_VUS": "8"}


def instrument(scripts, destination):
    """Copy only the runtime scripts; request labels exist in this fixture only."""
    destination.mkdir()
    shutil.copytree(scripts.parent / "lib", destination.parent / "lib")
    for name in ("benchmark.js", "run-benchmark.sh", "lib/plan.js", "lib/summary.js"):
        target = destination / name
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(scripts / name, target)
    source = (destination / "benchmark.js").read_text()
    old = "headers: { ...spec.headers, Range: request.range },"
    source = replace_once(source, old, "headers: { ...spec.headers, Range: request.range, "
                            "'X-Acceptance-Worker': String(spec.worker_id), "
                            "'X-Acceptance-Iteration': String(exec.scenario.iterationInInstance) },")
    (destination / "benchmark.js").write_text(source)
    write(destination / "instrumentation.json", {"request_labels_only": True,
          "benchmark_sha256": hashlib.sha256(source.encode()).hexdigest()})


def check_worker(directory, records, plan_path, worker_id, expect="completed", require_loops=False):
    logged = worker_records(directory / "worker.log")
    spec = logged["WORKER_SPEC_V1"]
    expected_rates = [9, 8] if read(plan_path)["worker_count"] == 2 else [7, 6, 4]
    assert spec["worker_id"] == worker_id and spec["rate"] == expected_rates[worker_id], spec
    plan = read(plan_path)
    shard = read(plan_path.parent / plan["shards"][worker_id]["path"])
    objects = [o["url"] for o in read(Path(spec["manifest"]["path"]))["objects"]]
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
    execution = logged["WORKER_EXIT_V1"]
    assert execution["termination_reason"] == expect, execution
    native = logged.get("K6_SUMMARY_V1")
    metrics = native["native_summary"]["metrics"] if native else None
    if expect == "completed":
        assert execution["exit_code"] == 0 and native is not None
        count = metrics["plan_consumed"]["values"]["count"]
        assert sorted(seen) == list(range(int(count))), (worker_id, seen, count)
        assert metrics["http_reqs"]["values"]["count"] == count
        assert metrics["http_req_failed"]["values"]["rate"] == 0
        if require_loops:
            assert count >= 3 * len(shard), (worker_id, count, len(shard))
            assert len({k % len(shard) for k in seen}) == len(shard)
    else:
        assert execution["exit_code"] != 0
    assert not list(directory.glob("proxy-bench-fixed-pool.*")), "temporary input directory leaked"
    assert not (directory / "results").exists()
    return {"worker_id": worker_id, "state": expect, "metrics": metrics,
            "wire_requests": len(records), "raw_k6_exit": execution["exit_code"]}


def check_timing(records, workers, name, interrupted_at=None):
    windows = []
    for worker in range(workers):
        times = [r["received_at"] for r in records if r["headers"].get("X-Acceptance-Worker") == str(worker)]
        windows.append({"first": min(times), "last": max(times)} if times else None)
    if name == "MULTI-01":
        assert all(windows), (name, windows)
        assert max(w["first"] for w in windows) < min(w["last"] for w in windows), (name, windows)
    if name == "MULTI-02":
        assert interrupted_at is not None
        assert windows[0] and windows[0]["last"] > interrupted_at, (windows, interrupted_at)
    return windows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--delivery", type=Path, required=True)
    args = parser.parse_args()
    if read(args.delivery / "delivery.json").get("engine") == "vegeta":
        from vegeta import multi
        multi(args)
        return
    root = args.work_dir.resolve()
    scripts = args.delivery.resolve() / "runtime"
    completed = []
    with prepare(root, args.delivery) as (server, base):
        k6_version = runtime_k6(root)
        for name, workers in (("MULTI-01", 3), ("MULTI-02", 2)):
            case = root / name
            case.mkdir()
            (case / "plan-command").mkdir()
            result = invoke(["proxy-bench-prepare", "plan", "--manifest", base["MANIFEST"], "--range", "16384",
                             "--workers", str(workers), "--out", str(case / "plans")], case / "plan-command")
            plan_path = Path(result["plan"]["path"])
            plan = read(plan_path)
            server.mode = "slow"
            offset = len(server.records)
            interrupted_at = None
            processes, directories = [], []
            with ExitStack() as resources:
                for worker in range(workers):
                    directory = case / str(worker)
                    directory.mkdir()
                    instrument(scripts, directory / "scripts")
                    env = {**base, "PLAN": str(plan_path), "WORKER_ID": str(worker), "RUN_ID": name,
                           "TMPDIR": str(directory)}
                    log = resources.enter_context((directory / "worker.log").open("w"))
                    directories.append(directory)
                    write(directory / "environment.json", env)
                    processes.append(resources.enter_context(managed_process(
                        ["/bin/sh", str(directory / "scripts/run-benchmark.sh")],
                        env=env, stdout=log, stderr=subprocess.STDOUT)))
                if name == "MULTI-02":
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
            if name == "MULTI-02":
                expected[1] = 130
            assert codes == expected, (name, codes)
            records = copy.deepcopy(server.records[offset:])
            write(case / "requests.json", records)
            windows = check_timing(records, workers, name, interrupted_at)
            write(case / "request-windows.json", windows)
            outputs = []
            for worker, directory in enumerate(directories):
                own = [r for r in records if r["headers"].get("X-Acceptance-Worker") == str(worker)]
                state = "interrupted" if name == "MULTI-02" and worker == 1 else "completed"
                outputs.append(check_worker(directory, own, plan_path, worker, state,
                                            name == "MULTI-01"))
                logged = worker_records(directory / "worker.log")
                if "K6_SUMMARY_V1" in logged:
                    executor = logged["K6_SUMMARY_V1"]["effective_options"]["scenarios"]["fixed_pool"]
                    expected_rate = ([9, 8] if workers == 2 else [7, 6, 4])[worker]
                    assert executor["executor"] == "constant-arrival-rate" and executor["timeUnit"] == "1s", executor
                    assert executor["rate"] == expected_rate, executor
            runs = [worker_records(d / "worker.log")["RUN_SPEC_V1"] for d in directories]
            assert all("k6" not in r["tool_versions"] for r in runs)
            rates = [a["rate"] for a in runs[0]["workers"]]
            assert rates == ([9, 8] if workers == 2 else [7, 6, 4]), rates
            assert all([a["rate"] for a in r["workers"]] == rates and r["total_rate"] == 17 for r in runs)
            if name in ("MULTI-01", "MULTI-02"):
                assert all(r == runs[0] for r in runs), name
            completed.append({"case": name, "exit_codes": codes, "rates": rates, "workers": outputs})
            write(root / "completed-cases.json", completed)
            print(f"PASS {name}: {codes}, rates={rates}", flush=True)
    write(root / "result.json", {"k6_version": k6_version, "cases": completed, "production_runtime_modified": False})


if __name__ == "__main__":
    main()
