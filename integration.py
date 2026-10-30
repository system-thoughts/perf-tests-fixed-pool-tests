#!/usr/bin/env python3
"""Run the shipped worker with the image's k6 against a controlled HTTP proxy.

Requires preparation binaries, worker-support and k6 on PATH. All inputs,
results and logs are retained in a new --work-dir. This protocol fixture
cannot establish real Dragonfly semantics or performance capacity.
"""
import argparse
import copy
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from delivery import record_candidate


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, directory, address=("127.0.0.1", 0)):
        super().__init__(address, Handler)
        self.directory, self.mode, self.records = directory, "normal", []
        self.lock = threading.Lock()
        self.concurrent_gate = threading.Barrier(2)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_GET(self):
        server = self.server
        body = (server.directory / urlsplit(self.path).path.lstrip("/")).read_bytes()
        range_value = self.headers.get("Range")
        start, end, status = 0, len(body) - 1, 200
        if range_value:
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_value)
            assert match, range_value
            start, end = map(int, match.groups())
            assert 0 <= start <= end < len(body)
            status = 206
        with server.lock:
            mode = server.mode
            record = {"url": self.path, "method": "GET", "version": self.request_version,
                      "received_at": time.time(), "range": range_value, "headers": dict(self.headers),
                      "iteration": self.headers.get("X-Acceptance-Iteration", self.headers.get("X-Vegeta-Seq")),
                      "vu": self.headers.get("X-Acceptance-VU", str(self.client_address[1])), "mode": mode}
            server.records.append(record)
        # Hold only the first two iterations until both requests are in flight.
        if mode == "concurrent" and record["iteration"] in ("0", "1"):
            try:
                server.concurrent_gate.wait(timeout=2)
                record["gate_passed"] = True
            except threading.BrokenBarrierError:
                record["gate_passed"] = False
        if mode in ("slow", "timeout"):
            time.sleep(0.4 if mode == "timeout" else 0.25)
        record["responded_at"] = time.time()
        payload = body[start:end + 1]
        if mode == "wrong-status":
            status = 200
        if mode == "redirect":
            status = 302
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Dragonfly-Task-ID", hashlib.sha256(self.path.encode()).hexdigest())
            self.send_header("X-Dragonfly-Server-IP", "192.0.2.1")
            self.send_header("X-Dragonfly-Task-Download-Finished", "true")
            if range_value:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            if mode == "redirect":
                self.send_header("Location", "http://fixture.invalid/must-not-follow")
            self.end_headers()
            if mode == "truncated":
                self.wfile.write(payload[:len(payload) // 2])
                self.wfile.flush()
                self.close_connection = True
            else:
                self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    if path.exists():
        # Tamper only acceptance-owned copies; published plan files are read-only.
        path.chmod(0o600)
    path.write_text(json.dumps(value, separators=(",", ":")) + "\n")


def stop_process(process):
    """Stop only the session created for this test, including shell children."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


@contextmanager
def managed_process(args, **kwargs):
    process = subprocess.Popen(args, start_new_session=True, **kwargs)
    try:
        yield process
    finally:
        stop_process(process)


def run_command(args, timeout=60, **kwargs):
    with managed_process(args, **kwargs) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_process(process)
            stdout, stderr = process.communicate(timeout=5)
            raise subprocess.TimeoutExpired(args, timeout, output=stdout, stderr=stderr) from None
        return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)


@contextmanager
def running_proxy(directory, address=("127.0.0.1", 0)):
    server = Proxy(directory, address)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def replace_once(source, old, new):
    assert source.count(old) == 1, f"test instrumentation anchor changed: {old}"
    return source.replace(old, new)


def invoke(args, directory):
    try:
        completed = run_command(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as error:
        (directory / "stdout.log").write_text(error.output or "")
        (directory / "stderr.log").write_text(error.stderr or "")
        write(directory / "command.json", {"argv": [str(a) for a in args], "timed_out": True,
                                          "timeout_seconds": error.timeout})
        raise
    (directory / "stdout.log").write_text(completed.stdout)
    (directory / "stderr.log").write_text(completed.stderr)
    write(directory / "command.json", {"argv": [str(a) for a in args], "exit_code": completed.returncode})
    assert completed.returncode == 0, (args, completed.returncode, completed.stderr, completed.stdout)
    return json.loads(completed.stdout)


def runtime_k6(root):
    """Record the engine extracted from the actual worker image, without a version gate."""
    version = run_command(["k6", "version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    write(root / "runtime-k6.json", {"argv": ["k6", "version"],
                                    "exit_code": version.returncode, "stdout": version.stdout,
                                    "stderr": version.stderr})
    assert version.returncode == 0, version
    return version.stdout.strip()


def worker_records(path):
    """Read the worker's log records without reconstructing a derived report."""
    records = {}
    prefix = "PROXY_BENCH_FIXED_POOL_"
    for line in Path(path).read_text().splitlines():
        if line.startswith(prefix):
            kind, value = line[len(prefix):].split(" ", 1)
            assert kind not in records, f"duplicate log record: {kind}"
            records[kind] = json.loads(value)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--delivery", type=Path, required=True)
    args = parser.parse_args()
    if read(args.delivery / "delivery.json").get("engine") == "vegeta":
        from vegeta import single
        single(args)
        return
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    record_candidate(args.delivery, root)
    scripts = args.delivery.resolve() / "runtime"
    for name in ("k6", "proxy-bench-worker-support", "proxy-bench-prepare"):
        assert shutil.which(name), f"missing {name} on PATH"
    k6_version = runtime_k6(root)
    data_log = root / "data-command"
    data_log.mkdir()
    data = invoke(["proxy-bench-prepare", "data", "--size", "65536", "--count", "2",
                   "--root", str(root / "data"), "--base-url", "http://fixture.invalid"], data_log)
    manifest = Path(data["manifest"]["path"])
    with running_proxy(root / "data") as server:
        proxy_url = f"http://127.0.0.1:{server.server_port}"
        target = {"schema_version": 1, "target": {"ref": "controlled-proxy", "proxy": proxy_url,
                  "expected_server_ip": "192.0.2.1"}}
        config = root / "target-config.yaml"
        write(config, target)
        plan_log = root / "plan-command"
        plan_log.mkdir()
        plan_result = invoke(["proxy-bench-prepare", "plan", "--manifest", str(manifest), "--range", "16384",
                              "--out", str(root / "plans")], plan_log)
        plan_path = Path(plan_result["plan"]["path"])
        plan = read(plan_path)
        shard = read(plan_path.parent / plan["shards"][0]["path"])
        objects = [o["url"] for o in read(manifest)["objects"]]
        prepare_log = root / "prepare-command"
        prepare_log.mkdir()
        invoke(["proxy-bench-prepare", "seed-cache", "--manifest", str(manifest), "--target-config", str(config),
                "--out", str(root / "target")], prepare_log)
        base = {"PATH": os.environ["PATH"], "HOME": str(root), "XDG_CONFIG_HOME": str(root / "clean-config"),
                "MANIFEST": str(manifest), "PLAN": str(plan_path), "TARGET_CONFIG": str(config),
                "TARGET_PREPARATION": str(root / "target/target-preparation.json"), "HTTP_PROXY": "http://unused.invalid:9",
                "RATE": "10", "DURATION": "1s", "TIMEOUT": "1s", "PRE_ALLOCATED_VUS": "2", "MAX_VUS": "4"}
        results = []

        def run_case(name, mode="normal", changes=None, instrument=False):
            case = root / name
            case.mkdir()
            env = {**base, "RUN_ID": name, "TMPDIR": str(case), **(changes or {})}
            workload = scripts
            if instrument:
                workload = case / "instrumented"
                shutil.copytree(scripts, workload, ignore=shutil.ignore_patterns("internal", "cmd", "tests", "deploy", "schemas"))
                shutil.copytree(scripts.parent / "lib", workload.parent / "lib")
                source = (workload / "benchmark.js").read_text()
                source = replace_once(source, "headers: { ...spec.headers, Range: request.range },",
                                        "headers: { ...spec.headers, Range: request.range, 'X-Acceptance-Iteration': String(exec.scenario.iterationInInstance), 'X-Acceptance-VU': String(exec.vu.idInTest) },")
                (workload / "benchmark.js").write_text(source)
            server.mode = mode
            offset = len(server.records)
            with (case / "worker.log").open("w") as log, managed_process(
                    ["/bin/sh", str(workload / "run-benchmark.sh")], env=env, stdout=log, stderr=subprocess.STDOUT) as process:
                code = process.wait(timeout=45)
            records = copy.deepcopy(server.records[offset:])
            write(case / "requests.json", records)
            logged = worker_records(case / "worker.log")
            assert set(logged) == {"RUN_SPEC_V1", "WORKER_SPEC_V1", "K6_SUMMARY_V1", "WORKER_EXIT_V1"}
            native = logged["K6_SUMMARY_V1"]
            run = logged["RUN_SPEC_V1"]
            spec = logged["WORKER_SPEC_V1"]
            execution = logged["WORKER_EXIT_V1"]
            assert "k6_version" not in native and "k6" not in run["tool_versions"]
            assert execution["exit_code"] == code
            assert not list(case.glob("proxy-bench-fixed-pool.*")), "temporary input directory leaked"
            assert not (case / "results").exists(), "worker still requires an archive directory"
            write(case / "invocation.json", {"exit_code": code, "environment": env, "instrumented": instrument})
            metrics = native["native_summary"]["metrics"]
            text = (case / "worker.log").read_text()
            assert "\nproxy-bench (fixed-pool)\n" in text
            load = next(line for line in text.splitlines() if line.startswith("  Load"))
            assert "peak" not in load
            consumed = next(line for line in text.splitlines() if line.startswith("  Consumed"))
            assert consumed.split() == ["Consumed", str(int(metrics["plan_consumed"]["values"]["count"])),
                                        "plan", "entries;", "shard", str(spec["region_count"]),
                                        "regions", "(repeats", "included)"]
            header = next(line for line in text.splitlines() if line.startswith("  Latency (ms)"))
            assert header.split()[2:] == ["min", "avg", "med", "p(90)", "p(95)", "p(99)", "max"]
            assert all(label in text for label in ("  Requests", "  Failed", "  Throughput"))
            latency = next(line for line in text.splitlines() if line.startswith("    request"))
            assert latency.split()[1:] == [
                f"{metrics['http_req_duration']['values'][stat]:.2f}"
                for stat in native["native_summary"]["options"]["summaryTrendStats"]]
            result = next(line for line in text.splitlines() if line.startswith("  Result"))
            assert ("FAILED" if code == 99 else "NOT EVALUATED (no thresholds configured)") in result
            assert native["effective_options"]["scenarios"]["fixed_pool"]["executor"] == "constant-arrival-rate"
            for record in records:
                assert record["method"] == "GET" and record["version"] == "HTTP/1.1"
                assert record["url"] in objects and record["range"] is not None
                headers = {k.lower(): v for k, v in record["headers"].items()}
                assert headers["accept-encoding"] == "identity"
                assert headers["x-dragonfly-prefetch"] == "false" and headers["x-dragonfly-use-p2p"] == "true"
                if instrument:
                    k = int(record["iteration"])
                    g = shard[k % len(shard)]
                    assert record["url"] == objects[g // plan["regions_per_object"]]
                    start = (g % plan["regions_per_object"]) * plan["range_bytes"]
                    assert record["range"] == f"bytes={start}-{start + plan['range_bytes'] - 1}"
            if instrument:
                numbers = sorted(int(record["iteration"]) for record in records)
                count = metrics["plan_consumed"]["values"]["count"]
                assert numbers == list(range(int(count))), (name, numbers, count)
            results.append({"case": name, "code": code, "requests": len(records),
                            "metrics": metrics, "raw_k6_exit": execution["exit_code"]})
            write(root / "completed-cases.json", results)
            return code, spec, native, records

        code, spec, native, records = run_case("RUN-01", "concurrent",
                                               {"DURATION": "3s", "TIMEOUT": "5s"}, instrument=True)
        assert code == 0 and len(records) >= 2 * len(shard)
        first = [record for record in records if record["iteration"] in ("0", "1")]
        assert len(first) == 2 and all(record["gate_passed"] for record in first), first
        assert len({int(record["vu"]) for record in first}) == 2, first
        assert max(record["received_at"] for record in first) < min(record["responded_at"] for record in first)
        required = {"RUN_SPEC_V1", "WORKER_SPEC_V1", "K6_SUMMARY_V1", "WORKER_EXIT_V1"}
        logged = worker_records(root / "RUN-01/worker.log")
        assert set(logged) == required, sorted(logged)
        assert all(value["run_id"] == "RUN-01" for value in logged.values())
        assert logged["WORKER_SPEC_V1"]["worker_id"] == 0 and logged["WORKER_SPEC_V1"]["rate"] == 10
        summary = logged["K6_SUMMARY_V1"]
        assert summary["effective_options"]["scenarios"]["fixed_pool"]["rate"] == 10
        assert summary["native_summary"]["metrics"]["http_reqs"]["values"]["count"] > 0
        assert logged["WORKER_EXIT_V1"]["exit_code"] == 0
        assert logged["WORKER_EXIT_V1"]["termination_reason"] == "completed"
        write(root / "RUN-01/log-facts.json", {"records": sorted(logged),
              "concurrent_vus": sorted(int(record["vu"]) for record in first), "overlap_confirmed": True})
        # Coverage is observable from native consumption and shard facts, with
        # a normal exit and consistent counters. It is not a second result model.
        counts = summary["native_summary"]["metrics"]
        consumed = counts["plan_consumed"]["values"]["count"]
        assert consumed == counts["iterations"]["values"]["count"] == counts["http_reqs"]["values"]["count"]
        unique = len({int(r["iteration"]) % len(shard) for r in records})
        assert unique == min(consumed, spec["region_count"])
        original_plan = plan_path.read_bytes()
        code, spec, native, records = run_case("RUN-04-repeat", changes={"DURATION": "3s"}, instrument=True)
        assert code == 0 and plan_path.read_bytes() == original_plan
        assert native["effective_options"]["scenarios"]["fixed_pool"]["rate"] == 10
        code, spec, native, records = run_case("RUN-04", changes={"RATE": "20", "DURATION": "3s", "WORKER_ID": "00"}, instrument=True)
        assert code == 0 and plan_path.read_bytes() == original_plan
        assert native["effective_options"]["scenarios"]["fixed_pool"]["rate"] == 20
        code, spec, native, records = run_case("RUN-03", "wrong-status", instrument=True)
        assert code == 0 and native["native_summary"]["metrics"]["http_req_failed"]["values"]["rate"] == 1
        code, spec, native, records = run_case("RUN-01-dropped", "slow",
                                                {"RATE": "100", "PRE_ALLOCATED_VUS": "1", "MAX_VUS": "1"}, instrument=True)
        assert code == 0 and native["native_summary"]["metrics"]["dropped_iterations"]["values"]["count"] > 0
        config_home = root / "config-home"
        (config_home / "k6").mkdir(parents=True)
        write(config_home / "k6/config.json", {"thresholds": {"http_req_failed": ["rate<0"]}})
        code, spec, native, _ = run_case("TERM-01", changes={"XDG_CONFIG_HOME": str(config_home)})
        assert code == 99 and results[-1]["raw_k6_exit"] == 99
        assert native["effective_options"]["thresholds"]["http_req_failed"] == ["rate<0"]

        # An engine configuration error can occur before handleSummary. Keep
        # the native failure and available facts without synthesizing a summary.
        case = root / "TERM-01-startup"
        case.mkdir()
        env = {**base, "RUN_ID": "TERM-01-startup", "TMPDIR": str(case), "K6_MAX_REDIRECTS": "invalid"}
        with (case / "worker.log").open("w") as log:
            process = run_command(["/bin/sh", str(scripts / "run-benchmark.sh")], env=env,
                                  stdout=log, stderr=subprocess.STDOUT, timeout=45)
        logged = worker_records(case / "worker.log")
        assert process.returncode != 0 and logged["WORKER_EXIT_V1"]["exit_code"] == process.returncode
        assert logged["WORKER_EXIT_V1"]["termination_reason"] == "failed"
        assert "K6_SUMMARY_V1" not in logged and "WORKER_RESULT_V1" not in logged
        assert not list(case.glob("proxy-bench-fixed-pool.*"))
        results.append({"case": "TERM-01-startup", "code": process.returncode, "summary_present": False})
        write(root / "completed-cases.json", results)

        rejected = []
        for name in ("nonready",):
            case = root / "RUN-02"
            case.mkdir()
            env = {**base, "RUN_ID": "reject-" + name, "TMPDIR": str(case)}
            prepared = read(root / "target/target-preparation.json")
            shutil.copy(root / "target/target-object-results.json", case / "target-object-results.json")
            prepared["status"] = "failed"
            write(case / "target-preparation.json", prepared)
            env["TARGET_PREPARATION"] = str(case / "target-preparation.json")
            before = len(server.records)
            completed = run_command(["/bin/sh", str(scripts / "run-benchmark.sh")], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            (case / "worker.log").write_text(completed.stdout)
            assert completed.returncode == 2, (name, completed.stdout)
            assert len(server.records) == before and not list(case.glob("proxy-bench-fixed-pool.*"))
            assert "PROXY_BENCH_FIXED_POOL_ERROR_V1" in completed.stdout
            rejected.append({"case": "RUN-02", "variant": name})
        write(root / "rejected-inputs.json", rejected)
        write(root / "result.json", {"k6_version": k6_version, "fixture_only": True, "cases": results})
        print(json.dumps({"work_dir": str(root), "passed": len(results), "rejected_inputs": len(rejected)}))


if __name__ == "__main__":
    main()
