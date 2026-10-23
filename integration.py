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
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, directory, address=("127.0.0.1", 0)):
        super().__init__(address, Handler)
        self.directory, self.mode, self.records = directory, "normal", []
        self.lock = threading.Lock()
        self.resend_closed = False


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
            server.records.append({"url": self.path, "method": "GET", "version": self.request_version,
                                   "received_at": time.time(),
                                   "range": range_value, "headers": dict(self.headers),
                                   "iteration": self.headers.get("X-Acceptance-Iteration"), "mode": mode})
            index = len(server.records)
            close_reused = mode == "transport-resend" and not server.resend_closed and self.headers.get("X-Acceptance-Iteration") == "1"
            if close_reused:
                server.resend_closed = True
        if close_reused:
            # First GET completed on this VU's connection. Close the reused
            # connection before headers once; the native transport may resend.
            self.close_connection = True
            return
        if mode in ("slow", "timeout"):
            time.sleep(0.25 if mode == "slow" else 0.4)
        payload = body[start:end + 1]
        if mode == "wrong-status" or mode == "mixed" and index % 2 == 0:
            status = 200
        if mode == "redirect":
            status = 302
        if mode == "wrong-body":
            payload = bytes(value ^ 1 for value in payload)
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
                self.connection.shutdown(socket.SHUT_WR)
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


def runtime_k6(root, scripts):
    """Keep regressions on the image dependency without restricting the worker."""
    dockerfile = scripts.parent / "Dockerfile"
    base = re.search(r"(?im)^FROM\s+(?:--\S+\s+)*(\S+)", dockerfile.read_text())
    assert base, f"missing k6 base image in {dockerfile}"
    image = base.group(1)
    dependency = image.split("@", 1)[0]
    assert ":" in dependency.rsplit("/", 1)[-1], "pin the k6 image tag in Dockerfile"
    expected = re.sub(r"^v", "", dependency.rsplit(":", 1)[1])
    version = run_command(["k6", "version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    fields = version.stdout.split()
    write(root / "runtime-k6.json", {"image": image, "argv": ["k6", "version"],
                                    "exit_code": version.returncode, "stdout": version.stdout,
                                    "stderr": version.stderr})
    assert version.returncode == 0 and len(fields) >= 2 and fields[0] == "k6", version
    actual = re.sub(r"^v", "", fields[1])
    assert actual == expected, f"regressions require Dockerfile engine {image}; PATH has k6 {actual}"
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    root = parser.parse_args().work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parents[1]
    for name in ("k6", "proxy-bench-worker-support", "proxy-bench-data-prepare",
                 "proxy-bench-plan-prepare", "proxy-bench-seed-cache-prepare"):
        assert shutil.which(name), f"missing {name} on PATH"
    k6_version = runtime_k6(root, scripts)
    data_log = root / "data-command"
    data_log.mkdir()
    data = invoke(["proxy-bench-data-prepare", "--size", "65536", "--count", "2",
                   "--root", str(root / "data"), "--base-url", "http://fixture.invalid"], data_log)
    manifest = Path(data["manifest"]["path"])
    with running_proxy(root / "data") as server:
        rules_path = root / "proxy-rules.json"
        write(rules_path, {"schema_version": 1, "filtered_query_params": [], "rewrite_url": False, "use_tls": False})
        proxy_url = f"http://127.0.0.1:{server.server_port}"
        target = {"schema_version": 1, "target": {"ref": "controlled-proxy", "proxy": proxy_url,
                  "expected_server_ip": "192.0.2.1", "instance_ref": "controlled-fixture-v1"},
                  "proxy_rules_ref": {"path": "proxy-rules.json", "sha256": hashlib.sha256(rules_path.read_bytes()).hexdigest()}}
        config = root / "target-config.yaml"
        write(config, target)
        plan_log = root / "plan-command"
        plan_log.mkdir()
        plan_result = invoke(["proxy-bench-plan-prepare", "--manifest", str(manifest), "--range", "16384",
                              "--plan-dir", str(root / "plans")], plan_log)
        plan_path = Path(plan_result["plan"]["path"])
        plan = read(plan_path)
        shard = read(plan_path.parent / plan["shards"][0]["path"])
        objects = read(plan_path.parent / plan["objects_file"]["path"])
        prepare_log = root / "prepare-command"
        prepare_log.mkdir()
        invoke(["proxy-bench-seed-cache-prepare", "--manifest", str(manifest), "--target-config", str(config),
                "--out", str(root / "target")], prepare_log)
        base = {"PATH": os.environ["PATH"], "HOME": str(root), "XDG_CONFIG_HOME": str(root / "clean-config"),
                "MANIFEST": str(manifest), "PLAN": str(plan_path), "TARGET_CONFIG": str(config),
                "TARGET_PREPARATION": str(root / "target/target-preparation.json"), "HTTP_PROXY": proxy_url,
                "RATE": "10", "DURATION": "1s", "TIMEOUT": "1s", "PRE_ALLOCATED_VUS": "2", "MAX_VUS": "4"}
        results = []
        zero_dropped_cases = []

        def run_case(name, mode="normal", changes=None, interrupt=None, instrument=False):
            case = root / name
            case.mkdir()
            env = {**base, "RUN_ID": name, "RESULT_DIR": str(case / "results"), **(changes or {})}
            workload = scripts
            if instrument:
                workload = case / "instrumented"
                shutil.copytree(scripts, workload, ignore=shutil.ignore_patterns("internal", "cmd", "tests", "deploy", "schemas"))
                source = (workload / "benchmark.js").read_text()
                source = replace_once(source, "import http from 'k6/http';", "import { sleep } from 'k6';\nimport http from 'k6/http';")
                source = replace_once(source, "headers: { ...spec.headers, Range: request.range },",
                                        "headers: { ...spec.headers, Range: request.range, 'X-Acceptance-Iteration': String(exec.scenario.iterationInInstance) },")
                if name == "threshold-failed":
                    source = replace_once(source, "  discardResponseBodies: true,", "  thresholds: { http_req_failed: ['rate<0'] },\n  discardResponseBodies: true,")
                if name == "script-abort":
                    source = replace_once(source, "  consumed.add(1);", "  if (exec.scenario.iterationInInstance === 1) exec.test.abort('acceptance script failure');\n  consumed.add(1);")
                if name == "number-gap":
                    source = replace_once(source, "  consumed.add(1);", "  if (exec.scenario.iterationInInstance === 1) sleep(10);\n  consumed.add(1);")
                (workload / "benchmark.js").write_text(source)
            server.mode = mode
            server.resend_closed = False
            offset = len(server.records)
            with (case / "worker.log").open("w") as log, managed_process(
                    ["/bin/sh", str(workload / "run-benchmark.sh")], env=env, stdout=log, stderr=subprocess.STDOUT) as process:
                if interrupt is not None:
                    deadline = time.monotonic() + 30
                    while len(server.records) == offset and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.05)
                    assert len(server.records) > offset, "worker never began requests"
                    time.sleep(interrupt)
                    process.send_signal(signal.SIGTERM)
                code = process.wait(timeout=45)
            records = copy.deepcopy(server.records[offset:])
            write(case / "requests.json", records)
            report = read(case / "results/worker-summary.json")
            native = read(case / "results/k6-summary.json")
            run = read(case / "results/run-spec.json")
            assert "k6_version" not in native and "k6" not in run["tool_versions"]
            execution = read(case / "results/worker-execution.json")
            write(case / "invocation.json", {"exit_code": code, "environment": env, "instrumented": instrument})
            metrics = report["workers"][0]["metrics"]
            native_metrics = native["native_summary"]["metrics"]
            if "dropped_iterations" in native_metrics:
                assert metrics["DROPPED_ITERATIONS"] == native_metrics["dropped_iterations"]["values"]["count"]
            elif report["result_complete"]:
                assert metrics["DROPPED_ITERATIONS"] == 0
                zero_dropped_cases.append(name)
            for key, metric, field in [("ACTUAL_QPS", "http_reqs", "rate"), ("REQUESTS_TOTAL", "http_reqs", "count"),
                                       ("P99_MS", "http_req_duration", "p(99)"), ("P50_MS", "http_req_duration", "med"),
                                       ("DATA_RECEIVED_BYTES", "data_received", "count")]:
                assert metrics[key] == native["native_summary"]["metrics"].get(metric, {}).get("values", {}).get(field), (name, key)
            assert metrics["TOOL_DURATION_S"] == native["native_summary"]["state"]["testRunDurationMs"] / 1000
            rows = dict(re.findall(r"^([A-Z_0-9]+)=(null|[0-9.eE+\-]+)$", (case / "worker.log").read_text(), re.M))
            for key, value in metrics.items():
                if value is None:
                    assert rows[key] == "null", (name, key)
                else:
                    assert math.isclose(float(rows[key]), value, rel_tol=1e-10, abs_tol=1e-10), (name, key)
            failed = native["native_summary"]["metrics"].get("http_req_failed", {}).get("values", {})
            assert report["workers"][0]["successful_samples"] == failed.get("fails")
            assert report["workers"][0]["failed_samples"] == failed.get("passes")
            if name == "script-abort":
                assert report["coverage"] is None and report["execution_state"] == "failed", (name, report)
                if "http_reqs" not in native["native_summary"]["metrics"]:
                    assert not report["result_complete"] and metrics["REQUESTS_TOTAL"] is None
            else:
                assert report["result_complete"], (name, report)
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
            if instrument and name not in ("number-gap", "script-abort"):
                numbers = sorted(int(record["iteration"]) for record in records)
                count = report["workers"][0]["plan_consumed"]["count"]
                if name == "timeout":
                    # A timed-out attempt may never reach the server. Received
                    # requests must still map correctly in this timeout fixture.
                    assert len(numbers) == len(set(numbers)) and all(0 <= k < count for k in numbers)
                elif name == "transport-resend":
                    assert server.resend_closed and len(numbers) > count, (name, numbers, count)
                    assert sorted(set(numbers)) == list(range(int(count))), (name, numbers, count)
                    assert metrics["REQUESTS_TOTAL"] == count
                else:
                    assert numbers == list(range(int(count))), (name, numbers, count)
            results.append({"case": name, "code": code, "requests": len(records), "execution_state": report["execution_state"],
                            "metrics": metrics, "coverage": report["coverage"], "raw_k6_exit": execution["exit_code"]})
            write(root / "completed-cases.json", results)
            return code, report, native, records

        code, report, native, records = run_case("normal", changes={"DURATION": "5s"})
        assert code == 0 and report["execution_state"] == "completed" and report["coverage"]["ratio"] == 1
        assert 5 <= report["aggregate"]["TOOL_DURATION_S"] <= 6, "frozen 1s smoke tolerance exceeded"
        assert report["aggregate"]["SUCCESS_PCT"] == 100
        # The suite has checked the Dockerfile engine. The product entry must
        # run that engine without probing or comparing its version.
        wrapper = root / "no-version-probe-bin"
        wrapper.mkdir()
        marker = root / "version-probe-called"
        real_k6 = shutil.which("k6")
        (wrapper / "k6").write_text(
            '#!/bin/sh\nif [ "$1" = version ]; then\n'
            f'  printf invoked > {shlex.quote(str(marker))}\n  exit 97\nfi\n'
            f'exec {shlex.quote(real_k6)} "$@"\n')
        (wrapper / "k6").chmod(0o700)
        code, report, _, _ = run_case("no-version-probe", changes={"PATH": str(wrapper) + ":" + os.environ["PATH"]})
        assert code == 0 and report["execution_state"] == "completed" and not marker.exists()
        config_home = root / "config-home"
        (config_home / "k6").mkdir(parents=True)
        write(config_home / "k6/config.json", {"thresholds": {"http_req_failed": ["rate<0"]}})
        code, report, native, _ = run_case("ambient-config", changes={"XDG_CONFIG_HOME": str(config_home)})
        assert code == 3 and report["execution_state"] == "failed"
        assert read(root / "ambient-config/results/worker-execution.json")["exit_code"] == 99
        assert native["effective_options"]["thresholds"]["http_req_failed"] == ["rate<0"]
        code, report, native, _ = run_case("native-env", changes={"K6_NO_CONNECTION_REUSE": "true"})
        assert code == 0 and native["effective_options"]["noConnectionReuse"] is True
        code, report, _, records = run_case("transport-resend", "transport-resend",
                                            {"RATE": "2", "DURATION": "3s", "PRE_ALLOCATED_VUS": "1", "MAX_VUS": "1"}, instrument=True)
        assert code == 0 and report["execution_state"] == "completed" and report["aggregate"]["SUCCESS_PCT"] == 100
        for mode in ("wrong-status", "redirect", "truncated", "timeout", "mixed", "wrong-body"):
            changes = {"TIMEOUT": "100ms"} if mode == "timeout" else {}
            code, report, _, records = run_case(mode, mode, changes, instrument=True)
            assert code == 0 and report["execution_state"] == "completed", (mode, report)
            if mode in ("wrong-status", "redirect", "truncated", "timeout"):
                assert report["aggregate"]["SUCCESS_PCT"] == 0, (mode, report)
            if mode == "wrong-body":
                assert report["aggregate"]["SUCCESS_PCT"] == 100, "hot path must not add body hashing"
        code, report, _, records = run_case("dropped", "slow", {"RATE": "100", "PRE_ALLOCATED_VUS": "1", "MAX_VUS": "1"}, instrument=True)
        assert code == 0 and report["aggregate"]["DROPPED_ITERATIONS"] > 0
        assert [int(r["iteration"]) for r in records] == list(range(len(records)))
        code, report, _, records = run_case("dynamic-vus", "slow", {"RATE": "20", "DURATION": "3s", "PRE_ALLOCATED_VUS": "1", "MAX_VUS": "8"}, instrument=True)
        assert code == 0 and len(records) >= 2 * len(shard)
        assert sorted(int(r["iteration"]) for r in records) == list(range(len(records)))
        code, report, _, _ = run_case("interrupted", changes={"DURATION": "10s"}, interrupt=2)
        assert code == 130 and report["coverage"] is None and report["execution_state"] == "interrupted"
        code, report, _, records = run_case("number-gap", changes={"DURATION": "10s", "MAX_VUS": "8"}, interrupt=2, instrument=True)
        numbers = sorted(int(r["iteration"]) for r in records)
        assert 0 in numbers and 1 not in numbers and 2 in numbers and 3 in numbers
        assert code == 130 and report["coverage"] is None
        code, report, _, _ = run_case("threshold-failed", instrument=True)
        assert code == 3 and report["coverage"] is None and results[-1]["raw_k6_exit"] == 99
        code, report, _, _ = run_case("script-abort", instrument=True)
        assert code == 3 and report["coverage"] is None and results[-1]["raw_k6_exit"] == 108
        # CAR can drop even against a fast server under scheduler pressure. Keep
        # those native counts and require an actual omission sample separately.
        assert zero_dropped_cases, "no complete native zero-dropped omission sample observed"
        write(root / "zero-dropped-cases.json", zero_dropped_cases)

        rejected = []
        for name in ("nonready", "manifest", "identity", "target", "evidence", "record-alias", "plan-manifest",
                     "shard-bounds", "shard-hash", "object-urls", "proxy", "rate", "worker", "mode", "range", "duration"):
            case = root / ("reject-" + name)
            case.mkdir()
            env = {**base, "RUN_ID": "reject-" + name, "RESULT_DIR": str(case / "results")}
            if name in ("nonready", "manifest", "identity", "target", "evidence", "record-alias"):
                prepared = read(root / "target/target-preparation.json")
                shutil.copy(root / "target/target-object-results.json", case / "target-object-results.json")
                if name == "nonready":
                    prepared["status"] = "failed"
                elif name == "manifest":
                    prepared["manifest_sha256"] = "0" * 64
                elif name == "identity":
                    prepared["identity_sha256"] = "0" * 64
                elif name == "target":
                    prepared["target"]["instance_ref"] = "other-instance"
                elif name == "record-alias":
                    prepared["STATUS"] = prepared["status"]
                else:
                    prepared["evidence_file"]["sha256"] = "0" * 64
                write(case / "target-preparation.json", prepared)
                env["TARGET_PREPARATION"] = str(case / "target-preparation.json")
            elif name in ("plan-manifest", "shard-bounds", "shard-hash", "object-urls"):
                shutil.copytree(plan_path.parent, case / "plan")
                changed = read(case / "plan/request-plan.json")
                if name == "plan-manifest":
                    changed["manifest_sha256"] = "0" * 64
                else:
                    ref = changed["shards"][0] if name in ("shard-bounds", "shard-hash") else changed["objects_file"]
                    filename = case / "plan" / ref["path"]
                    items = read(filename)
                    if name == "shard-bounds":
                        items[0] = changed["region_count"]
                    else:
                        items[0], items[1] = items[1], items[0]
                    write(filename, items)
                    if name != "shard-hash":
                        ref["sha256"] = hashlib.sha256(filename.read_bytes()).hexdigest()
                write(case / "plan/request-plan.json", changed)
                env["PLAN"] = str(case / "plan/request-plan.json")
            else:
                key, value = {"proxy": ("HTTP_PROXY", "http://other.test:4001"), "rate": ("RATE", "0"),
                              "worker": ("WORKER_ID", "1"), "mode": ("MODE", "repeat"), "range": ("RANGE", "0-1"),
                              "duration": ("DURATION", "100ms")}[name]
                env[key] = value
            before = len(server.records)
            completed = run_command(["/bin/sh", str(scripts / "run-benchmark.sh")], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            (case / "worker.log").write_text(completed.stdout)
            assert completed.returncode == 2, (name, completed.stdout)
            assert len(server.records) == before and not (case / "results/worker-spec.json").exists()
            assert "PROXY_BENCH_FIXED_POOL_ERROR_V1" in completed.stdout
            rejected.append(name)
        write(root / "rejected-inputs.json", rejected)
        rejected_plans = []
        for alias in ("SCHEMA_VERSION", "BLOCK_BYTES"):
            case = root / ("reject-plan-alias-" + alias.lower())
            case.mkdir()
            changed = read(manifest)
            if alias == "SCHEMA_VERSION":
                changed[alias] = changed["schema_version"]
            else:
                changed["generator"][alias] = changed["generator"]["block_bytes"]
            write(case / "dataset-manifest.json", changed)
            args = ["proxy-bench-plan-prepare", "--manifest", str(case / "dataset-manifest.json"),
                    "--range", "16384", "--plan-dir", str(case / "plans")]
            completed = run_command(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            (case / "stdout.log").write_text(completed.stdout)
            (case / "stderr.log").write_text(completed.stderr)
            write(case / "command.json", {"argv": args, "exit_code": completed.returncode})
            assert completed.returncode == 2 and json.loads(completed.stdout)["plan"] is None
            assert not list(case.glob("plans/*/request-plan.json"))
            rejected_plans.append(alias)
        write(root / "rejected-plan-inputs.json", rejected_plans)
        incomplete = []
        for mode in ("missing-summary", "truncated-summary", "missing-count", "invalid-dropped"):
            case = root / mode
            shutil.copytree(root / "normal/results", case)
            (case / "worker-summary.json").unlink()
            old_execution = read(case / "worker-execution.json")
            (case / "worker-execution.json").unlink()
            summary_path = case / "k6-summary.json"
            summary = read(summary_path)
            if mode == "missing-summary":
                summary_path.unlink()
            elif mode == "truncated-summary":
                summary_path.write_text('{"schema_version":1,')
            elif mode == "missing-count":
                del summary["native_summary"]["metrics"]["http_reqs"]["values"]["count"]
                write(summary_path, summary)
            else:
                summary["native_summary"]["metrics"]["dropped_iterations"] = {"values": {}}
                write(summary_path, summary)
            env = {"PATH": os.environ["PATH"], "RESULT_DIR": str(case)}
            completed = run_command(["proxy-bench-worker-support", "write-report", "--started-at", old_execution["process_started_at"],
                                        "--ended-at", old_execution["process_ended_at"], "--exit-code", "0", "--reason", "completed"],
                                    env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            (case / "report.log").write_text(completed.stdout)
            result = read(case / "worker-summary.json")
            assert completed.returncode == 4 and not result["result_complete"] and result["coverage"] is None
            incomplete.append(mode)
        write(root / "incomplete-results.json", incomplete)

        write(root / "result.json", {"k6_version": k6_version, "fixture_only": True, "cases": results})
        print(json.dumps({"work_dir": str(root), "passed": len(results), "rejected_inputs": len(rejected),
                          "rejected_plan_inputs": len(rejected_plans), "incomplete_results": len(incomplete)}))


if __name__ == "__main__":
    main()
