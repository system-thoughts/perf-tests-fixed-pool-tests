#!/usr/bin/env python3
"""PREP-01 protocol variants: bounded dispatch and interrupted preparation.

The controlled fixture observes command/HTTP/signal wiring, not real Seed readiness.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

from integration import Handler, managed_process, write
from multi_worker import prepare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--delivery', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    args = parser.parse_args()
    root = args.work_dir.resolve()
    with prepare(root, args.delivery, object_count=5) as (server, env):
        server.mode = 'slow'
        lock, gate = threading.Lock(), threading.Barrier(2)
        active = maximum = starts = 0
        original = Handler.do_GET

        def observe(handler):
            nonlocal active, maximum, starts
            with lock:
                active += 1
                maximum = max(maximum, active)
                starts += 1
                first = starts <= 2
            try:
                if first:
                    gate.wait(timeout=10)
                original(handler)
            finally:
                with lock:
                    active -= 1

        config = json.loads(Path(env['TARGET_CONFIG']).read_text())
        config['prepare'] = {'concurrency': 2}
        write(Path(env['TARGET_CONFIG']), config)
        offset = len(server.records)
        Handler.do_GET = observe
        try:
            command = ['proxy-bench-seed-cache-prepare', '--manifest', env['MANIFEST'],
                       '--target-config', env['TARGET_CONFIG'], '--out', str(root / 'concurrent')]
            result = subprocess.run(command, capture_output=True, timeout=30,
                                    env={**os.environ, "NO_PROXY": "*", "HTTP_PROXY": "http://invalid.invalid:9"})
            (root / 'concurrent.stdout').write_bytes(result.stdout)
            (root / 'concurrent.stderr').write_bytes(result.stderr)
            assert result.returncode == 0 and json.loads(result.stdout)['status'] == 'ready'
            assert maximum == 2, maximum
            own = server.records[offset:]
            assert len(own) == 15 and sum(r["range"] is None for r in own) == 5
        finally:
            Handler.do_GET = original
        # These probe lifecycle and task-uniqueness wiring, not the unit-level
        # header/status matrix. A successful body alone must never imply ready.
        send_header = Handler.send_header
        probes = {}

        def final_marker(handler, name, value):
            if name == 'X-Dragonfly-Task-Download-Finished' and handler.headers.get('Range'):
                probes[handler.path] = probes.get(handler.path, 0) + 1
                if probes[handler.path] == 2:
                    value = 'false'
            return send_header(handler, name, value)

        def repeated_task(handler, name, value):
            return send_header(handler, name, 'shared-task' if name == 'X-Dragonfly-Task-ID' else value)

        failures = []
        for label, injection, stage in [('final-marker', final_marker, 'final_check'),
                                        ('shared-task', repeated_task, 'full_get')]:
            command[-1] = str(root / label)
            Handler.send_header = injection
            try:
                result = subprocess.run(command, capture_output=True, timeout=30)
            finally:
                Handler.send_header = send_header
            (root / (label + '.stdout')).write_bytes(result.stdout)
            (root / (label + '.stderr')).write_bytes(result.stderr)
            outcome = json.loads(result.stdout)
            assert result.returncode == 3 and outcome['status'] != 'ready', outcome
            assert any(error['stage'] == stage for error in outcome['errors']), outcome
            failures.append({'variant': label, 'exit_code': result.returncode, 'status': outcome['status']})
        before = len(server.records)
        command[-1] = str(root / 'cancelled')
        with managed_process(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            deadline = time.monotonic() + 10
            while len(server.records) == before:
                assert time.monotonic() < deadline and process.poll() is None
                time.sleep(0.01)
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=15)
        (root / 'cancelled.stdout').write_bytes(stdout)
        (root / 'cancelled.stderr').write_bytes(stderr)
        assert process.returncode == 130 and json.loads(stdout)['status'] != 'ready'
        write(root / 'result.json', {'status': 'passed', 'case': 'PREP-01-controlled-variants', 'max_active': maximum,
                                    'cancel_exit': process.returncode, 'real_seed': False, 'failure_variants': failures})


if __name__ == '__main__':
    main()
