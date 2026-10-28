#!/usr/bin/env python3
"""PREP-01 protocol variants: bounded dispatch and interrupted preparation.

The controlled fixture observes command/HTTP/signal wiring, not real Seed readiness.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time

from integration import Handler, managed_process, write
from multi_worker import prepare


def transport_variants(root, server, env):
    """Observe retries and dispatch through the shipped command's real socket path."""
    config = json.loads(Path(env['TARGET_CONFIG']).read_text())
    respond = Handler.send_response
    outcomes = []
    for label, persistent, budget, full in [
            ('range-reset-once', False, 2000, False),
            ('range-reset-concurrent', False, 5000, False),
            ('range-reset-persistent', True, 2000, False),
            ('range-reset-budget', True, 50, False),
            ('full-get-reset', True, 2000, True)]:
        concurrent = label == 'range-reset-concurrent'
        config['prepare'] = {'concurrency': 2 if concurrent else 1, 'ready_timeout_ms': budget}
        write(Path(env['TARGET_CONFIG']), config)
        probes, failures = {}, []
        first_failed, dispatched = threading.Event(), threading.Event()
        full_requests = 0
        progress_observed = []

        def disconnect(handler, *args, **kwargs):
            nonlocal full_requests
            is_range = bool(handler.headers.get('Range'))
            if concurrent and not is_range:
                with server.lock:
                    full_requests += 1
                    ordinal = full_requests
                if ordinal == 2:
                    first_failed.wait(5)
                if ordinal >= 3 and first_failed.is_set():
                    dispatched.set()
            if is_range:
                probes[handler.path] = probes.get(handler.path, 0) + 1
            candidate = (not is_range) if full else is_range
            if candidate and (persistent or not failures):
                failures.append(handler.path)
                first_failed.set()
                handler.close_connection = True
                handler.connection.shutdown(socket.SHUT_RDWR)
                raise ConnectionResetError('injected connection loss before response headers')
            if concurrent and is_range and failures and handler.path == failures[0]:
                # Keep recovery unfinished until another worker dispatches a
                # third object. This observes progress without a timing race.
                progress_observed.append(dispatched.wait(1))
            return respond(handler, *args, **kwargs)

        offset = len(server.records)
        server.mode = 'normal'
        Handler.send_response = disconnect
        command = ['proxy-bench-prepare', 'seed-cache', '--manifest', env['MANIFEST'],
                   '--target-config', env['TARGET_CONFIG'], '--out', str(root / label)]
        try:
            result = subprocess.run(command, capture_output=True, timeout=15)
        finally:
            Handler.send_response = respond
        (root / (label + '.stdout')).write_bytes(result.stdout)
        (root / (label + '.stderr')).write_bytes(result.stderr)
        outcome = json.loads(result.stdout)
        own = server.records[offset:]
        full_count = sum(r['range'] is None for r in own)
        if not persistent:
            assert result.returncode == 0 and outcome['status'] == 'ready', outcome
            assert full_count == 5 and len(own) == 11, own
            assert b'succeeded after 2 attempts' in result.stderr, result.stderr
            if concurrent:
                assert progress_observed and all(progress_observed), own
        else:
            assert result.returncode == 3 and outcome['status'] != 'ready', outcome
            assert len(failures) == (1 if full or budget == 50 else 3), failures
            assert full_count == 1, own
            evidence = json.loads((root / label / 'target-object-results.json').read_text())['objects']
            assert evidence[0]['error']['stage'] == ('full_get' if full else 'range_check')
            assert evidence[0]['finished_check_at'] is None, evidence[0]
            for entry in evidence[1:]:
                assert 'not dispatched' in entry['error']['message'], entry
                assert 'object 0 failed at' in entry['error']['message'], entry
                assert entry['full_read_completed_at'] is None, entry
        outcomes.append({'variant': label, 'exit_code': result.returncode, 'status': outcome['status'],
                         'full_get_count': full_count, 'request_count': len(own), 'injected_failures': len(failures),
                         'dispatch_during_recovery': bool(progress_observed) if concurrent else None})
        write(root / (label + '-requests.json'), own)
    write(root / 'transport-result.json', outcomes)
    return outcomes


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
        respond = Handler.send_response

        def observe(handler):
            nonlocal active, maximum, starts
            with lock:
                active += 1
                maximum = max(maximum, active)
                starts += 1
                first = starts <= 2
            if first:
                gate.wait(timeout=10)
            original(handler)

        def release(handler, *args, **kwargs):
            nonlocal active
            # End before response delivery: handler cleanup can lag behind the
            # client's next sequential request and overcount its concurrency.
            with lock:
                active -= 1
            return respond(handler, *args, **kwargs)

        config = json.loads(Path(env['TARGET_CONFIG']).read_text())
        config['prepare'] = {'concurrency': 2}
        write(Path(env['TARGET_CONFIG']), config)
        offset = len(server.records)
        Handler.do_GET = observe
        Handler.send_response = release
        try:
            command = ['proxy-bench-prepare', 'seed-cache', '--manifest', env['MANIFEST'],
                       '--target-config', env['TARGET_CONFIG'], '--out', str(root / 'concurrent')]
            result = subprocess.run(command, capture_output=True, timeout=30,
                                    env={**os.environ, "NO_PROXY": "*", "HTTP_PROXY": "http://invalid.invalid:9"})
            (root / 'concurrent.stdout').write_bytes(result.stdout)
            (root / 'concurrent.stderr').write_bytes(result.stderr)
            assert result.returncode == 0 and json.loads(result.stdout)['status'] == 'ready'
            assert maximum == 2, maximum
            own = server.records[offset:]
            assert len(own) == 10 and sum(r["range"] is None for r in own) == 5
        finally:
            Handler.do_GET = original
            Handler.send_response = respond
        # The task marker is a start-state snapshot, not a data-readiness gate.
        # Exercise the whole command with that header absent throughout preparation.
        send_header = Handler.send_header

        def without_marker(handler, name, value):
            if name != 'X-Dragonfly-Task-Download-Finished':
                return send_header(handler, name, value)

        command[-1] = str(root / 'marker-absent')
        offset = len(server.records)
        Handler.send_header = without_marker
        try:
            result = subprocess.run(command, capture_output=True, timeout=30)
        finally:
            Handler.send_header = send_header
        (root / 'marker-absent.stdout').write_bytes(result.stdout)
        (root / 'marker-absent.stderr').write_bytes(result.stderr)
        outcome = json.loads(result.stdout)
        assert result.returncode == 0 and outcome['status'] == 'ready', outcome
        assert outcome['method'] == 'http-full-body-and-finished-v1'
        own = server.records[offset:]
        assert len(own) == 10 and sum(r['range'] is None for r in own) == 5, own

        # A bad per-object Range must still prevent publication of ready.
        probes = {}
        def bad_range(handler, name, value):
            if name == 'X-Dragonfly-Task-Download-Finished' and handler.headers.get('Range'):
                probes[handler.path] = probes.get(handler.path, 0) + 1
            if name == 'Content-Range' and probes.get(handler.path) == 1:
                value = 'bytes 1-65536/65536'
            return send_header(handler, name, value)

        def repeated_task(handler, name, value):
            return send_header(handler, name, 'shared-task' if name == 'X-Dragonfly-Task-ID' else value)

        failures = []
        for label, injection, stage in [('range', bad_range, 'range_check'),
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
        transport = transport_variants(root, server, env)
        server.mode = 'slow'
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
                                    'cancel_exit': process.returncode, 'real_seed': False, 'failure_variants': failures,
                                    'transport_variants': transport})


if __name__ == '__main__':
    main()
