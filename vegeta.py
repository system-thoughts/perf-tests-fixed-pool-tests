"""Vegeta worker regressions using delivered executables and existing HTTP fixtures."""
from collections import Counter
from contextlib import ExitStack
import copy
import os
from pathlib import Path
import shlex
import signal
import subprocess
import time

from integration import invoke, managed_process, read, worker_records, write
from multi_worker import prepare, check_timing


def check(directory, records, plan_path, worker, completed=True):
    logged = worker_records(directory / 'worker.log')
    assert set(logged) == {'RUN_SPEC_V1', 'WORKER_SPEC_V1', 'VEGETA_SUMMARY_V1', 'WORKER_EXIT_V1'}
    report, spec = logged['VEGETA_SUMMARY_V1'], logged['WORKER_SPEC_V1']
    native = report['native_summary']
    assert report['engine'] == 'vegeta' and report['engine_version']
    assert report['worker_id'] == spec['worker_id'] == worker
    assert report['run_id'] == spec['run_id']
    assert report['effective_options']['rate'] == spec['rate']
    assert native['requests'] > 0 and native['latencies']['99th'] > 0 and native['rate'] > 0
    plan = read(plan_path)
    shard = read(plan_path.parent / plan['shards'][worker]['path'])
    objects = [o['url'] for o in read(Path(spec['manifest']['path']))['objects']]
    expected = []
    for g in shard:
        start = g % plan['regions_per_object'] * plan['range_bytes']
        expected.append((objects[g // plan['regions_per_object']], f'bytes={start}-{start + plan["range_bytes"] - 1}'))
    for record in records:
        assert (record['url'], record['range']) in expected
        headers = {k.lower(): v for k, v in record['headers'].items()}
        assert headers['accept-encoding'] == 'identity' and headers['x-dragonfly-prefetch'] == 'false'
        assert headers['x-dragonfly-use-p2p'] == 'true' and record['version'] == 'HTTP/1.1'
    if completed:
        assert native['requests'] == len(records)
        # Arrival/completion ordering is not the Targeter consumption ordering.
        assert Counter((r['url'], r['range']) for r in records) == Counter(expected[i % len(expected)] for i in range(len(records)))
        assert logged['WORKER_EXIT_V1']['exit_code'] == 0
        assert not report['interrupted']
    assert not list(directory.glob('proxy-bench-fixed-pool.*'))
    assert not (directory / 'results').exists()
    return report


def plan_for(root, base, workers=1):
    (root / 'plan-command').mkdir()
    result = invoke(['proxy-bench-prepare', 'plan', '--manifest', base['MANIFEST'], '--range', '16384',
                     '--workers', str(workers), '--out', str(root / 'plans')], root / 'plan-command')
    return Path(result['plan']['path'])


def single(args):
    root = args.work_dir.resolve()
    scripts = args.delivery.resolve() / 'runtime'
    completed = []
    with prepare(root, args.delivery) as (server, base):
        plan = plan_for(root, base)
        original_plan = plan.read_bytes()
        base.update(PLAN=str(plan), RATE='20', DURATION='1s', TIMEOUT='1s', PRE_ALLOCATED_VUS='2')

        def run_case(name, mode='normal', changes=None, stop=False, runtime=scripts):
            directory = root / name
            directory.mkdir()
            env = {**base, 'RUN_ID': name, 'TMPDIR': str(directory), **(changes or {})}
            server.mode = mode
            offset = len(server.records)
            with (directory / 'worker.log').open('w') as log, managed_process(
                    ['/bin/sh', str(runtime / 'run-benchmark.sh')], env=env,
                    stdout=log, stderr=subprocess.STDOUT) as process:
                if stop:
                    deadline = time.monotonic() + 10
                    while len(server.records) == offset:
                        assert process.poll() is None and time.monotonic() < deadline
                        time.sleep(0.01)
                    process.send_signal(signal.SIGTERM)
                code = process.wait(timeout=15)
            records = copy.deepcopy(server.records[offset:])
            write(directory / 'requests.json', records)
            logged = worker_records(directory / 'worker.log')
            if 'VEGETA_SUMMARY_V1' in logged:
                report = check(directory, records, plan, 0, completed=not stop)
                assert code == (130 if stop else 0), (name, code)
                if mode == 'wrong-status':
                    assert report['expected_status_success_ratio'] == 0
                    assert report['native_summary']['success'] == 1
                elif not stop:
                    assert report['expected_status_success_ratio'] == 1
                    assert report['native_summary']['bytes_in']['total'] == len(records)*16384
                if mode == 'concurrent':
                    first = [r for r in records if r['iteration'] in ('0', '1')]
                    assert len(first) == 2 and all(r['gate_passed'] for r in first)
                    assert len({r['vu'] for r in first}) == 2
                if stop:
                    assert report['interrupted'] and logged['WORKER_EXIT_V1']['termination_reason'] == 'interrupted'
            else:
                assert code != 0 and not records
            completed.append({'case': name, 'exit_code': code, 'wire_requests': len(records)})
            write(root / 'completed-cases.json', completed)
            print('PASS', name, code, flush=True)

        run_case('RUN-01', 'concurrent')
        run_case('RUN-03', 'wrong-status')
        run_case('RUN-04-repeat')
        run_case('RUN-04-rate', changes={'RATE': '30'})
        run_case('RUN-01-overload', 'slow', {'RATE': '100', 'PRE_ALLOCATED_VUS': '1', 'MAX_VUS': '1'})
        run_case('TERM-01', 'slow', {'DURATION': '5s'}, stop=True)
        for name, changes in [('rate', {'RATE': '0'}), ('worker', {'WORKER_ID': '99'}),
                              ('manifest', {'MANIFEST': str(root / 'missing.json')}),
                              ('plan', {'PLAN': str(root / 'missing-plan.json')})]:
            run_case('RUN-02-' + name, changes=changes)
        bad = root / 'nonready.json'
        value = read(Path(base['TARGET_PREPARATION']))
        value['status'] = 'failed'
        write(bad, value)
        run_case('RUN-02-nonready', changes={'TARGET_PREPARATION': str(bad)})
        # A failed engine launch is distinct from a successful benchmark run.
        startup = root / 'startup-bin'
        startup.mkdir()
        real = args.delivery.resolve() / 'bin/proxy-bench-worker-support'
        wrapper = startup / 'proxy-bench-worker-support'
        wrapper.write_text('#!/bin/sh\nif [ "$1" = run ]; then exit 127; fi\nexec ' + shlex.quote(str(real)) + ' "$@"\n')
        wrapper.chmod(0o700)
        run_case('TERM-01-startup', changes={'PATH': str(startup) + os.pathsep + base['PATH']})
        assert plan.read_bytes() == original_plan
    write(root / 'result.json', {'engine': 'vegeta', 'cases': completed})


def labeled_script(scripts, destination):
    """Only the test copy adds a worker header to the private validated input."""
    destination.mkdir()
    source = (scripts / 'run-benchmark.sh').read_text()
    launch = 'proxy-bench-worker-support run --spec "$WORKER_SPEC" &'
    assert source.count(launch) == 1
    code = 'import json,os,sys; p=sys.argv[1]; v=json.load(open(p)); v["headers"]["X-Acceptance-Worker"]=os.environ["WORKER_ID"]; open(p,"w").write(json.dumps(v))'
    (destination / 'run-benchmark.sh').write_text(source.replace(launch, 'python3 -c ' + shlex.quote(code) + ' "$WORKER_SPEC"\n' + launch))


def multi(args):
    root = args.work_dir.resolve()
    scripts = args.delivery.resolve() / 'runtime'
    completed = []
    with prepare(root, args.delivery) as (server, base):
        for name, workers in [('MULTI-01', 3), ('MULTI-02', 2)]:
            case = root / name
            case.mkdir()
            plan = plan_for(case, base, workers)
            server.mode = 'slow'
            offset = len(server.records)
            interrupted_at = None
            directories, processes = [], []
            with ExitStack() as resources:
                for worker in range(workers):
                    directory = case / str(worker)
                    directory.mkdir()
                    labeled_script(scripts, directory / 'scripts')
                    env = {**base, 'PLAN': str(plan), 'RUN_ID': name, 'WORKER_ID': str(worker), 'TMPDIR': str(directory)}
                    log = resources.enter_context((directory / 'worker.log').open('w'))
                    directories.append(directory)
                    processes.append(resources.enter_context(managed_process(
                        ['/bin/sh', str(directory / 'scripts/run-benchmark.sh')], env=env,
                        stdout=log, stderr=subprocess.STDOUT)))
                if name == 'MULTI-02':
                    deadline = time.monotonic() + 10
                    while not any(r['headers'].get('X-Acceptance-Worker') == '1' for r in server.records[offset:]):
                        assert processes[1].poll() is None and time.monotonic() < deadline
                        time.sleep(0.01)
                    processes[1].send_signal(signal.SIGTERM)
                    interrupted_at = time.time()
                codes = [p.wait(timeout=15) for p in processes]
            assert codes == ([0, 0, 0] if workers == 3 else [0, 130]), codes
            records = copy.deepcopy(server.records[offset:])
            write(case / 'requests.json', records)
            check_timing(records, workers, name, interrupted_at)
            for worker, directory in enumerate(directories):
                own = [r for r in records if r['headers'].get('X-Acceptance-Worker') == str(worker)]
                report = check(directory, own, plan, worker, completed=codes[worker] == 0)
                assert report['effective_options']['rate'] == ([7, 6, 4] if workers == 3 else [9, 8])[worker]
            runs = [worker_records(d / 'worker.log')['RUN_SPEC_V1'] for d in directories]
            assert all(r == runs[0] for r in runs)
            assert sum(w['rate'] for w in runs[0]['workers']) == 17
            completed.append({'case': name, 'exit_codes': codes})
            print('PASS', name, codes, flush=True)
    write(root / 'result.json', {'engine': 'vegeta', 'cases': completed})
