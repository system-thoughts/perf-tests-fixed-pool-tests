#!/usr/bin/env python3
"""Exercise nydus_hot_dev's legacy preparation interface and Vegeta result archives.

Uses executables/scripts extracted from the supplied images. Controlled HTTP
fixtures establish functional behavior, not Dragonfly or generator capacity.
"""
import argparse
from collections import Counter
import copy
import math
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from delivery import extract_image, digest, record_candidate
from integration import invoke, managed_process, read, running_proxy, worker_records, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--tools-image', required=True)
    parser.add_argument('--worker-image', required=True)
    args = parser.parse_args()
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    delivery = root / 'delivery'
    delivery.mkdir()
    tools = extract_image(args.tools_image, {
        f'/usr/local/bin/proxy-bench-{name}': f'bin/proxy-bench-{name}'
        for name in ('data-prepare', 'plan-prepare', 'seed-cache-prepare')}, delivery)
    worker = extract_image(args.worker_image, {
        '/usr/local/bin/proxy-bench-worker-support': 'bin/proxy-bench-worker-support',
        '/home/k6/fixed-pool/.': 'runtime'}, delivery)
    write(delivery / 'delivery.json', {'engine': 'vegeta', 'tools_image': tools,
        'worker_image': worker, 'files': {str(p.relative_to(delivery)): digest(p)
            for p in sorted(delivery.rglob('*')) if p.is_file()}})
    record_candidate(delivery, root)

    def command(name, argv):
        directory = root / name
        directory.mkdir()
        return invoke(argv, directory)

    data = command('data-command', ['proxy-bench-data-prepare', '--size', '65536',
        '--count', '2', '--root', str(root / 'data'), '--base-url', 'http://fixture.invalid'])
    manifest = Path(data['manifest']['path'])
    image_user = json.loads(subprocess.check_output(['docker', 'image', 'inspect', worker['id']]))[0]['Config']['User']
    uid = int(image_user.split(':')[0])
    gid = int(image_user.split(':')[1]) if ':' in image_user else uid
    cases = []
    with running_proxy(root / 'data') as server:
        rules = root / 'proxy-rules.json'
        write(rules, {'schema_version': 1, 'filtered_query_params': [],
                      'rewrite_url': False, 'use_tls': False})
        config = root / 'target-config.yaml'
        proxy = f'http://127.0.0.1:{server.server_port}'
        write(config, {'schema_version': 1, 'target': {'ref': 'fixture', 'proxy': proxy,
            'expected_server_ip': '192.0.2.1', 'instance_ref': 'nydus-vegeta-fixture'},
            'proxy_rules_ref': {'path': rules.name, 'sha256': digest(rules)}})
        plan_result = command('plan-command', ['proxy-bench-plan-prepare', '--manifest',
            str(manifest), '--range', '16384', '--plan-dir', str(root / 'plans')])
        plan_path = Path(plan_result['plan']['path'])
        command('seed-command', ['proxy-bench-seed-cache-prepare', '--manifest', str(manifest),
            '--target-config', str(config), '--out', str(root / 'target')])
        base = {'PATH': os.environ['PATH'], 'HOME': str(root), 'MANIFEST': str(manifest),
            'PLAN': str(plan_path), 'TARGET_CONFIG': str(config), 'HTTP_PROXY': proxy,
            'TARGET_PREPARATION': str(root / 'target/target-preparation.json'),
            'RATE': '20', 'DURATION': '1s', 'TIMEOUT': '1s', 'PRE_ALLOCATED_VUS': '2', 'MAX_VUS': '4'}
        plan = read(plan_path)
        shard = read(plan_path.parent / plan['shards'][0]['path'])
        objects = read(plan_path.parent / plan['objects_file']['path'])
        expected = [(objects[g // plan['regions_per_object']],
                     f'bytes={g % plan["regions_per_object"] * 16384}-{(g % plan["regions_per_object"] + 1) * 16384 - 1}')
                    for g in shard]

        def run_case(name, mode='normal', changes=None, interrupt=False, in_container=False):
            case = root / name
            case.mkdir()
            if in_container:
                # Docker bind mounts retain host ownership. Use the actual image UID.
                os.chown(case, uid, gid)
            server.mode = mode
            offset = len(server.records)
            env = {**base, 'RUN_ID': name, 'RESULT_DIR': str(case / 'results'), **(changes or {})}
            argv = ['/bin/sh', str(delivery / 'runtime/run-benchmark.sh')]
            if in_container:
                argv = ['docker', 'run', '--rm', '--network', 'host', '-v', f'{root}:{root}',
                        '--entrypoint', '/bin/sh']
                for key, value in env.items():
                    if key != 'PATH':
                        argv += ['-e', f'{key}={value}']
                argv += [worker['id'], '/home/k6/fixed-pool/run-benchmark.sh']
            with (case / 'worker.log').open('w') as log, managed_process(argv, env=env,
                    stdout=log, stderr=subprocess.STDOUT) as process:
                if interrupt:
                    deadline = time.monotonic() + 10
                    while len(server.records) == offset:
                        assert process.poll() is None and time.monotonic() < deadline
                        time.sleep(0.01)
                    process.send_signal(signal.SIGTERM)
                code = process.wait(timeout=20)
            records = copy.deepcopy(server.records[offset:])
            write(case / 'requests.json', records)
            logged = worker_records(case / 'worker.log')
            if name == 'unsupported-k6':
                assert code == 2 and not records and 'VEGETA_SUMMARY_V1' not in logged
                cases.append({'case': name, 'exit_code': code})
                return
            assert code == (130 if interrupt else 0), (name, code, (case / 'worker.log').read_text())
            native = read(case / 'results/vegeta-summary.json')
            report = read(case / 'results/worker-summary.json')
            metrics = native['native_summary']
            assert report['result_complete'] and native['engine'] == 'vegeta'
            assert math.isclose(report['aggregate']['P99_MS'], metrics['latencies']['99th'] / 1e6)
            assert report['aggregate']['ACTUAL_QPS'] == metrics['rate']
            assert report['aggregate']['DROPPED_ITERATIONS'] is None
            assert native == logged['VEGETA_SUMMARY_V1']
            for record in records:
                assert (record['url'], record['range']) in expected
                headers = {k.lower(): v for k, v in record['headers'].items()}
                assert headers['accept-encoding'] == 'identity' and headers['x-dragonfly-prefetch'] == 'false'
                assert headers['x-dragonfly-use-p2p'] == 'true'
            if not interrupt:
                assert metrics['requests'] == native['plan_consumed']
                if mode == 'timeout':
                    # An HTTP attempt can time out before the fixture accepts it.
                    assert len(records) <= metrics['requests']
                    for record in records:
                        assert (record['url'], record['range']) == expected[int(record['iteration']) % len(expected)]
                else:
                    assert metrics['requests'] == len(records)
                    assert Counter((r['url'], r['range']) for r in records) == Counter(expected[i % len(expected)] for i in range(len(records)))
                assert report['coverage'] is not None
                if mode in ('normal', 'wrong-status', 'redirect', 'slow'):
                    assert metrics['bytes_in']['total'] == len(records) * 16384
                if mode in ('wrong-status', 'redirect', 'truncated', 'timeout'):
                    assert native['expected_status_success_ratio'] == 0
                if mode == 'wrong-status':
                    assert metrics['success'] == 1
                if mode == 'truncated':
                    assert metrics['bytes_in']['total'] == len(records) * 8192
                if name == 'worker-cap':
                    assert metrics['rate'] < 100
                if mode == 'normal':
                    assert native['expected_status_success_ratio'] == 1
            else:
                assert native['interrupted'] and report['coverage'] is None
                assert report['execution_state'] == 'interrupted'
            for name, ref in report['references'].items():
                assert digest(case / 'results' / name) == ref['sha256']
            cases.append({'case': case.name, 'exit_code': code, 'requests': metrics['requests'],
                          'qps': metrics['rate'], 'p99_ns': metrics['latencies']['99th']})

        for mode in ('normal', 'wrong-status', 'redirect', 'truncated', 'timeout'):
            run_case(mode, mode, {'TIMEOUT': '50ms'} if mode == 'timeout' else None)
        run_case('worker-cap', 'slow', {'RATE': '100', 'PRE_ALLOCATED_VUS': '1', 'MAX_VUS': '1'})
        run_case('interrupted', 'slow', {'DURATION': '5s'}, interrupt=True)
        run_case('unsupported-k6', changes={'K6_SCENARIOS': '{}'})
        run_case('container', in_container=True)
        multi = command('multi-plan-command', ['proxy-bench-plan-prepare', '--manifest',
            str(manifest), '--range', '16384', '--workers', '2', '--plan-dir', str(root / 'multi-plans')])
        multi_path = Path(multi['plan']['path'])
        multi_plan = read(multi_path)
        server.mode = 'normal'
        offset = len(server.records)
        processes = []
        logs = []
        try:
            for worker_id in range(2):
                directory = root / f'multi-{worker_id}'
                directory.mkdir()
                log = (directory / 'worker.log').open('w')
                logs.append(log)
                env = {**base, 'PLAN': str(multi_path), 'RUN_ID': 'multi',
                    'WORKER_ID': str(worker_id), 'RESULT_DIR': str(directory / 'results')}
                processes.append(subprocess.Popen(['/bin/sh', str(delivery / 'runtime/run-benchmark.sh')],
                    env=env, stdout=log, stderr=subprocess.STDOUT))
            for process in processes:
                assert process.wait(timeout=20) == 0
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            for log in logs:
                log.close()
        expected_multi = Counter()
        for worker_id in range(2):
            directory = root / f'multi-{worker_id}'
            native = read(directory / 'results/vegeta-summary.json')
            report = read(directory / 'results/worker-summary.json')
            spec = read(directory / 'results/worker-spec.json')
            assert spec['rate'] == 10 and native['worker_id'] == worker_id
            assert report['result_complete'] and report['aggregate'] is None
            assert report['workers'][0]['coverage']['unique_regions'] == 4
            shard = read(multi_path.parent / multi_plan['shards'][worker_id]['path'])
            cycle = [(objects[g // 4], f'bytes={g % 4 * 16384}-{(g % 4 + 1) * 16384 - 1}') for g in shard]
            expected_multi.update(cycle[i % 4] for i in range(native['native_summary']['requests']))
        records = copy.deepcopy(server.records[offset:])
        write(root / 'multi-requests.json', records)
        assert Counter((r['url'], r['range']) for r in records) == expected_multi
        cases.append({'case': 'multi-worker', 'workers': 2, 'requests': len(records)})
    write(root / 'result.json', {'fixture_only': True, 'cases': cases})
    print(f'{len(cases)} cases passed: {root}')


if __name__ == '__main__':
    main()
