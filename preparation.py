#!/usr/bin/env python3
"""Maintainer Kubernetes regression for image-only preparation and metadata handoff.

Requires PyYAML, envsubst, kubectl and existing immutable tools/worker/nginx
images. The caller supplies an isolated source NodePort and a real Seed proxy
with an observed instance reference. Two PVCs exercise independent copies;
placing them on one node does not establish cross-node storage compatibility.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

import yaml

from source import Regression, write


class Preparation(Regression):
    def metadata(self, index):
        name = self.name + '-metadata-' + str(index)
        values = {**self.settings, 'TRANSFER_POD_NAME': name + '-files',
                  'METADATA_PVC': name, 'METADATA_NODE': self.args.node,
                  'METADATA_STORAGE_CLASS': self.args.storage_class, 'METADATA_CAPACITY': '128Mi'}
        self.create(self.render_template('metadata/pvc.tpl.yaml', values,
                                        self.root / (name + '-pvc.yaml')))
        self.create(self.render_template('metadata/metadata-transfer-pod.tpl.yaml', values,
                                        self.root / (name + '-pod.yaml')))
        pod = values['TRANSFER_POD_NAME']
        self.kube('wait', '--for=condition=Ready', 'pod/' + pod, '--timeout=120s', timeout=130)
        return values, pod

    def upload(self, pod, local, parent):
        self.kube('exec', pod, '--', 'mkdir', '-p', parent)
        self.kube('exec', pod, '--', 'test', '!', '-e', parent.rstrip('/') + '/' + local.name)
        # Existing parent + named local child: the child name is retained once.
        self.kube('cp', '--no-preserve', str(local), pod + ':' + parent, timeout=120)

    def check_copy(self, pod, relative):
        local = self.root / 'bundle' / relative
        expected = hashlib.sha256(local.read_bytes()).hexdigest()
        observed = self.kube('exec', pod, '--', 'sha256sum', '/inputs/' + relative).stdout.decode().split()[0]
        assert observed == expected, relative
        return expected

    def prepare(self, kind, values, name, success=True):
        settings = {**values, 'JOB_NAME': name}
        self.create(self.render_template('prepare/' + kind + '-job.tpl.yaml', settings,
                                        self.root / (name + '.yaml')))
        self.wait_job(name, success)
        return name

    def read_result(self, pod, name):
        raw = self.kube('exec', pod, '--', 'cat', '/inputs/reports/' + name + '.json').stdout
        (self.root / (name + '-report.json')).write_bytes(raw)
        return json.loads(raw)

    def execute(self):
        settings, (pvc, job, server) = self.render()
        # Keep a reused NodePort from reusing an earlier run's Seed cache.
        mount = '/usr/share/nginx/html/' + self.name + '/objects'
        settings.update(B=str(4 * 1024**2), N='2', BASE_URL=self.args.source_base_url.rstrip('/') + '/' + self.name,
                        R='65536', W='2', PREPARATION_NAME='prepare-001',
                        PREPARE_CPU_REQUEST='100m', PREPARE_CPU_LIMIT='2',
                        PREPARE_MEMORY_REQUEST='128Mi', PREPARE_MEMORY_LIMIT='512Mi',
                        PLAN_MEMORY_BUDGET='', PLAN_FILE_BUDGET='')
        self.settings = settings
        write(self.root / 'settings.json', settings)
        self.create(pvc)
        self.create(self.data_job(self.name + '-data'))
        self.wait_job(self.name + '-data', True)
        source = self.transfer(settings, self.name + '-source-files')
        result = self.report(source, self.name + '-data')
        assert result['errors'] == []
        manifest_path = self.kube('exec', source, '--', 'jq', '-er', '.manifest.path',
                                  '/data/reports/' + self.name + '-data.json').stdout.decode().strip()
        bundle = self.root / 'bundle'
        bundle.mkdir()
        manifest = bundle / 'dataset-manifest.json'
        self.kube('cp', source + ':' + manifest_path, str(manifest))
        assert hashlib.sha256(manifest.read_bytes()).hexdigest() == result['manifest']['sha256']
        self.completed('returned-manifest-exported-without-source-body-copy')

        resources = list(yaml.safe_load_all(server.read_text()))
        resources[0]['spec']['type'] = 'NodePort'
        resources[0]['spec']['ports'][0]['nodePort'] = self.args.source_node_port
        resources[1]['spec']['template']['spec']['containers'][0]['volumeMounts'][0]['mountPath'] = mount
        server.write_text(yaml.safe_dump_all(resources))
        self.create(server)
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        rules = bundle / 'proxy-rules.json'
        write(rules, {'schema_version': 1, 'filtered_query_params': [], 'rewrite_url': False, 'use_tls': False})
        write(bundle / 'target-config.yaml', {'schema_version': 1,
              'target': {'ref': self.name, 'proxy': self.args.proxy,
                         'expected_server_ip': self.args.expected_server_ip, 'instance_ref': self.args.instance_ref},
              'proxy_rules_ref': {'path': rules.name, 'sha256': hashlib.sha256(rules.read_bytes()).hexdigest()}})
        slots = [self.metadata(index) for index in range(2)]
        for _, pod in slots:
            for name in ('dataset-manifest.json', 'target-config.yaml', 'proxy-rules.json'):
                self.upload(pod, bundle / name, '/inputs')
                self.check_copy(pod, name)
        self.completed('source-to-local-to-independent-metadata-volumes')

        values, first = slots[0]
        plan_job = self.prepare('plan', values, self.name + '-plan')
        plan = self.read_result(first, plan_job)
        assert plan['errors'] == [] and plan['reused'] is False
        plan_path = self.kube('exec', first, '--', 'jq', '-er', '.plan.path',
                              '/inputs/reports/' + plan_job + '.json').stdout.decode().strip()
        key = Path(plan_path).parent.name
        assert plan_path == '/inputs/plans/' + key + '/request-plan.json'
        (bundle / 'plans').mkdir()
        self.kube('cp', first + ':/inputs/plans/' + key, str(bundle / 'plans' / key))
        assert (bundle / 'plans' / key / 'shards' / '1.json').is_file()
        reused_job = self.prepare('plan', values, self.name + '-plan-reuse')
        reused = self.read_result(first, reused_job)
        assert reused['reused'] is True and reused['plan'] == plan['plan']
        self.completed('plan-job-keyed-output-and-reuse')

        # SRC-02 observes the failure through the real preparation command,
        # before this new origin URL can have populated the Seed cache.
        patch = {'spec': {'template': {'spec': {'containers': [{'name': 'file-server', 'volumeMounts': [
            {'name': 'source', 'mountPath': mount, 'subPath': 'dataset', 'readOnly': True}]}]}}}}
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        bad = {**values, 'PREPARATION_NAME': 'wrong-source'}
        failed_job = self.prepare('seed', bad, self.name + '-wrong-source', success=False)
        failed = self.read_result(first, failed_job)
        assert failed['status'] != 'ready', failed
        evidence = json.loads(self.kube('exec', first, '--', 'cat',
                              '/inputs/preparations/wrong-source/target-object-results.json').stdout)
        assert any(o['error'] and o['error']['stage'] == 'full_get' for o in evidence['objects'])
        self.kube('apply', '-f', str(server))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        self.completed('SRC-02-wrong-source-is-located-by-preparation')

        seed_job = self.prepare('seed', values, self.name + '-seed')
        ready = self.read_result(first, seed_job)
        assert ready['status'] == 'ready' and ready['errors'] == []
        (bundle / 'preparations').mkdir()
        prep = values['PREPARATION_NAME']
        self.kube('cp', first + ':/inputs/preparations/' + prep, str(bundle / 'preparations' / prep))
        assert (bundle / 'preparations' / prep / 'target-object-results.json').is_file()
        assert ready['preparation_id'] != prep
        failed_job = self.prepare('seed', values, self.name + '-seed-conflict', success=False)
        failed = self.read_result(first, failed_job)
        assert failed['status'] != 'ready' and any(e['code'] == 'DATA_CONFLICT' for e in failed['errors'])
        self.completed('PREP-01-real-Seed-ready-and-consumable-evidence')

        _, second = slots[1]
        self.upload(second, bundle / 'plans' / key, '/inputs/plans')
        self.upload(second, bundle / 'preparations' / prep, '/inputs/preparations')
        hashes = {}
        for path in sorted(bundle.rglob('*')):
            if path.is_file():
                relative = path.relative_to(bundle).as_posix()
                for _, pod in slots:
                    hashes[relative] = self.check_copy(pod, relative)
        write(self.root / 'copied-sha256.json', hashes)
        self.completed('whole-plan-and-evidence-copies-preserve-layout-and-bytes')

        run_id = self.name + '-run'
        jobs = []
        for index, (slot, _) in enumerate(slots):
            name = run_id + '-' + str(index)
            values = {**slot, 'JOB_NAME': name, 'RUN_ID': run_id, 'WORKER_ID': str(index),
                      'WORKER_NODE': self.args.node, 'WORKER_IMAGE': self.args.worker_image,
                      'PLAN_KEY': key, 'HTTP_PROXY': self.args.proxy, 'RATE': '4', 'DURATION': '3s',
                      'WORKER_CPU_REQUEST': '100m', 'WORKER_CPU_LIMIT': '2',
                      'WORKER_MEMORY_REQUEST': '256Mi', 'WORKER_MEMORY_LIMIT': '1Gi'}
            self.create(self.render_template('worker/worker-job.tpl.yaml', values,
                                            self.root / (name + '.yaml')))
            jobs.append(name)
        for index, name in enumerate(jobs):
            self.wait_job(name, True)
            raw = self.kube('logs', 'job/' + name).stdout
            (self.root / (name + '.log')).write_bytes(raw)
            records = {}
            for line in raw.decode().splitlines():
                if line.startswith('PROXY_BENCH_FIXED_POOL_'):
                    prefix, data = line.split(' ', 1)
                    assert prefix not in records, prefix
                    records[prefix] = json.loads(data)
            assert len(records) == 5, records.keys()
            for prefix, record in records.items():
                write(self.root / f'worker-{index}-{prefix}.json', record)
            summary = records['PROXY_BENCH_FIXED_POOL_WORKER_RESULT_V1']
            assert summary['result_complete'] and not summary['errors'], summary
            metrics = summary['workers'][0]['metrics']
            assert metrics['REQUESTS_TOTAL'] > 0 and metrics['SUCCESS_PCT'] == 100, metrics
            worker = records['PROXY_BENCH_FIXED_POOL_WORKER_SPEC_V1']
            assert worker['worker_id'] == index and worker['rate'] == 2, worker
            pod = self.pod_for_job(name)
            assert pod['status']['containerStatuses'][0]['restartCount'] == 0
        self.completed('MULTI-01-real-jobs-consume-independent-readonly-metadata')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('work-dir', 'namespace', 'node', 'storage-class', 'tools-image', 'file-server-image',
                'worker-image', 'source-base-url', 'proxy', 'expected-server-ip', 'instance-ref'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--source-node-port', type=int, required=True)
    parser.add_argument("--delivery", type=Path, required=True)
    args = parser.parse_args()
    for value in (args.tools_image, args.file_server_image, args.worker_image):
        if not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', value):
            parser.error('images must be repository@sha256:...')
    test = Preparation(args)
    try:
        test.execute()
    finally:
        test.cleanup()
    write(test.root / 'result.json', {'status': 'passed', 'cases': test.cases,
                                    'placement': 'caller-selected node; no cross-node claim'})
    print('Preparation regression passed; artifacts:', test.root)


if __name__ == '__main__':
    main()
