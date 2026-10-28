#!/usr/bin/env python3
"""Maintainer Kubernetes regression for image-only preparation and metadata handoff.

Requires PyYAML, envsubst, kubectl and existing immutable tools/worker/nginx
images. The caller supplies an isolated source NodePort and a real Seed proxy
with an expected server IP. Two PVCs exercise independent copies;
placing them on one node does not establish cross-node storage compatibility.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

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
        write(bundle / 'target-config.yaml', {'schema_version': 1,
              'target': {'ref': self.name, 'proxy': self.args.proxy,
                         'expected_server_ip': self.args.expected_server_ip}})
        slots = [self.metadata(index) for index in range(2)]
        for _, pod in slots:
            for name in ('dataset-manifest.json', 'target-config.yaml'):
                self.upload(pod, bundle / name, '/inputs')
                self.check_copy(pod, name)
        self.completed('source-to-local-to-independent-metadata-volumes')

        values, first = slots[0]
        plan_job = self.prepare('plan', values, self.name + '-plan')
        plan = self.read_result(first, plan_job)
        assert plan['errors'] == [] and 'reused' not in plan
        plan_path = self.kube('exec', first, '--', 'jq', '-er', '.plan.path',
                              '/inputs/reports/' + plan_job + '.json').stdout.decode().strip()
        assert plan_path == '/inputs/plans/request-plan.json'
        self.kube('cp', first + ':/inputs/plans', str(bundle / 'plans'))
        assert (bundle / 'plans' / 'shards' / '1.json').is_file()
        conflict_job = self.prepare('plan', values, self.name + '-plan-conflict', success=False)
        conflict = self.read_result(first, conflict_job)
        assert conflict['plan'] is None and conflict['errors'][0]['code'] == 'DATA_CONFLICT'
        self.completed('plan-job-explicit-output-refuses-existing-results')

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

        # A real Seed retains object zero when the origin rejects object one.
        # This fixture changes only the source's Nginx configuration, not Seed state.
        objects = json.loads(manifest.read_text())['objects']
        blocked_path = urlsplit(objects[1]['url']).path
        fault_name = self.name + '-source-fault'
        fault = self.root / 'source-fault.json'
        write(fault, {'apiVersion': 'v1', 'kind': 'ConfigMap',
                     'metadata': {'name': fault_name, 'namespace': self.args.namespace},
                     'data': {'default.conf': 'server { listen 80; root /usr/share/nginx/html; '
                              + 'location = ' + blocked_path + ' { return 503; } '
                              + 'location / { try_files $uri =404; } }'}})
        self.create(fault)
        source_patch = {'spec': {'template': {'spec': {
            'volumes': [{'name': 'source-fault', 'configMap': {'name': fault_name}}],
            'containers': [{'name': 'file-server', 'volumeMounts': [
                {'name': 'source-fault', 'mountPath': '/etc/nginx/conf.d/default.conf',
                 'subPath': 'default.conf', 'readOnly': True}]}]}}}}
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(source_patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        partial = {**values, 'PREPARATION_NAME': 'partial-source'}
        partial_job = self.prepare('seed', partial, self.name + '-partial-source', success=False)
        partial_result = self.read_result(first, partial_job)
        evidence = json.loads(self.kube('exec', first, '--', 'cat',
                            '/inputs/preparations/partial-source/target-object-results.json').stdout)['objects']
        assert partial_result['status'] != 'ready' and evidence[0]['status'] == 'ready', evidence
        assert evidence[1]['status'] != 'ready' and evidence[1]['error']['stage'] == 'full_get', evidence
        source_patch['spec']['template']['spec']['volumes'] = [{'name': 'source-fault', '$patch': 'delete'}]
        source_patch['spec']['template']['spec']['containers'][0]['volumeMounts'] = [
            {'mountPath': '/etc/nginx/conf.d/default.conf', '$patch': 'delete'}]
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(source_patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)

        seed_job = self.prepare('seed', values, self.name + '-seed')
        ready = self.read_result(first, seed_job)
        assert ready['status'] == 'ready' and ready['errors'] == []
        assert ready['method'] == 'http-full-body-and-finished-v1', ready
        (bundle / 'preparations').mkdir()
        prep = values['PREPARATION_NAME']
        self.kube('cp', first + ':/inputs/preparations/' + prep, str(bundle / 'preparations' / prep))
        assert (bundle / 'preparations' / prep / 'target-object-results.json').is_file()
        assert ready['preparation_id'] != prep
        failed_job = self.prepare('seed', values, self.name + '-seed-conflict', success=False)
        failed = self.read_result(first, failed_job)
        assert failed['status'] != 'ready' and any(e['code'] == 'DATA_CONFLICT' for e in failed['errors'])
        origin_log = self.kube('logs', self.name + '-0').stdout
        (self.root / 'recovery-origin.log').write_bytes(origin_log)
        assert objects[0]['sha256'].encode() not in origin_log, 'correct cached object unexpectedly refetched'
        assert objects[1]['sha256'].encode() in origin_log, 'missing object did not use repaired origin'
        self.completed('PREP-01-partial-cache-recovers-without-clearing-Seed')
        self.completed('PREP-01-real-Seed-ready-and-consumable-evidence')

        _, second = slots[1]
        self.upload(second, bundle / 'plans', '/inputs')
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
                      'RATE': '4', 'DURATION': '3s',
                      'WORKER_CPU_REQUEST': '100m', 'WORKER_CPU_LIMIT': '2',
                      'WORKER_MEMORY_REQUEST': '256Mi', 'WORKER_MEMORY_LIMIT': '1Gi'}
            path = self.render_template('worker/worker-job.tpl.yaml', values,
                                        self.root / (name + '.yaml'))
            resource = yaml.safe_load(path.read_text())
            assert resource['spec']['template']['spec']['containers'][0]['securityContext']['readOnlyRootFilesystem'] is True
            self.create(path)
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
            assert len(records) == 4, records.keys()
            for prefix, record in records.items():
                write(self.root / f'worker-{index}-{prefix}.json', record)
            execution = records['PROXY_BENCH_FIXED_POOL_WORKER_EXIT_V1']
            assert execution['exit_code'] == 0 and execution['termination_reason'] == 'completed'
            native = records['PROXY_BENCH_FIXED_POOL_K6_SUMMARY_V1']
            metrics = native['native_summary']['metrics']
            assert metrics['http_reqs']['values']['count'] > 0 and metrics['http_req_failed']['values']['rate'] == 0
            worker = records['PROXY_BENCH_FIXED_POOL_WORKER_SPEC_V1']
            assert worker['worker_id'] == index and worker['rate'] == 2, worker
            pod = self.pod_for_job(name)
            assert pod['status']['containerStatuses'][0]['restartCount'] == 0
            container = pod['spec']['containers'][0]
            assert container['securityContext']['readOnlyRootFilesystem']
            assert pod['spec']['securityContext']['runAsUser'] != 0
            assert any(mount['mountPath'] == '/tmp' for mount in container['volumeMounts'])
            assert 'NOT EVALUATED (no thresholds configured)' in raw.decode()
        self.completed('MULTI-01-real-jobs-consume-independent-readonly-metadata')
        self.completed('RUN-01-readonly-root-uses-temporary-volume-and-shared-report')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('work-dir', 'namespace', 'node', 'storage-class', 'tools-image', 'file-server-image',
                'worker-image', 'source-base-url', 'proxy', 'expected-server-ip'):
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
