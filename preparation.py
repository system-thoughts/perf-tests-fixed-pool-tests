#!/usr/bin/env python3
"""Consume ordinary deployment examples against a real Seed with explicit input files.

The fixtures provide file placement, isolated names and a source fault. They are
not benchmark commands. Two same-node volumes do not prove cross-node behavior.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

from source import Regression, option, write


class Preparation(Regression):
    def metadata(self, index):
        name = self.name + '-metadata-' + str(index)
        pvc = self.material('storage.yaml')[1]
        pvc['metadata']['name'] = name
        pvc['spec']['storageClassName'] = self.args.storage_class
        pvc['spec']['resources']['requests']['storage'] = '128Mi'
        self.create(self.file(name, [pvc]))
        return name, self.files_pod(name + '-files', name, '/inputs')

    def upload(self, pod, local, parent):
        self.kube('exec', pod, '--', 'mkdir', '-p', parent)
        self.kube('exec', pod, '--', 'test', '!', '-e', parent.rstrip('/') + '/' + local.name)
        self.kube('cp', '--no-preserve', str(local), pod + ':' + parent, timeout=120)

    def prepare(self, kind, claim, name, out, success=True, range_bytes=65536):
        job = self.material(kind + '.yaml')[-1]
        job['metadata']['name'] = name
        pod = job['spec']['template']['spec']
        pod['volumes'][0]['persistentVolumeClaim']['claimName'] = claim
        c = pod['containers'][0]
        c['image'] = self.args.tools_image
        option(c, '--out', out)
        if kind == 'plan':
            option(c, '--range', range_bytes)
            option(c, '--workers', 2)
        else:
            pod['volumes'][1]['configMap']['name'] = self.name + '-target'
        self.create(self.file(name, [job]))
        self.wait_job(name, success)
        return self.report(name)

    def execute(self):
        # Unique URLs prevent an old cache from masking this origin fault.
        self.setup(4 * 1024**2, 2, self.args.source_base_url.rstrip('/') + '/' + self.name)
        self.create(self.data_job(self.name + '-data'))
        self.wait_job(self.name + '-data', True)
        generated = self.report(self.name + '-data')
        source = self.files_pod(self.name + '-source-files', self.name, '/data')
        bundle = self.root / 'bundle'
        bundle.mkdir()
        manifest = bundle / 'dataset-manifest.json'
        self.kube('cp', source + ':' + generated['manifest']['path'], str(manifest))
        assert hashlib.sha256(manifest.read_bytes()).hexdigest() == generated['manifest']['sha256']
        slots = [self.metadata(index) for index in range(2)]
        for _, pod in slots:
            self.upload(pod, manifest, '/inputs')
        self.completed('explicit-manifest-placed-on-independent-input-volumes')

        mount = '/usr/share/nginx/html/' + self.name + '/objects'
        self.server[0]['spec']['type'] = 'NodePort'
        self.server[0]['spec']['ports'][0]['nodePort'] = self.args.source_node_port
        self.server[1]['spec']['template']['spec']['containers'][0]['volumeMounts'][0]['mountPath'] = mount
        server = self.file('server', self.server)
        self.create(server)
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        config = self.material('seed.yaml')[0]
        config['metadata']['name'] = self.name + '-target'
        config['data']['target-config.yaml'] = 'schema_version: 1\ntarget:\n  proxy: ' + self.args.proxy + '\n  expected_server_ip: ' + self.args.expected_server_ip + '\n'
        self.create(self.file('target', [config]))
        claim, first = slots[0]
        plan = self.prepare('plan', claim, self.name + '-plan', '/inputs/plan')
        assert plan['errors'] == [] and plan['plan']['path'] == '/inputs/plan/request-plan.json'
        self.kube('cp', first + ':/inputs/plan', str(bundle / 'plan'))
        assert (bundle / 'plan/shards/1.json').is_file()
        conflict = self.prepare('plan', claim, self.name + '-plan-conflict', '/inputs/plan', False)
        assert conflict['plan'] is None and conflict['errors'][0]['code'] == 'DATA_CONFLICT'
        changed = self.prepare('plan', claim, self.name + '-plan-r2', '/inputs/plan-r2', range_bytes=131072)
        assert changed['errors'] == []
        self.completed('plan-independent-output-conflict-and-range-change')

        patch = {'spec': {'template': {'spec': {'containers': [{'name': 'file-server', 'volumeMounts': [
            {'name': 'source', 'mountPath': mount, 'subPath': 'dataset', 'readOnly': True}]}]}}}}
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        failed = self.prepare('seed', claim, self.name + '-wrong-source', '/inputs/wrong-source', False)
        assert failed['status'] != 'ready'
        evidence = json.loads(self.kube('exec', first, '--', 'cat', '/inputs/wrong-source/target-object-results.json').stdout)
        assert any(o['error'] and o['error']['stage'] == 'full_get' for o in evidence['objects'])
        self.kube('apply', '-f', str(server))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        self.completed('SRC-02-wrong-source-located-in-log-and-evidence')

        objects = json.loads(manifest.read_text())['objects']
        fault_name = self.name + '-source-fault'
        fault = {'apiVersion': 'v1', 'kind': 'ConfigMap',
                 'metadata': {'name': fault_name, 'namespace': self.args.namespace},
                 'data': {'default.conf': 'server { listen 80; root /usr/share/nginx/html; '
                          + 'location = ' + urlsplit(objects[1]['url']).path + ' { return 503; } '
                          + 'location / { try_files $uri =404; } }'}}
        self.create(self.file('source-fault', [fault]))
        patch = {'spec': {'template': {'spec': {
            'volumes': [{'name': 'source-fault', 'configMap': {'name': fault_name}}],
            'containers': [{'name': 'file-server', 'volumeMounts': [
                {'name': 'source-fault', 'mountPath': '/etc/nginx/conf.d/default.conf',
                 'subPath': 'default.conf', 'readOnly': True}]}]}}}}
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        partial = self.prepare('seed', claim, self.name + '-partial', '/inputs/partial', False)
        observed = json.loads(self.kube('exec', first, '--', 'cat', '/inputs/partial/target-object-results.json').stdout)['objects']
        assert partial['status'] != 'ready' and observed[0]['status'] == 'ready'
        assert observed[1]['status'] != 'ready' and observed[1]['error']['stage'] == 'full_get'
        patch['spec']['template']['spec']['volumes'] = [{'name': 'source-fault', '$patch': 'delete'}]
        patch['spec']['template']['spec']['containers'][0]['volumeMounts'] = [
            {'mountPath': '/etc/nginx/conf.d/default.conf', '$patch': 'delete'}]
        self.kube('patch', 'statefulset', self.name, '--type=strategic', '-p', json.dumps(patch))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        ready = self.prepare('seed', claim, self.name + '-seed', '/inputs/preparation')
        assert ready['status'] == 'ready' and ready['errors'] == []
        self.kube('cp', first + ':/inputs/preparation', str(bundle / 'preparation'))
        conflict = self.prepare('seed', claim, self.name + '-seed-conflict', '/inputs/preparation', False)
        assert conflict['status'] != 'ready' and conflict['errors'][0]['code'] == 'DATA_CONFLICT'
        origin_log = self.kube('logs', self.name + '-0').stdout
        (self.root / 'recovery-origin.log').write_bytes(origin_log)
        assert objects[0]['sha256'].encode() not in origin_log, 'correct cache unexpectedly refetched'
        assert objects[1]['sha256'].encode() in origin_log, 'missing object did not use repaired origin'
        self.completed('PREP-01-repair-and-rerun-without-Seed-clear-or-plan-regeneration')
        # A failed ready record must stop the delivered worker before k6 starts.
        bad = self.worker(claim, 0, self.name + '-nonready', '/inputs/partial/target-preparation.json')
        self.wait_job(bad, False)
        raw = self.kube('logs', 'job/' + bad).stdout
        assert b'TARGET_PREPARATION' in raw and b'Inputs checked:' not in raw
        self.completed('nonready-file-rejected-before-pressure')

        _, second = slots[1]
        for child in ('plan', 'preparation'):
            self.upload(second, bundle / child, '/inputs')
        run_id = self.name + '-run'
        jobs = [self.worker(c, i, run_id + '-' + str(i)) for i, (c, _) in enumerate(slots)]
        common = []
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
            for prefix, value in records.items():
                write(self.root / f'worker-{index}-{prefix}.json', value)
            assert records['PROXY_BENCH_FIXED_POOL_WORKER_EXIT_V1']['exit_code'] == 0
            metrics = records['PROXY_BENCH_FIXED_POOL_K6_SUMMARY_V1']['native_summary']['metrics']
            assert metrics['http_reqs']['values']['count'] > 0 and metrics['http_req_failed']['values']['rate'] == 0
            worker = records['PROXY_BENCH_FIXED_POOL_WORKER_SPEC_V1']
            assert worker['worker_id'] == index and worker['rate'] == 2
            common.append(records['PROXY_BENCH_FIXED_POOL_RUN_SPEC_V1'])
            pod = self.pod_for_job(name)
            assert pod['spec']['securityContext']['runAsUser'] == 1000
            assert pod['spec']['containers'][0]['securityContext']['readOnlyRootFilesystem'] is True
            assert 'NOT EVALUATED (no thresholds configured)' in raw.decode()
        assert common[0] == common[1]
        self.completed('MULTI-01-independent-readonly-volumes-and-shared-target-config')

    def worker(self, claim, index, name, preparation='/inputs/preparation/target-preparation.json'):
        job = self.material('worker.yaml')[0]
        job['metadata']['name'] = name
        pod = job['spec']['template']['spec']
        pod['volumes'][0]['persistentVolumeClaim']['claimName'] = claim
        pod['volumes'][1]['configMap']['name'] = self.name + '-target'
        c = pod['containers'][0]
        c['image'] = self.args.worker_image
        values = {'RATE': '4', 'DURATION': '3s', 'RUN_ID': self.name + '-run',
                  'WORKER_ID': str(index), 'TARGET_PREPARATION': preparation}
        for env in c['env']:
            if env['name'] in values:
                env['value'] = values[env['name']]
        self.create(self.file(name, [job]))
        return name


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('work-dir', 'namespace', 'node', 'storage-class', 'tools-image', 'file-server-image',
                'worker-image', 'source-base-url', 'proxy', 'expected-server-ip'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--source-node-port', type=int, required=True)
    parser.add_argument('--delivery', type=Path, required=True)
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
