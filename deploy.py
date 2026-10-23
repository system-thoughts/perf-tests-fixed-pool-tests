#!/usr/bin/env python3
"""Validate deployment rendering and shell syntax without a Kubernetes cluster.

Requires envsubst and PyYAML. Runtime behavior and storage admission are tested
by the Kubernetes regressions, not inferred from this static check.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

import yaml


def settings(root):
    values = {}
    for path in sorted(root.rglob('*.env.example')):
        for line in path.read_text().splitlines():
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                values[key] = value
    values.update(JOB_NAME='fixed-pool-test-001', TRANSFER_POD_NAME='fixed-pool-transfer',
                  NAMESPACE='benchmark', SOURCE_NODE='source-node', METADATA_NODE='load-node',
                  WORKER_NODE='load-node', RUN_ID='test-001', PLAN_KEY='a' * 64,
                  PREPARATION_NAME='prepare-001', BASE_URL='http://source.benchmark.svc.cluster.local',
                  HTTP_PROXY='http://seed:4001')
    for key in ('TOOLS_IMAGE', 'WORKER_IMAGE', 'FILE_SERVER_IMAGE'):
        values[key] = 'example.invalid/fixture@sha256:' + '0' * 64
    return values


def render(root, template, values):
    required = set(re.findall(r'\$\{([A-Z][A-Z0-9_]*)\}', template.read_text()))
    missing = required - values.keys()
    if missing:
        raise ValueError(f'{template}: missing settings {sorted(missing)}')
    if 'JOB_NAME' in required or 'TRANSFER_POD_NAME' in required:
        name = values['JOB_NAME' if 'JOB_NAME' in required else 'TRANSFER_POD_NAME']
        subprocess.run(['/bin/sh', '-ec', '. "$1"; check_job_name "$2"', 'check',
                        str(root / 'check-env.sh'), name], check=True)
    result = subprocess.run(['envsubst', ' '.join('${' + key + '}' for key in sorted(required))],
                            input=template.read_text(), text=True, capture_output=True, check=True,
                            env={**os.environ, **values}).stdout
    # Only deployment placeholders are rendered. Container shell variables survive.
    assert not re.search(r'\$\{(?:' + '|'.join(sorted(required)) + r')\}', result), template
    return result


def validate(root, output):
    output.mkdir(parents=True, exist_ok=False)
    values = settings(root)
    results = []
    for template in sorted(root.rglob('*.yaml')):
        rendered = render(root, template, values)
        target = output / template.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered)
        originals = list(yaml.safe_load_all(template.read_text()))
        for original, document in zip(originals, yaml.safe_load_all(rendered)):
            assert document['apiVersion'] and document['kind'] and document['metadata']['name']
            if document['kind'] == 'Job':
                spec = document['spec']
                assert spec['backoffLimit'] == 0
                pod = spec['template']['spec']
                assert pod['restartPolicy'] == 'Never'
            elif document['kind'] == 'Pod':
                pod = document['spec']
            else:
                continue
            assert pod['automountServiceAccountToken'] is False
            assert pod['securityContext']['runAsNonRoot'] is True
            assert pod['securityContext']['seccompProfile']['type'] == 'RuntimeDefault'
            for container in pod['containers']:
                assert container['securityContext']['allowPrivilegeEscalation'] is False
                assert container['securityContext']['capabilities']['drop'] == ['ALL']
                if container['command'][:2] == ['/bin/sh', '-ec'] and container.get('args'):
                    original_pod = original['spec']['template']['spec']
                    original_script = original_pod['containers'][0]['args'][0]
                    assert container['args'][0] == original_script, 'envsubst expanded container shell variables'
                    subprocess.run(['/bin/sh', '-n'], input=container['args'][0], text=True, check=True)
                    assert 'set -C' in container['args'][0]
        results.append(str(template.relative_to(root)))
    cases = {'valid': ['a', '0', 'a-b', 'a' * 63],
             'invalid': ['', '-a', 'a-', 'A', 'a.b', 'a_b', '../a', 'a/b', 'a' * 64]}
    for expectation, names in cases.items():
        for name in names:
            result = subprocess.run(['/bin/sh', '-ec', '. "$1"; check_job_name "$2"', 'check',
                                     str(root / 'check-env.sh'), name], capture_output=True)
            assert (result.returncode == 0) == (expectation == 'valid'), name
    (output / 'result.json').write_text(json.dumps({'templates': results, 'job_names': cases}, indent=2) + '\n')
    print(f'Deployment checks passed: {len(results)} templates, 13 name cases')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir', required=True)
    args = parser.parse_args()
    validate(Path(__file__).resolve().parents[1] / 'deploy', Path(args.work_dir))
