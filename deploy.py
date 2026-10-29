#!/usr/bin/env python3
"""Check delivered Kubernetes examples; benchmark users do not run this check."""
import argparse
import json
from pathlib import Path
import subprocess

import yaml
from delivery import record_candidate


def validate(root, output):
    output.mkdir(parents=True, exist_ok=False)
    documents = {}
    for path in sorted(root.glob('*.yaml')):
        text = path.read_text()
        assert '${' not in text, path
        docs = list(yaml.safe_load_all(text))
        documents[path.name] = docs
        for doc in docs:
            assert doc['apiVersion'] and doc['kind'] and doc['metadata']['name']
            if doc['kind'] != 'Job':
                continue
            assert doc['spec']['backoffLimit'] == 0
            pod = doc['spec']['template']['spec']
            assert pod['restartPolicy'] == 'Never'
            assert pod['automountServiceAccountToken'] is False
            assert pod['securityContext']['runAsNonRoot'] is True
            assert pod['securityContext']['seccompProfile']['type'] == 'RuntimeDefault'
            for container in pod['containers']:
                security = container['securityContext']
                assert security['allowPrivilegeEscalation'] is False
                assert security['capabilities']['drop'] == ['ALL']
                assert security['readOnlyRootFilesystem'] is True
                if container['command'][:2] in (['/bin/sh', '-c'], ['/bin/sh', '-ec']):
                    subprocess.run(['/bin/sh', '-n'], input=container['args'][0],
                                   text=True, check=True)
    data = documents['data.yaml'][0]['spec']['template']['spec']
    server = documents['file-server.yaml'][1]['spec']['template']['spec']
    assert data['volumes'][0]['persistentVolumeClaim']['claimName'] == server['volumes'][0]['persistentVolumeClaim']['claimName']
    mounts = server['containers'][0]['volumeMounts']
    assert len(mounts) == 1 and mounts[0]['readOnly'] is True
    assert mounts[0]['subPath'] == 'dataset/objects'
    seed = documents['seed.yaml'][1]['spec']['template']['spec']
    worker = documents['worker.yaml'][0]['spec']['template']['spec']
    assert seed['volumes'][1]['configMap']['name'] == worker['volumes'][1]['configMap']['name']
    assert worker['containers'][0]['volumeMounts'][0]['readOnly'] is True
    assert any(v['name'] == 'tmp' and 'emptyDir' in v for v in worker['volumes'])
    assert any(v['name'] == 'tmp' and v['mountPath'] == '/tmp'
               for v in worker['containers'][0]['volumeMounts'])
    result = {'materials': list(documents), 'resources': sum(map(len, documents.values()))}
    (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print('Deployment checks passed:', result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--delivery', type=Path, required=True)
    args = parser.parse_args()
    validate(args.delivery.resolve() / 'deploy', Path(args.work_dir))
    record_candidate(args.delivery, Path(args.work_dir))
