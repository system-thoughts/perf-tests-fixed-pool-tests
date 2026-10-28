"""Stage test inputs from built images and their matching deployment directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def record_candidate(delivery, directory):
    if not __debug__:
        raise RuntimeError("Run without python -O; assertions are the test oracles")
    delivery = Path(delivery).resolve()
    value = json.loads((delivery / 'delivery.json').read_text())
    for name, expected in value['files'].items():
        path = (delivery / name).resolve()
        if not path.is_relative_to(delivery) or digest(path) != expected:
            raise ValueError(f'delivery file changed: {name}')
    os.environ['PATH'] = str(delivery / 'bin') + os.pathsep + os.environ['PATH']
    (Path(directory) / 'candidate.json').write_text(json.dumps(value, indent=2) + '\n')


def extract_image(image, files, output):
    # Resolve tags once; every copy uses the same immutable local image ID.
    info = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image]))[0]
    container = subprocess.check_output(['docker', 'create', '--entrypoint', '/bin/sh', info['Id']],
                                        text=True).strip()
    try:
        for source, destination in files.items():
            target = output / destination
            target.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(['docker', 'cp', f'{container}:{source}', str(target)], check=True)
    finally:
        subprocess.run(['docker', 'rm', container], check=True, stdout=subprocess.DEVNULL)
    return {'requested': image, 'id': info['Id'], 'repo_digests': info.get('RepoDigests', []),
            'os': info['Os'], 'architecture': info['Architecture']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tools-image', required=True)
    parser.add_argument('--worker-image', required=True)
    parser.add_argument('--deploy-dir', type=Path, required=True,
                        help='matching deploy directory containing source/dataset-job.tpl.yaml')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    deployment = args.deploy_dir.resolve()
    if not (deployment / 'source/dataset-job.tpl.yaml').is_file():
        parser.error('--deploy-dir must contain the matching fixed-pool deployment material')
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = extract_image(args.tools_image,
                          {'/usr/local/bin/proxy-bench-prepare': 'bin/proxy-bench-prepare'}, output)
    worker = extract_image(args.worker_image,
                           {'/usr/bin/k6': 'bin/k6',
                            '/usr/local/bin/proxy-bench-worker-support': 'bin/proxy-bench-worker-support',
                            '/home/k6/lib/.': 'lib',
                            '/home/k6/fixed-pool/.': 'runtime'}, output)
    for source in sorted(deployment.rglob('*')):
        if source.is_file() and (source.suffix == '.yaml' or source.name.endswith('.env.example')
                                 or source == deployment / 'check-env.sh'):
            target = output / 'deploy' / source.relative_to(deployment)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    files = {str(path.relative_to(output)): digest(path)
             for path in sorted(output.rglob('*')) if path.is_file()}
    value = {'tools_image': tools, 'worker_image': worker,
             'files': files}
    (output / 'delivery.json').write_text(json.dumps(value, indent=2) + '\n')
    print(output)


if __name__ == '__main__':
    main()
