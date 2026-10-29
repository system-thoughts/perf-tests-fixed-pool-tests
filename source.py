#!/usr/bin/env python3
"""Maintainer regression for the real Kubernetes source bundle, not a user preflight.

Requires an existing test namespace, source node, StorageClass and immutable
tools/nginx images. Creates only uniquely named test resources. Uses a local
port-forward to test HTTP paths; it does not prove Seed-to-origin reachability.
"""
import argparse
from contextlib import contextmanager
import hashlib
import http.client
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
from delivery import record_candidate

import yaml

def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def option(container, flag, value):
    container['args'][container['args'].index(flag) + 1] = str(value)


def result_json(raw):
    # CLI emits one compact JSON result line; diagnostics remain separate lines.
    values = []
    for line in raw.decode().splitlines():
        if line.startswith('{'):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if 'schema_version' in value and 'errors' in value:
                values.append(value)
    assert len(values) == 1, raw.decode()
    return values[0]


class Regression:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.work_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        record_candidate(args.delivery, self.root)
        self.source = Path(args.delivery).resolve() / 'deploy'
        self.name = 'fp-source-' + uuid.uuid4().hex[:10]
        self.created, self.cases, self.sequence = [], [], 0

    def run(self, command, timeout=60, data=None, check=True):
        self.sequence += 1
        prefix = self.root / f"command-{self.sequence:03}"
        write(prefix.with_suffix(".argv.json"), command)
        result = subprocess.run(command, input=data, capture_output=True, timeout=timeout)
        prefix.with_suffix(".stdout").write_bytes(result.stdout)
        prefix.with_suffix(".stderr").write_bytes(result.stderr)
        prefix.with_suffix(".exit").write_text(str(result.returncode) + "\n")
        if check and result.returncode:
            raise RuntimeError(f"command failed; inspect {prefix}.stderr")
        return result

    def kube(self, *command, **kwargs):
        return self.run(["kubectl", "-n", self.args.namespace, *command], **kwargs)

    def get(self, kind, name):
        return json.loads(self.kube("get", kind, name, "-o", "json").stdout)

    def create(self, path):
        result = self.kube("create", "-f", str(path), "-o", "json", check=False)
        # kubectl can emit consecutive JSON objects for a multi-document file.
        raw = result.stdout.decode().lstrip()
        while raw:
            value, end = json.JSONDecoder().raw_decode(raw)
            for resource in value.get("items", [value]):
                self.created.append((resource["kind"], resource["metadata"]["name"],
                                     resource["metadata"]["uid"]))
            raw = raw[end:].lstrip()
        if result.returncode:
            raise RuntimeError("resource creation failed; inspect command stderr")

    def completed(self, name):
        self.cases.append(name)
        write(self.root / "completed-cases.json", self.cases)

    def wait_job(self, name, success):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status = self.get("job", name).get("status", {})
            if status.get("succeeded", 0):
                assert success, "generation unexpectedly succeeded"
                return
            if status.get("failed", 0):
                assert not success, "generation failed; inspect Job/Pod artifacts"
                return
            time.sleep(1)
        raise TimeoutError(f"Job {name}: inspect Pod scheduling/image/storage events")

    def material(self, name):
        docs = list(yaml.safe_load_all((self.source / name).read_text()))
        for doc in docs:
            doc['metadata']['namespace'] = self.args.namespace
            if doc['kind'] in ('Job', 'StatefulSet'):
                doc['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': self.args.node}
        return docs

    def file(self, name, docs):
        path = self.root / (name + '.yaml')
        path.write_text(yaml.safe_dump_all(docs, sort_keys=False))
        return path

    def setup(self, size=64, count=2, base_url=None):
        pvc = self.material('storage.yaml')[0]
        pvc['metadata']['name'] = self.name
        pvc['spec']['storageClassName'] = self.args.storage_class
        pvc['spec']['resources']['requests']['storage'] = '64Mi'
        self.create(self.file('source-pvc', [pvc]))
        data = self.material('data.yaml')[0]
        data['spec']['template']['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = self.name
        c = data['spec']['template']['spec']['containers'][0]
        c['image'] = self.args.tools_image
        option(c, '--size', size)
        option(c, '--count', count)
        option(c, '--base-url', base_url or 'http://' + self.name + '.' + self.args.namespace + '.svc.cluster.local')
        self.data = data
        self.server = self.material('file-server.yaml')
        for doc in self.server:
            doc['metadata']['name'] = self.name
        labels = {'app.kubernetes.io/name': self.name}
        self.server[0]['spec']['selector'] = labels
        spec = self.server[1]['spec']
        spec['serviceName'] = self.name
        spec['selector']['matchLabels'] = labels
        spec['template']['metadata']['labels'] = labels
        spec['template']['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = self.name
        spec['template']['spec']['containers'][0]['image'] = self.args.file_server_image

    def data_job(self, name):
        self.data['metadata']['name'] = name
        return self.file(name, [self.data])

    def files_pod(self, name, claim, mount):
        # Test-owned access fixture, not a second copy of a product template.
        pod = {'apiVersion': 'v1', 'kind': 'Pod',
               'metadata': {'name': name, 'namespace': self.args.namespace},
               'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                        'nodeSelector': {'kubernetes.io/hostname': self.args.node},
                        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000,
                                            'runAsGroup': 1000, 'fsGroup': 1000,
                                            'seccompProfile': {'type': 'RuntimeDefault'}},
                        'containers': [{'name': 'files', 'image': self.args.tools_image,
                                        'command': ['/bin/sleep', '3600'],
                                        'securityContext': {'allowPrivilegeEscalation': False,
                                                            'readOnlyRootFilesystem': True,
                                                            'capabilities': {'drop': ['ALL']}},
                                        'volumeMounts': [{'name': 'files', 'mountPath': mount}]}],
                        'volumes': [{'name': 'files', 'persistentVolumeClaim': {'claimName': claim}}]}}
        self.create(self.file(name, [pod]))
        self.kube('wait', '--for=condition=Ready', 'pod/' + name, '--timeout=120s', timeout=130)
        return name

    def report(self, name):
        raw = self.kube('logs', 'job/' + name).stdout
        (self.root / (name + '.log')).write_bytes(raw)
        value = result_json(raw)
        write(self.root / (name + '-result.json'), value)
        return value

    def pod_for_job(self, name):
        items = json.loads(self.kube("get", "pods", "-l",
                                    "batch.kubernetes.io/job-name=" + name, "-o", "json").stdout)["items"]
        assert len(items) == 1, "inspect unexpected replacement Pods"
        return items[0]

    @contextmanager
    def forward(self):
        log = self.root / f"forward-{self.sequence:03}.log"
        with log.open("w") as output:
            process = subprocess.Popen(["kubectl", "-n", self.args.namespace, "port-forward",
                                        "service/" + self.name, ":80", "--address=127.0.0.1"],
                                       stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", log.read_text())
                    if match:
                        yield int(match.group(1))
                        return
                    if process.poll() is not None:
                        raise RuntimeError(f"port-forward failed; inspect {log}")
                    time.sleep(0.1)
                raise TimeoutError("port-forward readiness")
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def http(self, port, path, method="GET", headers=None):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            client.request(method, path, headers=headers or {})
            response = client.getresponse()
            body = response.read(65)  # The fixed fixture is 64 bytes; never collect arbitrary bodies.
            facts = {"method": method, "path": path, "status": response.status,
                     "headers": dict(response.getheaders()), "received_bytes": len(body),
                     "sha256": hashlib.sha256(body).hexdigest()}
            write(self.root / f"http-{self.sequence:03}-{len(list(self.root.glob('http-*.json'))):02}.json", facts)
            return response, body
        finally:
            client.close()

    def execute(self):
        self.setup()
        self.create(self.data_job(self.name + '-data'))
        self.wait_job(self.name + '-data', True)
        generated = self.report(self.name + '-data')
        assert generated['errors'] == [] and generated['generated_objects'] == 2
        files = self.files_pod(self.name + '-files', self.name, '/data')
        raw = self.kube('exec', files, '--', 'cat', generated['manifest']['path']).stdout
        assert hashlib.sha256(raw).hexdigest() == generated['manifest']['sha256']
        manifest = json.loads(raw)
        (self.root / 'dataset-manifest.json').write_bytes(raw)
        self.create(self.file('server', self.server))
        self.kube('rollout', 'status', 'statefulset/' + self.name, '--timeout=120s', timeout=130)
        with self.forward() as port:
            for obj in manifest['objects']:
                path = '/' + obj['relative_path']
                response, body = self.http(port, path)
                assert response.status == 200 and len(body) == 64
                assert hashlib.sha256(body).hexdigest() == obj['sha256']
                full_body = body
                response, body = self.http(port, path, 'HEAD')
                assert response.status == 200 and response.getheader('Content-Length') == '64' and not body
                response, body = self.http(port, path, headers={'Range': 'bytes=0-15'})
                assert response.status == 206 and response.getheader('Content-Range') == 'bytes 0-15/64'
                assert body == full_body[:16]
            response, _ = self.http(port, '/objects/sha256/absent')
            assert response.status == 404
        self.completed('SRC-01-generated-objects-GET-HEAD-Range')
        conflict = self.name + '-conflict'
        self.create(self.data_job(conflict))
        self.wait_job(conflict, False)
        failed = self.report(conflict)
        assert failed['manifest'] is None and failed['errors'][0]['code'] == 'DATA_CONFLICT'
        preserved = self.kube('exec', files, '--', 'cat', generated['manifest']['path']).stdout
        assert preserved == raw
        assert b'DATA.root' in (self.root / (conflict + '.log')).read_bytes()
        self.completed('source-conflict-is-visible-in-log-and-preserves-manifest')

    def cleanup(self):
        if not self.created:
            return
        # Keep the last UID when a test deliberately deletes/recreates its Job.
        expected = {(kind, name): uid for kind, name, uid in self.created}
        owned_uids = {uid for _, _, uid in self.created}
        pods = json.loads(self.kube("get", "pods", "-o", "json").stdout)["items"]
        owned_pods = [pod for pod in pods if
                      ("Pod", pod["metadata"]["name"]) in expected or
                      any(owner["uid"] in owned_uids for owner in pod["metadata"].get("ownerReferences", []))]
        write(self.root / "cleanup-pods.json", owned_pods)
        for pod in owned_pods:
            self.kube("logs", pod["metadata"]["name"], "--pod-running-timeout=2s", check=False)
        self.kube("get", "events", "-o", "json", check=False)
        # Batch observation avoids one discovery/get/delete round trip per object.
        refs = [kind + "/" + name for kind, name in expected]
        result = self.kube("get", *refs, "-o", "json", "--ignore-not-found")
        observed = json.loads(result.stdout)
        resources = observed.get("items", [observed])
        selected = []
        for resource in resources:
            key = resource["kind"], resource["metadata"]["name"]
            assert resource["metadata"]["uid"] == expected[key], "resource identity changed; not deleting"
            selected.append(key[0] + "/" + key[1])
        write(self.root / "cleanup-uid-check.json", resources)
        if selected:
            self.kube("delete", *selected, "--wait=true", "--timeout=120s", timeout=130)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("work-dir", "namespace", "node", "storage-class", "tools-image", "file-server-image"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--delivery", type=Path, required=True)
    args = parser.parse_args()
    for value in (args.tools_image, args.file_server_image):
        if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", value):
            parser.error("images must be repository@sha256:...")
    test = Regression(args)
    try:
        test.execute()
    finally:
        test.cleanup()
    write(test.root / "result.json", {"status": "passed", "cases": test.cases})
    print("Source regression passed; artifacts:", test.root)


if __name__ == "__main__":
    main()
