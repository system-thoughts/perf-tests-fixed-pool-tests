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

import yaml

from deploy import render as render_deployment


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Regression:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.work_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.source = Path(__file__).resolve().parents[1] / "deploy/source"
        self.name = "fp-source-" + uuid.uuid4().hex[:10]
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

    def render(self):
        settings = dict(line.split("=", 1) for line in
                        (self.source / "source.env.example").read_text().splitlines()
                        if line and not line.startswith("#"))
        settings.update(SOURCE_NAME=self.name, PVC_NAME=self.name,
                        JOB_NAME=self.name + "-data",
                        NAMESPACE=self.args.namespace, SOURCE_NODE=self.args.node,
                        TOOLS_IMAGE=self.args.tools_image, FILE_SERVER_IMAGE=self.args.file_server_image,
                        PVC_STORAGE_CLASS=self.args.storage_class, PVC_CAPACITY="64Mi", B="64", N="2")
        settings["BASE_URL"] = f'http://{self.name}.{self.args.namespace}.svc.cluster.local'
        write(self.root / "settings.json", settings)
        self.settings = settings
        paths = []
        for template in ("pvc.example.yaml", "dataset-job.tpl.yaml", "file-server.tpl.yaml"):
            path = self.root / template.replace(".tpl", "").replace(".example", "")
            rendered = render_deployment(self.source.parent, self.source / template, settings)
            path.write_text(rendered)
            self.kube("apply", "--dry-run=server", "-f", str(path))
            paths.append(path)
        return settings, paths

    def render_template(self, name, settings, output):
        output.write_text(render_deployment(self.source.parent, self.source.parent / name, settings))
        self.kube("apply", "--dry-run=server", "-f", str(output))
        return output

    def transfer(self, settings, name):
        values = {**settings, "TRANSFER_POD_NAME": name}
        path = self.render_template("metadata/source-transfer-pod.tpl.yaml", values,
                                    self.root / (name + ".yaml"))
        self.create(path)
        self.kube("wait", "--for=condition=Ready", "pod/" + name, "--timeout=120s", timeout=130)
        return name

    def report(self, pod, name):
        raw = self.kube("exec", pod, "--", "cat", "/data/reports/" + name + ".json").stdout
        (self.root / (name + "-report.json")).write_bytes(raw)
        return json.loads(raw)

    def data_job(self, name, **changes):
        settings = {**self.settings, "JOB_NAME": name, **changes}
        return self.render_template("source/dataset-job.tpl.yaml", settings,
                                    self.root / (name + ".yaml"))

    def pod_for_job(self, name):
        items = json.loads(self.kube("get", "pods", "-l",
                                    "batch.kubernetes.io/job-name=" + name, "-o", "json").stdout)["items"]
        assert len(items) == 1, "inspect unexpected replacement Pods"
        return items[0]

    def report_failures(self, transfer, initial_report):
        # Recreating the original Job must fail at redirection before Go starts.
        name = self.name + "-data"
        self.create(self.data_job(name))
        self.wait_job(name, success=False)
        log = self.kube("logs", "job/" + name).stdout
        assert b"Preparing a new fixed dataset" not in log
        assert self.report(transfer, name) == initial_report
        self.completed("existing-report-is-not-overwritten")

        # The data root remains writable: only the report mount is read-only.
        name = self.name + "-readonly-report"
        path = self.data_job(name)
        spec = yaml.safe_load(path.read_text())
        pod = spec["spec"]["template"]["spec"]
        pod["volumes"].append({"name": "readonly-report", "emptyDir": {}})
        pod["containers"][0]["volumeMounts"].append(
            {"name": "readonly-report", "mountPath": "/data/reports", "readOnly": True})
        path.write_text(json.dumps(spec))
        self.create(path)
        self.wait_job(name, success=False)
        assert b"Preparing a new fixed dataset" not in self.kube("logs", "job/" + name).stdout
        assert self.kube("exec", transfer, "--", "test", "!", "-e",
                         "/data/reports/" + name + ".json", check=False).returncode == 0
        self.completed("report-write-failure-does-not-start-generation")

        # Two test Jobs deliberately collide on one report path. The dataset
        # already exists, so the single admitted generator reports DATA_CONFLICT.
        shared = self.name + "-race"
        names = [shared + "-0", shared + "-1"]
        for name in names:
            path = self.data_job(name)
            spec = yaml.safe_load(path.read_text())
            for entry in spec["spec"]["template"]["spec"]["containers"][0]["env"]:
                if entry["name"] == "JOB_NAME":
                    entry["value"] = shared
            path.write_text(json.dumps(spec))
            self.create(path)
        admitted = 0
        for name in names:
            self.wait_job(name, success=False)
            admitted += b"Preparing a new fixed dataset" in self.kube("logs", "job/" + name).stdout
        assert admitted == 1, "exclusive report creation admitted more than one generator"
        report = self.report(transfer, shared)
        assert report["manifest"] is None and report["errors"][0]["code"] == "DATA_CONFLICT"
        self.completed("concurrent-report-creation-admits-one-writer")

        # Startup failure leaves an empty reserved report, never a success JSON.
        name = self.name + "-missing-command"
        path = self.data_job(name)
        spec = yaml.safe_load(path.read_text())
        args = spec["spec"]["template"]["spec"]["containers"][0]["args"]
        args[0] = args[0].replace("/usr/local/bin/proxy-bench-data-prepare", "/nonexistent-command")
        path.write_text(json.dumps(spec))
        self.create(path)
        self.wait_job(name, success=False)
        raw = self.kube("exec", transfer, "--", "cat", "/data/reports/" + name + ".json").stdout
        assert raw == b""
        status = self.pod_for_job(name)["status"]["containerStatuses"][0]["state"]["terminated"]
        assert status["exitCode"] == 127
        self.completed("startup-failure-empty-report-is-not-success")

    def interrupt_generation(self):
        # A separate pool leaves the successful source and its report untouched.
        name = self.name + "-interrupt"
        pvc = yaml.safe_load((self.root / "pvc.yaml").read_text())
        pvc["metadata"]["name"] = name
        pvc["spec"]["resources"]["requests"]["storage"] = "4Gi"
        path = self.root / (name + "-pvc.json")
        write(path, pvc)
        self.create(path)
        self.create(self.data_job(name, PVC_NAME=name, B=str(4 * 1024**3), N="1"))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            items = json.loads(self.kube("get", "pods", "-l",
                                        "batch.kubernetes.io/job-name=" + name, "-o", "json").stdout)["items"]
            if items and items[0]["status"]["phase"] == "Running":
                pod = items[0]["metadata"]["name"]
                result = self.kube("exec", pod, "--", "/bin/sh", "-ec",
                                   'test "$(readlink /proc/1/exe)" = /usr/local/bin/proxy-bench-data-prepare; '
                                   'test -d /data/dataset/.fixed-pool-incomplete; kill -TERM 1', check=False)
                if result.returncode == 0:
                    break
            time.sleep(0.1)
        else:
            raise TimeoutError("could not signal the running generator")
        self.wait_job(name, success=False)
        status = self.pod_for_job(name)["status"]["containerStatuses"][0]["state"]["terminated"]
        assert status["exitCode"] == 130, status
        transfer = self.transfer({**self.settings, "PVC_NAME": name}, name + "-files")
        report = self.report(transfer, name)
        assert report["manifest"] is None and any(e["code"] == "INTERRUPTED" for e in report["errors"])
        self.completed("sigterm-retains-failure-report")

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
        settings, (pvc, job, server) = self.render()
        self.create(pvc)
        self.create(job)
        self.wait_job(self.name + "-data", success=True)
        generation = self.get("job", self.name + "-data")["spec"]["template"]["spec"]
        assert generation["volumes"][0]["persistentVolumeClaim"]["claimName"] == settings["PVC_NAME"]
        pods = json.loads(self.kube("get", "pods", "-l",
                                   "batch.kubernetes.io/job-name=" + self.name + "-data", "-o", "json").stdout)["items"]
        assert len(pods) == 1 and pods[0]["status"]["phase"] == "Succeeded"
        self.kube("logs", pods[0]["metadata"]["name"])
        transfer = self.transfer(settings, self.name + "-files")
        generated = self.report(transfer, self.name + "-data")
        assert generated["errors"] == []
        assert generated["generated_objects"] == 2 and generated["generated_bytes"] == 128
        manifest_id = generated["manifest"]["sha256"]
        assert generated["manifest"]["path"] == f"/data/dataset/manifests/{manifest_id}/dataset-manifest.json"
        self.kube("delete", "job", self.name + "-data", "--wait=true")
        self.create(server)
        self.kube("rollout", "status", "statefulset/" + self.name, "--timeout=120s", timeout=130)
        serving = self.get("statefulset", self.name)["spec"]["template"]["spec"]
        assert serving["volumes"][0]["persistentVolumeClaim"]["claimName"] == settings["PVC_NAME"]
        mounts = {m["mountPath"]: m for m in serving["containers"][0]["volumeMounts"]}
        assert mounts["/usr/share/nginx/html/objects"]["subPath"] == "dataset/objects"
        assert mounts["/usr/share/nginx/html/objects"]["readOnly"] is True
        returned = self.kube("exec", transfer, "--", "jq", "-er", ".manifest.path",
                             "/data/reports/" + self.name + "-data.json").stdout.decode().strip()
        raw = self.kube("exec", transfer, "--", "cat", returned).stdout
        assert hashlib.sha256(raw).hexdigest() == manifest_id
        path = f"/fixed-pool-data/manifests/{manifest_id}/dataset-manifest.json"
        raw = self.kube("exec", self.name + "-0", "--", "cat", path).stdout
        assert hashlib.sha256(raw).hexdigest() == manifest_id
        (self.root / "dataset-manifest.json").write_bytes(raw)
        manifest = json.loads(raw)
        vectors = json.loads((self.source.parents[1] / "testdata/vectors.json").read_text())
        prefix = bytes.fromhex(vectors["prefix_hex"])
        with self.forward() as port:
            for index, obj in enumerate(manifest["objects"]):
                object_path = "/objects/sha256/" + vectors["sha256"][index]
                assert obj["url"] == settings["BASE_URL"] + object_path
                response, body = self.http(port, object_path)
                assert response.status == 200 and len(body) == 64
                assert hashlib.sha256(body).hexdigest() == vectors["sha256"][index]
                full_body = body
                if index == 0:
                    assert body[:16] == prefix
                response, body = self.http(port, object_path, "HEAD")
                assert response.status == 200 and response.getheader("Content-Length") == "64" and not body
                response, body = self.http(port, object_path, headers={"Range": "bytes=0-15"})
                assert response.status == 206 and response.getheader("Content-Range") == "bytes 0-15/64"
                assert response.getheader("Content-Length") == "16" and body == full_body[:16]
                response, _ = self.http(port, "/incorrect-prefix" + object_path)
                assert response.status == 404
        self.completed("generated-object-url-head-get-range")

        # Mutate only this run's server: wrong mount must be observed as a failed object GET.
        patch = {"spec": {"template": {"spec": {"containers": [{"name": "file-server", "volumeMounts": [
            {"name": "source", "mountPath": "/usr/share/nginx/html/objects", "subPath": "dataset", "readOnly": True}]}]}}}}
        self.kube("patch", "statefulset", self.name, "--type=strategic", "-p", json.dumps(patch))
        self.kube("rollout", "status", "statefulset/" + self.name, "--timeout=120s", timeout=130)
        with self.forward() as port:
            response, _ = self.http(port, "/objects/sha256/" + vectors["sha256"][0])
            assert response.status == 404
        self.completed("wrong-mount-is-visible")
        self.kube("apply", "-f", str(server))
        self.kube("rollout", "status", "statefulset/" + self.name, "--timeout=120s", timeout=130)
        restored = self.kube("exec", self.name + "-0", "--", "cat", path).stdout
        assert restored == raw
        with self.forward() as port:
            response, body = self.http(port, "/objects/sha256/" + vectors["sha256"][0])
            assert response.status == 200 and hashlib.sha256(body).hexdigest() == vectors["sha256"][0]
        self.completed("restore-without-regeneration")

        # A fresh Job against the nonempty root must fail without publishing a new result.
        conflict = self.name + "-conflict"
        self.create(self.data_job(conflict))
        self.wait_job(conflict, success=False)
        failed = self.report(transfer, conflict)
        assert failed["manifest"] is None and any(e["code"] == "DATA_CONFLICT" for e in failed["errors"])
        assert self.report(transfer, self.name + "-data") == generated
        assert self.kube("exec", self.name + "-0", "--", "cat", path).stdout == raw
        self.completed("generation-conflict-preserves-dataset")
        self.report_failures(transfer, generated)
        self.interrupt_generation()

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
