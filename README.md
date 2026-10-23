# Fixed-pool functional regressions

These tests protect the benchmark tool's behavior through its commands, HTTP
requests, exit status and result files. They run the real preparation tools,
worker helper, shell entry and k6 against a small controlled HTTP proxy.
They do not measure Dragonfly capacity or establish real Seed cache readiness.
An opt-in Kubernetes source regression is described separately below; it uses
real generation Jobs, storage and nginx instead of the controlled proxy.

## When to run

| Change or event | Required regression |
| --- | --- |
| Dataset, plan, preparation, input checks, request script, reporting or shared execution | `integration.py` |
| Shared runtime or contracts affecting single and multi-worker execution | Both suites |
| Shards, rates, worker identity, independent processes or peer failures | Both suites |
| Harness, k6, toolchain, dependency or image changes affecting execution | Both suites |
| Before merging fixed-pool changes or submitting phase acceptance | Both suites against the final candidate |
| Prose-only documentation changes | Check documentation; unchanged steps and behavior need no regression rerun |
| Changed build or run instructions | Execute changed steps and the affected suite; both for shared changes |
| Source templates, mappings, handoff or image | Also run `source.py` in a Kubernetes test environment |

During development, the affected suite gives feedback; the full run is the
functional regression gate. The [workflow](../../../../.github/workflows/fixed-pool.yaml)
runs Go race tests, platform builds and both suites for relevant main-branch pushes
and pull requests, and supports manual dispatch. It preserves regression artifacts
on failure. Successful runs also upload tested tool/OCI distributions; see the
[download instructions](../README.md#download-a-ci-build). A workflow definition
is not evidence that a particular revision passed
GitHub Actions; record its actual run separately.
They complement Go unit tests and applicable real Seed, Kubernetes and scale tests.
Benchmark users do not need to run them before every measurement.

## Prerequisites and execution

Use Linux with Python 3 (standard library only), POSIX shell, and the k6 version
selected by [`tools/proxy-bench/Dockerfile`](../../Dockerfile). This image dependency
is the regression baseline for the distributed worker; do not pin another version
in the suites. Both suites check the k6 binary on PATH against the Dockerfile tag
and retain `runtime-k6.json`, including the image reference and version output.
CI obtains the binary from that image. When the image dependency changes, run both
complete suites to detect API, summary and transport incompatibilities.
Allow local loopback HTTP sockets and child processes. No Docker, Kubernetes,
real Dragonfly service, credentials or external source server is needed to run
the suites. Installing dependencies and building binaries may require downloads.

From the repository root, build native binaries with the repository's Go toolchain:

```bash
make build-proxy-bench-data-prepare build-proxy-bench-plan-prepare \
  build-proxy-bench-seed-cache-prepare build-proxy-bench-worker-support
export PATH="$PWD/bin:$PATH"
```

Put k6 on `PATH`, then run both suites into a **new** directory:

```bash
make test-proxy-bench-fixed-pool WORK_DIR=/tmp/fixed-pool-regression-001
```

For focused development, run either script directly:

```bash
python3 tools/proxy-bench/fixed-pool/tests/integration.py --work-dir /tmp/fixed-pool-single-001
python3 tools/proxy-bench/fixed-pool/tests/multi_worker.py --work-dir /tmp/fixed-pool-multi-001
```

Do not use `python -O`: assertions are the test oracles. Both suites must exit 0
for the combined gate to pass. A timeout or assertion failure is a failed run;
inspect its artifacts and retain them before rerunning in a new directory.
The combined run normally takes a few minutes; timing depends on host load.

## Scope and expectations

`integration.py` checks normal execution, request numbering and Range mapping,
transport resend, HTTP status/redirect/body/timeout handling, dynamic VUs and
dropped iterations. It also checks interruption, numbering gaps, threshold and
script failures, absence of runtime version probing, native user-local k6 configuration
inheritance and effective-option recording, rejected inputs (including subsecond
DURATION), incomplete results, and native metric mapping.

`multi_worker.py` runs two or three local independent worker processes. It checks
shard loops, fixed allocations (17 requests/s to 9/8 or 7/6/4), per-worker input
rates and k6-inspected executor options, overlapping request windows, invalid
peers, interruption with a continuing healthy peer, missing summaries, dropped
iterations and intentionally non-overlapping starts. It does not test physical
host distribution or orchestrate production workers.

The fixtures deliberately use two 64 KiB objects and 16 KiB ranges. The proxy
listens on a dynamic loopback port. `192.0.2.1` is a simulated response identity,
not a deployment address. These fixed values are regression inputs, not user examples.
Real benchmark usage belongs in the tool and deployment documentation.

Expected allocations are fixed literals. Request mapping is checked against the
prepared plan; metrics are checked against native k6 output. Measured latency,
QPS and request totals can vary between runs and are not performance baselines.
Test copies add request labels or inject faults; the shipped runtime is unchanged.
Instrumentation fails explicitly if its source anchors no longer match.

## Results and cleanup

The combined command creates `single/` and `multi/` under `WORK_DIR`. Each retains
generated inputs, per-case logs, received requests and results. Preparation calls
retain arguments, exit codes and stdout/stderr. Multi-worker cases also retain
per-worker k6 inspection output and request-window evidence.

`completed-cases.json` records progress before a failure; a successful suite writes
`result.json` with the observed k6 version. The single-worker suite separately records rejected inputs and
incomplete-result cases. Raw k6 results, execution records and worker summaries
remain available for diagnosis. Fault-case nonzero worker exits are expected only
when asserted by the suite; the suite itself still exits 0 on success.

Commands have bounded waits. On normal completion, assertion failure or timeout,
the harness closes its proxy and terminates its own child process groups. Results
are retained for inspection; remove only the work directory when no longer needed.

## Kubernetes source regression

`source.py` is a maintainer integration/E2E test, not a source verification command
for benchmark users. Run it when the source bundle changes and before accepting
such changes. It is separate from the two controlled suites and their CI gate,
because it needs real Kubernetes storage and nginx. Its result does not establish
Seed readiness, cross-node reachability or performance capacity.

Use a dedicated test cluster/context, an existing namespace, a source node, a
StorageClass available there, and immutable tools/FileServer images built from
the candidate. The operator needs kubectl, envsubst and Python 3. This test creates
uniquely named Jobs, a PVC, a StatefulSet and a Service. It deletes only resources
it created, checking their UIDs before deletion, including the test PVC and its
128 bytes of generated object data. It does not delete the namespace or existing
benchmark data. Logs, resources, HTTP facts and failures remain in WORK_DIR.

Set deployment values for that test environment, then from the repository root:

```bash
python3 tools/proxy-bench/fixed-pool/tests/source.py \
  --work-dir "$WORK_DIR" --namespace "$NAMESPACE" --node "$SOURCE_NODE" \
  --storage-class "$PVC_STORAGE_CLASS" \
  --tools-image "$TOOLS_IMAGE" --file-server-image "$FILE_SERVER_IMAGE"
```

WORK_DIR must be new. The test runs the shipped templates and data command with
two 64-byte objects. It checks manifest handoff, independently fixed SHA vectors,
real GET/HEAD/206 Range and incorrect URL behavior. It injects a wrong object
mount, requires the failure to be visible, restores the original template without
regeneration, and requires a second generation Job against the nonempty root to
fail with DATA_CONFLICT while preserving the published manifest.

HTTP requests go through a temporary local port-forward; object paths are checked
against manifest URLs, but the test does not prove that a Seed can reach the
manifest hostname. The test closes its own port-forward processes and retains
only bounded HTTP facts, not full-body evidence files. A successful run writes
result.json; completed-cases.json and command stdout/stderr locate failures.

Separately walk the normal deployment-to-Seed preparation flow without running
manual source checks, using a fresh dataset and real Seed in the target-version
environment. Source deployment regression and real Dragonfly preparation cover
different boundaries; neither replaces the other.
