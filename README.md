# fixed-pool maintainer regressions

This repository contains integration/E2E tests and fixtures for the added
fixed-pool capability. It consumes perf-tests build deliverables; it does not
contain, check out or compile the perf-tests source tree. Go unit tests stay in
perf-tests and run with its standard `testing` package.

## Inputs and execution

Obtain tools and worker images built by perf-tests, plus their matching `deploy/`
material. The directory contains six ordinary editable Kubernetes YAML files. Load the images into local Docker before running `delivery.py`.
No source archive or deployment archive is required. The tests do not obtain,
compile or retain the perf-tests source tree, or maintain a second template copy.

Development builds are published by the fork's
[publication workflow](https://github.com/system-thoughts/perf-tests/actions/workflows/publish-fixed-pool.yaml).
Select a successful run for the candidate commit and download its `fixed-pool-<branch>-<commit>-run-<run-id>-<attempt>`
artifact. Extract the downloaded artifact, then verify and unpack its deployment bundle:

```bash
sha256sum -c SHA256SUMS
tar -xzf fixed-pool-deploy.tar.gz
```

`build-info.txt` gives the tools, worker and reused Docker Hub FileServer digest
references for that build. Use them as `TOOLS_IMAGE`, `WORKER_IMAGE` and
`FILE_SERVER_IMAGE`; pull the first two into Docker and set
`DEPLOY_DIR` to the extracted `fixed-pool-deploy/deploy` directory. Kubernetes
must be able to pull the same image references. The download archive is a transport
format; `delivery.py` continues to accept a directory. Other matching build sources
remain usable. The fork publication workflow is separate from upstream release policy.

Use images and deployment material from the same build. The test evidence records
resolved local image IDs, repository digests, platform and input file hashes.
A source commit is provenance, not a dependency that tests build themselves.
Obtaining/building images and obtaining the matching deployment directory happens
before testing. Publishing images and cross-repository scheduling are outside this
repository; no particular download or archive format is required.

```bash
# Paths and image references select an already built candidate.
python3 delivery.py --tools-image "$TOOLS_IMAGE" --worker-image "$WORKER_IMAGE" \
  --deploy-dir "$DEPLOY_DIR" --out /tmp/fp-delivery-001
export DELIVERY=/tmp/fp-delivery-001

python3 integration.py --delivery "$DELIVERY" --work-dir /tmp/fp-single-001
python3 multi_worker.py --delivery "$DELIVERY" --work-dir /tmp/fp-multi-001
python3 controlled_prepare.py --delivery "$DELIVERY" --work-dir /tmp/fp-prepare-001
python3 deploy.py --delivery "$DELIVERY" --work-dir /tmp/fp-deploy-001
```

`delivery.py` copies the unified preparation executable from the tools image, and
the worker-support executable and runtime script from the worker image. For k6
images it also copies k6 and its JS libraries; delivery.json records the engine. The controlled
process tests execute those delivered files on a compatible Linux host, without
Go or a separately installed k6. They do not claim to run inside a container;
the Kubernetes cases below exercise the actual images in Pods. Instrumentation
adds labels only to temporary copies of delivered scripts or private worker inputs.
Vegeta tests reuse the existing HTTP fixtures and assert native metrics directly;
legacy k6 paths remain available for their corresponding images.

The first three tests use Python's standard library and bounded local HTTP
fixtures. `deploy.py` also needs PyYAML. A fixture is a protocol peer,
not proof of real Dragonfly behavior or capacity. The image extraction needs
Docker access; the process tests need `/bin/sh` and executables compatible with
the host architecture. The extracted binaries/runtime are temporary test inputs,
kept outside Git along with test output. Do not run Python with `-O`: assertions
are the test oracles.

Every test requires `--delivery` and a new `--work-dir`. It verifies the extracted
input hashes, selects the delivered executables before ambient PATH entries, and
saves `candidate.json`. k6 comes from the worker image being tested; no version
string is pinned or compared with a Dockerfile. For Kubernetes tests, supply the
same tools/worker images as the extraction and retain their immutable references.

## Active cases and their boundaries

| ID / capability | Test name | Script |
| --- | --- | --- |
| SRC-01 / source | Generated objects are served at manifest URLs | source.py |
| SRC-02 / source | Mapping errors are located during preparation | preparation.py |
| PREP-01 / preparation | Real Seed prepares a consumable pool | preparation.py |
| PREP-01 variants | Bounded dispatch, final completion, unique tasks, cancellation | controlled_prepare.py |
| RUN-01 / worker | Concurrent VUs consume the shard; logs retain run facts and native summary | integration.py |
| RUN-02 / worker | Invalid input prevents requests | integration.py |
| RUN-03 / worker | Unexpected status reaches native failure statistics | integration.py |
| RUN-04 / worker | Reused inputs accept a new rate | integration.py |
| MULTI-01 / multi-worker | Each worker consumes its own shard and rate | multi_worker.py |
| MULTI-02 / multi-worker | A healthy peer continues after another is interrupted | multi_worker.py |
| TERM-01 / termination | Native engine failure is retained | integration.py |

Run SRC after source bundle changes; PREP after plan/Seed interface or process changes;
RUN after plan, script, input or option changes; MULTI/TERM after process lifecycle changes.
After Dockerfile/build/entrypoint changes, rerun the corresponding actual-image
Kubernetes scenarios. They exercise required commands, paths and permissions as
part of the source/plan/Seed/worker flow; no separate packaging suite is maintained.
Retained full-set acceptance checks all relevant boundaries.

The controlled PREP variants have separate observations. Concurrency is counted
before responses become visible; callback cleanup can outlive a client request.

| Subcase | Name | Evidence |
| --- | --- | --- |
| PREP-01-P01 | Five objects respect concurrency two and the explicit proxy | concurrent.stdout, result.json |
| PREP-01-P02 | Complete data is ready without a task-finished marker; no polling | marker-absent.stdout |
| PREP-01-P03 | Distinct objects cannot share one task | shared-task.stdout |
| PREP-01-P04 | SIGTERM cannot publish ready | cancelled.stdout |
| PREP-01-P05 | A bad final Range prevents ready | final-range.stdout |
| PREP-01-P06 | Transient loss recovers; other workers dispatch; no full GET replay | transport-result.json |
| PREP-01-P07 | Persistent loss is bounded; skipped objects identify stop cause | *-reset-persistent.stdout |
| PREP-01-P08 | Shared timeout bounds backoff; full GET is not retried | transport-result.json |

RUN-01 has normal and engine-specific overload variants; their result directories identify the
variant. The PREP protocol fixtures do not replace PREP-01 against a real Seed.
The normal variant holds the first two requests at a bounded rendezvous and
requires overlapping requests on distinct connections/workers. k6 additionally
checks distinct VUs; Vegeta uses its native sequence header and server observations. It parses the four runtime
log records, checking the run identity, worker/rate, effective options, native
request count, and raw exit. It does not compare five output files
field by field. `RUN-01/log-facts.json` records these observations.

IDs classify the capability, not a new execution framework. Variants share the
primary ID. Multi-process execution on one host does not prove cross-node storage
or network behavior. Regressions do not assess QPS limits or natural variance.

Unit tests own independent object/permutation/partition/rate vectors and detailed
response/input conditions. External cases read the published plan and observe
real requests and process wiring. They do not implement a second plan generator.
Source checks compare served bytes with generated objects; deterministic generation
is already covered by independent vectors in the unit layer.

## Kubernetes cases

Use an existing isolated namespace, usable StorageClass, node and immutable
candidate tools/worker/FileServer images. `source.py` needs kubectl and PyYAML. It creates uniquely named resources and deletes only the recorded UIDs.

```bash
python3 source.py --delivery "$DELIVERY" --work-dir "$WORK_DIR" \
  --namespace "$NAMESPACE" --node "$SOURCE_NODE" --storage-class "$STORAGE_CLASS" \
  --tools-image "$TOOLS_IMAGE" --file-server-image "$FILE_SERVER_IMAGE"
```

`preparation.py` also requires a real Seed proxy and its expected responding IP,
a free NodePort, and a source address reachable by that Seed. It tests independent
metadata copies and worker Jobs. Its placement is caller-selected; one node is
not reported as a cross-node result.

```bash
python3 preparation.py --delivery "$DELIVERY" --work-dir "$WORK_DIR" \
  --namespace "$NAMESPACE" --node "$SOURCE_NODE" --storage-class "$STORAGE_CLASS" \
  --tools-image "$TOOLS_IMAGE" --file-server-image "$FILE_SERVER_IMAGE" \
  --worker-image "$WORKER_IMAGE" --source-base-url "$SOURCE_BASE_URL" \
  --source-node-port "$SOURCE_NODE_PORT" --proxy "$SEED_PROXY" \
  --expected-server-ip "$SEED_IP"
```

It deliberately breaks the origin mount before the first Seed preparation, checks
that the preparation reports the failing stage, restores it, then requires success.
These are developer fault injections, not steps benchmark users must perform.
A unique URL prefix and matching temporary Nginx mount isolate each PREP run,
so a reused NodePort cannot let an earlier Seed cache hide the broken origin.
SRC-01 separately exercises the unmodified source path mapping.

## Migration and retirement

The initial commit preserves the unmodified six-file import from perf-tests
`0e6f8eb`; `source-baseline.json` records every original digest. Git history retains
that comparison baseline instead of a second active legacy suite.

SL-01 removes duplicate native-result mapping matrices, exhaustive invalid CLI
inputs, same-path HTTP faults, staggered peers and repeated multi-worker faults.
Input details stay in Go units; one external case verifies entrypoint wiring.
Transport resend remains a dependency-upgrade observation, not a required feature
regression. SL-02 retires the derived report/file checks; RUN and MULTI read native metrics
and execution facts directly from worker.log. SL-04 consumes ordinary YAML and obtains the manifest using test-owned volume
access fixtures. It retires rendering, forced-report and report-race checks,
private FileServer export mounts and large generation cancellation in the source
walkthrough. Generator cancellation remains in the Go unit layer; the source
regression still exercises the actual data Job, nonempty-root refusal and Nginx
GET/HEAD/Range. No manifest HTTP/download feature or dedicated package suite is added.
Retirement must not drop the retained source-to-Seed-to-worker boundary.

The short catalogue above maps primary risks. Deployment structure/security checks remain in deploy.py; source/preparation
exercise actual images. Temporary volume-access Pods are test fixtures, not
additional benchmark deliverables. They may also be needed by operators using
local-volume file transfer; removing product templates does not remove that cost.

## Results and cleanup

A passed command exits zero and writes `result.json`. Timeouts/assertions fail;
retain the directory and rerun in a new one after diagnosis. `completed-cases.json`,
command arguments, stdout/stderr, HTTP facts and worker logs locate the last success.
Child process groups, local proxy servers and port-forwards are closed on exit.
Kubernetes cleanup checks recorded resource UIDs before deletion. Evidence remains
on disk; remove only the selected test work directory when it is no longer needed.

Run relevant cases after changes, and the retained full set before accepting a
candidate. Main-repo test CI runs Go units independently;
there is no automatic cross-repository pipeline or publication in this change.

## SL-04 deployment checks and explicit inputs

`deploy.py` reads the delivered YAML directly. It checks Job retry policies,
applicable security contexts, source/data volume wiring, the shared target
ConfigMap, worker read-only inputs and writable temporary volume. Shell syntax
is checked only if inline shell exists. It does not test envsubst, custom name
validation, a reports-directory convention or the presence of a particular shell
statement. Kubernetes admission and actual mounts are exercised by the real cases.

`source.py` reads the generated manifest using a test-owned temporary access Pod;
`preparation.py` copies that selected file to two metadata volumes and invokes the
independent plan/Seed Jobs. The file transport is fixture setup, not a new product
helper. Preparation faults verify origin failure, repair without clearing correct
Seed cache or rebuilding the plan, nonready rejection and two actual worker Jobs
sharing the target ConfigMap. Output results come from Pod logs; business artifacts
remain on PVCs. No extra manifest URL, downloader or download-error matrix exists.

`nydus_hot_dev` uses its legacy three preparation executables, objects-file
plans and result volume. Its fixed-pool Vegeta image regression is separate:

```bash
sudo python3 nydus_vegeta.py --work-dir /absolute/new-test-directory \
  --tools-image <nydus_hot_dev-tools-image> \
  --worker-image <nydus_hot_dev-worker-image>
```

This extracts matching executables/scripts, exercises the unchanged preparation
and shell interface, checks native and normalized archives, and runs a full
worker container. Docker and permission to assign the new bind mount to the
image UID are required. The controlled HTTP fixture checks
functional behavior; it does not measure real Seed or generator capacity.
