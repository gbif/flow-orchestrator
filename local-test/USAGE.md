# flow-orchestrator: local integration environment (minikube)

Runs the validation domain in minikube with the repository's Helm charts: Postgres, RabbitMQ, the
engine (`helm/flow-orchestrator`), the validation web service (`helm/flow-orchestrator-ws`) and the
real DwC-DP analyser (its own chart, from the dwc-dp-analyser-service checkout). The charts are
installed with their `values-local.yaml`; real environments use the same charts with values kept
outside this repository. Run the commands from `local-test/`.

Everything runs inside the cluster; nothing talks to a GBIF environment. A stub of the GBIF API
stands in for registry's user endpoints (login and user lookup), and Mailpit catches the emails.
Only the images are pulled from registries.

Requirements: minikube (about 8 GB of memory for it), `kubectl`, `helm`,
[Task](https://taskfile.dev/), `curl`, and a checkout of dwc-dp-analyser-service next to this
repository (or `ANALYSER_DIR` in `.env`). No Python.

## Setup

```sh
cp .env.template .env                # optional: analyser checkout, base image registry
minikube start --memory 8g
(cd .. && mvn package -DskipTests)
(cd ../../dwc-dp-analyser-service && mvn package -DskipTests)
task build build:ws build:analyser   # images built inside minikube from the JARs
task up                              # infrastructure, GBIF API stub, Mailpit, engine, analyser, ws
task forward                         # in a second terminal, keep it running
```

`task up` waits until every deployment is ready (Actuator readiness probes). `task logs`
follows the engine, ws and analyser.

All three images are built inside minikube, so they match its architecture. The published
analyser image is amd64 only and fails on Apple Silicon with `exec format error`. The base image
comes from `docker.gbif.org/third-party`, which minikube reaches only over the GBIF VPN. If the
VM cannot resolve names, point its DNS at the VPN's server. `BASE_REGISTRY=docker.io/library`
uses the public image instead.

Every `kubectl`, `helm` and `minikube` call is pinned to the `minikube` context and profile
(`KUBE_CONTEXT` in `.env`), whichever context is current. To look around:
`k9s --context minikube -n flow-orchestrator-local` or `minikube dashboard`.

The workflow is `validation-dwcdp` (the chart's default): the analysis, then the five pipelines
steps, selected by `fileFormat: DWCDP`, the parameter the ws sends.

## 1. Upload through the validation API (the Round A path)

The ws authenticates every request against the GBIF API stub (`k8s/gbif-api-stub.yaml`), which
knows three users; the password is the user name:

| User | Roles | Email locale |
|---|---|---|
| `alice` | `USER` | en |
| `bob` | `USER` | es |
| `admin` | `USER`, `REGISTRY_ADMIN` | en |

Any other user or password gets 401.

```sh
curl -u alice:alice -F file=@my-datapackage.zip http://127.0.0.1:18081/validation
curl -u alice:alice http://127.0.0.1:18081/validation/<key>
task status UUID=<key>
```

The upload is unzipped into the shared store under `<key>/`, the ws sends a `WorkflowCommand`, and
the analyser reads the package from the same store. A DwC-DP keeps its folders
(`<key>/data/event.parquet`), with the folder of its `datapackage.json` as `<key>/`, so a zip
with an extra top folder works too. Other formats are flattened, as validator-ws always did.

- **Invalid package:** the analysis rejects it; the workflow ends `FINISHED`, rejected, the ws
  receives `WorkflowFinished` and the validation is `FINISHED` with `indexeable=false`.
- **Valid package:** the workflow continues with `VALIDATOR_DWCDP_STAGE`, a pipelines step. The
  pipelines jobs are not part of this environment; continue as in section 2.

The status in `GET /validation/<key>` is composed from the workflow tables. When the workflow
ends, the email is in Mailpit (<http://127.0.0.1:18025>), addressed to `alice@example.org`;
upload as `bob` to get it in Spanish.

Ownership checks: `curl -u bob:bob -X DELETE http://127.0.0.1:18081/validation/<alice's key>` is
refused with 403, and `admin` may do it.

## 2. The whole chain, pipelines steps simulated

The pipelines requests go to the balancer (`occurrence` / `occurrence.pipelines.balancer`); a
stand-in queue makes them routable and visible in the management UI (<http://127.0.0.1:15672>,
`localtest`/`localtest`, queue `local.balancer`).

```sh
task balancer
task complete UUID=… STEP=VALIDATOR_DWCDP_STAGE
task complete UUID=… STEP=VALIDATOR_DWCDP_TO_VERBATIM        # outputs datasetType and counts
task complete UUID=… STEP=VALIDATOR_VERBATIM_TO_IDENTIFIER   # OUTCOME=INVALID rejects
task complete UUID=… STEP=VALIDATOR_VERBATIM_TO_INTERPRETED
task complete UUID=… STEP=VALIDATOR_COLLECT_METRICS
task status UUID=…
```

`OUTCOME=FAILED` fails a step (the workflow ends `FAILED`). Without `task balancer` the requests
are unroutable: the outbox keeps them and retries with backoff (`status` shows the attempts).

## 3. The engine only, without the ws

To test the engine and the analyser without the ws, start validations with the command the ws
would send. Put the data package in the shared store first, where the analyser looks for it:

```sh
task datapackage UUID=673f2308-3501-474f-85d5-a832825bb46c FILE=/path/to/package.zip
task trigger UUID=673f2308-3501-474f-85d5-a832825bb46c
task status UUID=673f2308-3501-474f-85d5-a832825bb46c
```

Then continue as in section 2.

The analyser looks for the package per attempt (`<uuid>/<uuid>.<attempt>/`), so a rerun as a new
attempt needs its own copy: `task datapackage UUID=… ATTEMPT=2 FILE=…`, then
`task trigger UUID=… ATTEMPT=2`. A new UUID works just as well.

The analyser fetches the published DwC-DP 1.0 profile from `rs.tdwg.org`, so pods need DNS and
internet access. If the analyser logs `Fetching DwC-DP profile … failed` and calls a good package
`valid: false`, restart CoreDNS (`kubectl --context minikube -n kube-system rollout restart
deploy/coredns`) after fixing the VM's DNS, or point its `forward` at a reachable DNS server.

## 4. DwC-A, tabular and checklists (Round B)

Every format the ws accepts now runs as a workflow; the pipelines workers are simulated as in
section 2. A DwC-A upload:

```sh
curl -u alice:alice -F file=@occurrences-dwca.zip http://127.0.0.1:18081/validation   # note "key"
task balancer
task complete UUID=<key> STEP=VALIDATOR_VALIDATE_ARCHIVE        # OUTCOME=INVALID rejects
task complete UUID=<key> STEP=VALIDATOR_DWCA_TO_VERBATIM
task complete UUID=<key> STEP=VALIDATOR_VERBATIM_TO_IDENTIFIER
task complete UUID=<key> STEP=VALIDATOR_VERBATIM_TO_INTERPRETED
task complete UUID=<key> STEP=VALIDATOR_COLLECT_METRICS
curl -u alice:alice http://127.0.0.1:18081/validation/<key>
```

A DwC-A whose `meta.xml` core is Taxon (or a ColDP) is a checklist: after the archive check it
waits for ChecklistBank. The engine sends the `VALIDATOR_CHECKLISTBANK` request to the ws, which
records the wait (`GET /validation/<key>` shows `WAITING_FOR_CHECKLISTBANK` on the archive step).
Simulate ChecklistBank calling back, then the pipelines checklist validator's evaluation:

```sh
task complete UUID=<key> STEP=VALIDATOR_VALIDATE_ARCHIVE        # answers with clbDatasetKey 4711
curl -X POST -H 'Content-Type: application/json' \
  -d '{"datasetKey":4711,"status":"finished","format":"dwca"}' \
  http://127.0.0.1:18081/validation/clbValidationCallback/<key>  # the ws forwards it (queue
                                                                 # occurrence.pipelines.checklist.validator)
task complete UUID=<key> STEP=VALIDATOR_CHECKLISTBANK
```

A ColDP ends there; a checklist DwC-A continues with `VALIDATOR_DWCA_TO_VERBATIM` as above.

## What runs where

Namespace `flow-orchestrator-local`. Local ports through `task forward`:

| Service | Local port | Notes |
|---|---|---|
| Postgres | 15432 | Database `registry`, schema `orchestrator`, created on an empty data directory from `local-test/db/` and the repository's `db/` scripts. The engine connects as role `orchestrator` (owner), the ws as `orchestrator_ws`; `task status` reads as role `registry` (SELECT only). |
| RabbitMQ | 5672, UI 15672 | `localtest`/`localtest` |
| Engine | 18080 | Release `flow-orchestrator`; Actuator `/actuator/health`, `/actuator/prometheus` |
| flow-orchestrator-ws | 18081 | Release `flow-orchestrator-ws`; `GBIF_API_URL` and `CLB_API_URL` point at the stub |
| GBIF API stub | — | WireMock, service `gbif-api`: `POST /user/auth/basic` and `GET /admin/user/{name}` for the users above, 404 for anything else |
| Mailpit | 18025 (UI) | SMTP `mailpit:1025` inside the cluster |
| Analyser | — | Release `dwc-dp-analyser`; queue `dwcdp-validator-validation`, bound by the analyser to `orchestrator` / `validation.dwcdp.analysis` |
| Shared store | — | PVC `validation-store`: `/data/validation-store` on the minikube node, read and written by the ws, read by the analyser |

Postgres keeps its data in an `emptyDir`: a Postgres pod restart starts from an empty database.
After changing the `db/` scripts, run `task down` and `task up`. `task down` removes everything,
including the database and the shared store. After rebuilding an image, run
`kubectl --context minikube -n flow-orchestrator-local rollout restart deploy/<release>` (the tag
stays `local`).
