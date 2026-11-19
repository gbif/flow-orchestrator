# flow-orchestrator: local integration environment

Runs the orchestrator (validation domain) against Postgres and RabbitMQ, optionally with the real
DwC-DP analyser and the validation web service (flow-orchestrator-ws). Run the commands from `local-test/`.

Requirements: Podman with a Compose provider, [Task](https://taskfile.dev/), `curl`. No Python.

## Analysis against the real analyser

1. Build: `mvn package` in the repository root, then `task build`.
2. Create `.env` from `.env.template` with **absolute** paths: `DATAPACKAGES_DIR` holds the
   datapackages (`{uuid}/datapackage.json`, or `{uuid}/{uuid}.{attempt}/…`, see the analyser
   README), `DWCDP_WORKDIR` is writable scratch space.
3. Start and trigger:

   ```sh
   task up:analyser
   task trigger UUID=673f2308-3501-474f-85d5-a832825bb46c
   task status UUID=673f2308-3501-474f-85d5-a832825bb46c
   ```

   `status` shows the workflow `FINISHED` (rejected when the data package is invalid) and the stored
   `DWCDP_ANALYSIS` report. The analyser must be 0.0.9 or later (it binds its own queue and sends
   the whole `AnalysisExecution`).

## The whole chain, pipelines steps simulated

The pipelines jobs are not part of this environment. Their requests go to the balancer
(`occurrence` / `occurrence.pipelines.balancer`); a stand-in queue makes them routable and visible
in the management UI (<http://127.0.0.1:15672>, `localtest`/`localtest`, queue `local.balancer`).

```sh
task up:analyser
task balancer
task trigger UUID=… CHAIN=full
task complete UUID=… STEP=VALIDATOR_DWCDP_STAGE
task complete UUID=… STEP=VALIDATOR_DWCDP_TO_VERBATIM        # outputs datasetType and counts
task complete UUID=… STEP=VALIDATOR_VERBATIM_TO_IDENTIFIER   # OUTCOME=INVALID rejects
task complete UUID=… STEP=VALIDATOR_VERBATIM_TO_INTERPRETED
task complete UUID=… STEP=VALIDATOR_COLLECT_METRICS
task status UUID=…
```

`OUTCOME=FAILED` fails a step (the workflow ends `FAILED`). Without `task balancer` the requests
are unroutable: the outbox keeps them and retries with backoff (`status` shows the attempts).

## Through the validation API (flow-orchestrator-ws)

The ws authenticates users against a GBIF API, so set `GBIF_API_URL` (for example UAT) and an app
key in `.env`, and log in with a user of that API.

```bash
(cd .. && mvn package -DskipTests)
task build && task build:ws && task up:ws
curl -u USER:PASSWORD -F file=@my-datapackage.zip http://127.0.0.1:18081/validation
curl -u USER:PASSWORD http://127.0.0.1:18081/validation/<key>
```

A DwC-DP upload starts the `validation-dwcdp` workflow with a `WorkflowCommand`; its status and
steps in the response come from the workflow tables. Other formats send today's pipelines start
message, which nothing consumes locally. If an older database volume exists, run `task down`
first: Postgres only runs its init scripts on an empty volume.

## What runs where

| Service | Port | Notes |
|---|---|---|
| Postgres | 127.0.0.1:15432 | Database `registry`, schema `orchestrator`, created from the repository's `db/` scripts. The orchestrator connects as role `orchestrator` (owner); `task status` reads as role `registry` (SELECT only) |
| RabbitMQ | 127.0.0.1:5672, UI 15672 | `localtest`/`localtest` |
| Orchestrator | 127.0.0.1:18080 | Actuator: `/actuator/health`, `/actuator/prometheus`; config `application-local.yml` |
| flow-orchestrator-ws | 127.0.0.1:18081 | Profile `ws`. Connects as role `orchestrator_ws` (writes only the validation tables); validation store in `VALIDATION_STORE_DIR` (default `./store`) |
| Analyser | — | Queue `dwcdp-validator-validation`, bound by the analyser to `orchestrator` / `validation.dwcdp.analysis` |

`task down` removes everything, including the database volume.
