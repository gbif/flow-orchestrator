# flow-orchestrator: local integration environment

Runs the orchestrator (validation domain) against Postgres and RabbitMQ, optionally with the real
DwC-DP analyser. Run the commands from `local-test/`.

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

## What runs where

| Service | Port | Notes |
|---|---|---|
| Postgres | 127.0.0.1:15432 | Database `registry`; the orchestrator tables come from the test schema (a copy of registry changeset 189) |
| RabbitMQ | 127.0.0.1:5672, UI 15672 | `localtest`/`localtest` |
| Orchestrator | 127.0.0.1:18080 | Actuator: `/actuator/health`, `/actuator/prometheus`; config `application-local.yml` |
| Analyser | — | Queue `dwcdp-validator-validation`, bound by the analyser to `orchestrator` / `validation.dwcdp.analysis` |

`task down` removes everything, including the database volume.
