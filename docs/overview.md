# flow-orchestrator in brief

A short overview for people. The full design and its decisions are in
[`architecture.md`](architecture.md), the current state and branches in
[`UPDATE.md`](../UPDATE.md), and deployment in [`deployment.md`](deployment.md).

## What it is

flow-orchestrator is one service that runs multi-step workflows across GBIF's workers. The workers
include the pipelines steps, the DwC-DP analyser and ChecklistBank. The orchestrator decides what
runs next, records every step, and knows when a workflow is finished.

It started as the `validator-orchestrator` inside pipelines. It now has its own repository and
runs data validation, the "data validator" at `/v1/validation`. Ingestion is planned to follow.

## Why

Before, each worker chained the work itself. It called validator-ws over HTTP to read and
overwrite the whole validation, worked out the overall status, and sent the next step's message.
This caused:

- **Races:** two workers could overwrite each other's status (the ChecklistBank callback and the
  checklist validators, for example).
- **No single owner of the flow:** the order of the steps lived in many places, and a broken link
  went unnoticed. The DwC-DP chain was broken for exactly this reason.
- **HTTP calls from Spark jobs**, and failures that were only logged.

## How it works now

```text
 user / portal ──HTTP──► flow-orchestrator-ws ──START / CANCEL──► engine ◄──► database
                          (the /validation API)                     │        (schema "orchestrator")
                                 ▲                                  │ step requests
                                 └────── workflow finished ─────────┤
                                                                    ▼  ▲ "step done" + report
                                              workers: pipelines steps, DwC-DP analyser,
                                                       ChecklistBank (via the ws)
```

- **flow-orchestrator-ws** replaced validator-ws. It keeps the same API, so the portal didn't
  change. It accepts uploads, checks them, detects the format, starts a workflow, and sends the
  email at the end.
- **The engine** (`flow-orchestrator-app`) holds the workflow definitions. It sends each step to
  its worker and records the result. Then it decides what runs next, or whether the workflow is
  finished.
- **Workers** only do their own step and reply "done" with a report. They don't touch the status
  and don't start the next step.
- **Everything goes through RabbitMQ.** A message that can't be delivered or processed goes to a
  dead-letter queue instead of being lost.
- **State lives in the registry database**, in a separate `orchestrator` schema. Only this
  repository's services write to it. Registry can read it.

## Key decisions

- **Same behaviour for users.** We support exactly the formats validator-ws supported: DwC-A,
  tabular, spreadsheet, ColDP and DwC-DP (XML is not supported). Users see the same steps as
  before.
- **Workers report facts; the orchestrator decides.** A worker that finds a fatal problem still
  reports "done" with its findings. The orchestrator then stops the workflow and marks the data as
  not indexable.
- **One workflow per format**, set in configuration, for example `validation-dwca` and
  `validation-dwcdp`. A Taxon-core DwC-A also includes the ChecklistBank step.
- **ChecklistBank is a real step** now. Before, it was a hidden wait. Users still see it inside the
  archive check, as before.
- **The database is changed by hand** with the scripts in `db/`. The services run no migrations.
- **EML validation is one shared library** (`gbif-eml-validation`), used by the pipelines archive
  check, the DwC-DP analyser and the ws.

## Where we are

- **Done and running on dev:** validation of every supported format through the orchestrator
  (Rounds A and B). Tested on dev with DwC-DP and DwC-A uploads.
- **Still to test on dev:** Taxon-core DwC-A, ColDP, tabular, spreadsheet, and cancel.
- **Open decision:** whether flow-orchestrator-ws runs on Kubernetes (needs an Ingress and a
  Varnish rule) or on the VM where validator-ws ran (no routing changes).
- **Work on feature branches:** the changes in pipelines, gbif-api, postal-service and crawler
  are on `feature/orchestration`. They get merged and released before production.

## Next

1. Decide where the ws runs, and finish the dev tests.
2. Remove the old validator-ws code from pipelines.
3. Release the feature branches and deploy to the next environments.
4. Retry, rerun and recovery of steps. Today a lost reply leaves a step waiting.
5. Crawling and ingestion through the orchestrator.
