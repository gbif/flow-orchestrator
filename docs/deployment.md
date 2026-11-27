# Deploying validation through the orchestrator (Rounds A and B)

A checklist for an environment (dev first), with every service, queue and setting that changes.
Values for the environment live in the Helm config repository; this repository only has the
charts and examples (`helm/*/values-dev.yaml.example`).

Round A (flow-orchestrator-ws replaces validator-ws) and Round B (every format through the engine)
are deployed together.

## 1. What runs, and which build

| Component | Artifact | Version | Deployed with |
|---|---|---|---|
| Engine | `docker.gbif.org/flow-orchestrator` | flow-orchestrator `dev` build | `helm/flow-orchestrator` |
| Validation API | `docker.gbif.org/flow-orchestrator-ws` | flow-orchestrator `dev` build | `helm/flow-orchestrator-ws` |
| DwC-DP analyser (validation instance) | `docker.gbif.org/dwc-dp-analyser-service` | `0.0.9-SNAPSHOT` (`dev`, analyser 0.0.17) | its chart (`dwc-dp-analyser-service/helm`) |
| Pipelines CLI workers (archive validator, validator DwC-A to verbatim, checklist validator, balancer) | `tasks-coordinator-cli` | `4.0.55-FEATURE-ORCHESTRATION-SNAPSHOT` | the existing worker deployments |
| Spark validator steps | `docker.gbif.org/ingest-spark-coordinator` and `ingest-spark-jobs` | `4.0.55-FEATURE-ORCHESTRATION-SNAPSHOT` | the existing coordinator deployments and Airflow DAGs |
| Old validator-ws | `pipelines-validator-ws` | — | **stopped** after the data copy |

Pipelines must be the feature build: the workers need orchestrator mode (Round 5 and B), and the
balancer must keep the step header of `PipelinesDwcaMessage`. The feature build is compatible with
ingestion messages (they carry no header), so the balancer can be shared.

### Before you start

- [ ] The pipelines build of branch `feature/orchestration` (Rounds 5, A and B; the earlier
      `feature/dwcdp-data-validator` is in review and no longer published) is in the repository.
      Its version is the branch name in capitals (`4.0.55-FEATURE-ORCHESTRATION-SNAPSHOT`):
      ```sh
      curl -s https://repository.gbif.org/repository/snapshots/org/gbif/pipelines/tasks-coordinator-cli/maven-metadata.xml | grep FEATURE-ORCHESTRATION
      ```
      The images (`ingest-spark-coordinator`, `ingest-spark-jobs`) carry the same tag.
- [ ] The flow-orchestrator images of `dev` exist (`flow-orchestrator`, `flow-orchestrator-ws`).
- [ ] The analyser service image from `dev` (with analyser 0.0.17) exists.
- [ ] Nothing is validating, or you accept that validations running on the old path are lost.

## 2. Database (registry database, by hand)

Scripts in `db/`, in order. The role names are placeholders: confirm them for the environment.

- [ ] Roles exist: `orchestrator` (owner of the schema, the engine's), `orchestrator_ws` (the
      ws's), `registry` (reads).
- [ ] `001-orchestrator-schema.sql`, as a role that may create schemas.
- [ ] `002-orchestrator-tables.sql` and `003-validation-tables.sql`, as `orchestrator`.
- [ ] `004-validation-grants.sql`, as `orchestrator` or an admin.
- [ ] The validator-ws data copy: [`db/migration/validator-ws-data.md`](../db/migration/validator-ws-data.md)
      (stop validator-ws first, as described there).

## 3. RabbitMQ

The services declare their own exchanges, queues and bindings. By hand you only need the users,
with permissions on the vhost (configure, write and read on the names below).

| Exchange | Routing key | Queue (declared by) | Producer → consumer |
|---|---|---|---|
| `orchestrator` | `validation.command` | `orchestrator.validation.commands` (engine) | ws → engine (START, CANCEL) |
| `orchestrator` | `validation.step.completed` | `orchestrator.validation.step.completed` (engine) | pipelines workers → engine |
| `orchestrator` | `validation.dwcdp.analysis` | `dwcdp-validator-validation` (analyser) | engine → analyser |
| `orchestrator` | `validation.dwcdp.analysis.finished` | `orchestrator.validation.dwcdp.analysis.finished` (engine) | analyser → engine |
| `orchestrator` | `validation.checklistbank` | `orchestrator-ws.validation.checklistbank` (ws) | engine → ws (the ChecklistBank wait) |
| `orchestrator` | `validation.workflow.finished` | `orchestrator-ws.validation.workflow.finished` (ws) | engine → ws (status, email) |
| `occurrence` | `occurrence.pipelines.archive.validator.validator` | the archive validator's queue (worker) | engine → archive validator |
| `occurrence` | `occurrence.pipelines.balancer` | the balancer's queue (balancer) | engine → balancer → DwC-A to verbatim and the Spark steps |
| `occurrence` | `occurrence.pipelines.checklist.validator` | the checklist validator's queue (worker) | ws → checklist validator |
| `occurrence` | `occurrence.pipelines.cleaner.validator` | the validator cleaner's queue | ws retention job → cleaner (unchanged) |

- Each engine and ws queue has a dead-letter queue `<queue>.dlq` behind `orchestrator.dlx`.
- The balancer re-publishes DwC-A to verbatim on `crawler` / `crawl.dwca.validation.finished.validator`
  and the Spark steps on their usual validator keys, as before.
- The analyser for validation is **its own deployment**. The crawler's analyser uses the crawler
  exchange and keys, so the two can't share one instance.

## 4. Engine (`helm/flow-orchestrator`)

- [ ] Secret with `DB_PASSWORD` and `RABBIT_PASSWORD`.
- [ ] Values: start from `helm/flow-orchestrator/values-dev.yaml.example`, with `domain:
      VALIDATION` and `messagingPrefix: validation`, the DB URL, the Rabbit host and vhost, and
      the image tag. The workflows and step routing come from the chart defaults; leave `config`
      out unless routing differs in the environment.
- [ ] Release name: one per domain, for example `flow-orchestrator-validation`.
- [ ] Logging: `logbackConfig` with the environment's Logstash appender (see the example), as
      for the ws.
- [ ] Ready: `/actuator/health/readiness` is UP, and the log shows the workflows `validation-dwcdp`,
      `validation-dwca`, `validation-dwca-checklist`, `validation-tabular`,
      `validation-spreadsheet` and `validation-coldp`.

## 5. Validation API (`helm/flow-orchestrator-ws`)

- [ ] Secret with `DB_PASSWORD`, `RABBIT_PASSWORD`, `GBIF_APP_SECRET`, and `CLB_API_PASSWORD`
      (and `SPRING_MAIL_PASSWORD` if the SMTP server needs one).
- [ ] Values: start from `helm/flow-orchestrator-ws/values-dev.yaml.example`:
  - `GBIF_API_URL` and `GBIF_APP_KEY` (the app key validator-ws used);
  - `GBIF_VALIDATOR_URL` (dev `https://old.gbif-dev.org`) and `GBIF_PORTAL_URL` (dev
    `https://www.gbif-dev.org`): required, no default, the base URL only;
  - mail and ChecklistBank, as validator-ws had them;
  - `volumes.store`: the **same** volume the archive validator, the validator DwC-A to verbatim
    and the validation analyser use as their archive repository, with an owner the ws can write
    as.
- [ ] Logging: `logbackConfig`, a Logback XML replacing the packaged console logging, to send logs
      to Logstash (ELK). The example has a TCP appender; take the destination from the environment's
      other services.
- [ ] Ingress: `ingress.enabled: true` and `ingress.domain` (host
      `flow-orchestrator-ws.<domain>`, or `ingress.host`). The service answers `/validation/...` at
      its root, with no `/v1`. The default annotations raise nginx's body limit to 1 GB for uploads
      (keep it ≥ `UPLOAD_MAX_SIZE`). Check: `curl https://<host>/validation/enumeration/FileFormat`.
- [ ] Gateway: `/v1/validation*` → the ingress host (the paths are unchanged).
- [ ] Ready: `/actuator/health/readiness` is UP, and the log shows it listening for WorkflowFinished
      and the ChecklistBank step requests.

## 6. DwC-DP analyser (validation instance)

Its chart, with values such as:
```yaml
image: { repository: docker.gbif.org, name: dwc-dp-analyser-service, tag: 0.0.9-SNAPSHOT }
rabbit:
  inputQueue: dwcdp-validator-validation
  inputExchange: orchestrator
  inputRoutingKey: validation.dwcdp.analysis
  outputExchange: orchestrator
  outputRoutingKey: validation.dwcdp.analysis.finished
volumes:
  incoming: { persistentVolumeClaim: { claimName: validation-store, readOnly: true } }
```

## 7. Pipelines workers (feature build)

For each validator worker, the version above and these settings (CLI flags, or the equivalent
in the worker's config):

| Worker (command) | Change |
|---|---|
| Archive validator (`pipelines-validator-archive-validator`) | `--validator-only true` (as before); `--archive-repository` = the validation store; `--orchestrator-completion-routing-key validation.step.completed` (new); `--clb-api-url/user/password` as before; `--clb-callback-url` = `<gateway>/v1/validation/clbValidationCallback`, or the ingress directly: `https://flow-orchestrator-ws.<domain>/validation/clbValidationCallback` |
| Validator DwC-A to verbatim (`validator-verbatim-to-avro-from-dwca`) | `--archive-repository` = the validation store; `--orchestrator-completion-routing-key validation.step.completed` (new) |
| Checklist validator (`pipelines-validator-checklist-validator`) | `--orchestrator-completion-routing-key validation.step.completed` (new); ChecklistBank settings as before |
| Balancer (`pipelines-balancer`) | the feature build only (keeps the step header); no new settings |
| Spark coordinator, validator modes (identifier, interpretation, metrics, DwC-DP stage and to verbatim; standalone and distributed) | the feature image; in `pipelines-spark.yaml`: `orchestrator: { exchange: orchestrator, completionRoutingKey: validation.step.completed }` (from Round 5) |
| Airflow validator DAGs | the feature `ingest-spark-jobs` image |
| Validator cleaner (`pipelines-validator-cleaner`) | unchanged |

In a worker's YAML config, the setting is `orchestrator: { exchange: orchestrator,
completionRoutingKey: validation.step.completed }`, **inside `stepConfig`** for the archive validator
and the validator DwC-A to verbatim, and **at the root** for the checklist validator (it has no
`stepConfig`). The balancer needs none.

`--orchestrator-exchange` defaults to `orchestrator`. A worker without the routing key fails each
orchestrated step it receives (`orchestrator.completionRoutingKey is not configured`), and the
step stays dispatched.

## 8. Cut-over order

1. [ ] Build checks (section 1).
2. [ ] Database: `001`–`004` (section 2).
3. [ ] Deploy the engine, the validation analyser and the pipelines feature workers. They are idle
       until the ws sends commands.
4. [ ] Stop validator-ws and copy its data (section 2).
5. [ ] Deploy flow-orchestrator-ws and switch the gateway.
6. [ ] Smoke tests (section 9).

**Rollback:** point the gateway back to validator-ws and start it again. The new workers' legacy
mode still serves it: a message without a step header behaves as before. Validations made through
the ws in the meantime stay in the `orchestrator` schema.

## 9. Smoke tests

One upload per format, through the portal or `curl -u … -F file=@… <gateway>/v1/validation`.
Follow it with `GET /v1/validation/{key}`, the engine log, and the `orchestrator` tables
(`workflow_execution`, `step_execution`, `workflow_report`).

| Upload | Workflow | Expect |
|---|---|---|
| DwC-A, Occurrence core | `validation-dwca` | the archive check, DwC-A to verbatim, identifiers, interpretation and metrics `FINISHED`; `metrics.files` has eml.xml and the core with terms and indexed counts; an email |
| DwC-A, Event core with occurrences | `validation-dwca` | as above, dataset type `SAMPLING_EVENT` |
| DwC-A with a fatal issue (for example no eml.xml) | `validation-dwca` | `FINISHED`, not indexeable, `error` names the issue, the other steps skipped |
| DwC-A, Taxon core | `validation-dwca-checklist` | `WAITING_FOR_CHECKLISTBANK` on the archive step, then after ChecklistBank's callback the occurrence steps; the ChecklistBank file infos in `metrics.files` |
| ColDP | `validation-coldp` | the archive step waits for ChecklistBank, then `FINISHED`, indexeable |
| CSV (tabular) | `validation-tabular` | as the occurrence DwC-A |
| Spreadsheet | `validation-spreadsheet` | as the occurrence DwC-A |
| DwC-DP | `validation-dwcdp` | the analysis, then the five pipelines steps; the report through `…/validationreport/1/DWCDP_ANALYSIS` |
| Cancel a running validation | — | `ABORTED`; the engine logs the cancel and ignores late replies |

Things to watch:
- Nothing in any `*.dlq`.
- The outbox drains (`outbox_message` has no unpublished rows piling up). Unroutable requests
  mean a worker isn't bound.
- No worker log says the completion routing key is missing.

## 10. Afterwards

When dev has run every format: Round B's B6 (remove the legacy validator-ws reporting from
pipelines, delete validator-ws). Then the same checklist for the next environment.
