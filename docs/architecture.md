# flow-orchestrator — Architecture and decisions

This document tracks the redesign of the former `validator-orchestrator` (pipelines repo,
`gbif/validator/validator-orchestrator`) into a standalone workflow orchestrator for both
validation and ingestion.

Each topic is labelled:

- **Decided**: agreed; implement accordingly.
- **Proposed**: the recommended approach, not yet agreed.
- **Open**: needs a decision, information or an experiment.

Reference for code not yet ported: pipelines commit `dffcecb0b`
(`gbif/validator/validator-orchestrator`). It contains the step definitions, hooks, messaging
configuration, the DwC-DP integration and the `local-test/` Compose environment.

---

## 0. Migration phases (Decided)

| Phase | Scope | Exit criteria |
|---|---|---|
| **A** | Orchestrator only, in several rounds inside this repo. Worker wire contracts are unchanged. | Orchestrator runs the DwC-DP validation step end to end against the real analyser and local Postgres/RabbitMQ. |
| **B** | Adapt the validation pieces: validation workers in pipelines, and registry-ws start/retry commands. | Validation deployed and tested through the new orchestrator. |
| **C** | Adapt the ingestion pieces in pipelines. | Ingestion runs through the orchestrator. |

Phase A rounds (Proposed order):

1. Repository scaffold, pure engine port, this document. *(done)*
2. Contracts: the `flow-orchestrator-api` module (section 6a). *(done)* It holds the command messages, the
   completion envelope, the workflow-finished event and the report type constants.
3. Persistence: table DDL as registry Liquibase changesets, a mirrored H2 test schema, `JdbcClient`
   repositories, state loading with locking, and command handling (`workflow_command`).
4. Messaging: transactional outbox, relay with publisher confirms, completion consumers.
5. Step contract redesign, the DwC-DP analyser adapter (existing wire contract) and the summary
   report builder, plus a ported `local-test/`.
6. Retry, rerun and recovery (timeouts, stale-attempt handling).

**Decided:** during Phase A the orchestrator keeps the existing worker contracts: the DwC-DP
analyser request JSON and the `DwcDpValidationFinished` completion, correlated by
`datasetUuid + attempt`. The new envelope is defined in Phase A (round 2), and workers adopt it in
Phase B.

**Decided (Phase B order):** the **DwC-DP** validation chain is adapted end to end first, then the
**DwC-A** chain (DWCA, TABULAR, SPREADSHEET, XML, and ChecklistBank for checklists and COLDP).
Ingestion follows in Phase C.

## 1. Repository and modules

**Decided**

- Own repository `gbif/flow-orchestrator`, groupId `org.gbif`, packages `org.gbif.orchestrator.*`.
- Modules:
  - `flow-orchestrator-engine`: pure Java (no Spring, DB or RabbitMQ). DAG definition, join
    policies, transition decisions.
  - `flow-orchestrator-app`: Spring Boot service. Configuration, persistence, messaging, step
    integrations.
  - `flow-orchestrator-api`: message contracts as plain records (Jackson only, no postal
    annotations). Added in round 2; see section 6a.
- Steps are identified by gbif-api `StepType`. The engine depends on gbif-api for that enum only.
- Step type columns in the orchestrator's tables are `varchar`, not a Postgres enum. Registry's
  `pipeline_step_type` enum needs a hand-applied `ALTER TYPE` for every new step; we avoid that.

Maven dependencies between repositories (target; no cycles, and the orchestrator never depends
on registry, pipelines or the analyser):

```text
                        gbif-api  (StepType)
                           ▲
                           │
               flow-orchestrator-api   (commands, completion envelope,
                ▲        ▲        ▲        ▲      workflow-finished event, report types)
                │        │        │        │
  flow-orchestrator-app  registry  pipelines  dwc-dp-analyser-service
   ├─ flow-orchestrator-engine
   └─ postal-service (transport only)
```

**Proposed:** move `StepType` and the workflow model out of the monolithic gbif-api into their
own artifact. Facts and options are in section 1a.
- The old `validator-orchestrator` module is removed from pipelines.

**Open**

- gbif-api is consumed from the feature snapshot `2.4.30-FEATURE-ADD-DWCDP-DATA-VALIDATION-SNAPSHOT`,
  because the `VALIDATOR_*DWCDP*` step types are not released. Switch to a release once merged.

### 1a. StepType and workflow model in gbif-api (Proposed: extract)

Facts:

- gbif-api is a single-module repository. Its `org.gbif.api.model.pipelines` package (`StepType`,
  `PipelinesWorkflow`, `PipelineStep`/`PipelineExecution`/`PipelineProcess`, `StepRunner`, …) plus
  `service/pipelines/*HistoryService` is nearly self-contained. It only uses
  `crawler.DatasetProcessStatus`, `registry.LenientEquals` and some Jackson utilities.
- About 95 files use it: roughly 75 in pipelines, 12 in registry and 7 in postal-service.
- Every new step currently needs a gbif-api release plus a hand-applied registry `ALTER TYPE`.

Proposal:

- Make gbif-api multi-module and add a small artifact (for example `gbif-api-pipelines`) that
  contains `StepType` and the pipelines model.
- Keep the package name `org.gbif.api.model.pipelines`, so no consumer has to change imports.
- gbif-api depends on the new artifact, so existing consumers are unaffected.
  `flow-orchestrator-api` and `-engine` depend only on the small artifact.
- `PipelinesWorkflow` (the graph definitions) is legacy. The orchestrator's DAG configuration
  becomes the source of truth. Workers still use the graph today (`getLevel`, balancer routing),
  so it is removed after Phase C, not moved.

**Parked (side task, not part of this migration):** a broader restructuring of gbif-api.

- Each `model/*` package (and its matching service interfaces) would become its own submodule,
  under a `model` parent, so the package hierarchy is kept.
- The root keeps the `gbif-api` coordinates as an aggregate that depends on all of them. Nothing
  changes for existing consumers, and new consumers can depend on just the pieces they need.
- The pipelines package is the first candidate. Feasibility (cross-package references such as
  `LenientEquals` and the Jackson utilities) is not yet assessed.

### Engine changes made during the port

- `SequentialWorkflowEngine` was removed. A sequential workflow is `DagDefinition.linear(steps)`.
  Behaviour difference: on failure, downstream steps become `ABORTED` instead of staying `PENDING`,
  so every step ends terminal. `LinearDagTest` covers this.
- `SET_VALIDATION_STATUS` was renamed `SET_WORKFLOW_STATUS` (domain-neutral).
- `WorkflowEngine.evaluate(state, event, changes)` is the single entry point.
- `ValidationWorkflowMapper` (validator-api specific) was not ported. State will be loaded from
  the orchestrator's own tables.

## 2. Service responsibilities and table ownership

**Decided**

- **registry-ws** (absorbing validator-ws) is the only user-facing API. Start, retry and rerun are
  requested through its existing secured endpoints (for example `PipelinesHistoryResource`
  `run/{datasetKey}` and `run/{datasetKey}/{attempt}`). Registry turns each request into a
  **RabbitMQ command** to the orchestrator, as `DefaultRegistryPipelinesHistoryTrackingService`
  already does with `MessagePublisher`. Commands queue while the orchestrator is down or
  redeploying.
- The orchestrator has **no business HTTP API**. An internal ops endpoint (for example replaying
  stuck outbox rows) may be added later if operations need it. It is not part of the registry
  contract.
- **Tables** live in **registry's existing Postgres schema**. Their **DDL goes in registry's
  Liquibase changelog** (`registry-persistence/src/main/resources/liquibase/`, following
  `185-validation-report.xml`). The orchestrator runs no migrations.
  *Current practice:* registry's Liquibase changelog documents SQL changes, but the changes are
  applied to each environment **by hand**. Every orchestrator round that needs schema changes
  must therefore deliver the changeset and a note listing the manual SQL to apply per
  environment. (Adopting Liquibase to apply changes automatically is encouraged, but outside this
  project.)
- **Only the orchestrator writes** workflow, step, command and report tables, and **registry only
  reads** them. This is enforced with DB grants: `SELECT` for the registry role, and
  `SELECT, INSERT, UPDATE` for the orchestrator role (as in changeset 188).
- **Workers** receive a request, do the work, write artifacts (HDFS), and publish a completion.
  They do not touch workflow state.

Consequence: an orchestrator change that touches tables needs a matching registry changeset and
registry release. Rounds that change the schema list the registry changesets they need.

### 2a. Commands (Decided)

- **One flat record**, `WorkflowCommand` in `flow-orchestrator-api` (`api.command`):
  - `commandId`: UUID, the idempotency key.
  - `type`: `START`, `RETRY_STEP`, `RERUN_FROM` or `RESTART`.
  - `domain`: `VALIDATION` or `INGESTION`.
  - `datasetKey` and `attempt`: the subject.
  - `step`: the target step.
  - `parameters`: a free-form `Map<String,String>`.
  - `requestedBy`, `reason` and `requestedAt`.

  The record's constructor validates the fields each type needs: `START` requires `domain`, and
  `RETRY_STEP`/`RERUN_FROM` require `step`.
- **`START` = domain + parameters.** Registry does not name a workflow. The orchestrator's
  configuration maps domain plus parameters (for example `fileFormat=DWCDP`) to a workflow
  definition. The step choice that validator-ws makes today in `StepResolverService` moves into
  the orchestrator.
- Commands are handled in the same locked transaction as completions (section 3). The orchestrator
  inserts a `workflow_command` row with a unique `commandId`, so a duplicate delivery is a no-op.
- The outcome is stored on the row: `ACCEPTED`, or `REJECTED` with a reason (for example "step is
  not failed"). Registry reads it to show the result. The HTTP caller gets "accepted for
  processing", not a synchronous verdict.
- Registry may pre-check requests using its read access to give fast feedback. The orchestrator's
  check is authoritative.
- Command contracts live in the `flow-orchestrator-api` module, which registry depends on.

**Open: where upload preparation happens.** Today validator-ws receives the multipart upload or a
URL, writes it to its store directory, then unzips and detects media type and format
asynchronously (`FileStoreManager`, `DownloadFileManager`, `MediaTypeAndFormatDetector`). During
this the status is SUBMITTED or DOWNLOADING. The detected format chooses the steps. Registry-ws has
no NFS mount today; only `registry-cli` `DwcaDataIndexer` touches crawler directories.

- *Option 1, registry does it all:* registry knows the archive content before the workflow
  starts, and sends `START` with the detected format. Cost: registry-ws needs the NFS mount and the
  asynchronous unzip and download work, and the orchestrator sees nothing until `START`.
- *Option 2, first workflow step:* registry only writes the raw multipart bytes (a URL needs
  nothing). A `VALIDATOR_UPLOAD_ARCHIVE` worker downloads, unzips and detects, and returns the
  format in its outputs. A decision hook then skips the other formats' branches
  (`WorkflowChange.SKIP`). The work is retryable and visible as a step. Registry reads the
  detected format from the step outputs.

`START`'s free-form `parameters` support both options, so the contracts do not depend on this
decision.

## 3. Workflow lifecycle and hooks

The existing flow (see `OrchestrationService.handle` and `CompletionProcessor.complete` in the
reference commit) reads state over HTTP, runs completion hooks with free I/O (report upload),
writes state over HTTP, then publishes. None of this is atomic.

**Proposed**: every inbound command or completion is handled in one DB transaction:

1. Deserialize the message and identify `(workflow execution, step, attempt)`.
2. `SELECT … FOR UPDATE` on the `workflow_execution` row. This serializes events per workflow,
   whatever the consumer thread count or number of instances.
3. Reject stale or duplicate events by checking the attempt and the step status.
4. Interpret the completion and outcome through the step contract (section 4).
5. Run *decision hooks*. These are pure functions of `(state, completion)` that return
   `WorkflowChange`s (for example `SKIP`) and step outputs. They are the successor of
   `WorkflowChangeHook`.
6. `WorkflowEngine.evaluate(...)` produces the `WorkflowDecision`.
7. Persist the status changes, outputs and report rows, and write an outbox row for each `DISPATCH`.
8. Commit, then ack the message. postal's `MessageConsumer` acks after the callback returns and
   nacks without requeue on exception, so a synchronous transactional callback gives
   ack-after-commit.

What this replaces:

- Free-I/O `CompletionHook`s are dropped. The DwC-DP report upload becomes a `report` row written
  in step 7.
- The "outgoing handoff" (`MessageAdapter`/`handoffAfterCompletion`) becomes an ordinary outbox
  message produced by a decision.
- Required versus optional hooks: decision hooks are always required, because a failure rolls
  back the transaction and the message is dead-lettered. Truly optional side effects
  (notifications) are emitted as outbox messages consumed elsewhere, never run inline.

## 4. Step contract and data between steps

`StepDefinition` today mixes request construction (from a validator-api `Validation`),
completion parsing, and, through `StepRegistration`, hooks, adapters and the routing lookup.

**Proposed** split per step type:

- `RequestFactory`: `StepContext → payload`. `StepContext` holds the workflow identifiers, the
  attempt, and the *resolved inputs* (outputs of completed dependencies, keyed by name).
- `CompletionInterpreter`: `payload → StepCompleted` (section 6a). Workers that already use the
  envelope need only the identity interpreter. Legacy completions (the DwC-DP analyser's
  `DwcDpValidationFinished`) get a per-step adapter.
- Optional decision hooks (section 3).
- Routing (exchange, routing key, completion queue) stays in configuration
  (`orchestrator.steps.<StepType>.*`, as in today's `OrchestratorProperties`), not in the
  definition or the message class.

Data placement (hybrid, as in the brief):

| Where | What |
|---|---|
| Postgres `step_attempt.outputs` (JSON stored as text) | Small values and references: HDFS paths, counts, flags. |
| HDFS | Large artifacts. Workers write to an attempt-specific path and report it in the completion. |
| RabbitMQ | Identifiers, outcome, small control data, references. |

A worker never knows which step produced its input; the orchestrator resolves inputs from
dependency outputs. **Open:** how named outputs map to inputs at a DAG join. Proposal: explicit
per-step input mapping in config, only when the first real join appears.

## 5. Persistence

**Decided:** DDL in registry's Liquibase changelog, in registry's schema (section 2); the
orchestrator runs no migrations. **Proposed:** Spring `JdbcClient` with explicit SQL and Spring
transactions (no JPA, no MyBatis). Use one registry changeset file per orchestrator-owned table,
written in portable SQL where possible so the H2 test mirror (section 8) stays simple.

### Current state in validator-ws (verified)

Today validation status and results share one `json` column, `validation.metrics`
(`validator-api` `Metrics`):

| Field in `metrics` | What it really is | Written by |
|---|---|---|
| `stepTypes[]` {stepType, status, message, executionOrder} | Workflow status per step | `coordinator/tasks` `Validations.updateStatus`, `ValidatorStatusService` |
| `files[]` (counts, terms, issues, samples) | The general report | Archive validators, `validator-core` `Validations` merge, `ChecklistValidator`, Spark `ValidatorMetricsPipeline` (via HDFS) |
| `indexeable` | Business verdict: "GBIF could index this dataset" | `ValidatorMetricsPipeline` (`indexedCount > 0`), `ValidationUtil` (true once the metrics step is reached), `ChecklistValidatorCallback` (ColDP import finished) |
| `error` | Diagnostic message | Various |

ColDP also added the ad-hoc columns `clb_dataset_key` and `clb_validation_message` to `validation`.

How it is written: every worker GETs the whole `Validation` over HTTP, mutates it and PUTs it back.
`ValidationMapper.update` overwrites the whole `metrics` column, with no version check. This has
three consequences:

- Writers that overlap lose each other's updates.
- Every status change rewrites the (possibly large) report data.
- When a later step reports, all earlier steps are blindly marked `FINISHED`.

Registry's ingestion model (`pipeline_process` → `pipeline_execution` → `pipeline_step`) already
uses one row per step, updated in place.

### Status model (Decided: rows, attempts as history)

- `workflow_execution`: one row per execution, with overall status and timestamps.
- `step_execution`: one row per step, with current status and current attempt number. Updated in
  place by a single-row `UPDATE` under the workflow row lock (section 3).
- `step_attempt`: one row per try, with dispatched/finished, outcome, diagnostic message (today's
  `error`) and outputs. This is the history.

There is no JSON status document and no separate transition log. Writes are small and never
read-modify-write a document, and registry reads status with plain `SELECT`s. Volume is about
5–10 steps per workflow with a few writes each.

### Tables

Proposed tables (registry schema):

| Table | Purpose |
|---|---|
| `workflow_execution` | id, domain (validation/ingestion), subject key (dataset uuid), external attempt, definition id/hash, status, timestamps, `version`. |
| `step_execution` | One row per (workflow execution, step type): current status, current attempt number. |
| `step_attempt` | Unique `(workflow_execution_id, step_type, attempt_no)`: status, dispatched/finished timestamps, outcome, diagnostic message, outputs (JSON stored as text). |
| `outbox_message` | id, exchange, routing key, payload, headers, created, published_at, attempts, last_error. |
| `workflow_command` | Unique `command_id`, type, subject, target step, requested_by, reason, received, outcome (`ACCEPTED`/`REJECTED`), outcome reason. |
| `processed_message` | Optional deduplication by message id (if workers supply one). |
| `validation_report` | Reused from **validator-ws** (see below), not registry's changeset 185 version. |

### Reports (Decided: reuse the validator-ws design)

The validator-ws table (`pipelines-validator-ws/.../liquibase/003-validation-report.xml`) is the
model to keep:

```sql
validation_key uuid, attempt integer, report_type varchar(255), report text,
comment text NULL,   -- added (Decided)
created timestamptz, modified timestamptz,
PRIMARY KEY (validation_key, attempt, report_type)
```

- **`comment`** is free text and nullable. By convention the orchestrator writes the producing step
  and attempt there (for example `VALIDATOR_VALIDATE_DWCDP attempt 2`), but anything is allowed. It
  is for people, not for queries.

- **Several reports per attempt**, one per `report_type`. For example:
  - `DWCDP_ANALYSIS`, from the DwC-DP analyser.
  - The "general" report for the DwC-A translation, including checklist/taxonomy lookups. Today
    this is stored inside the `validation` table; it should move to its own report type.
  - Reports for other formats (ColDP, ABCD, …), which may use the `validation` table in similar
    ad-hoc ways today.
- **Reports are decoupled from executions and attempts.** A report is its own row keyed by
  subject, attempt and type. It is not a column of the execution or validation row, so new report
  kinds need no schema change and leave no unused columns behind.
- `report` is `text` (the JSON is parsed in Java), which also suits the H2 tests.
- Writes are upserts on the key (as today's `createOrUpdateValidationReport`), so repeated
  completion processing is safe.

### Report inventory (verified against the code; type names Proposed)

Partial reports, one per producer. Each arrives inline in the producer's completion (section 6a):

| Report type | Content | Producer | Today |
|---|---|---|---|
| `DWCDP_ANALYSIS` | Valid flag, per-resource analysis results | VALIDATE_DWCDP (dwc-dp-analyser-service) | Analyser PUTs it to registry `dataset/{key}/validationreport/{attempt}` |
| `ARCHIVE_VALIDATION` | EML issues, core issues, term counts | VALIDATE_ARCHIVE (old coordinator `BaseDwcaArchiveValidator` and subclasses) | Merged into `metrics.files[]` |
| `CHECKLIST` | ChecklistBank results (`FileInfo`s), import status (today's `clb_dataset_key` and `clb_validation_message` columns), `indexeable` for COLDP | ChecklistBank resume (`ChecklistValidatorCallback`) | `metrics.files[]` and two `validation` columns |
| `DWCDP_CONVERSION` | Compact, detailed and statistics conversion reports | VALIDATOR_DWCDP_TO_VERBATIM (Spark `DwcDpVerbatimConverter`) | Written to HDFS `dwcdp-to-verbatim-report/*.json`; **never surfaced** |
| `INTERPRETATION_METRICS` | `indexedCount`, interpreted term counts, sample values, issues, `indexeable = indexedCount > 0` | VALIDATOR_COLLECT_METRICS (Spark `ValidatorMetricsPipeline`) | HDFS `collect-metrics.json`, then merged into `metrics.files[]` by the Spark job over HTTP |
| *(candidate)* `IDENTIFIERS` | Duplicate-ID and threshold findings | VALIDATOR_VERBATIM_TO_IDENTIFIER | Only the step `message` text |

**Decided: summary report.** Each producer's partial report is stored as its own row. When the
workflow reaches a terminal state, the orchestrator builds a `SUMMARY` report from the partial
reports, in today's `Metrics` shape (`files[]` merged by rowType, plus `indexeable`). This keeps
the UI and existing readers compatible.

- The summary is built in the same transaction as the terminal status change, so FINISHED and the
  summary are always committed together.
- Building it is pure (partial reports in, summary out). It is a per-domain decision hook
  (section 3); ingestion may have none.
- Reuse or port the existing merge logic: `ValidationUtil.mergeMetrics` (spark-jobs) and
  `Validations.mergeFileInfo` (validator-core). Do not invent new merge rules.

Other `metrics` content (Phase B; Phase A leaves the `validation` table untouched):

| Today | Becomes | Status |
|---|---|---|
| `metrics.indexeable` | A field inside the report that computes it, carried into `SUMMARY` | **Decided** |
| `metrics.error`, step `message` | `step_attempt` diagnostic | **Decided** |
| `metrics.stepTypes[]` | `step_execution` / `step_attempt` rows | **Decided** |

**Proposed**

- Replace registry's changeset 185 version (`(dataset_key, attempt)`, single `jsonb` report) with
  the validator-ws design when validator-ws is merged into registry.
- The validator-ws FK to `validation(key)` follows whatever table represents a validation after
  the merge. The orchestrator relies only on the natural key.

**Out of scope for now:** registry's existing execution, validation and report models have similar
"everything in one table" flaws. We note them here but don't fix them as part of this migration.
The orchestrator's own tables should not repeat that pattern.

**Open**

- Moving the "general" report out of the `validation` table changes what registry/validator-ws
  read. This belongs in Phase B (adapting the validation pieces).
- Phase B: the UI and registry readers of `metrics.stepTypes` and `metrics.indexeable` must switch
  to the status tables and reports. Decide then whether a temporary compatibility projection into
  `validation` is needed during the switch.

Workflow definitions stay in application configuration. Each execution records the definition id
and hash it started with, so a configuration change does not reinterpret running workflows.

## 6. RabbitMQ messaging

**Proposed**

- Keep **one completion queue per step** (current behaviour, `StepCompletionListenerConfiguration`),
  with routing in configuration.
- **Outbox relay:** a scheduled poller (a single relay thread per instance) picks unpublished
  rows, publishes them, waits for the broker confirm, and marks them published. Correctness must
  not depend on `FOR UPDATE SKIP LOCKED`: with several instances the worst case is a duplicate
  publish, which consumers tolerate anyway. `SKIP LOCKED` may be used on Postgres only as an
  optimisation, because H2 tests cannot verify its semantics (section 8). A crash between publish
  and mark gives a duplicate publish, not a lost one. Workers and the orchestrator must tolerate
  duplicates, which the attempt check in section 3 handles.
- Dead-letter exchanges for orchestrator queues, so nacked messages are kept and visible.

**Postal facts (verified)**

- `@MessageBinding` and the `Message` interface only feed postal's convenience overloads:
  `send(Message)`, and `listen(...)` without an exchange or routing key. `DefaultMessageRegistry`
  scans only postal's own `org.gbif.common.messaging.api.messages` package.
- The explicit `listen(queue, routingKey, exchange, threads, MessageCallback<T>)` takes any
  type. It declares the durable topic exchange, the durable queue and the binding itself.
  `send(Object, exchange, routingKey)` also takes any object.
- The annotations' only infrastructure effect: the publisher declares the exchanges of all
  annotated classes at startup.

**Decided:** new contracts are plain records without postal annotations. Topology is owned by
configuration on the side that uses it:

- The listener declares its queue and binding.
- The publisher declares, idempotently, the exchange it sends to.
- The outbox relay publishes with `mandatory=true` and treats a returned (unroutable) message as a
  publish failure. postal does not, so a message sent before the worker's queue exists would
  otherwise be dropped silently.

**Open**

- postal's `DefaultMessagePublisher` never enables publisher confirms (`confirmSelect`). It only
  calls `waitForConfirmsOrDie` on close. The relay therefore needs either a postal change or a
  small publisher on the RabbitMQ client using the same `ConnectionParameters`.
- Validation and ingestion isolation: separate vhosts, or a shared vhost with prefixed exchanges.
  Proposal: one codebase, one deployment per domain, each configured with its own vhost or prefix.
  The engine is unaffected either way.

## 6a. Message contracts: `flow-orchestrator-api` (Decided)

Plain records, serialized with Jackson, carrying no exchange or routing key. Implemented in round 2:

| Package | Types |
|---|---|
| `api.command` | `WorkflowCommand`, `CommandType`, `Domain` |
| `api.step` | `StepHeader`, `StepCompleted`, `Outcome`, `Report` |
| `api.event` | `WorkflowFinished`, `WorkflowResult` |
| `api.report` | `ReportTypes` (String constants) |
| `api.json` | `OrchestratorJson`: the Jackson settings for the contracts |

- **Dependencies.**
  - Jackson (annotations, databind, jsr310) at compile scope.
  - `gbif-api` at **`provided`** scope, used only for `StepType`. Each consumer keeps its own
    gbif-api version; registry is on an older release.
  - No Spring and no postal.
- **Step types.** A receiver must know every `StepType` it deserializes. The orchestrator always
  has the newest. `WorkflowFinished` deliberately carries no `StepType`, so registry only ever
  *sends* step types (in commands).
- **Compatibility.**
  - Every record has `@JsonIgnoreProperties(ignoreUnknown = true)`, so a newer sender does not
    break an older consumer.
  - Consumers using their own `ObjectMapper` must at least register `JavaTimeModule`.
    `OrchestratorJson.configure(mapper)` applies all the settings: ISO-8601 instants, nulls left
    out, and unknown properties ignored.
- **Validation.** The rules are enforced in the record constructors, so both sender and receiver
  fail fast:
  - required fields;
  - attempts ≥ 1;
  - a diagnostic when the outcome is not `SUCCEEDED`;
  - no duplicate report types;
  - `rejected` only with `FAILED`.
- **Golden fixtures.** The JSON in `flow-orchestrator-api/src/test/resources/contracts/` locks the
  wire format. Change a fixture only together with a deliberate contract change.

```java
// worker → orchestrator: one envelope for all adapted workers
record StepCompleted(
    UUID messageId,                 // deduplication key for redelivery
    StepHeader header,              // copied back unchanged from the request
    Outcome outcome,                // SUCCEEDED | FAILED | REJECTED
    String diagnostic,              // reason for FAILED/REJECTED; becomes the step_attempt diagnostic
    Map<String, String> outputs,    // small machine values for later steps
    List<Report> reports,           // human-facing results; one validation_report row each
    Instant startedAt, Instant finishedAt) {}

// orchestrator → worker: included in every step request, echoed back by the worker
record StepHeader(
    UUID workflowId,                // workflow_execution id
    UUID datasetKey, int attempt,   // subject; also used for logs and HDFS paths
    StepType stepType,
    int stepAttempt) {}             // orchestrator's attempt counter, used to drop stale completions

record Report(
    String type,                    // constants in ReportTypes (section 5); a String, so new types need no API release
    JsonNode content,               // raw JSON, not an escaped string
    String comment) {}              // optional; the orchestrator fills "<StepType> attempt <n>" when null

enum Outcome { SUCCEEDED, FAILED, REJECTED }
```

- **`outcome`** describes what happened to the work, not what should run next.
  - `SUCCEEDED`: the step finishes.
  - `FAILED`: the worker broke (exception, infrastructure problem). This is the retryable case.
  - `REJECTED`: the worker ran correctly but the data is invalid. Examples: the analyser's
    `valid=false`, or the identifier duplicate threshold being exceeded.

  `FAILED` and `REJECTED` both make the step `FAILED` and stop the workflow. The attempt keeps
  the distinction, so the UI can say "data invalid" versus "processing error", and retrying a
  `REJECTED` attempt can be refused.
- **`outputs`** are for machines: small named values (HDFS paths, counts, flags such as
  `hasOccurrences`/`hasEvents`). They are stored on `step_attempt.outputs` and resolved into later
  requests (section 4).
- **`reports`** are for people. They form a list of objects because each element is a table row,
  and fields can be added later. A completion that contains a duplicate `type` is rejected.
  Content travels inline: the same data already went over HTTP as one `Validation` PUT, and it is
  far below RabbitMQ's message size limit.
- Also in the module: the command messages (section 2a), a `WorkflowFinished` event (orchestrator
  → registry, which sends the user email that validator-ws sends today), and `ReportTypes`.
- **Worker request messages** move from postal into this module **one worker at a time**, as each
  worker is adapted. The order is the DwC-DP chain, then the DwC-A chain, then ingestion
  (Phase C). Each request record contains a `StepHeader` plus the worker's own fields. During the
  transition an adapted worker accepts both the old postal message (used by ingestion) and the
  new request (used by validation). Once Phase C is done, the postal pipeline message classes can
  be retired.
- **Legacy adapter:** the DwC-DP analyser keeps `DwcDpValidationFinished` during Phase A. A
  per-step adapter maps it to `StepCompleted` (analysis report as `DWCDP_ANALYSIS`;
  `valid=false` → `REJECTED`).

Target message flow, using the DwC-DP slice as the example. Every worker only replies; the
orchestrator dispatches the next step. Today each worker sends the next step's message itself,
directly or through the balancer.

```text
 user ─HTTP─► registry-ws ──[StartWorkflow]──────────► orchestrator ◄──► DB (status, reports,
                  ▲  reads status/reports (SQL)            │  ▲            commands, outbox)
                  └──────────[WorkflowFinished]────────────┤  │
                                                           ▼  │ StepCompleted + DWCDP_ANALYSIS
                                                   dwc-dp-analyser
                                                           ▼  │ StepCompleted
                                                   VALIDATOR_DWCDP_STAGE
                                                           ▼  │ StepCompleted + DWCDP_CONVERSION
                                                   VALIDATOR_DWCDP_TO_VERBATIM (Spark/Airflow)
                                                           ▼  │ StepCompleted
                                                   VALIDATOR_VERBATIM_TO_IDENTIFIER
                                                           ▼  │ StepCompleted
                                                   VALIDATOR_VERBATIM_TO_INTERPRETED
                                                           ▼  │ StepCompleted + INTERPRETATION_METRICS
                                                   VALIDATOR_COLLECT_METRICS
                                     orchestrator: FINISHED + SUMMARY in one transaction
```

Workers then touch only NFS/HDFS, RabbitMQ and Airflow. They make no HTTP calls to validator-ws
or registry for workflow purposes.

## 7. Retry, rerun and recovery

**Proposed**

- **Retry step:** a new `step_attempt` for a `FAILED` step. Previous attempts and reports are
  kept.
- **Rerun from X:** X and its transitive downstream steps get new attempts and return to
  `PENDING`. Completed steps elsewhere (parallel branches not downstream of X) are reused.
- **Restart:** a new `workflow_execution`.
- Concurrent commands are serialized by the row lock. A command that conflicts with the current
  state (for example retrying a running step) is rejected and logged.
- A completion for an attempt that is not current is acked and ignored.
- **Lost work:** a scanner flags step attempts that have been dispatched for longer than a per-step
  timeout. They are surfaced through metrics and logs, not retried automatically.
- **No automatic retries** without an explicit per-step policy.
- Correlation needs an attempt identifier in messages, which only arrives in Phase B. Until then,
  `datasetUuid + attempt` plus the "step is active" check (as today) is the stale filter.

## 8. Non-functional requirements

- Actuator health probes and Prometheus metrics (in place). To add: workflow and step transition
  counters, outbox backlog, and the age of the oldest unpublished message.
### Testing (Decided: H2 + handwritten fakes)

- JUnit 5 with handwritten fakes; no Mockito, no Testcontainers, no embedded Postgres.
- Persistence tests run against **H2** using `src/test/resources/schema-h2.sql`. That file mirrors
  the registry changesets for orchestrator-owned tables and is updated in the same change as the
  registry changeset.
- Messaging is tested with in-memory fake publishers and consumers. That includes failure
  injection between commit, publish and ack.
- Real RabbitMQ behaviour (publisher confirms, ack/nack, dead-lettering) and multi-instance
  behaviour are verified in the Compose `local-test` environment, not in `mvn verify`.

Caveats and how the design handles them:

| H2 gap | Mitigation |
|---|---|
| No Postgres `jsonb` type or operators | Map JSON columns as text or H2 `JSON` in the test schema. Orchestrator queries never use jsonb operators; JSON is parsed in Java. |
| `FOR UPDATE SKIP LOCKED` semantics differ | Outbox correctness does not depend on it (section 6). |
| Row-lock behaviour under real concurrency | Single-instance correctness is covered with H2 `FOR UPDATE`. Multi-instance behaviour is checked in Compose. |
| Registry changesets are Postgres SQL | Keep orchestrator changesets portable, and keep the H2 mirror alongside. |

## 9. Validation workflows today (verified inventory, pipelines `feature/dwcdp-data-validator`)

Steps per format. These come from validator-ws `StepResolverService`, not from the gbif-api graph
(`PipelinesWorkflow.getValidatorWorkflow`):

| Format | Steps after VALIDATOR_UPLOAD_ARCHIVE | Start message |
|---|---|---|
| DWCA, TABULAR, SPREADSHEET | VALIDATE_ARCHIVE → DWCA_TO_VERBATIM → VERBATIM_TO_IDENTIFIER → VERBATIM_TO_INTERPRETED → COLLECT_METRICS | `PipelinesArchiveValidatorMessage` |
| XML | The same, with XML_TO_VERBATIM | `PipelinesArchiveValidatorMessage` |
| COLDP | Only VALIDATE_ARCHIVE. Workers continue after the ChecklistBank import anyway. | `PipelinesArchiveValidatorMessage` |
| DWCDP | VALIDATE_DWCDP → DWCDP_STAGE → DWCDP_TO_VERBATIM → VERBATIM_TO_IDENTIFIER → VERBATIM_TO_INTERPRETED → COLLECT_METRICS | Plain JSON `{datasetUuid, attempt}` |

`VALIDATOR_ABCD_TO_VERBATIM` and `VALIDATOR_TABULAR_TO_VERBATIM` exist in gbif-api, but nothing
schedules or handles them.

Who handles each step:

| Step | Handler |
|---|---|
| VALIDATE_ARCHIVE, DWCA_TO_VERBATIM, ChecklistBank resume, cleaner | Old coordinator (`coordinator/tasks`) |
| XML_TO_VERBATIM | Old coordinator, via the *ingestion* `PipelinesCallback` |
| DWCDP_STAGE, DWCDP_TO_VERBATIM, VERBATIM_TO_IDENTIFIER, VERBATIM_TO_INTERPRETED, COLLECT_METRICS | spark-coordinator, either standalone or distributed through Airflow |
| VALIDATE_DWCDP | dwc-dp-analyser-service |

No step is handled by both coordinators.

What Phase B removes or replaces:

- **Status over HTTP.** Both coordinators' `ValidatorCallback`s GET and PUT the whole
  `Validation` for every step (RUNNING, FINISHED or FAILED, plus a message). Each worker works out
  the overall status itself and marks earlier steps FINISHED using the deprecated
  `Graph.getLevel`.
  → Replaced by `StepCompleted`; the orchestrator owns all transitions.
- **Spark makes HTTP calls.** `ValidatorMetricsPipeline` reads validator-ws (supplied terms) and
  merges metrics through GET/PUT from inside the Spark driver.
  → It writes HDFS only. Report content goes in the completion. The supplied terms needed as
  input come from the request instead.
- **Workers chain steps themselves.** Each worker publishes the next step's message, directly or
  through the balancer.
  → Workers only reply; the orchestrator dispatches.
- **Emails are triggered by PUTs.** validator-ws sends the email when a PUT leaves the status
  terminal. The CLB cleaner also sends them.
  → Registry sends them on `WorkflowFinished`.
- **ChecklistBank is an external wait.** Its HTTP callback goes to validator-ws, and a scheduled
  cleaner polls CLB.
  → Registry receives the callback and turns it into a completion for the step that is waiting.
- **The analyser writes to registry.** It PUTs its report to registry; failures are only logged.
  → The report travels in the completion (Phase A: via the adapter).
- **Known defects:**
  - The DwC-DP chain is broken today. Nothing consumes `DwcDpValidationFinished` and nothing sends
    the validator `DwcDpStageMessage`; the removed orchestrator's handoff adapter did this, so
    port it.
  - XML_TO_VERBATIM writes to registry's ingestion history, not to the validation.
  - The ChecklistBank validators do an async GET/PUT that can overwrite the callback's status
    PUT, and the reverse.
  - The `@Value` keys for validator-ws's DwC-DP start message may not match its YAML. Check the
    deployed config.
