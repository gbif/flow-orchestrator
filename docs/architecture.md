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
3. Persistence: table DDL (since 2026-11-19 in this repository's `db/` scripts, schema `orchestrator`), H2 tests on the same script, `JdbcClient`
   repositories, state loading with optimistic versioning, START handling and outbox rows. *(done)*
4. Messaging: outbox relay with publisher confirms, command and completion listeners with
   dead-letter queues (postal-service extended with builders, section 6), completion handling,
   the `workflow_report` table, the contract change from section 4a (no `REJECTED` outcome,
   verdict on `WorkflowFinished`) and the `reason` columns. *(done)*
5. The DwC-DP validation chain end to end (scope widened on 2026-11-18, see "Round 5 plan" below), *(done)*:
   step contract redesign, the analyser adapter, report interpreters and `SKIP_FROM`, the
   pipelines DwC-DP validator jobs in orchestrator mode, and a ported `local-test/`. The summary
   report builder moves to Phase B.
6. **Round A** (Phase B starts; revised 2026-11-19): validator-ws moves into a new module of this
   repository, `flow-orchestrator-ws`, and DwC-DP validations run through the orchestrator (see
   "Rounds A and B" below).
7. **Round B:** the DwC-A validation chain in orchestrator mode, then all validator-ws reporting is
   removed from pipelines.
8. Retry, rerun and recovery (timeouts, stale-attempt handling).

### Rounds A and B (Decided 2026-11-18, Round A revised 2026-11-19)

**Revision 2026-11-19 (with a colleague):** validator-ws does **not** go into registry-ws. It
becomes `flow-orchestrator-ws`, a separately deployed module of this repository (the
"orchestration-ws"), so workflow and validation live together and registry gets smaller. Its
tables go into the `orchestrator` schema. Registry is not involved in validation any more.

- **Round A, one round:** validator-ws moves into `flow-orchestrator-ws` **and** DwC-DP
  validations run through the orchestrator. DwC-A, XML, tabular and ColDP keep today's legacy
  path (workers GET/PUT `/validation/{key}`) until Round B; the ws serves both paths meanwhile.
  Ingestion is untouched.
- **Two deployables, one schema.** `flow-orchestrator-ws` serves the user-facing `/validation` API
  (same paths, so the gateway route `/v1/validation*` only changes target), uploads and URL
  downloads, admission, emails, the ChecklistBank callback and the scheduled jobs. It writes the
  `validation` table and sends `WorkflowCommand`s. `flow-orchestrator-app` keeps running the
  workflows and remains the only writer of the workflow tables. Both are "the orchestrator" for
  schema ownership: everything in `orchestrator` is written by this repository's services.
- **Built from the security-fixed validator-ws:** pipelines branch
  `origin/fix/validator-ownership-and-upload` (`d80a64934a`, gbif/security-issues#46: owner/admin/
  app checks on update, cancel and delete; sanitised upload names; no undeleting; the
  upload/insert race). The ChecklistBank callback stays unauthenticated by design (`412eddc9e6`).
- **Status and metrics are composed at read time** for orchestrated validations:
  `GET /validation/{key}` builds `status`, `metrics.stepTypes` and `metrics.fileInfos` from the
  workflow tables and reports, in today's response shape, so the data-validator UI is unchanged.
  Later, together with the frontend, the API moves to exposing workflow, steps and reports
  directly. Legacy validations keep their stored `metrics`.
- **Admission stays in front of the workflow**, in the ws: the per-user running cap, the file and
  URL checks (size, reachable URL, content type, format detection, which selects the workflow),
  and "installationKey requires a notification email". What runs, in which order and whether it
  may retry belongs to the engine.
- **Emails** (validator-mail) move into the ws; for orchestrated validations they are sent on
  `WorkflowFinished`.
- **Jobs:** ChecklistBank polling and retention run in the ws, with ShedLock so each runs on one
  instance.
- **Data access:** ported to `JdbcClient` (no MyBatis), like the app.
- **Ingestion guards in registry** (pipelines history "already running", reruns from stored
  messages, the crawl refusal) are untouched until Phase C.
- **Round B** adapts the old-coordinator DwC-A chain (archive validation, DwC-A and XML to-verbatim,
  the ChecklistBank wait) and then deletes the legacy validator-ws reporting from the shared steps;
  doing it earlier would break DwC-A validation.
- **Modules** (decided 2026-11-18):
  - validator-mail moves into `flow-orchestrator-ws` (revised 2026-11-19, was registry).
    validator-api is still used by the legacy workers until Round B; see the Round A plan for
    how the ws depends on it.
  - validator-eml moves to a **new, small EML validation library in its own repository**, which
    registry, the pipelines archive check and the DwC-DP analyser share. It combines
    well-formedness, validation against the declared schema with bundled schemas (EML 2.2.0 and
    the GBIF profile versions, no network), the GBIF metadata quality rules (today
    `BasicMetadataEvaluator` in validator-core) and a neutral result each caller maps to its own
    issues. The repository is created and mounted by the user before the work starts.
    **Built 2026-11-18** as `org.gbif:gbif-eml-validation:1.0.0-SNAPSHOT` (package
    `org.gbif.eml.validation`, motherpom 59, Java 17). Decided with it:
    - Bundled schemas: EML 2.1.1, EML 2.2.0 and GBIF profile 1.0, 1.0.1, 1.0.2, 1.1, 1.2, 1.3,
      chosen by the root namespace's `xsi:schemaLocation`. A missing or unbundled location uses
      the caller's default schema with a warning (registry and pipelines: GBIF profile 1.3; the
      analyser: EML 2.2.0); without a default it is an error and no schema validation runs.
      Nothing is fetched.
    - The quality rules keep gbif-api's `Dataset`/`CitationGenerator` and gbif-metadata's
      `DatasetEmlParser`, so they match registry's interpretation. They are warnings.
    - A DOCTYPE is refused (`NOT_WELL_FORMED`): `DatasetEmlParser` (gbif-metadata-profile-eml
      3.1) resolves external entities, so it must only see documents the library has accepted.
      Registry's other `DatasetEmlParser` uses are exposed the same way (to raise separately).
    - pipelines' `MetadataPath` moved too, as `ArchiveMetadata.find` (ignores a `meta.xml`
      location outside the archive).
    - Consumers switch in Round A (the ws's `/validation/eml`) and Round B (the archive check,
      the analyser's `EmlValidator`); pipelines' `validator-eml` is deleted then.
  - validator-core is validation *steps*: the archive checks (`DwcaValidator`,
    `DwcaFileTermCounter`, `BasicMetadataEvaluator`) and the ChecklistBank submission and
    evaluation. It is redesigned in Round B. Its four unused classes (`LineCounter`,
    `RawToInterpreted`, `IndexableRules`, `ColdpTermDeserializer`) are deleted.
  - `ChecklistbankWsClient` is an integration client for ChecklistBank's API (submit, import
    status, verbatim records), not validator logic. It belongs with ChecklistBank (an official
    COL client if one exists) or in a small client artifact; the worker uses submit and verbatim,
    the waiting side (the orchestrator, Round B) uses the import status. Round A carries only
    what the ws's poller needs.

### Round A plan (Decided 2026-11-19; implemented, not deployed)

**Implemented 2026-11-19, with these deviations from the plan below:**
- **XML detection is not fixed.** An XML upload would run `XML_TO_VERBATIM`, which writes
  registry's ingestion history instead of the validation (section 9). It keeps failing as an
  unsupported format until Round B fixes that step.
- **Reports:** `GET /validation/{key}/validationreport/{attempt}/{type}` reads `workflow_report`.
  The PUT counterpart is gone; it had no production caller.
- **ChecklistBank imports stay raw JSON.** The callback body and the poller's import are passed
  on as ChecklistBank sent them, and only `status` is read. This avoids validator-core's ColDP
  term deserializer and the catalogue dependency. (Correction to "Modules" above:
  `ColdpTermDeserializer` *is* used, through `ObjectMapperUtils`, by the coordinator's
  checklist validators.)
- **Commands are sent directly**, with publisher confirms, not through an outbox. A failed start
  marks the validation `FAILED` (with an email); a failed `CANCEL` is logged, and the validation
  is cancelled for the user anyway.
- **Emails are best effort:** the status is stored first, and a mail or user-lookup failure is
  logged. Otherwise a `WorkflowFinished` would be dead-lettered for an email.
- **Listing by status:** an orchestrated validation's stored status is `QUEUED` until its
  workflow ends (the composed status is only computed when it is read). A search for `RUNNING`
  therefore misses running DwC-DP validations. Fixed when the API exposes workflows (later).
- **Kept from validator-ws:** POSTs answer 201 and void responses 204 (gbif-common-ws).
- **Schema scripts moved to the repository root** (`db/`), so both services' tests run them.
  H2 needs `NON_KEYWORDS=KEY`, because the `key` column keeps its name.


Source: validator-ws (`pipelines-validator-ws`, `validator-api`, `validator-mail`) as on pipelines
`origin/fix/validator-ownership-and-upload`. Survey findings that shaped this are listed at the end.

**flow-orchestrator-api**
- `CommandType.CANCEL`: ends a running workflow. Needed to cancel an orchestrated validation.

**flow-orchestrator-app (engine)**
- `CANCEL`: the execution ends `ABORTED`. `PENDING` and dispatched steps become `ABORTED` with the
  reason "Cancelled by <user>". A `WorkflowFinished` (`ABORTED`) is sent. A completion that arrives
  later for an aborted attempt is ignored and logged. Workers are not stopped; a running Spark job
  finishes and its result is dropped.

**Schema (`db/`, applied by hand)**
- `003-validation-tables.sql`: `orchestrator.validation` with today's columns (including
  `clb_dataset_key` and `clb_validation_message`). `status` and `file_format` become `varchar`
  instead of Postgres enums, like the workflow tables, so a new format needs no `ALTER TYPE`
  (validator-ws's missing changeset 004 shows why). Indexes for the per-user cap and the listing.
  Also the ShedLock table `orchestrator.shedlock`.
- Grants: a separate role for the ws (placeholder `orchestrator_ws`) with `SELECT, INSERT,
  UPDATE` on `validation` and `shedlock`, and `SELECT` on the workflow tables and reports. The
  engine's role gets `SELECT` on `validation`. Registry keeps `SELECT` on everything.
- `db/migration/validator-ws-data.md`: the one-off copy per environment, run while validator-ws is
  stopped. The `validation` rows (`pg_dump --data-only` from the validator database, loaded into
  the `orchestrator` schema; enum values cast to text) and any `validation_report` rows into
  `workflow_report` (`validation_key` becomes `dataset_key`, `report` text cast to jsonb).
  validator-ws's `validation_report` table is not recreated; it has no production writer.

**flow-orchestrator-ws (new module, own deployable)**
- **Security:** remote authentication against the GBIF API (gbif-common-ws
  `RemoteAuthWebSecurityConfigurer`), as validator-ws does, with the same roles and the
  colleague's ownership checks.
- **Endpoints, same paths and shapes:**
  - `/validation` (submit by upload or URL, get, list, cancel, delete, legacy PUT, eml, running,
    `clbValidationCallback`, report GET/PUT).
  - `/validation/eml` and `/validation/eml/schemas`, now on gbif-eml-validation. The response is
    still `IssueInfo`, mapped from `EmlIssue`.
  - `/validation/sequence` and `/validation/enumeration`.
- **Submit:**
  - Admission checks as today, then the `validation` row.
  - **DWCDP:** a `WorkflowCommand` `START` (domain `VALIDATION`, dataset key = validation key,
    attempt 1, parameter `fileFormat=DWCDP`). The row stays `QUEUED`.
  - **Other formats:** today's legacy start messages.
- **GET (orchestrated):** when a workflow execution exists for the validation, `status`,
  `metrics.stepTypes` (from `step_execution`) and `metrics.fileInfos` (from the reports) are
  composed from the workflow tables. Legacy rows return their stored `metrics`.
- **WorkflowFinished listener:**
  - Sets the row's terminal status: `FINISHED` (a rejection is `FINISHED` with
    `indexeable=false`), `FAILED` or `ABORTED`.
  - Sends the email to the stored notification emails, or else the owner.
- **Cancel:**
  - Orchestrated: the row becomes `ABORTED` and a `CANCEL` command is sent.
  - Legacy: as today.
- **Legacy PUT** to an orchestrated validation: refused with 409.
- **Mail:** validator-mail moves into the module (templates en/es/ru, subjects, logo).
- **Jobs:** `ClbValidationCleaner` and `RetentionPolicyTask` run under ShedLock and page through
  all due rows, not only the first 20.
- **ChecklistBank:** a minimal client with only `checkImport`, for the poller.
- **Model:** the validator-api types (`Validation`, `Metrics`, `ValidationRequest`,
  `ValidationSearchRequest`, `FileFormat`, the evaluation enums, `ClbDatasetImport`) are copied
  into the module, which becomes their home. The JSON stays identical. pipelines' validator-api
  stays for the legacy workers until Round B.
- **Fixes beyond the security branch:**
  - Notification emails kept on worker-triggered emails.
  - An email for upload and download failures.
  - An email only on the first transition to a terminal status (no duplicates).
  - `NOT_FOUND` returns 404.
  - XML uploads detected as `XML`.
  - Default ordering for the listing (`created DESC`).
- **Logged, not fixed:**
  - `MAX_RUNNING_VALIDATIONS` stays 420.
  - Search dates still snap to month boundaries.
  - URL downloads still have no size limit.
- **Tests:** H2 with the `db/` scripts, MockMvc for the resources, handwritten fakes for
  messaging, mail and ChecklistBank (as in the app).

**Deployment and other repos**
- The gateway route `/v1/validation*` points at `flow-orchestrator-ws`. Workers keep calling
  `registry.wsUrl/validation`, so pipelines does not change in Round A.
- The ws mounts the shared validation store (`storePath`), as validator-ws does today.
- validator-ws is switched off after the data copy. Its code in pipelines is deleted in Round B,
  with the legacy reporting.
- local-test gains the ws service.

**Survey notes (validator-ws, 2026-11-19)**
- Its own database (`validation.datasource.*`, requires `testdb=false`).
- `master.xml` misses changeset 004 (DWCDP).
- The DwC-DP start keys in the code (`validator.dwcdp.exchange` and `.routing-key`) differ from
  the sample yml.
- The ChecklistBank cleaner finishes and emails before the checklist results message is
  processed.
- Workers reach it via the gateway.

### Round 5 plan (Decided 2026-11-18)

Workflow `validation-dwcdp`: VALIDATE_DWCDP → DWCDP_STAGE → DWCDP_TO_VERBATIM →
VERBATIM_TO_IDENTIFIER → VERBATIM_TO_INTERPRETED → COLLECT_METRICS.

**flow-orchestrator**
- Engine: `SKIP_FROM(step)` (the step and everything downstream of it become `SKIPPED`).
- Per step type: a `RequestFactory` (`StepContext` → payload; the context holds the header, the
  execution, the outputs of completed steps and the stored reports) and the routing from config.
- Report interpreters by domain and step: `DWCDP_ANALYSIS` with `metadata.isValid=false` and
  `IDENTIFIERS` with `isResultValid=false` skip the rest; the workflow ends `FINISHED`, rejected.
- Analyser adapter: an adapter queue for `DwcDpValidationFinished`, correlated by
  `datasetUuid + attempt` to the running VALIDATE_DWCDP attempt and mapped to `StepCompleted`
  (`SUCCEEDED`, report `DWCDP_ANALYSIS` = the `AnalysisExecution`).
- Pipelines requests: the existing postal message class with a `stepHeader`, wrapped in
  `PipelinesBalancerMessage` and sent to the balancer. Counts and `datasetType` come from earlier
  steps' outputs. The orchestrator does not send `containsOccurrences`/`containsEvents` (see
  below).
- **No data-shape flags in validation.** The flags existed because registry had to register the
  whole workflow up front, and they selected the flow. The orchestrator now writes every step of
  the chosen definition as a `PENDING` row at START, so registry and the portal can show future
  steps from `step_execution` (Phase B readers). In orchestrator mode, to-verbatim derives events
  and occurrences from the staged counts (`dwcdp-stage.yml`, `COUNT_<RESOURCE>` > 0) and reports
  what it produced (`datasetType`) as an output. Ingestion keeps the flags until Phase C.
- Senders know exchange and routing key; consumers declare their queue and bind it.

**dwc-dp-analyser-service**
- `DwcDpValidationFinished.validationReport` becomes the whole `AnalysisExecution`
  (`{result, metadata: {started, finished, features, isValid}, version}`); `valid` stays.
  **Breaking for the crawler** (`crawl.dwcdp.validation.finished`): analyser-service-api, postal's
  `DwcDpValidationFinishedMessage` and the crawler are released together.
- New options for the input exchange and routing key; the analyser binds its own queue.
- Its `ObjectMapper` ignores unknown properties.

**postal-service**
- Optional `stepHeader` on `DwcDpStageMessage`, `DwcDpToVerbatimMessage`,
  `PipelinesVerbatimMessage` and `PipelinesInterpretedMessage`, typed as `Map<String, Object>`:
  postal does not depend on `flow-orchestrator-api`. pipelines converts it to `StepHeader`
  (`convertValue`), so the record's checks run before any work starts. The map is replaced when
  the request records move into `flow-orchestrator-api` (section 6a); the JSON stays the same.
- `DwcDpValidationFinishedMessage` follows the analyser change.
- Release order: flow-orchestrator-api, analyser-service-api and postal (any order) → pipelines
  and crawler.

**crawler** (done 2026-11-18, `feature/orchestration`): postal
`2.1.16-FEATURE-ORCHESTRATION-SNAPSHOT` and analyser-service-api `0.0.9-SNAPSHOT`. Metasync only
reads `valid`, so no code change; a test reads the analyser's new message with postal's mapper.
Switch both to releases when they are cut. Its Jenkinsfile gained the feature-branch version stage
(as in postal), so the branch publishes `2.0.15-FEATURE-ORCHESTRATION-SNAPSHOT`.

**pipelines (validation only; ingestion and DwC-A validation keep today's behaviour)**
- A validator callback whose message has a `stepHeader` runs in orchestrator mode: no
  validator-ws calls, no next message, and a `StepCompleted` at the end (`FAILED` with the
  exception message on error), sent to the configured exchange and routing key. Without a
  header, it behaves exactly as today.
- The balancer handlers keep the `stepHeader` when they re-route.
- Identifier validation: invalid results are a `SUCCEEDED` step with an `IDENTIFIERS` report.
- To-verbatim: the conversion reports as `DWCDP_CONVERSION`. Metrics: `collect-metrics.json` as
  `INTERPRETATION_METRICS`, and the supplied terms derived from the data, not from validator-ws.
- Outputs for later steps: the record counts and `datasetType`.
- To-verbatim in orchestrator mode ignores the message flags and reads the staged counts instead.

**local-test:** Compose with Postgres (the `db/` scripts, schema `orchestrator`), RabbitMQ, the orchestrator
and the real analyser, plus a trigger that publishes a `WorkflowCommand`. Implemented as `local-test/` (USAGE.md):
the pipelines steps can be simulated with `task complete`.

**Decided during the round**

- **java-time in messages.** The analyser's `AnalysisExecution` carries `LocalDateTime` values.
  postal registers Jackson's java-time module on every mapper it uses (`MessageUtils.configure`),
  so postal consumers such as the crawler read them. The analyser writes them as ISO
  `LocalDateTime` (UTC) in messages; its registry PUT keeps the `+00:00` form. The orchestrator
  adapter accepts both.
- **Outputs between steps** are named in `OutputKeys`; a request takes an output from the latest
  finished step that reported it.
- **Request routing for pipelines steps** is the balancer (`occurrence` /
  `occurrence.pipelines.balancer`) in configuration; the balancer handlers copy the `stepHeader`
  when they re-route.

**Known limitations (round 6 or later)**

- The analyser sends nothing when it fails, and a pipelines job whose reply cannot be published
  only logs it: the step stays dispatched until timeout handling (round 6).
- A request that needs an output no earlier step reported fails inside the completion
  transaction, so that completion is dead-lettered (`MissingOutputException` in the log).
- An event-only DwC-DP dataset (events, no occurrences) takes the balancer's events path, which
  is not validator-aware; it never reaches the identifier step. Already broken before the
  orchestrator.
- A rejection while parallel branches are still running does not end the workflow, and the
  verdict is not kept (logged as a warning). The DwC-DP DAG is linear.

**Decided:** during Phase A the orchestrator keeps the existing worker *request* contracts: the
DwC-DP analyser request JSON and the pipelines postal messages (with an added `stepHeader`). The
analyser keeps its `DwcDpValidationFinished` completion, correlated by `datasetUuid + attempt`
(round 5 changed its `validationReport` to the whole `AnalysisExecution`). The pipelines DwC-DP
validator jobs adopted the `StepCompleted` envelope in round 5 (scope widened by decision); the
other workers adopt it in Phase B.

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

- **Validation** (revised 2026-11-19): **`flow-orchestrator-ws`** is the user-facing validation
  API (today's `/validation` paths, taken over from validator-ws). It turns requests into
  **RabbitMQ commands** to the orchestrator engine (`flow-orchestrator-app`), which has no business
  HTTP API of its own.
- **Ingestion** (Phase C, unchanged for now): start, retry and rerun stay behind registry-ws's
  existing secured endpoints (for example `PipelinesHistoryResource` `run/{datasetKey}`), which
  will send the same commands, as `DefaultRegistryPipelinesHistoryTrackingService` already does
  with `MessagePublisher`. Those endpoints may later move into `flow-orchestrator-ws` too.
- Commands queue while the engine is down or redeploying. An internal ops endpoint (for example
  replaying stuck outbox rows) may be added later if operations need it.
- **Tables** live in their own Postgres **schema `orchestrator`** in the registry database
  (decided 2026-11-19, replacing "registry's schema, registry changesets"). flow-orchestrator owns
  the schema and its DDL: `db/` (`changelog.xml`,
  `001-orchestrator-schema.sql`, `002-orchestrator-tables.sql`). The orchestrator runs no
  migrations.
  *Why:* a clean boundary. Registry is large, and the process-related registry-ws endpoints may
  later move into an **orchestration-ws** that owns this schema; the schema could also move to its
  own database then without touching registry's.
  *Current practice:* like registry's Liquibase changelog, `db/changelog.xml` documents the SQL;
  the changes are applied to each environment **by hand**. Every round that changes the schema
  delivers the script and the list of manual SQL per environment.
- **Only this repository's services write** the schema: the engine (`flow-orchestrator-app`)
  writes the workflow, step, command, outbox and report tables; `flow-orchestrator-ws` writes the
  validation tables (Round A). **Registry only reads.** Enforced with DB grants (`001-orchestrator-schema.sql`): the orchestrator role owns
  the schema and its tables; registry has `USAGE` on the schema and `SELECT` on its tables through
  default privileges, so new tables are readable without another grant. Registry reads with
  schema-qualified SQL (`orchestrator.workflow_execution`); the orchestrator connects with
  `orchestrator` as its current schema (`spring.datasource.hikari.schema`, `DB_SCHEMA`) and uses
  unqualified names.
- **Workers** receive a request, do the work, write artifacts (HDFS), and publish a completion.
  They do not touch workflow state.

Consequence: a schema change is released with the orchestrator, not with registry. Registry
changes only when what it reads changes (Round A onwards); those reads are the contract a later
orchestration-ws API would replace.

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
2. Load the workflow (execution row with its `version`, plus its step rows). No row lock is
   taken.
3. Reject stale or duplicate events by checking the attempt and the step status.
4. Interpret the completion and outcome through the step contract (section 4).
5. Run the *report interpreter* for the step, if one is configured (section 4a). It is a pure
   function of `(state, completion)` that returns `WorkflowChange`s (for example `SKIP_FROM`),
   a verdict and reasons. It is the successor of `WorkflowChangeHook`.
6. `WorkflowEngine.evaluate(...)` produces the `WorkflowDecision`.
7. Persist the status changes, outputs and report rows, and write an outbox row for each `DISPATCH`.
8. Finish with the optimistic version check: `UPDATE workflow_execution … SET version =
   version + 1 WHERE id = ? AND version = ?`. If no row changed, another transaction decided on
   the same workflow in the meantime: roll back.
9. Commit, then ack the message. postal's `MessageConsumer` acks after the callback returns and
   nacks without requeue on exception, so a synchronous transactional callback gives
   ack-after-commit.

**Decided: optimistic concurrency** (implemented in round 3, `CommandService`/`DecisionApplier`).

- Every decision reads the *whole* workflow, then decides. Example: parallel steps B and C finish
  together and both see the other as done, so both would dispatch the join step D. Decisions on
  the same workflow must therefore be serialized.
- The version on the aggregate root does this without holding locks. On conflict, and also on a
  unique-key violation (for example two concurrent STARTs for the same subject), the handling is
  re-run from a fresh read, at most 3 times. After that the exception reaches the listener, and the
  message is nacked to the dead-letter queue (round 4).
- The primary key `(workflow_execution_id, step_type, attempt_no)` on `step_attempt` is a second
  safety net against double dispatch.
- Most rows are effectively immutable: commands, attempts and outbox rows are inserted and updated
  at most once. Only the current status on the execution and step rows changes in place.

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

### 4a. Steps report facts, the orchestrator decides (Decided)

- **A step ends in one of two ways.** `SUCCEEDED`: it ran, and here are its reports and outputs.
  `FAILED`: it could not run (exception, infrastructure), with a diagnostic. Whether the *data* is
  acceptable is never the step's call, so there is no `REJECTED` outcome.
- **Report interpreters.** A small adapter in the orchestrator, chosen by domain and step type,
  reads only the report fields it needs and turns them into decisions. Steps without an
  interpreter just continue. The same report can mean different things in different workflows,
  and that knowledge stays in the orchestrator. Examples:

  | Domain | Step | Report says | Decision |
  |---|---|---|---|
  | VALIDATION | DwC-DP analysis | `valid=false` | Skip the remaining steps; workflow ends rejected. |
  | INGESTION | DwC-DP analysis | `valid=false` | Metasync still runs (it does extra work); staging and everything after it is skipped; workflow ends rejected. |
  | INGESTION | Crawl download | `modified=false` | Skip everything after the download; workflow ends `FINISHED`, not rejected. |

- **The trail.** The report that drove a decision is stored in the same transaction as the
  decision, and the decision's reason is stored on the rows (section 5). Every skip or rejection
  can be traced to a stored report.
- **Unreadable reports.** If an interpreter cannot read the report it needs (missing field, wrong
  shape), the completion is handled as a step failure with the parse error as the diagnostic. A
  bad report is never read as "continue".
- **Engine change: `SKIP_FROM(step)`.** Today `SKIP` marks one pending step `SKIPPED`, and the
  `NON_ERROR` join treats `SKIPPED` as fine, so the steps after it would still run. Interpreters
  need "skip this step and everything that depends on it".
- **Business errors are not technical errors.** A rejection is a verdict on the data, not a
  failure: the workflow ends **`FINISHED` with verdict `REJECTED`** and a reason. `FAILED` is kept
  for technical errors only, so alerts on `FAILED` mean real breakage.
- **Timing.** The `Outcome` change, the verdict and reason on `WorkflowFinished` and the `reason`
  and `rejected` columns are in place since round 4 (`DecisionNotes` carries them into a decision;
  failures already fill them). The interpreter framework, `SKIP_FROM` and the DwC-DP analysis and
  identifier interpreters are implemented in round 5 (`app.interpret`).

## 5. Persistence

**Decided:** DDL in this repository (`db/`), in the `orchestrator` schema (section 2); the
orchestrator runs no migrations. **Proposed:** Spring `JdbcClient` with explicit SQL and Spring
transactions (no JPA, no MyBatis). Write the table DDL
in portable SQL where possible so the H2 test mirror (section 8) stays simple.

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
  place by a single-row `UPDATE`, in the same transaction as the optimistic version check on the
  workflow (section 3).
- `step_attempt`: one row per try, with dispatched/finished, outcome, diagnostic message (today's
  `error`) and outputs. This is the history.

There is no JSON status document and no separate transition log. Writes are small and never
read-modify-write a document, and registry reads status with plain `SELECT`s. Volume is about
5–10 steps per workflow with a few writes each.

### Tables

**Decided and implemented in rounds 3 and 4:** the six tables, since 2026-11-19 in
`db/002-orchestrator-tables.sql` (schema `orchestrator`, names schema-qualified in the script).
The H2 tests run that same file; there is no copy. There is no table prefix. (They were registry
changesets 189/190 before; those were never applied anywhere and are removed from registry.)

- **`domain` is only on the aggregate root `workflow_execution`.** The other tables reach it
  through their foreign keys.
- **One domain per instance.** Each orchestrator instance is configured with one domain
  (`orchestrator.domain`) and only processes its own executions.
- **Command domain.** `workflow_command.domain` is just the value the command carried: a START has
  no execution yet, or never gets one if it is rejected.

| Table | Content |
|---|---|
| `workflow_execution` | `id`, `domain`, `dataset_key`, `attempt`, `run` (1 for START, +1 per RESTART), `workflow_name`, `definition_hash`, `parameters` (JSON text), `status`, `version`, `created`/`modified`/`finished`, `rejected` (verdict, only with `FINISHED`), `reason`. Unique `(domain, dataset_key, attempt, run)`. |
| `step_execution` | PK `(workflow_execution_id, step_type)`: `step_order` (topological), `status`, `current_attempt`, `modified`, `reason`. |
| `step_attempt` | PK `(workflow_execution_id, step_type, attempt_no)`: `status`, `outcome`, `diagnostic`, `outputs` (JSON text), `created` (dispatched), `started` (worker), `finished`. |
| `workflow_command` | PK `command_id`: `command_type`, `domain`, `dataset_key`, `attempt`, `step_type`, `parameters`, `requested_by`, `reason`, `requested`, `received`, `workflow_execution_id` (nullable), `outcome` (`ACCEPTED`/`REJECTED`), `outcome_reason`. |
| `outbox_message` | See *Outbox* below. |
| `workflow_report` | PK `(dataset_key, attempt, report_type)`: `report` (**jsonb**), `comment`, `created`, `modified`. No foreign key. See *Reports* below. |

**Decided (round 4): reasons are stored.** A nullable `reason` on `step_execution` (why a step
failed, was skipped or aborted; for a failed step the worker's diagnostic, for aborted steps for
example "Not run: VALIDATOR_VALIDATE_DWCDP failed") and on `workflow_execution` (why it ended, for
example "VALIDATOR_VALIDATE_DWCDP failed: Archive not found"), plus the verdict `rejected`. They are
meant for people, including a future frontend, and are copied into `WorkflowFinished`. Changeset
The table script had not been applied anywhere, so it was edited in place.

The SQL is portable: no enums or partial indexes; identity columns; `timestamp with time zone`.
Step types and statuses are `varchar`, so a new `StepType` needs no `ALTER TYPE`. JSON is `text`
except `workflow_report.report`, which is `jsonb` (see *Reports*).

### Outbox

An outbox row is "a message we have decided to send". It is written in the same transaction as
the state change that needs it.

| Column | Meaning |
|---|---|
| `id` | Identity; publishing happens in id order. |
| `message_id` | UUID, unique. Sent as the AMQP message id so receivers can drop duplicates. |
| `workflow_execution_id` | Required FK. The relay joins on it to filter by domain; also used for tracing. |
| `step_type`, `step_attempt` | The dispatched step attempt (null for workflow-level events). |
| `exchange`, `routing_key` | From configuration (`orchestrator.steps.<StepType>.request`). |
| `payload` | JSON, built inside the transaction: a step request (`{"header": StepHeader}` until the per-step requests of round 5), or a `WorkflowFinished` event. |
| `created`, `published` | `published` stays null until the broker confirms. |
| `publish_attempts`, `last_error`, `next_attempt` | Publish failures. The relay retries after `next_attempt` (exponential backoff 1s, 2s, 4s… up to `orchestrator.relay.max-backoff`). |

Lifecycle:

1. The decision transaction writes state and outbox rows, then commits.
2. The relay (`OutboxRelay`, one thread per instance) picks due unpublished rows of its domain in
   id order, every `poll-interval` and right after a committing transaction that wrote outbox
   rows. It publishes each with confirms and `mandatory=true` and sets `published` once the broker
   confirmed. A failed row gets `next_attempt`; the rows after it still go out.
3. Metrics: `orchestrator.outbox.unpublished`, `orchestrator.outbox.oldest.unpublished.age.seconds`
   and `orchestrator.outbox.publish.failures`.

Guarantees:

- No message is sent for state that rolled back.
- No committed decision is left unsent.
- The worst case is a duplicate send, after a crash between the publish and setting `published`.
  Receivers deduplicate using `message_id` or `stepAttempt`.

**Decided:** published rows are kept indefinitely. Even millions of rows is a small table; a cleanup job can be added later if needed.

### Schema changes to apply by hand

`db/changelog.xml` documents the SQL; each environment is changed by hand. In order:

| Step | File | Run as | Notes |
|---|---|---|---|
| 1 | `db/001-orchestrator-schema.sql` | A role that may create schemas | Creates schema `orchestrator` owned by the orchestrator role; `USAGE` and default `SELECT` for registry. PostgreSQL only. Role names are placeholders, so confirm them first. |
| 2 | `db/002-orchestrator-tables.sql` | The orchestrator role | Creates the six tables (including `workflow_report`), owned by the orchestrator. If an admin ran it instead, apply the commented `OWNER TO` and `GRANT SELECT` lines at the end of the file. |

The orchestrator's datasource keeps pointing at the registry database; set `DB_SCHEMA` only if the
schema is named differently.

Later, not part of Phase A: Phase B copies validator-ws `validation_report` rows into
`orchestrator.workflow_report` (`validation_key` → `dataset_key`) when validator-ws merges into registry. Phase C
migrates registry 185 rows as `DWCDP_ANALYSIS`, points `DatasetValidationResource` at
`workflow_report`, and drops 185/188 once the analyser no longer PUTs there.

### Reports: `workflow_report` (Decided, implemented in round 4)

One table for both domains, modelled on the validator-ws `validation_report`
(`pipelines-validator-ws/.../liquibase/003-validation-report.xml`):

```sql
dataset_key uuid, attempt integer, report_type varchar(64), report jsonb, comment text NULL,
created timestamptz, modified timestamptz,
PRIMARY KEY (dataset_key, attempt, report_type)
```

- **Name `workflow_report`.** The key has no step, and `SUMMARY` comes from the orchestrator, not
  from a step; the table also holds ingestion reports (for example `INTERPRETATION_METRICS`).
- **`dataset_key`** is the dataset key, or the validation key for validations. Validator-ws already
  uses the validation key as `datasetUuid` in messages, so both domains share the
  `(uuid, attempt)` subject, which is also the subject of `workflow_execution`.
- **No foreign key.** A foreign key to `validation` would break ingestion reports, and one to
  `workflow_execution` does not fit its `run` column; reports outlive a run. The shared key format
  is the link. Cleanup (for example validator-ws's retention task) deletes reports explicitly.
- **`report` is `jsonb`**, as in registry's changeset 185, so reports can be queried in SQL. H2
  accepts the type, so the tests run the production script; only the bind expression differs
  (`JsonSql`: `CAST(:p AS jsonb)` in production, `:p FORMAT JSON` in tests).
- **`comment`** is free text and nullable. When a report carries no comment the orchestrator writes
  the producing step and attempt (for example `VALIDATOR_VALIDATE_DWCDP attempt 2`). It is for
  people, not for queries.
- **Several reports per attempt**, one per `report_type`, for example `DWCDP_ANALYSIS`, the
  "general" DwC-A report (today inside the `validation` table), and reports of other formats.
- **Reports are decoupled from executions and attempts**: new report kinds need no schema change.
- **Writes are upserts** (update, then insert; portable to H2), keeping `created`. Repeated
  completion processing is safe; a concurrent insert of the same key is retried.
- Only the orchestrator writes the table; registry reads it (grants in `db/001-orchestrator-schema.sql`).

**Existing report tables (kept, migrated later).** Registry's changeset 185 `validation_report`
(one `jsonb` report per dataset and attempt, PUT over HTTP by dwc-dp-analyser-service) stays
untouched while the analyser still writes it. Phase C migrates its rows as `DWCDP_ANALYSIS`,
switches gbif-api `DatasetValidationService` / `DatasetValidationResource` to `workflow_report`
(`get`/`getLatest` filter on `DWCDP_ANALYSIS`, plus a typed `get`), and drops 185/188. Phase B
copies validator-ws `validation_report` rows over when validator-ws merges into registry.

| | registry (185) | validator-ws (003) | `workflow_report` |
|---|---|---|---|
| Key | `(dataset_key, attempt)` | `(validation_key, attempt, report_type)` | `(dataset_key, attempt, report_type)` |
| Report column | `jsonb` | `text` | `jsonb` |
| Timestamps | `created` without time zone; the upsert resets it | `created`, `modified` with time zone | `created` (kept), `modified`, with time zone |
| Foreign key | none | `validation(key)`, `ON DELETE CASCADE` | none |
| Writer | dwc-dp-analyser (HTTP PUT) | pipelines workers via validator-ws | the orchestrator |

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

Workflow definitions stay in application configuration (`orchestrator.workflows.<name>`: `domain`,
`match`, `parallel`, `default-join-policy` and `nodes` as a list of `{step, dependencies,
join-policy}`). START selects the single definition whose `match` entries all equal the command
parameters; no match or several matches means the command is rejected. Each execution records the
definition name and a SHA-256 hash of its canonical form, so a configuration change is visible
and does not silently reinterpret running workflows. Invalid definitions (cycles, unknown
dependencies, identical matches, steps without request routing) stop the application at startup.

## 6. RabbitMQ messaging (Decided, implemented in round 4)

**Transport: postal-service, extended.** postal could not publish with confirms or `mandatory`,
and declared queues without arguments, so it could not set up dead-lettering. Round 4 adds
builder-style configuration to postal (2.1.16-SNAPSHOT; the orchestrator release needs a postal
release). Everything is additive, existing constructors and callers are unchanged:

- `DefaultMessagePublisher.builder(params)`: `.registry`, `.mapper`,
  `.declareRegisteredExchanges(bool)` (default true, as before), `.declareExchangeOnSend(bool)`,
  `.publisherConfirms(timeout)` (every send waits for the confirm; a nack or timeout throws) and
  `.mandatory(bool)` (requires confirms; a returned message throws `UnroutableMessageException` and
  is not retried).
- `send(message, exchange, routingKey, SendOptions)` with per-message `persistent`, `messageId`,
  `contentType` and `headers`. A `default` method on `MessagePublisher`, so other implementations
  (pipelines' test stubs) keep compiling.
- `MessageListener.builder(params)` and `listen(QueueBinding, callback)`. `QueueBinding` holds the
  queue, exchange, routing key, threads, queue arguments and `.deadLetter(dlx)`, which declares
  the queue with `x-dead-letter-exchange`/`x-dead-letter-routing-key=<queue>`, the durable topic
  DLX, and the durable `<queue>.dlq` bound with the queue name.

**Topology** (names per instance, from `ORCHESTRATOR_MESSAGING_PREFIX`, for example `validation`):

| What | Exchange | Routing key | Queue |
|---|---|---|---|
| Commands (registry → orchestrator) | `orchestrator` | `<prefix>.command` | `orchestrator.<prefix>.commands` (+ `.dlq`) |
| Step completions, all steps (workers → orchestrator) | `orchestrator` | `<prefix>.step.completed` | `orchestrator.<prefix>.step.completed` (+ `.dlq`) |
| DwC-DP analyser completions (legacy, optional) | `orchestrator` | `<prefix>.dwcdp.analysis.finished` | `orchestrator.<prefix>.dwcdp.analysis.finished` (+ `.dlq`) |
| `WorkflowFinished` (orchestrator → registry) | `orchestrator` | `<prefix>.workflow.finished` | declared by the consumer |
| Step requests (orchestrator → workers) | `orchestrator.steps.<StepType>.request` | | declared by the worker |
| Dead letters | `orchestrator.dlx` | the queue name | `<queue>.dlq` |

- **One completion queue per domain.** The `StepCompleted` envelope carries the step type in its
  header, so adding a step needs no queue. The analyser's legacy `DwcDpValidationFinished` has
  its own adapter queue (round 5), until the analyser adopts the envelope.
- **Owner declares.** The orchestrator declares its own queues, bindings and DLQs, and declares
  (idempotently, durable topic) every exchange it publishes to. Workers declare their queues.
- **Unroutable is a failure.** The relay publishes with `mandatory=true`, so a request sent before
  the worker's queue exists is returned and retried with backoff instead of being dropped silently.
- **Ack after commit.** postal acks after the callback returns and nacks without requeue when it
  throws; a message it cannot deserialize (including contract validation errors) is rejected.
  With the DLX, both end in the DLQ, visible and replayable by hand. Duplicate commands, rejected
  commands and stale completions are acked normally (their outcome is stored or logged).
- **Known limitation.** A transient database outage makes handling throw, so those messages are
  dead-lettered rather than retried later. Replaying the DLQ is manual for now; a delayed-retry
  queue can be added if this happens in practice.
- **Outbox relay:** see *Outbox* in section 5. Correctness does not depend on
  `FOR UPDATE SKIP LOCKED`: with several instances of one domain the worst case is a duplicate
  publish, which receivers tolerate (`message_id`, `stepAttempt`).
- **Messaging off:** `orchestrator.messaging.enabled=false` starts without RabbitMQ (used by the
  tests); state handling and outbox rows work the same, nothing is published.

**Postal facts (verified)**

- `@MessageBinding` and the `Message` interface only feed postal's convenience overloads:
  `send(Message)`, and `listen(...)` without an exchange or routing key. `DefaultMessageRegistry`
  scans only postal's own `org.gbif.common.messaging.api.messages` package.
- The explicit `listen(...)` variants take any type and declare the durable topic exchange, the
  durable queue and the binding themselves. `send(Object, exchange, routingKey)` takes any object.
- New contracts are plain records without postal annotations.

**Open**

- Validation and ingestion isolation: separate vhosts, or one vhost with the prefixes above. The
  prefixes make a shared vhost possible; the deployment decides.

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
  - `rejected` only with `FINISHED` (a business rejection is not a failure, section 4a).
- **Golden fixtures.** The JSON in `flow-orchestrator-api/src/test/resources/contracts/` locks the
  wire format. Change a fixture only together with a deliberate contract change.

```java
// worker → orchestrator: one envelope for all adapted workers
record StepCompleted(
    UUID messageId,                 // deduplication key for redelivery
    StepHeader header,              // copied back unchanged from the request
    Outcome outcome,                // SUCCEEDED | FAILED
    String diagnostic,              // reason for FAILED; becomes the step_attempt diagnostic
    Map<String, String> outputs,    // small machine values for later steps
    List<Report> reports,           // human-facing results; one workflow_report row each
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

enum Outcome { SUCCEEDED, FAILED }
```

- **`outcome`** describes what happened to the work, not what should run next.
  - `SUCCEEDED`: the step finishes.
  - `FAILED`: the worker broke (exception, infrastructure problem). This is the retryable case.
  - There is no `REJECTED` (removed in round 4, section 4a). Invalid data is a `SUCCEEDED` step whose
    report the orchestrator interprets; the workflow then ends `FINISHED` with verdict `REJECTED`.
- **`outputs`** are for machines: small named values (HDFS paths, counts). They are stored on
  `step_attempt.outputs` and resolved into later requests (section 4). Data-shape flags such as
  `hasOccurrences`/`hasEvents` are not passed between steps any more (section 9a).
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
  per-step adapter maps it to `StepCompleted` (`SUCCEEDED`, analysis report as `DWCDP_ANALYSIS`).
  The analysis interpreter then reads `valid` from the report (section 4a).

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
- Concurrent commands are serialized by the optimistic version check (section 3). A command that conflicts with the current
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
- Persistence tests run against **H2** using the production `db/002-orchestrator-tables.sql` (the
  JDBC URL creates the schema, as `001` does in PostgreSQL). `SchemaTest` checks that every table
  is in `orchestrator` and that connections use it as their current schema.
- Messaging is tested with in-memory fake publishers and consumers. That includes failure
  injection between commit, publish and ack.
- Real RabbitMQ behaviour (publisher confirms, ack/nack, dead-lettering) and multi-instance
  behaviour are verified in the Compose `local-test` environment, not in `mvn verify`.

Caveats and how the design handles them:

| H2 gap | Mitigation |
|---|---|
| No Postgres `jsonb` type or operators | Map JSON columns as text or H2 `JSON` in the test schema. Orchestrator queries never use jsonb operators; JSON is parsed in Java. |
| `FOR UPDATE SKIP LOCKED` semantics differ | Outbox correctness does not depend on it (section 6). |
| Concurrency under real load | The optimistic version check behaves the same on H2, and conflict handling is tested with a repository that forces a conflict. Real multi-instance behaviour is checked in Compose. |
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

## 9a. Crawl and ingestion through the orchestrator (Phase C, direction agreed)

Today's DwC-DP crawl → ingest chain (routing keys on the `crawler` exchange), confirmed by the
user:

```text
crawl.start → crawl.request → [crawler downloads]
  → crawl.dwcdp.download.finished          (modified, lastModified)   stops here if not modified
  → [analyser] PUT registry validation_report
  → crawl.dwcdp.validation.finished         (valid)
  → [crawler metasync]                      runs even when invalid (does extra work)
  → crawl.dwcdp.metadatasync.finished       (containsOccurrences, containsEvents)  stops if invalid
  → pipelines balancer picks the pipeline from the two flags → occurrence.dwcdp.stage → …
```

Direction:

- **Crawl steps become steps.** Download, DwC-DP analysis and metasync are not in gbif-api's
  `StepType` today, because crawling was its own flow that then started ingestion. They are added
  to `StepType` (ties in with section 1a).
- **The analyser becomes a plain worker**, with the same encapsulation as in validation: it gets a
  step request, replies with `StepCompleted` and the `DWCDP_ANALYSIS` report. It no longer writes
  to registry, sends crawler messages or writes ingest status. The same analyser serves both
  domains; each orchestrator instance routes the step to its own queue.
- **Stop conditions are report interpretations** (section 4a): `modified=false` in the download
  report skips the rest; `valid=false` in the analysis report lets metasync run and skips staging
  onward. Metasync receives the analysis result in its request, built by the orchestrator.
- **No `containsOccurrences`/`containsEvents` flags.** Each step checks its own input (for example
  `datapackage.json` or the staged schema) and, when it has nothing to do, returns `SUCCEEDED`
  with a comment. This gives **one fixed DAG** from crawl through ingestion, with no branch chosen
  mid-flow, and removes today's job failures caused by wrong flags. A step must check before it
  launches anything expensive (Spark, Airflow).

**Open:** what registry and other readers use for crawl and ingest status today (crawl history,
pipeline process and execution rows, ZooKeeper), and whether any of it must keep being written
for compatibility. Needs the crawler repository.
