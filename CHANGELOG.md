# Changelog

## 0.0.1 (unreleased)

- Multi-module repository: `flow-orchestrator-engine` and `flow-orchestrator-app`.
- Ported the DAG workflow engine from pipelines `validator-orchestrator` (commit `dffcecb0b`).
  - Removed `SequentialWorkflowEngine`; use `DagDefinition.linear(...)`. On failure, downstream
    steps are now `ABORTED`.
  - Renamed `SET_VALIDATION_STATUS` to `SET_WORKFLOW_STATUS`.
- Spring Boot service skeleton with Actuator probes, Prometheus metrics, and DAG validation at startup.
- Docker image and Jenkins pipeline.
- New `flow-orchestrator-api` module with the message contracts: `WorkflowCommand`, `StepHeader`,
  `StepCompleted` (outcome, outputs, reports), `WorkflowFinished`, `ReportTypes` and the
  `OrchestratorJson` helper. Golden JSON fixtures lock the wire format.
- Persistence in the registry database: tables `workflow_execution`, `step_execution`,
  `step_attempt`, `workflow_command` and `outbox_message` (registry changesets 189/190, applied by
  hand; H2 copy for tests). `JdbcClient` repositories, optimistic versioning on the execution.
- START command handling (`CommandService`): one transaction records the command, creates the
  execution and steps, applies the engine decision and writes outbox rows; conflicts are retried.
- Configuration: `orchestrator.domain`, named `orchestrator.workflows` selected by domain and
  parameters, and per-step request routing; replaces `orchestrator.workflow`. Nodes are now a list
  (a map entry with `{}` was silently dropped by Spring binding).
- Messaging (round 4), through postal-service 2.1.16-SNAPSHOT (new builders: publisher confirms,
  `mandatory`, `SendOptions`, `QueueBinding` with dead-lettering):
  - `OutboxRelay` publishes outbox rows of the instance's domain with broker confirms, marks them
    published, and backs off failed rows (1s, 2s, 4s… up to `max-backoff`). Metrics for the
    backlog, its oldest age and publish failures.
  - Command and step-completion listeners on `orchestrator.<prefix>.commands` and
    `orchestrator.<prefix>.step.completed`, each with a `<queue>.dlq` behind `orchestrator.dlx`.
  - `orchestrator.messaging.enabled=false` runs without RabbitMQ.
- Step completion handling (`CompletionService`): records the attempt's outcome, diagnostic,
  outputs and timing, stores reports, advances the workflow, ignores stale or redelivered
  completions and refuses foreign ones; conflicts are retried.
- `WorkflowFinished` is written to the outbox when a workflow ends.
- Contracts: `Outcome` is `SUCCEEDED`/`FAILED` (no `REJECTED`; invalid data is a successful step
  whose report the orchestrator interprets). `WorkflowFinished` gains `reason`; `rejected` is only
  valid with `FINISHED`.
- Schema (changeset 189, edited in place): `workflow_report` table (jsonb reports), `rejected` and
  `reason` on `workflow_execution`, `reason` on `step_execution`, `started` on `step_attempt`,
  `next_attempt` on `outbox_message`.
- Round 5, the DwC-DP validation chain end to end:
  - Engine: `WorkflowChange.SKIP_FROM` skips a step and everything downstream of it.
  - Per-step requests (`StepRequestFactory`, `StepContext` with the outputs of finished steps; the
    latest step that reported an output wins). The analyser keeps its `{datasetUuid, attempt}`
    request; the pipelines validator steps get their postal message with a `stepHeader`, wrapped in
    `PipelinesBalancerMessage`. Steps without a factory get `{"header": StepHeader}`.
  - `OutputKeys` (`datasetType`, `occurrenceRecords`, `eventRecords`) and report type `IDENTIFIERS`
    in the API.
  - Report interpreters (`ReportInterpreter`, per domain and step): an invalid `DWCDP_ANALYSIS`
    (`metadata.isValid`) or `IDENTIFIERS` result (invalid, or duplicates) skips the rest; the
    workflow ends `FINISHED`, rejected, with a reason. An unreadable or missing report fails the
    step; the report is still stored.
  - DwC-DP analyser adapter: optional queue `orchestrator.<prefix>.dwcdp.analysis.finished`
    (`orchestrator.messaging.dwcdp-analysis-finished`) for the analyser's `DwcDpValidationFinished`,
    correlated by dataset and attempt to the latest run; `validationReport` (the whole
    `AnalysisExecution`) is stored as `DWCDP_ANALYSIS`.
  - `local-test/`: Postgres, RabbitMQ, the orchestrator and the real analyser in Compose, with
    tasks to trigger a validation, simulate pipelines steps and inspect the tables.
  - Companion changes: postal-service (`stepHeader` on four pipeline messages, java-time support,
    `DwcDpValidationFinishedMessage` follows the analyser), dwc-dp-analyser-service 0.0.9 (whole
    `AnalysisExecution` in the message, ISO times, input binding options, unknown fields ignored),
    pipelines (validator jobs reply to the orchestrator when the message has a step header).
