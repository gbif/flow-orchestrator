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
2. Persistence: schema, Liquibase, `JdbcClient` repositories, state loading with locking.
3. Messaging: transactional outbox, relay with publisher confirms, completion consumers.
4. Step contract redesign and the DwC-DP step adapter (existing wire contract), plus a ported `local-test/`.
5. Retry, rerun and recovery (timeouts, stale-attempt handling).

**Decided:** during Phase A the orchestrator keeps the existing worker contracts: the DwC-DP
analyser request JSON and the `DwcDpValidationFinished` completion, correlated by
`datasetUuid + attempt`. Envelopes with execution and attempt identifiers come in Phase B.

## 1. Repository and modules

**Decided**

- Own repository `gbif/flow-orchestrator`, groupId `org.gbif`, packages `org.gbif.orchestrator.*`.
- Modules:
  - `flow-orchestrator-engine`: pure Java (no Spring, DB or RabbitMQ). DAG definition, join
    policies, transition decisions.
  - `flow-orchestrator-app`: Spring Boot service. Configuration, persistence, messaging, step
    integrations.
  - `flow-orchestrator-api` will be added when registry-ws needs the command contracts (Phase B).
- Steps are identified by gbif-api `StepType`. The engine depends on gbif-api for that enum only.
- The old `validator-orchestrator` module is removed from pipelines.

**Open**

- gbif-api is consumed from the feature snapshot `2.4.30-FEATURE-ADD-DWCDP-DATA-VALIDATION-SNAPSHOT`,
  because the `VALIDATOR_*DWCDP*` step types are not released. Switch to a release once merged.

### Engine changes made during the port

- `SequentialWorkflowEngine` was removed. A sequential workflow is `DagDefinition.linear(steps)`.
  Behaviour difference: on failure, downstream steps become `ABORTED` instead of staying `PENDING`,
  so every step ends terminal. `LinearDagTest` covers this.
- `SET_VALIDATION_STATUS` was renamed `SET_WORKFLOW_STATUS` (domain-neutral).
- `WorkflowEngine.evaluate(state, event, changes)` is the single entry point.
- `ValidationWorkflowMapper` (validator-api specific) was not ported. State will be loaded from
  the orchestrator's own tables.

## 2. Service responsibilities and table ownership

**Proposed**

- **registry-ws** (absorbing validator-ws) sends commands only: `StartWorkflow`, `RetryStep`,
  `RerunFrom`. It reads workflow status and reports, either through orchestrator-owned tables
  (read-only grants) or a small read API. It never writes workflow or step status.
- **Orchestrator** owns all writes to the workflow tables. They live in their own Postgres schema
  (`orchestrator`) with **Liquibase** (Decided) and its own changelog and lock tables
  (`databaseChangeLogTableName`), so it never shares registry's `master.xml` history.
- **Workers** receive a request, do the work, write artifacts (HDFS), and publish a completion.
  They do not touch workflow state.

**Open**

- Transport for registry-ws commands: RabbitMQ, which is consistent with today's start message
  and needs no new endpoint, or HTTP. RabbitMQ is the proposal.
- How registry-ws reads status and reports: shared-schema read grants or an orchestrator read API.

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
- `CompletionInterpreter`: `payload → (correlation, outcome, outputs)`. The outcome is one of:
  - `SUCCEEDED`
  - `FAILED`: processing error
  - `REJECTED`: valid negative result, for example an invalid datapackage. This keeps "the worker
    worked, the data is bad" separate from "the worker broke".
- Optional decision hooks (section 3).
- Routing (exchange, routing key, completion queue) stays in configuration
  (`orchestrator.steps.<StepType>.*`, as in today's `OrchestratorProperties`), not in the
  definition or the message class.

Data placement (hybrid, as in the brief):

| Where | What |
|---|---|
| Postgres `step_attempt.outputs` (jsonb) | Small values and references: HDFS paths, counts, flags. |
| HDFS | Large artifacts. Workers write to an attempt-specific path and report it in the completion. |
| RabbitMQ | Identifiers, outcome, small control data, references. |

A worker never knows which step produced its input; the orchestrator resolves inputs from
dependency outputs. **Open:** how named outputs map to inputs at a DAG join. Proposal: explicit
per-step input mapping in config, only when the first real join appears.

## 5. Persistence

**Decided:** Liquibase. **Proposed:** Spring `JdbcClient` with explicit SQL and Spring
transactions (no JPA, no MyBatis).

Proposed tables (schema `orchestrator`):

| Table | Purpose |
|---|---|
| `workflow_execution` | id, domain (validation/ingestion), subject key (dataset uuid), external attempt, definition id/hash, status, timestamps, `version`. |
| `step_execution` | One row per (workflow execution, step type): current status, current attempt number. |
| `step_attempt` | Unique `(workflow_execution_id, step_type, attempt_no)`: status, dispatched/finished timestamps, outcome, diagnostic message, outputs (jsonb). |
| `outbox_message` | id, exchange, routing key, payload, headers, created, published_at, attempts, last_error. |
| `processed_message` | Optional deduplication by message id (if workers supply one). |
| `report` | (workflow execution, step attempt, report type) to content (jsonb or text). |

Workflow definitions stay in application configuration. Each execution records the definition id
and hash it started with, so a configuration change does not reinterpret running workflows.

## 6. RabbitMQ messaging

**Proposed**

- Keep **one completion queue per step** (current behaviour, `StepCompletionListenerConfiguration`),
  with routing in configuration.
- **Outbox relay:** a scheduled poller picks unpublished rows with `FOR UPDATE SKIP LOCKED`,
  publishes them, waits for the broker confirm, and marks them published. A crash between publish
  and mark gives a duplicate publish, not a lost one. Workers and the orchestrator must tolerate
  duplicates, which the attempt check in section 3 handles.
- Dead-letter exchanges for orchestrator queues, so nacked messages are kept and visible.

**Open**

- postal's `DefaultMessagePublisher` never enables publisher confirms (`confirmSelect`). It only
  calls `waitForConfirmsOrDie` on close. The relay therefore needs either a postal change or a
  small publisher on the RabbitMQ client using the same `ConnectionParameters`.
- Validation and ingestion isolation: separate vhosts, or a shared vhost with prefixed exchanges.
  Proposal: one codebase, one deployment per domain, each configured with its own vhost or prefix.
  The engine is unaffected either way.

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
- Tests: JUnit 5 with handwritten fakes. Integration tests with Postgres and RabbitMQ, using
  Testcontainers or Compose (**Open**). Failure injection between commit, publish and ack.
