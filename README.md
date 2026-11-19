# flow-orchestrator

Workflow orchestration service for GBIF validation and ingestion workflows.

The orchestrator owns workflow definitions, execution state and step transitions. It dispatches
work to isolated processing workers over RabbitMQ and interprets their completion messages.
Workers do not manage workflow state.

This repository replaces the former `validator-orchestrator` module in the pipelines repository.
The redesign is tracked in [docs/architecture.md](docs/architecture.md).

**Status:** Round A (Phase B) implemented, not deployed. The engine runs the DwC-DP validation
chain (rounds 1–5), and `flow-orchestrator-ws` takes over the validation API from
pipelines-validator-ws: DwC-DP validations run through the engine, other formats keep their
legacy path until Round B. Retry, rerun and recovery come later. A local environment is in
[local-test/USAGE.md](local-test/USAGE.md).

## Modules

| Module | Purpose |
|---|---|
| `flow-orchestrator-api` | Message contracts as plain records (commands, step completions, workflow events, report types). Jackson only; gbif-api `provided`. |
| `flow-orchestrator-engine` | Pure Java workflow engine: DAG definitions, join policies, transition decisions. No Spring, DB or messaging. |
| `flow-orchestrator-app` | The engine service (image `flow-orchestrator`): configuration, persistence, command and completion handling, messaging (postal), Actuator. |
| `flow-orchestrator-ws` | The validation web service (image `flow-orchestrator-ws`), formerly pipelines-validator-ws: `/validation` API, uploads, admission, emails, ChecklistBank callback and jobs. Starts workflows with commands; reads their state. |

The database schema `orchestrator` (in the registry database) is defined in [db/](db/) and applied
by hand per environment; [db/migration/](db/migration/) has the one-off copy of validator-ws's
data.

## Build

Requires Java 17 and Maven. Until postal-service 2.1.16 is released, the orchestrator uses
`2.1.16-FEATURE-ORCHESTRATION-SNAPSHOT`, which Jenkins publishes from postal's
`feature/orchestration` branch (the branch's pom stays `2.1.16-SNAPSHOT`). It adds the
publisher-confirm and dead-letter builders, the `stepHeader` on pipeline messages and java-time
support.

```bash
mvn clean verify         # build and run all tests
mvn spotless:apply       # format (CI runs spotless:check)
```

The runnable artifact is `flow-orchestrator-app/target/flow-orchestrator-app-<version>-shaded.jar`.

It needs the registry PostgreSQL database with the `orchestrator` schema (this repository's
`db/` scripts, applied by hand; see `docs/architecture.md`),
RabbitMQ, an instance domain and a messaging prefix:

```bash
DB_URL=jdbc:postgresql://localhost:5432/registry DB_USERNAME=orchestrator DB_PASSWORD=… \
ORCHESTRATOR_DOMAIN=VALIDATION ORCHESTRATOR_MESSAGING_PREFIX=validation \
RABBIT_HOST=localhost RABBIT_USERNAME=guest RABBIT_PASSWORD=guest \
  java -jar flow-orchestrator-app/target/flow-orchestrator-app-*-shaded.jar
curl localhost:8080/actuator/health
```

### flow-orchestrator-ws

`flow-orchestrator-ws/target/flow-orchestrator-ws-<version>-shaded.jar` serves the validation API
on port 8080. It connects to the same database as role `orchestrator_ws` (it writes only the
validation tables), sends commands to the engine's `validation` command queue, and mounts the
shared validation store. Its settings keep validator-ws's names where they existed; see
`flow-orchestrator-ws/src/main/resources/application.yml`. The main ones: `DB_URL`, `DB_USERNAME`,
`DB_PASSWORD`; `STORE_PATH` (shared store the workers read); `GBIF_API_URL`, `GBIF_APP_KEY`,
`GBIF_APP_SECRET` (remote authentication and user lookup for emails); `MAIL_HOST`, `MAIL_FROM`;
`CLB_API_URL`, `CLB_API_USER`, `CLB_API_PASSWORD`; `RABBIT_*`. The gateway route `/v1/validation*`
points at it instead of validator-ws.

## Docker images

```bash
mvn clean package -DskipTests
build/docker-image.sh docker.gbif.org/flow-orchestrator:<version>                              # engine
build/docker-image.sh docker.gbif.org/flow-orchestrator-ws:<version> '' flow-orchestrator-ws  # ws
```

CI (`Jenkinsfile`) publishes `docker.gbif.org/flow-orchestrator:<version>` and
`docker.gbif.org/flow-orchestrator-ws:<version>`.

## Configuration

See `flow-orchestrator-app/src/main/resources/application.yml`.

| Setting | Meaning |
|---|---|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | Registry PostgreSQL database. |
| `orchestrator.domain` (`ORCHESTRATOR_DOMAIN`) | The single domain this instance processes: `VALIDATION` or `INGESTION`. Required. |
| `orchestrator.workflows.<name>` | Workflow definitions: `domain`, `match` (command parameters that select it), `nodes` as a list of `{step, dependencies, join-policy}`. |
| `orchestrator.steps.<StepType>.request` | `exchange` and `routing-key` for each step request. |
| `RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USERNAME`, `RABBIT_PASSWORD`, `RABBIT_VHOST` | RabbitMQ connection (`orchestrator.rabbitmq.*`). |
| `ORCHESTRATOR_MESSAGING_PREFIX` | Per-instance prefix of queue and routing-key names, for example `validation`. Required (also names the `WorkflowFinished` routing key, which is written to the outbox even without a broker). |
| `ORCHESTRATOR_MESSAGING_ENABLED` | `false` runs without RabbitMQ (nothing is consumed or published). Default `true`. |
| `orchestrator.messaging.*` | Dead-letter exchange, the `commands` and `completions` queues (exchange, routing key, queue, threads) and the `workflow-finished` destination. Defaults in `application.yml`. |
| `orchestrator.relay.*` | Outbox relay: `poll-interval`, `batch-size`, `confirm-timeout`, `max-backoff`. |

Invalid workflow or messaging configuration (cycles, unknown dependencies, identical matches, steps
without routing, missing or empty-prefixed queue names) stops the application at startup. See [docs/architecture.md](docs/architecture.md) §5.
